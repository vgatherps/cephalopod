use std::ffi::{CString, c_int};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::ptr;
use std::slice;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicU32, AtomicU64};

use ceph_rseq::{
    LEN_OFFSET, Private, SEQ_OFFSET, STREAM_OFFSET, TAG_OFFSET, record_total, tag_for,
};
#[cfg(target_os = "linux")]
use ceph_rseq::{Push, ThreadArea, push_record};

const MAGIC: u64 = u64::from_le_bytes(*b"cephring");
const TAILS_OFFSET: usize = 4096;

#[repr(C)]
struct Header {
    magic: AtomicU64,
    nr_rings: u64,
    ring_size: u64,
    max_record: u64,
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    lock: AtomicU32,
}

pub struct Config {
    pub ring_size: usize,
    pub max_record: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushError {
    Full,
    TooLarge,
}

/// The metadata pages of one shm object, and each ring mapped twice back to back.
struct Map {
    meta: *mut u8,
    meta_bytes: usize,
    rings: Vec<*mut u8>,
    ring_size: usize,
}

impl Map {
    fn new(fd: &OwnedFd, nr_rings: usize, ring_size: usize) -> io::Result<Map> {
        let fd = fd.as_raw_fd();
        let rw = libc::PROT_READ | libc::PROT_WRITE;
        let meta_bytes = meta_bytes(nr_rings);
        // SAFETY: no address hint, so the kernel picks unused address space.
        let meta = unsafe { mmap(ptr::null_mut(), meta_bytes, rw, libc::MAP_SHARED, fd, 0)? };
        let mut map = Map {
            meta,
            meta_bytes,
            rings: Vec::with_capacity(nr_rings),
            ring_size,
        };
        for i in 0..nr_rings {
            let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
            // SAFETY: no address hint.
            let ring = unsafe {
                mmap(
                    ptr::null_mut(),
                    2 * ring_size,
                    libc::PROT_NONE,
                    flags,
                    -1,
                    0,
                )?
            };
            map.rings.push(ring);
            let offset = meta_bytes + i * ring_size;
            for half in [ring, ring.wrapping_add(ring_size)] {
                // SAFETY: `half` is one half of the reservation just made and owned by `map`.
                unsafe {
                    mmap(
                        half,
                        ring_size,
                        rw,
                        libc::MAP_SHARED | libc::MAP_FIXED,
                        fd,
                        offset,
                    )?
                };
            }
        }
        Ok(map)
    }

    /// Ring `i`'s base address, private line, and consumer tail. Panics if `i` is not a ring.
    fn ring(&self, i: usize) -> (*mut u8, &Private, &AtomicU64) {
        let base = self.rings[i];
        // SAFETY: the compact tails and aligned private lines are inside the metadata mapping.
        let tail = unsafe { &*self.meta.add(TAILS_OFFSET).cast::<AtomicU64>().add(i) };
        let private = unsafe {
            &*self
                .meta
                .add(private_offset(self.rings.len()))
                .cast::<Private>()
                .add(i)
        };
        (base, private, tail)
    }

    #[cfg(not(target_os = "linux"))]
    fn lock(&self) -> &AtomicU32 {
        // SAFETY: `Header` starts at the beginning of the metadata mapping.
        unsafe { &(*self.meta.cast::<Header>()).lock }
    }
}

impl Drop for Map {
    fn drop(&mut self) {
        // SAFETY: these are the mappings made in `Map::new`; nothing borrows them past `self`.
        unsafe {
            for &ring in &self.rings {
                libc::munmap(ring.cast(), 2 * self.ring_size);
            }
            libc::munmap(self.meta.cast(), self.meta_bytes);
        }
    }
}

/// Owns the shm object: unmaps and `shm_unlink`s it on drop.
pub struct Reader {
    map: Map,
    name: CString,
    max_record: usize,
    tails: Vec<u64>,
}

pub struct Record<'a> {
    pub stream: u64,
    pub seq: u64,
    pub payload: &'a [u8],
}

impl Reader {
    pub fn create(prefix: &str, cfg: Config) -> io::Result<Reader> {
        let Config {
            ring_size,
            max_record,
        } = cfg;
        assert!(
            ring_size.is_power_of_two() && ring_size % page_size() == 0,
            "ring_size {ring_size} must be a power of two and a multiple of the page size"
        );
        // Equivalent to record_total(max_record) + 8 <= ring_size, written so a huge
        // max_record cannot overflow the addition in record_total.
        assert!(
            max_record <= ring_size - ceph_rseq::HEADER_LEN as usize - 8,
            "record_total({max_record}) + 8 exceeds ring_size {ring_size}"
        );
        #[cfg(target_os = "linux")]
        let nr_rings = possible_cpus()?;
        #[cfg(not(target_os = "linux"))]
        let nr_rings = 1;
        let name = shm_name(prefix);
        // SAFETY: `name` is NUL-terminated. A missing object is not an error here.
        unsafe { libc::shm_unlink(name.as_ptr()) };
        let fd = shm_open(&name, libc::O_CREAT | libc::O_EXCL)?;
        let len = meta_bytes(nr_rings) + nr_rings * ring_size;
        // SAFETY: plain syscall on an owned fd.
        if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let map = Map::new(&fd, nr_rings, ring_size)?;
        let header = map.meta.cast::<Header>();
        // SAFETY: the header is in the mapping; writers read the other fields only after seeing
        // `magic`.
        unsafe {
            (*header).nr_rings = nr_rings as u64;
            (*header).ring_size = ring_size as u64;
            (*header).max_record = max_record as u64;
            (*header).magic.store(MAGIC, Release);
        }
        Ok(Reader {
            map,
            name,
            max_record,
            tails: vec![0; nr_rings],
        })
    }

    pub fn nr_rings(&self) -> usize {
        self.tails.len()
    }

    /// Visits committed records in `ring`. Stops before the first record for which `f` returns
    /// false and leaves it in the ring. Returns the number of records consumed.
    pub fn read(&mut self, ring: usize, mut f: impl FnMut(Record<'_>) -> bool) -> usize {
        let (base, _, shared_tail) = self.map.ring(ring);
        let mask = self.map.ring_size as u64 - 1;
        let mut tail = self.tails[ring];
        let mut consumed = 0;
        loop {
            // SAFETY: record starts are 8-aligned and the ring is mapped twice contiguously.
            let record = unsafe {
                let start = base.add((tail & mask) as usize);
                let tag = (&*start.add(TAG_OFFSET).cast::<AtomicU64>()).load(Acquire);
                if tag != tag_for(tail) {
                    break;
                }
                let len = start.add(LEN_OFFSET).cast::<u64>().read() as usize;
                assert!(
                    len <= self.max_record,
                    "ring {ring}: record length {len} at {tail}"
                );
                Record {
                    stream: start.add(STREAM_OFFSET).cast::<u64>().read(),
                    seq: start.add(SEQ_OFFSET).cast::<u64>().read(),
                    payload: slice::from_raw_parts(start.add(ceph_rseq::HEADER_LEN as usize), len),
                }
            };
            let total = record_total(record.payload.len() as u64);
            if !f(record) {
                break;
            }
            tail += total;
            consumed += 1;
        }
        if tail != self.tails[ring] {
            self.tails[ring] = tail;
            shared_tail.store(tail, Release);
        }
        consumed
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        // SAFETY: `name` is NUL-terminated.
        unsafe { libc::shm_unlink(self.name.as_ptr()) };
    }
}

pub struct Writer {
    map: Map,
    max_record: usize,
}

// SAFETY: the mappings are process-wide. `try_push` writes a ring only inside `push_record` on the
// calling thread's current CPU (off Linux, only under the header spinlock), and the tails and
// private lines are atomics.
unsafe impl Send for Writer {}
unsafe impl Sync for Writer {}

impl Writer {
    pub fn attach(prefix: &str) -> io::Result<Writer> {
        #[cfg(target_os = "linux")]
        if !ceph_rseq::registered() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "glibc did not register rseq",
            ));
        }
        let fd = shm_open(&shm_name(prefix), 0)?;
        let page = page_size();
        // SAFETY: no address hint. The header is read and the temporary mapping removed below.
        let header = unsafe {
            mmap(
                ptr::null_mut(),
                page,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )?
            .cast::<Header>()
        };
        // SAFETY: the header is in the temporary mapping.
        let magic = unsafe { (*header).magic.load(Acquire) };
        if magic != MAGIC {
            // SAFETY: this is the temporary mapping made above.
            unsafe {
                libc::munmap(header.cast(), page);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shm object has no ring magic",
            ));
        }
        // SAFETY: seeing `magic` with Acquire synchronizes with `create` publishing the layout.
        let (nr_rings, ring_size, max_record) = unsafe {
            let fields = (
                (*header).nr_rings as usize,
                (*header).ring_size as usize,
                (*header).max_record as usize,
            );
            libc::munmap(header.cast(), page);
            fields
        };
        Ok(Writer {
            map: Map::new(&fd, nr_rings, ring_size)?,
            max_record,
        })
    }

    pub fn try_push(&self, payload: &[u8], stream: u64, seq: u64) -> Result<(), PushError> {
        if payload.len() > self.max_record {
            return Err(PushError::TooLarge);
        }
        self.push(payload, stream, seq)
    }

    #[cfg(target_os = "linux")]
    fn push(&self, payload: &[u8], stream: u64, seq: u64) -> Result<(), PushError> {
        let ring_size = self.map.ring_size as u64;
        let mut refreshed = None;
        loop {
            let area = ThreadArea::current().expect("attach checked that rseq is registered");
            let cpu = area.cpu_id_start();
            let (base, private, tail) = self.map.ring(cpu as usize);
            // SAFETY: `private` is CPU `cpu`'s aligned private line, `base` is its double-mapped
            // ring, and create ensures the record plus its following tag word fit in the mapping.
            let push = unsafe {
                push_record(
                    area,
                    cpu,
                    private,
                    base,
                    ring_size,
                    payload.as_ptr(),
                    payload.len() as u64,
                    stream,
                    seq,
                )
            };
            match push {
                Push::Done => return Ok(()),
                Push::Aborted => refreshed = None,
                Push::Full => {
                    if refreshed == Some(cpu) {
                        return Err(PushError::Full);
                    }
                    // Use the same CPU for both accesses; migration can only cause another refresh.
                    private.tail.store(tail.load(Acquire), Release);
                    refreshed = Some(cpu);
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn push(&self, payload: &[u8], stream: u64, seq: u64) -> Result<(), PushError> {
        use std::sync::atomic::Ordering::Relaxed;

        static WARN: std::sync::Once = std::sync::Once::new();
        WARN.call_once(|| {
            eprintln!(
                "WARNING: ceph-ring: rseq is unavailable on this OS, so every push takes a \
                 spinlock on a single ring. Development only, NOT for production."
            )
        });
        let ring_size = self.map.ring_size as u64;
        let (base, private, tail) = self.map.ring(0);
        let lock = self.map.lock();
        while lock.compare_exchange(0, 1, Acquire, Relaxed).is_err() {
            std::hint::spin_loop();
        }
        let mask = ring_size - 1;
        let mut at = private.head.load(Relaxed);
        loop {
            // SAFETY: `at` is 8-aligned and `base` is ring 0 mapped twice.
            let start = unsafe { base.add((at & mask) as usize) };
            // SAFETY: record tags are aligned AtomicU64 values in the shared ring.
            let tag = unsafe { &*start.add(TAG_OFFSET).cast::<AtomicU64>() }.load(Acquire);
            if tag != tag_for(at) {
                break;
            }
            // SAFETY: acquiring the committed tag makes the record header visible.
            let len = unsafe { start.add(LEN_OFFSET).cast::<u64>().read() };
            at += record_total(len);
        }
        let total = record_total(payload.len() as u64);
        let result = if at + total + 8 > tail.load(Acquire) + ring_size {
            Err(PushError::Full)
        } else {
            private.head.store(at, Relaxed);
            // SAFETY: create ensures this record and the following tag word fit in the double
            // mapping. The consumer has released the space, and other writers hold the lock.
            unsafe {
                let start = base.add((at & mask) as usize);
                (&*start.add(total as usize).cast::<AtomicU64>()).store(0, Relaxed);
                start
                    .add(LEN_OFFSET)
                    .cast::<u64>()
                    .write(payload.len() as u64);
                start.add(STREAM_OFFSET).cast::<u64>().write(stream);
                start.add(SEQ_OFFSET).cast::<u64>().write(seq);
                ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    start.add(ceph_rseq::HEADER_LEN as usize),
                    payload.len(),
                );
                (&*start.add(TAG_OFFSET).cast::<AtomicU64>()).store(tag_for(at), Release);
            }
            Ok(())
        };
        lock.store(0, Release);
        result
    }
}

fn page_size() -> usize {
    // SAFETY: sysconf has no preconditions.
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

fn meta_bytes(nr_rings: usize) -> usize {
    (private_offset(nr_rings) + nr_rings * size_of::<Private>()).next_multiple_of(page_size())
}

fn private_offset(nr_rings: usize) -> usize {
    (TAILS_OFFSET + nr_rings * size_of::<AtomicU64>()).next_multiple_of(align_of::<Private>())
}

/// Max id in the possible-CPU list, plus one.
#[cfg(target_os = "linux")]
fn possible_cpus() -> io::Result<usize> {
    let list = std::fs::read_to_string("/sys/devices/system/cpu/possible")?;
    let mut max = 0;
    for id in list.trim().split([',', '-']) {
        max = max.max(id.parse::<usize>().map_err(io::Error::other)?);
    }
    Ok(max + 1)
}

fn shm_name(prefix: &str) -> CString {
    CString::new(format!("/{prefix}")).expect("shm prefix contains NUL")
}

fn shm_open(name: &CString, flags: c_int) -> io::Result<OwnedFd> {
    // SAFETY: `name` is NUL-terminated.
    let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_RDWR | flags, 0o600) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a fresh descriptor owned by nobody else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// # Safety
/// With `MAP_FIXED`, `addr..addr + len` must be address space the caller owns.
unsafe fn mmap(
    addr: *mut u8,
    len: usize,
    prot: c_int,
    flags: c_int,
    fd: c_int,
    offset: usize,
) -> io::Result<*mut u8> {
    // SAFETY: forwarded to the caller.
    let p = unsafe { libc::mmap(addr.cast(), len, prot, flags, fd, offset as libc::off_t) };
    if p == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(p.cast())
    }
}
