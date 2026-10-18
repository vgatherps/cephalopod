use std::ffi::{CString, c_int};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::ptr;
use std::slice;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Release};

use ceph_rseq::{Push, ThreadArea, push_record};

const MAGIC: u64 = u64::from_le_bytes(*b"cephring");
const LINES_OFFSET: usize = 4096;

#[repr(C)]
struct Header {
    magic: AtomicU64,
    nr_rings: u64,
    ring_size: u64,
    max_record: u64,
}

#[repr(C, align(128))]
struct Line(AtomicU64);

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

    /// Ring `i`'s base address, head, and tail. Panics if `i` is not a ring.
    fn ring(&self, i: usize) -> (*mut u8, &AtomicU64, &AtomicU64) {
        let base = self.rings[i];
        // SAFETY: `nr_rings` heads then `nr_rings` tails start at LINES_OFFSET inside `meta`.
        let line = |j: usize| unsafe { &(*self.meta.add(LINES_OFFSET).cast::<Line>().add(j)).0 };
        (base, line(i), line(self.rings.len() + i))
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
        assert!(
            max_record.checked_add(8).is_some_and(|n| n <= ring_size),
            "max_record {max_record} + 8 exceeds ring_size {ring_size}"
        );
        let nr_rings = possible_cpus()?;
        let name = shm_name(prefix)?;
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
        // SAFETY: the header is in the mapping; writers read the fields only after seeing `magic`.
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

    /// Visits records of `ring` from the local tail up to the head loaded with Acquire. Stops
    /// before the first record for which `f` returns false and leaves it in the ring. Stores the
    /// new tail with Release once at the end if it moved. Returns the number of records consumed.
    pub fn read(&mut self, ring: usize, mut f: impl FnMut(&[u8]) -> bool) -> usize {
        let (base, head, shared_tail) = self.map.ring(ring);
        let head = head.load(Acquire);
        let mask = self.map.ring_size as u64 - 1;
        let mut tail = self.tails[ring];
        let mut consumed = 0;
        while tail < head {
            // SAFETY: [tail, head) holds committed records that producers do not touch until the
            // tail passes them. A record starts 8-aligned below `ring_size`, and its length is
            // checked against `max_record` (<= ring_size - 8), so it ends inside the double mapping.
            let record = unsafe {
                let start = base.add((tail & mask) as usize);
                let len = start.cast::<u64>().read() as usize;
                assert!(
                    len <= self.max_record,
                    "ring {ring}: record length {len} at {tail}"
                );
                slice::from_raw_parts(start.add(8), len)
            };
            if !f(record) {
                break;
            }
            tail += 8 + record.len().next_multiple_of(8) as u64;
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
// calling thread's current CPU, and heads and tails are atomics.
unsafe impl Send for Writer {}
unsafe impl Sync for Writer {}

impl Writer {
    pub fn attach(prefix: &str) -> io::Result<Writer> {
        if !ceph_rseq::registered() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "glibc did not register rseq",
            ));
        }
        let fd = shm_open(&shm_name(prefix)?, 0)?;
        let page = page_size();
        // SAFETY: no address hint. The header is read and the temporary mapping removed below.
        let (magic, nr_rings, ring_size, max_record) = unsafe {
            let header = mmap(
                ptr::null_mut(),
                page,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )?
            .cast::<Header>();
            let fields = (
                (*header).magic.load(Acquire),
                (*header).nr_rings as usize,
                (*header).ring_size as usize,
                (*header).max_record as usize,
            );
            libc::munmap(header.cast(), page);
            fields
        };
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shm object has no ring magic",
            ));
        }
        Ok(Writer {
            map: Map::new(&fd, nr_rings, ring_size)?,
            max_record,
        })
    }

    pub fn try_push(&self, record: &[u8]) -> Result<(), PushError> {
        if record.len() > self.max_record {
            return Err(PushError::TooLarge);
        }
        let ring_size = self.map.ring_size as u64;
        loop {
            let area = ThreadArea::current().expect("attach checked that rseq is registered");
            let cpu = area.cpu_id_start();
            let (base, head, tail) = self.map.ring(cpu as usize);
            let limit = tail.load(Acquire) + ring_size;
            // SAFETY: `head` is 128-aligned and written only by push_record on `cpu` (it aborts
            // when the thread is elsewhere). `base` is ring `cpu` mapped twice, and a record of at
            // most max_record + 8 <= ring_size bytes starting below ring_size stays inside it.
            let push = unsafe {
                push_record(
                    area,
                    cpu,
                    head.as_ptr(),
                    base,
                    ring_size - 1,
                    limit,
                    record.as_ptr(),
                    record.len() as u64,
                )
            };
            match push {
                Push::Done => return Ok(()),
                Push::Full => return Err(PushError::Full),
                Push::Aborted => {}
            }
        }
    }
}

fn page_size() -> usize {
    // SAFETY: sysconf has no preconditions.
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

fn meta_bytes(nr_rings: usize) -> usize {
    (LINES_OFFSET + 2 * nr_rings * size_of::<Line>()).next_multiple_of(page_size())
}

/// Max id in the possible-CPU list, plus one.
fn possible_cpus() -> io::Result<usize> {
    let list = std::fs::read_to_string("/sys/devices/system/cpu/possible")?;
    let mut max = 0;
    for id in list.trim().split([',', '-']) {
        max = max.max(id.parse::<usize>().map_err(io::Error::other)?);
    }
    Ok(max + 1)
}

fn shm_name(prefix: &str) -> io::Result<CString> {
    Ok(CString::new(format!("/{prefix}"))?)
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
