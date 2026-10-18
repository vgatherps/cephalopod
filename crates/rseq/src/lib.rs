#[cfg(not(target_os = "linux"))]
compile_error!("ceph-rseq requires Linux");
#[cfg(not(target_env = "gnu"))]
compile_error!("ceph-rseq requires the GNU libc environment");
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
compile_error!("ceph-rseq requires x86_64 or aarch64");

use std::arch::asm;
use std::marker::PhantomData;
use std::ptr;
use std::rc::Rc;

unsafe extern "C" {
    static __rseq_offset: isize;
    static __rseq_size: u32;
}

/// Returns whether glibc registered an rseq area for this process's threads.
pub fn registered() -> bool {
    // SAFETY: glibc exports this process-wide value for the life of the process.
    unsafe { ptr::read_volatile(ptr::addr_of!(__rseq_size)) != 0 }
}

/// The calling thread's `struct rseq`. This value is neither `Send` nor `Sync`.
#[derive(Clone, Copy)]
pub struct ThreadArea {
    ptr: *mut u8,
    not_send_or_sync: PhantomData<Rc<()>>,
}

impl ThreadArea {
    /// Returns this thread's rseq area, or `None` when glibc did not register it.
    pub fn current() -> Option<ThreadArea> {
        if !registered() {
            return None;
        }

        let thread_pointer: *mut u8;
        // SAFETY: this reads the architecture thread pointer without changing machine state.
        unsafe {
            #[cfg(target_arch = "x86_64")]
            asm!(
                "movq %fs:0, {thread_pointer}",
                thread_pointer = out(reg) thread_pointer,
                options(att_syntax, nostack, readonly),
            );
            #[cfg(target_arch = "aarch64")]
            asm!(
                "mrs {thread_pointer}, tpidr_el0",
                thread_pointer = out(reg) thread_pointer,
                options(nostack, readonly),
            );
        }

        // SAFETY: glibc exports the offset from its thread pointer to this thread's rseq area.
        let offset = unsafe { ptr::read_volatile(ptr::addr_of!(__rseq_offset)) };
        Some(ThreadArea {
            // SAFETY: glibc's offset identifies this thread's rseq area within its TLS allocation.
            ptr: unsafe { thread_pointer.offset(offset) },
            not_send_or_sync: PhantomData,
        })
    }

    /// Returns a volatile read of `cpu_id_start`. The result is always a valid CPU number.
    pub fn cpu_id_start(self) -> u32 {
        // SAFETY: `ThreadArea` can only be created for the calling thread's registered area.
        unsafe { ptr::read_volatile(self.ptr.cast::<u32>()) }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Push {
    Done,
    Full,
    Aborted,
}

/// Attempts to append one record to the ring for `cpu`.
///
/// # Safety
///
/// `head` must be 8-aligned and only written through this function by threads running on `cpu`.
/// `base + (*head & mask)` must be writable for `8 + round_up(len, 8)` bytes. `limit` must equal
/// a tail loaded with `Acquire` plus the ring size. A stale tail is safe because tail only grows.
/// Positions and their additions must not wrap. `src` must be readable for `len` bytes.
pub unsafe fn push_record(
    area: ThreadArea,
    cpu: u32,
    head: *mut u64,
    base: *mut u8,
    mask: u64,
    limit: u64,
    src: *const u8,
    len: u64,
) -> Push {
    let result: u64;
    let src_cursor = src;
    let len_cursor = len;

    // SAFETY: the caller provides the memory and per-CPU writer guarantees documented above.
    unsafe {
        #[cfg(target_arch = "x86_64")]
        asm!(
            ".pushsection __rseq_cs, \"aw\"",
            ".balign 32",
            "20:",
            ".long 0, 0",
            ".quad 30f, 40f - 30f, 60f",
            ".popsection",
            "leaq 20b(%rip), {tmp}",
            "movq {tmp}, 8({area})",
            "30:",
            "cmpl {cpu:e}, 4({area})",
            "jne 60f",
            "movq ({head}), {h}",
            "movq {h}, {dst}",
            "andq {mask}, {dst}",
            "addq {base}, {dst}",
            "movq {len}, {tmp}",
            "addq $7, {tmp}",
            "andq $-8, {tmp}",
            "addq $8, {tmp}",
            "addq {h}, {tmp}",
            "cmpq {limit}, {tmp}",
            "ja 70f",
            "movq {len}, ({dst})",
            "addq $8, {dst}",
            "50:",
            "cmpq $8, {len}",
            "jb 51f",
            "movq ({src}), {h}",
            "movq {h}, ({dst})",
            "addq $8, {src}",
            "addq $8, {dst}",
            "subq $8, {len}",
            "jmp 50b",
            "51:",
            "testq {len}, {len}",
            "je 52f",
            "movb ({src}), {h:l}",
            "movb {h:l}, ({dst})",
            "incq {src}",
            "incq {dst}",
            "decq {len}",
            "jmp 51b",
            "52:",
            "movq {tmp}, ({head})",
            "40:",
            "movq $0, {result}",
            "jmp 90f",
            ".byte 0x0f, 0xb9, 0x3d",
            ".long 0x53053053",
            "60:",
            "movq $2, {result}",
            "jmp 90f",
            "70:",
            "movq $1, {result}",
            "90:",
            area = in(reg) area.ptr,
            cpu = in(reg) cpu,
            head = in(reg) head,
            base = in(reg) base,
            mask = in(reg) mask,
            limit = in(reg) limit,
            src = inout(reg) src_cursor => _,
            len = inout(reg) len_cursor => _,
            h = out(reg) _,
            dst = out(reg) _,
            tmp = out(reg) _,
            result = lateout(reg) result,
            options(att_syntax, nostack),
        );

        #[cfg(target_arch = "aarch64")]
        asm!(
            ".pushsection __rseq_cs, \"aw\"",
            ".balign 32",
            "20:",
            ".long 0, 0",
            ".quad 30f, 40f - 30f, 60f",
            ".popsection",
            "adr {tmp}, 20b",
            "str {tmp}, [{area}, #8]",
            "30:",
            "ldr {h:w}, [{area}, #4]",
            "cmp {h:w}, {cpu:w}",
            "b.ne 60f",
            "ldr {h}, [{head}]",
            "and {dst}, {h}, {mask}",
            "add {dst}, {base}, {dst}",
            "mov {tmp}, {len}",
            "add {tmp}, {tmp}, #7",
            "and {tmp}, {tmp}, #-8",
            "add {tmp}, {tmp}, #8",
            "add {tmp}, {h}, {tmp}",
            "cmp {tmp}, {limit}",
            "b.hi 70f",
            "str {len}, [{dst}]",
            "add {dst}, {dst}, #8",
            "50:",
            "cmp {len}, #8",
            "b.lo 51f",
            "ldr {h}, [{src}], #8",
            "str {h}, [{dst}], #8",
            "sub {len}, {len}, #8",
            "b 50b",
            "51:",
            "cbz {len}, 52f",
            "ldrb {h:w}, [{src}], #1",
            "strb {h:w}, [{dst}], #1",
            "sub {len}, {len}, #1",
            "b 51b",
            "52:",
            "stlr {tmp}, [{head}]",
            "40:",
            "mov {result}, #0",
            "b 90f",
            ".inst 0xd428bc00",
            "60:",
            "mov {result}, #2",
            "b 90f",
            "70:",
            "mov {result}, #1",
            "90:",
            area = in(reg) area.ptr,
            cpu = in(reg) cpu,
            head = in(reg) head,
            base = in(reg) base,
            mask = in(reg) mask,
            limit = in(reg) limit,
            src = inout(reg) src_cursor => _,
            len = inout(reg) len_cursor => _,
            h = out(reg) _,
            dst = out(reg) _,
            tmp = out(reg) _,
            result = lateout(reg) result,
            options(nostack),
        );
    }

    match result {
        0 => Push::Done,
        1 => Push::Full,
        2 => Push::Aborted,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::UnsafeCell;
    use std::mem;
    use std::slice;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc, Barrier, Once};
    use std::thread;
    use std::time::{Duration, Instant};

    const RING_SIZE: usize = 64 * 1024;

    fn pin_to(cpu: usize) {
        // SAFETY: `set` is initialized before it is passed to `sched_setaffinity`.
        unsafe {
            let mut set: libc::cpu_set_t = mem::zeroed();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(cpu, &mut set);
            assert_eq!(
                libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set),
                0
            );
        }
    }

    fn available_cpus() -> Vec<usize> {
        // SAFETY: `set` is initialized by `sched_getaffinity` before it is inspected.
        unsafe {
            let mut set: libc::cpu_set_t = mem::zeroed();
            assert_eq!(
                libc::sched_getaffinity(0, mem::size_of::<libc::cpu_set_t>(), &mut set),
                0
            );
            (0..libc::CPU_SETSIZE as usize)
                .filter(|cpu| libc::CPU_ISSET(*cpu, &set))
                .collect()
        }
    }

    fn pinned_area() -> (ThreadArea, u32) {
        let cpu = available_cpus()[0];
        pin_to(cpu);
        let area = ThreadArea::current().expect("glibc did not register rseq");
        assert_eq!(area.cpu_id_start(), cpu as u32);
        (area, cpu as u32)
    }

    fn total(len: usize) -> usize {
        8 + (len + 7) / 8 * 8
    }

    unsafe fn push(
        area: ThreadArea,
        cpu: u32,
        head: &mut u64,
        buffer: &mut [u8],
        limit: u64,
        src: *const u8,
        len: usize,
    ) -> Push {
        // SAFETY: the test allocations cover the record and only this thread writes `head`.
        unsafe {
            push_record(
                area,
                cpu,
                head,
                buffer.as_mut_ptr(),
                (RING_SIZE - 1) as u64,
                limit,
                src,
                len as u64,
            )
        }
    }

    #[test]
    fn record_layout() {
        let (area, cpu) = pinned_area();
        for len in [0, 1, 7, 8, 9, 15, 16, 17, 100] {
            let payload: Vec<u8> = (0..len).map(|index| index as u8 ^ 0x5a).collect();
            let mut buffer = vec![0xaa; RING_SIZE + total(100)];
            let mut head = 0;

            // SAFETY: `buffer` and `payload` satisfy `push`'s test allocation contract.
            let result = unsafe {
                push(
                    area,
                    cpu,
                    &mut head,
                    &mut buffer,
                    RING_SIZE as u64,
                    payload.as_ptr(),
                    len,
                )
            };

            assert_eq!(result, Push::Done);
            assert_eq!(u64::from_ne_bytes(buffer[..8].try_into().unwrap()), len as u64);
            assert_eq!(&buffer[8..8 + len], payload.as_slice());
            assert_eq!(&buffer[8 + len..total(len)], vec![0xaa; total(len) - 8 - len]);
            assert_eq!(head, total(len) as u64);
        }
    }

    #[test]
    fn full_boundaries_do_not_write() {
        let (area, cpu) = pinned_area();
        let payload = [0x35; 17];
        let needed = total(payload.len()) as u64;

        let mut exact_buffer = vec![0xaa; RING_SIZE + total(payload.len())];
        let mut exact_head = 0;
        // SAFETY: the test allocations cover the record and only this thread writes the head.
        let exact = unsafe {
            push(
                area,
                cpu,
                &mut exact_head,
                &mut exact_buffer,
                needed,
                payload.as_ptr(),
                payload.len(),
            )
        };
        assert_eq!(exact, Push::Done);
        assert_eq!(exact_head, needed);

        let mut short_buffer = vec![0xaa; RING_SIZE + total(payload.len())];
        let original_short = short_buffer.clone();
        let mut short_head = 0;
        // SAFETY: the full check runs before the otherwise valid destination is written.
        let short = unsafe {
            push(
                area,
                cpu,
                &mut short_head,
                &mut short_buffer,
                needed - 1,
                payload.as_ptr(),
                payload.len(),
            )
        };
        assert_eq!(short, Push::Full);
        assert_eq!(short_head, 0);
        assert_eq!(short_buffer, original_short);

        let mut stale_buffer = vec![0xaa; RING_SIZE + total(payload.len())];
        let original_stale = stale_buffer.clone();
        let mut stale_head = 10 * RING_SIZE as u64;
        // SAFETY: the stale limit makes the full check fail before any destination write.
        let stale = unsafe {
            push(
                area,
                cpu,
                &mut stale_head,
                &mut stale_buffer,
                RING_SIZE as u64,
                payload.as_ptr(),
                payload.len(),
            )
        };
        assert_eq!(stale, Push::Full);
        assert_eq!(stale_head, 10 * RING_SIZE as u64);
        assert_eq!(stale_buffer, original_stale);
    }

    #[test]
    fn wrong_cpu_does_not_write() {
        let (area, cpu) = pinned_area();
        let payload = [0x71; 16];
        let mut buffer = vec![0xaa; RING_SIZE + total(payload.len())];
        let original = buffer.clone();
        let mut head = 0;

        // SAFETY: the CPU check aborts before the otherwise valid destination is written.
        let result = unsafe {
            push(
                area,
                cpu + 1,
                &mut head,
                &mut buffer,
                RING_SIZE as u64,
                payload.as_ptr(),
                payload.len(),
            )
        };

        assert_eq!(result, Push::Aborted);
        assert_eq!(head, 0);
        assert_eq!(buffer, original);
    }

    #[test]
    fn source_is_not_read_past_length() {
        let (area, cpu) = pinned_area();
        // SAFETY: the mapping is checked for failure, then split into one readable and one guard page.
        unsafe {
            let page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;
            let mapping = libc::mmap(
                ptr::null_mut(),
                page_size * 2,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            );
            assert_ne!(mapping, libc::MAP_FAILED);
            let guard = mapping.cast::<u8>().add(page_size);
            assert_eq!(libc::mprotect(guard.cast(), page_size, libc::PROT_NONE), 0);

            for len in 0..=17 {
                let src = guard.sub(len);
                for index in 0..len {
                    src.add(index).write(index as u8 ^ 0xa6);
                }
                let mut buffer = vec![0xaa; RING_SIZE + total(17)];
                let mut head = 0;
                let result = push(
                    area,
                    cpu,
                    &mut head,
                    &mut buffer,
                    RING_SIZE as u64,
                    src,
                    len,
                );
                assert_eq!(result, Push::Done);
                assert_eq!(&buffer[8..8 + len], slice::from_raw_parts(src, len));
            }

            assert_eq!(libc::munmap(mapping, page_size * 2), 0);
        }
    }

    extern "C" fn ignore_signal(_: libc::c_int) {}

    fn install_signal_handler() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            // SAFETY: the handler has C ABI and remains valid for the process lifetime.
            unsafe {
                let mut action: libc::sigaction = mem::zeroed();
                action.sa_sigaction = ignore_signal as usize;
                action.sa_flags = libc::SA_RESTART;
                libc::sigemptyset(&mut action.sa_mask);
                assert_eq!(libc::sigaction(libc::SIGUSR1, &action, ptr::null_mut()), 0);
            }
        });
    }

    fn thread_id() -> libc::pid_t {
        // SAFETY: `gettid` has no pointer arguments or caller obligations.
        unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t }
    }

    fn send_signal(tid: libc::pid_t) -> bool {
        // SAFETY: `tgkill` receives the current process and a thread id obtained from `gettid`.
        unsafe {
            libc::syscall(libc::SYS_tgkill, libc::getpid(), tid, libc::SIGUSR1) == 0
        }
    }

    #[test]
    fn signals_abort_one_writer() {
        install_signal_handler();
        let cpu = available_cpus()[0];
        let (tid_tx, tid_rx) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = thread::spawn(move || {
            pin_to(cpu);
            let area = ThreadArea::current().expect("glibc did not register rseq");
            tid_tx.send(thread_id()).unwrap();
            let max_len = 60_256;
            let mut payload = vec![0; max_len];
            let mut buffer = vec![0xaa; RING_SIZE + total(max_len)];
            let mut aborts = 0;

            for attempt in 0..4_000_u64 {
                let len = 60_000 + attempt as usize % 257;
                for (index, byte) in payload[..len].iter_mut().enumerate() {
                    *byte = (attempt * 29 ^ index as u64) as u8;
                }
                let mut head = 0;
                // SAFETY: the local buffer covers the largest record and this is its only writer.
                let result = unsafe {
                    push(
                        area,
                        cpu as u32,
                        &mut head,
                        &mut buffer,
                        RING_SIZE as u64,
                        payload.as_ptr(),
                        len,
                    )
                };
                match result {
                    Push::Done => {
                        assert_eq!(head, total(len) as u64);
                        assert_eq!(
                            u64::from_ne_bytes(buffer[..8].try_into().unwrap()),
                            len as u64
                        );
                        assert_eq!(&buffer[8..8 + len], &payload[..len]);
                    }
                    Push::Aborted => {
                        assert_eq!(head, 0);
                        aborts += 1;
                    }
                    Push::Full => panic!("record unexpectedly did not fit"),
                }
            }
            aborts
        });

        let writer_tid = tid_rx.recv().unwrap();
        let signal_stop = Arc::clone(&stop);
        let signaler = thread::spawn(move || {
            while !signal_stop.load(Ordering::Acquire) {
                if !send_signal(writer_tid) {
                    break;
                }
            }
        });

        let aborts = writer.join().unwrap();
        stop.store(true, Ordering::Release);
        signaler.join().unwrap();
        assert!(aborts > 0);
    }

    struct ConcurrentRing {
        head: AtomicU64,
        tail: AtomicU64,
        bytes: Box<[UnsafeCell<u8>]>,
    }

    // SAFETY: writers use the rseq section and the consumer follows the Acquire/Release protocol.
    unsafe impl Sync for ConcurrentRing {}

    impl ConcurrentRing {
        fn new() -> ConcurrentRing {
            ConcurrentRing {
                head: AtomicU64::new(0),
                tail: AtomicU64::new(0),
                bytes: (0..RING_SIZE + total(8 * 1024))
                    .map(|_| UnsafeCell::new(0xaa))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            }
        }

        fn base(&self) -> *mut u8 {
            self.bytes.as_ptr().cast::<u8>() as *mut u8
        }
    }

    fn record_len(writer: usize, index: u64) -> usize {
        16 + (index as usize * 7919 + writer * 101) % (8 * 1024 - 15)
    }

    fn fill_record(record: &mut [u8], writer: usize, index: u64) {
        record[..8].copy_from_slice(&(writer as u64).to_ne_bytes());
        record[8..16].copy_from_slice(&index.to_ne_bytes());
        for (offset, byte) in record[16..].iter_mut().enumerate() {
            *byte = (writer as u64 * 31 ^ index * 17 ^ (offset + 16) as u64) as u8;
        }
    }

    fn check_record(record: &[u8], expected: &mut [u64]) {
        let writer = u64::from_ne_bytes(record[..8].try_into().unwrap()) as usize;
        let index = u64::from_ne_bytes(record[8..16].try_into().unwrap());
        assert!(writer < expected.len());
        assert_eq!(index, expected[writer]);
        assert_eq!(record.len(), record_len(writer, index));
        let mut wanted = vec![0; record.len()];
        fill_record(&mut wanted, writer, index);
        assert_eq!(record, wanted);
        expected[writer] += 1;
    }

    #[test]
    fn same_cpu_writers_are_serialized() {
        const WRITERS: usize = 2;

        install_signal_handler();
        let cpus = available_cpus();
        assert!(cpus.len() >= 2, "test requires at least two available CPUs");
        let writer_cpu = cpus[0];
        let other_cpu = cpus[1];
        let ring = Arc::new(ConcurrentRing::new());
        let active = Arc::new(AtomicUsize::new(WRITERS));
        let aborts = Arc::new(AtomicU64::new(0));
        let start = Arc::new(Barrier::new(WRITERS + 1));
        let (tid_tx, tid_rx) = mpsc::channel();
        let mut writers = Vec::new();

        for writer_id in 0..WRITERS {
            let writer_ring = Arc::clone(&ring);
            let writer_active = Arc::clone(&active);
            let writer_aborts = Arc::clone(&aborts);
            let writer_start = Arc::clone(&start);
            let writer_tid = tid_tx.clone();
            writers.push(thread::spawn(move || {
                pin_to(writer_cpu);
                let area = ThreadArea::current().expect("glibc did not register rseq");
                assert_eq!(area.cpu_id_start(), writer_cpu as u32);
                writer_tid.send(thread_id()).unwrap();
                writer_start.wait();
                let deadline = Instant::now() + Duration::from_secs(3);
                let mut index = 0_u64;
                let mut record = vec![0; 8 * 1024];

                while Instant::now() < deadline {
                    let len = record_len(writer_id, index);
                    fill_record(&mut record[..len], writer_id, index);
                    let tail = writer_ring.tail.load(Ordering::Acquire);
                    // SAFETY: the ring allocation covers wrapped records and rseq serializes writers.
                    let result = unsafe {
                        push_record(
                            area,
                            writer_cpu as u32,
                            writer_ring.head.as_ptr(),
                            writer_ring.base(),
                            (RING_SIZE - 1) as u64,
                            tail + RING_SIZE as u64,
                            record.as_ptr(),
                            len as u64,
                        )
                    };
                    match result {
                        Push::Done => index += 1,
                        Push::Full => thread::yield_now(),
                        Push::Aborted => {
                            writer_aborts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                writer_active.fetch_sub(1, Ordering::Release);
                index
            }));
        }
        drop(tid_tx);
        let writer_tids: Vec<_> = tid_rx.iter().collect();
        assert_eq!(writer_tids.len(), WRITERS);

        let consumer_ring = Arc::clone(&ring);
        let consumer_active = Arc::clone(&active);
        let consumer = thread::spawn(move || {
            pin_to(other_cpu);
            let mut tail = 0_u64;
            let mut previous_head = 0_u64;
            let mut expected = vec![0_u64; WRITERS];

            loop {
                let head = consumer_ring.head.load(Ordering::Acquire);
                assert!(head >= previous_head);
                previous_head = head;
                while tail < head {
                    let offset = (tail as usize) & (RING_SIZE - 1);
                    // SAFETY: the acquired head exposes a complete header at this ring position.
                    let len = unsafe {
                        ptr::read_unaligned(consumer_ring.base().add(offset).cast::<u64>()) as usize
                    };
                    assert!((16..=8 * 1024).contains(&len));
                    let next = tail + total(len) as u64;
                    assert!(next <= head);
                    // SAFETY: the allocation includes enough trailing bytes for a wrapped record.
                    let record = unsafe {
                        slice::from_raw_parts(consumer_ring.base().add(offset + 8), len)
                    };
                    check_record(record, &mut expected);
                    tail = next;
                }
                consumer_ring.tail.store(tail, Ordering::Release);
                if consumer_active.load(Ordering::Acquire) == 0
                    && tail == consumer_ring.head.load(Ordering::Acquire)
                {
                    break;
                }
                if tail == head {
                    std::hint::spin_loop();
                }
            }
            expected
        });

        let signal_stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&signal_stop);
        let signaler = thread::spawn(move || {
            pin_to(other_cpu);
            while !thread_stop.load(Ordering::Acquire) {
                for &tid in &writer_tids {
                    send_signal(tid);
                }
            }
        });

        start.wait();
        let counts: Vec<_> = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect();
        let consumed = consumer.join().unwrap();
        signal_stop.store(true, Ordering::Release);
        signaler.join().unwrap();

        assert_eq!(consumed, counts);
        assert!(aborts.load(Ordering::Relaxed) > 0);
    }
}
