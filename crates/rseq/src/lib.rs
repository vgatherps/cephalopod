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

/// Outcome of one [`push_record`] attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum Push {
    /// The record is in the ring and `*head` points past it.
    Done,
    /// The record does not fit below `limit`. Nothing was written.
    Full,
    /// The attempt was cut short: the thread was not on `cpu`, or it was preempted, migrated, or
    /// signalled before the head store. `*head` is unchanged; bytes past it may be partly
    /// written. Retry.
    Aborted,
}

/// Appends one record to `cpu`'s ring inside one rseq critical section.
///
/// The record goes at `base + (*head & mask)`: the payload length as a u64, the `len` payload
/// bytes, then padding up to a multiple of 8 (padding is not written). `*head` advances by
/// `(len + 15) & !7`.
///
/// How the critical section works:
///
/// 1. The asm emits a `struct rseq_cs` descriptor into the `__rseq_cs` section. It holds the
///    start address of the section, its length, and the address of the abort handler.
/// 2. Storing the descriptor's address into `rseq->rseq_cs` arms it. While the thread's
///    instruction pointer is inside the section, any preemption, migration, or signal makes the
///    kernel resume the thread at the abort handler instead of where it was.
/// 3. That store is the last instruction before the start. If anything ran in between, a
///    preemption there would make the kernel clear `rseq_cs` (the instruction pointer is outside
///    the range) and the section would run unprotected.
/// 4. The section first checks `rseq->cpu_id == cpu`, so a thread only writes the ring of the CPU
///    it is on. Any other thread that gets this CPU before our head store aborts us first, so each
///    ring has exactly one writer at a time and no atomic read-modify-write is needed.
/// 5. It checks for room, writes the header and payload, then stores the new head. That store is
///    the commit and the last instruction of the section: before it the consumer sees nothing,
///    after it the consumer sees the whole record.
/// 6. The kernel only jumps to an abort handler preceded by the 4-byte signature glibc registered
///    (`RSEQ_SIG`).
///
/// Local labels, the same on both architectures:
///
/// | Label | Meaning |
/// |---|---|
/// | 20 | the `struct rseq_cs` descriptor (in `__rseq_cs`) |
/// | 30 | start of the critical section |
/// | 40 | end of the critical section, right after the head store |
/// | 50, 51, 52 | aarch64 copy loops (8 bytes at a time, then single bytes, then commit) |
/// | 60 | abort handler: result 2, `Aborted` |
/// | 70 | ring full: result 1, `Full` |
/// | 90 | common exit; success leaves result 0, `Done` |
///
/// # Safety
///
/// `head` must be 8-aligned and only written through this function by threads running on `cpu`.
/// `base + (*head & mask)` must be writable for `8 + round_up(len, 8)` bytes. `limit` must equal
/// a tail loaded with `Acquire` plus the ring size. A stale tail is safe because tail only grows.
/// Positions and their additions must not wrap. `src` must be readable for `len` bytes and must
/// not overlap the destination record or `*head`. The destination record must not overlap `*head`.
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

    // SAFETY: the caller provides the memory and per-CPU writer guarantees documented above.
    unsafe {
        // x86_64. `rep movsb` copies rcx bytes from rsi to rdi, so those three registers are fixed
        // (Rust does not allow names on fixed registers):
        //   rdi: the head position, then the destination cursor
        //   rsi: the source cursor, starts at `src`
        //   rcx: bytes left to copy, starts at `len`
        #[cfg(target_arch = "x86_64")]
        asm!(
            // Step 1: descriptor = { version 0, flags 0, start, length, abort handler }.
            ".pushsection __rseq_cs, \"aw\"",
            ".balign 32",
            "20:",
            ".long 0, 0",
            ".quad 30f, 40f - 30f, 60f",
            ".popsection",
            // Steps 2-3: rseq->rseq_cs (offset 8) = &descriptor, immediately before the start.
            "leaq 20b(%rip), {desc}",
            "movq {desc}, 8({rseq})",
            "30:",
            // Step 4: abort unless rseq->cpu_id (offset 4) is `cpu`.
            "cmpl {cpu:e}, 4({rseq})",
            "jne 60f",
            // Step 5a: new_head = head + ((len + 15) & !7), the header plus the padded payload.
            "movq ({head}), %rdi",
            "leaq 15(%rcx), {new_head}",
            "andq $-8, {new_head}",
            "addq %rdi, {new_head}",
            // Step 5b: full if the record would end past the consumer's tail plus the ring size.
            "cmpq {limit}, {new_head}",
            "ja 70f",
            // Step 5c: rdi = ring_base + (head & mask), where the record starts. Write the header.
            "andq {mask}, %rdi",
            "addq {ring_base}, %rdi",
            "movq %rcx, (%rdi)",
            "addq $8, %rdi",
            // Step 5d: copy the payload. String stores are not reordered with the later head
            // store (Intel SDM Vol. 3A 8.2.4.1), and the copy is interruptible.
            "rep movsb",
            // Step 5e: commit. Publish the new head; last instruction of the section.
            "movq {new_head}, ({head})",
            "40:",
            "movq $0, {result}",
            "jmp 90f",
            // Step 6: RSEQ_SIG 0x53053053, written as the tail of a `ud1` instruction so
            // disassemblers stay in sync. The kernel reads the 4 bytes before label 60.
            ".byte 0x0f, 0xb9, 0x3d",
            ".long 0x53053053",
            "60:",
            "movq $2, {result}",
            "jmp 90f",
            "70:",
            "movq $1, {result}",
            "90:",
            rseq = in(reg) area.ptr,
            cpu = in(reg) cpu,
            head = in(reg) head,
            ring_base = in(reg) base,
            mask = in(reg) mask,
            limit = in(reg) limit,
            inout("rsi") src => _,
            inout("rcx") len => _,
            out("rdi") _,
            desc = out(reg) _,
            new_head = out(reg) _,
            result = lateout(reg) result,
            options(att_syntax, nostack),
        );

        // aarch64. There is no `rep movsb`, so the copy is two loops over named cursors:
        //   src, dst: source and destination cursors
        //   left: bytes left to copy, starts at `len`
        //   word: the 8-byte or 1-byte value in flight
        #[cfg(target_arch = "aarch64")]
        asm!(
            // Step 1: descriptor = { version 0, flags 0, start, length, abort handler }.
            ".pushsection __rseq_cs, \"aw\"",
            ".balign 32",
            "20:",
            ".long 0, 0",
            ".quad 30f, 40f - 30f, 60f",
            ".popsection",
            // Steps 2-3: rseq->rseq_cs (offset 8) = &descriptor, immediately before the start.
            // adrp + add reaches the descriptor wherever the linker places __rseq_cs.
            "adrp {desc}, 20b",
            "add {desc}, {desc}, :lo12:20b",
            "str {desc}, [{rseq}, #8]",
            "30:",
            // Step 4: abort unless rseq->cpu_id (offset 4) is `cpu`.
            "ldr {cpu_now:w}, [{rseq}, #4]",
            "cmp {cpu_now:w}, {cpu:w}",
            "b.ne 60f",
            // Step 5a: new_head = pos + ((len + 15) & !7), the header plus the padded payload.
            "ldr {pos}, [{head}]",
            "add {new_head}, {left}, #15",
            "and {new_head}, {new_head}, #-8",
            "add {new_head}, {pos}, {new_head}",
            // Step 5b: full if the record would end past the consumer's tail plus the ring size.
            "cmp {new_head}, {limit}",
            "b.hi 70f",
            // Step 5c: dst = ring_base + (pos & mask), where the record starts. Write the header.
            "and {dst}, {pos}, {mask}",
            "add {dst}, {ring_base}, {dst}",
            "str {left}, [{dst}], #8",
            // Step 5d: copy the payload, 8 bytes at a time while at least 8 are left...
            "50:",
            "cmp {left}, #8",
            "b.lo 51f",
            "ldr {word}, [{src}], #8",
            "str {word}, [{dst}], #8",
            "sub {left}, {left}, #8",
            "b 50b",
            // ...then the last 0-7 bytes one at a time, so `src` is never read past `len`.
            "51:",
            "cbz {left}, 52f",
            "ldrb {word:w}, [{src}], #1",
            "strb {word:w}, [{dst}], #1",
            "sub {left}, {left}, #1",
            "b 51b",
            // Step 5e: commit. Store-release of the new head orders it after the payload stores;
            // last instruction of the section.
            "52:",
            "stlr {new_head}, [{head}]",
            "40:",
            "mov {result}, #0",
            "b 90f",
            // Step 6: RSEQ_SIG, the `brk #0x45e0` instruction. The kernel reads the 4 bytes
            // before label 60.
            ".inst 0xd428bc00",
            "60:",
            "mov {result}, #2",
            "b 90f",
            "70:",
            "mov {result}, #1",
            "90:",
            rseq = in(reg) area.ptr,
            cpu = in(reg) cpu,
            head = in(reg) head,
            ring_base = in(reg) base,
            mask = in(reg) mask,
            limit = in(reg) limit,
            src = inout(reg) src => _,
            left = inout(reg) len => _,
            desc = out(reg) _,
            cpu_now = out(reg) _,
            pos = out(reg) _,
            new_head = out(reg) _,
            dst = out(reg) _,
            word = out(reg) _,
            result = lateout(reg) result,
            options(nostack),
        );
    }

    match result {
        0 => Push::Done,
        1 => Push::Full,
        _ => Push::Aborted,
    }
}

#[cfg(test)]
mod tests {
    // `record_layout` checks the header, payload, padding, and head written by `push_record`.
    // `full_boundaries_do_not_write` checks that `Full` leaves the head and destination unchanged.
    // `wrong_cpu_does_not_write` checks that a mismatched CPU aborts before any write.
    // `source_is_not_read_past_length` checks that copying stops exactly at `len`.
    // `signals_abort_one_writer` checks that a signal aborts or completes one writer cleanly.
    // `same_cpu_writers_are_serialized` checks that two writers do not commit into one ring slot.
    // `descriptor_brackets_arming_and_commit` checks that the rseq descriptor covers both stores.
    use super::*;
    use std::cell::UnsafeCell;
    use std::mem;
    use std::slice;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc, Barrier, Once};
    use std::thread;
    use std::time::{Duration, Instant};

    const RING_SIZE: usize = 64 * 1024;

    /// Pins this test thread to `cpu` so its registered rseq CPU value is predictable.
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

    /// Returns the CPUs to which the current process is allowed to bind test threads.
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

    /// Pins the current thread and returns its registered rseq area with the matching CPU id.
    fn pinned_area() -> (ThreadArea, u32) {
        let cpu = available_cpus()[0];
        pin_to(cpu);
        let area = ThreadArea::current().expect("glibc did not register rseq");
        assert_eq!(area.cpu_id_start(), cpu as u32);
        (area, cpu as u32)
    }

    /// Returns the eight-byte header plus the payload rounded up to its eight-byte record size.
    fn total(len: usize) -> usize {
        8 + (len + 7) / 8 * 8
    }

    /// Calls `push_record` with the common test ring layout.
    ///
    /// The caller must provide storage for the record and keep this thread as the sole head writer.
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

    /// Retries `push` after rseq aborts caused by preemption, which pinning does not prevent.
    ///
    /// The retry count is bounded so an implementation that aborts every attempt fails promptly.
    unsafe fn push_until_not_aborted(
        area: ThreadArea,
        cpu: u32,
        head: &mut u64,
        buffer: &mut [u8],
        limit: u64,
        src: *const u8,
        len: usize,
    ) -> Push {
        for _ in 0..10_000 {
            let head_before = *head;
            // SAFETY: the caller satisfies `push`'s test allocation contract.
            let result = unsafe { push(area, cpu, head, buffer, limit, src, len) };
            if result != Push::Aborted {
                return result;
            }
            assert_eq!(*head, head_before);
        }
        panic!("push aborted 10000 consecutive times");
    }

    #[test]
    /// Writes payloads at alignment boundaries and checks their header, bytes, padding, and head.
    ///
    /// This catches a wrong length header, a bad payload copy, bad padding, or an incorrect head
    /// advance.
    fn record_layout() {
        let (area, cpu) = pinned_area();
        for len in [0, 1, 7, 8, 9, 15, 16, 17, 100] {
            let payload: Vec<u8> = (0..len).map(|index| index as u8 ^ 0x5a).collect();
            let mut buffer = vec![0xaa; RING_SIZE + total(100)];
            let mut head = 0;

            // SAFETY: `buffer` and `payload` satisfy `push`'s test allocation contract.
            let result = unsafe {
                push_until_not_aborted(
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
    /// Tries a record at its exact limit, one byte below it, and with a stale limit behind the
    /// head.
    ///
    /// The fitting record must finish. Both `Full` cases must leave the head and bytes unchanged,
    /// catching a full check that writes before it fails.
    fn full_boundaries_do_not_write() {
        let (area, cpu) = pinned_area();
        let payload = [0x35; 17];
        let needed = total(payload.len()) as u64;

        let mut exact_buffer = vec![0xaa; RING_SIZE + total(payload.len())];
        let mut exact_head = 0;
        // SAFETY: the test allocations cover the record and only this thread writes the head.
        let exact = unsafe {
            push_until_not_aborted(
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
            push_until_not_aborted(
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
            push_until_not_aborted(
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
    /// Supplies a CPU id different from the thread's pinned CPU for an otherwise valid record.
    ///
    /// It requires `Aborted` with an untouched head and buffer. This catches work done after a
    /// failed CPU check, which should abort the critical section.
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
    /// Places payloads immediately before a protected page and copies every length through
    /// seventeen bytes.
    ///
    /// Each call must finish and reproduce exactly `len` bytes; a read past `src + len` faults,
    /// enters the rseq abort handler, and appears here as endless `Aborted` retries rather than a
    /// process crash.
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
                let result = push_until_not_aborted(
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

    /// Accepts `SIGUSR1` so delivery can interrupt rseq without terminating the test process.
    extern "C" fn ignore_signal(_: libc::c_int) {}

    /// Installs `ignore_signal` once for all tests that inject `SIGUSR1` into writer threads.
    fn install_signal_handler() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            // SAFETY: the handler has C ABI and remains valid for the process lifetime.
            unsafe {
                let mut action: libc::sigaction = mem::zeroed();
                action.sa_sigaction = ignore_signal as *const () as usize;
                action.sa_flags = libc::SA_RESTART;
                libc::sigemptyset(&mut action.sa_mask);
                assert_eq!(libc::sigaction(libc::SIGUSR1, &action, ptr::null_mut()), 0);
            }
        });
    }

    /// Returns this Linux thread's kernel thread id for `tgkill`.
    fn thread_id() -> libc::pid_t {
        // SAFETY: `gettid` has no pointer arguments or caller obligations.
        unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t }
    }

    /// Sends the test signal to `tid`, returning false when that thread has already exited.
    fn send_signal(tid: libc::pid_t) -> bool {
        // SAFETY: `tgkill` receives the current process and a thread id obtained from `gettid`.
        unsafe {
            libc::syscall(libc::SYS_tgkill, libc::getpid(), tid, libc::SIGUSR1) == 0
        }
    }

    #[test]
    /// Sends `SIGUSR1` repeatedly while one pinned writer copies large records into a local ring.
    ///
    /// Completed records must be intact and aborted records must leave `head` at zero. This catches
    /// an abort path that exposes partial work or a critical section that cannot be interrupted
    /// safely.
    fn signals_abort_one_writer() {
        install_signal_handler();
        let cpu = available_cpus()[0];
        let (tid_tx, tid_rx) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(2));
        let writer_start = Arc::clone(&start);
        let writer = thread::spawn(move || {
            pin_to(cpu);
            let area = ThreadArea::current().expect("glibc did not register rseq");
            tid_tx.send(thread_id()).unwrap();
            writer_start.wait();
            let max_len = 60_256;
            let mut payload = vec![0; max_len];
            let mut buffer = vec![0xaa; RING_SIZE + total(max_len)];
            let mut aborts = 0;
            let mut done = 0;

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
                        done += 1;
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
            (aborts, done)
        });

        let writer_tid = tid_rx.recv().unwrap();
        let signal_stop = Arc::clone(&stop);
        let signaler = thread::spawn(move || {
            start.wait();
            while !signal_stop.load(Ordering::Acquire) {
                if !send_signal(writer_tid) {
                    break;
                }
            }
        });

        let (aborts, done) = writer.join().unwrap();
        stop.store(true, Ordering::Release);
        signaler.join().unwrap();
        assert!(aborts > 0);
        assert!(done > 0);
    }

    /// Ring storage shared by concurrent writers and one consumer in the serialization test.
    struct ConcurrentRing {
        head: AtomicU64,
        tail: AtomicU64,
        bytes: Box<[UnsafeCell<u8>]>,
    }

    // SAFETY: writers use the rseq section and the consumer follows the Acquire/Release protocol.
    unsafe impl Sync for ConcurrentRing {}

    impl ConcurrentRing {
        /// Allocates an initialized ring with trailing space for records that wrap at the ring
        /// boundary.
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

        /// Returns the byte address passed to `push_record` and used by the consumer to read
        /// records.
        fn base(&self) -> *mut u8 {
            self.bytes.as_ptr().cast::<u8>() as *mut u8
        }
    }

    /// Derives a nonconstant record length from its writer and per-writer sequence number.
    fn record_len(writer: usize, index: u64) -> usize {
        16 + (index as usize * 7919 + writer * 101) % (8 * 1024 - 15)
    }

    /// Encodes a writer id, sequence number, and deterministic payload into one test record.
    fn fill_record(record: &mut [u8], writer: usize, index: u64) {
        record[..8].copy_from_slice(&(writer as u64).to_ne_bytes());
        record[8..16].copy_from_slice(&index.to_ne_bytes());
        for (offset, byte) in record[16..].iter_mut().enumerate() {
            *byte = (writer as u64 * 31 ^ index * 17 ^ (offset + 16) as u64) as u8;
        }
    }

    /// Verifies a consumed record's writer sequence and contents, then advances that writer's
    /// expectation.
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
    /// Runs two signalled writers pinned to one CPU while a second CPU consumes their shared ring.
    ///
    /// The consumer must see every writer's records in order and no corrupted record. This catches
    /// two writers committing into one ring slot or publishing a partial record.
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
        let writer_tids: Vec<_> = (0..WRITERS)
            .map(|_| tid_rx.recv().unwrap())
            .collect();
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
        assert!(counts.iter().all(|&count| count >= 1000));
        assert!(aborts.load(Ordering::Relaxed) > 0);
    }

    #[repr(C)]
    /// Matches the kernel-visible descriptor addressed by the rseq area's `rseq_cs` field.
    struct RseqCs {
        version: u32,
        flags: u32,
        start_ip: u64,
        post_commit_offset: u64,
        abort_ip: u64,
    }

    #[test]
    /// Finds the descriptor published by a completed `push_record` and inspects its instruction
    /// boundaries.
    ///
    /// It requires glibc's abort signature, an arming store immediately before `start_ip`, and a
    /// commit store immediately before `post_commit_offset`, catching a missing signature or stores
    /// outside the declared critical section.
    fn descriptor_brackets_arming_and_commit() {
        let (area, cpu) = pinned_area();
        let payload = [0x5a; 8];
        let mut buffer = vec![0xaa; RING_SIZE + total(payload.len())];
        let mut descriptor_address = 0_u64;

        for _ in 0..10_000 {
            let mut head = 0;
            // SAFETY: the test allocations cover the record and only this thread writes `head`.
            let result = unsafe {
                push_until_not_aborted(
                    area,
                    cpu,
                    &mut head,
                    &mut buffer,
                    RING_SIZE as u64,
                    payload.as_ptr(),
                    payload.len(),
                )
            };
            assert_eq!(result, Push::Done);
            // SAFETY: offset 8 is the registered thread area's aligned `rseq_cs` field.
            descriptor_address = unsafe { ptr::read_volatile(area.ptr.add(8).cast::<u64>()) };
            if descriptor_address != 0 {
                break;
            }
        }
        assert_ne!(descriptor_address, 0);

        // SAFETY: a nonzero `rseq_cs` value points to this call site's static descriptor.
        let descriptor = unsafe { &*(descriptor_address as *const RseqCs) };
        assert_eq!(descriptor.version, 0);
        assert_eq!(descriptor.flags, 0);

        // SAFETY: the descriptor points into this function's emitted instructions.
        let signature = unsafe { ptr::read_unaligned((descriptor.abort_ip - 4) as *const u32) };

        #[cfg(target_arch = "aarch64")]
        {
            // `0xd428bc00` is `brk #0x45e0`, glibc's `RSEQ_SIG`, read from `abort_ip - 4`.
            assert_eq!(signature, 0xd428bc00);
            // SAFETY: the descriptor brackets aligned AArch64 instructions in this function.
            let arming = unsafe { ptr::read_unaligned((descriptor.start_ip - 4) as *const u32) };
            // SAFETY: the descriptor brackets aligned AArch64 instructions in this function.
            let commit = unsafe {
                ptr::read_unaligned(
                    (descriptor.start_ip + descriptor.post_commit_offset - 4) as *const u32,
                )
            };
            // With mask `0xFFFF_FC00`, `0xF900_0400` is a 64-bit `str` at immediate offset 8
            // with any registers. It is read before `start_ip` as the arming store to rseq_cs.
            assert_eq!(arming & 0xffff_fc00, 0xf900_0400);
            // With the same mask, `0xC89F_FC00` is a 64-bit `stlr` with any registers. It is read
            // before `start_ip + post_commit_offset` as the commit store to `head`.
            assert_eq!(commit & 0xffff_fc00, 0xc89f_fc00);
        }

        #[cfg(target_arch = "x86_64")]
        {
            // `0x53053053` is glibc's `RSEQ_SIG`, read from the four bytes before `abort_ip`.
            assert_eq!(signature, 0x53053053);
            // SAFETY: the descriptor's start follows at least the five-byte arming instruction.
            let arming = unsafe {
                slice::from_raw_parts((descriptor.start_ip - 5) as *const u8, 5)
            };
            // `byte & 0xF8 == 0x48` is REX.W and `0x89` is `mov r64` to memory. These ModRM
            // checks read `start_ip - 5` and accept `disp8(%reg)` with displacement 8, without SIB.
            let arming_without_sib = arming[1] & 0xf8 == 0x48
                && arming[2] == 0x89
                && arming[3] & 0xc0 == 0x40
                && arming[3] & 0x07 != 4
                && arming[4] == 0x08;
            // The same REX.W `mov r64` to memory at `start_ip - 5` uses SIB when the base is
            // rsp/r12.
            let arming_with_sib = arming[0] & 0xf8 == 0x48
                && arming[1] == 0x89
                && arming[2] & 0xc7 == 0x44
                && arming[3] == 0x24
                && arming[4] == 0x08;
            assert!(arming_without_sib || arming_with_sib);

            let post_commit = descriptor.start_ip + descriptor.post_commit_offset;
            // SAFETY: the post-commit address follows at least the four-byte commit instruction.
            let commit = unsafe { slice::from_raw_parts((post_commit - 4) as *const u8, 4) };
            // These bytes before `start_ip + post_commit_offset` are REX.W `mov r64` to memory.
            // The ModRM form is `(%reg)` except bases rsp/r12 and rbp/r13, which need other forms.
            let commit_without_suffix = commit[1] & 0xf8 == 0x48
                && commit[2] == 0x89
                && commit[3] & 0xc0 == 0
                && !matches!(commit[3] & 0x07, 4 | 5);
            // This is the SIB `(%reg)` form for an rsp/r12 base, read before the commit boundary.
            let commit_with_sib = commit[0] & 0xf8 == 0x48
                && commit[1] == 0x89
                && commit[2] & 0xc7 == 0x04
                && commit[3] == 0x24;
            // This is the zero-displacement form required for an rbp/r13 base before the commit
            // boundary.
            let commit_with_displacement = commit[0] & 0xf8 == 0x48
                && commit[1] == 0x89
                && commit[2] & 0xc7 == 0x45
                && commit[3] == 0x00;
            assert!(commit_without_suffix || commit_with_sib || commit_with_displacement);
        }
    }
}
