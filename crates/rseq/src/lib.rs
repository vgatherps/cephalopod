//! Record layout and the rseq critical section that writes a per-CPU ring.
//!
//! Absolute ring positions and their additions must never wrap.

#[cfg(all(target_os = "linux", not(target_env = "gnu")))]
compile_error!("ceph-rseq requires the GNU libc environment");
#[cfg(all(
    target_os = "linux",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
compile_error!("ceph-rseq requires x86_64 or aarch64");

#[cfg(target_os = "linux")]
use std::arch::asm;
#[cfg(target_os = "linux")]
use std::ptr;
use std::sync::atomic::AtomicU64;

/// Record header, 32 bytes, 8-aligned, followed by the payload padded to 8.
pub const HEADER_LEN: u64 = 32;
pub const TAG_OFFSET: usize = 0;
pub const LEN_OFFSET: usize = 8;
pub const STREAM_OFFSET: usize = 16;
pub const SEQ_OFFSET: usize = 24;

/// Bytes a record with `len` payload bytes occupies: the header plus the payload padded to
/// eight bytes.
pub const fn record_total(len: u64) -> u64 {
    HEADER_LEN + len.next_multiple_of(8)
}

/// The tag a committed record at `pos` carries.
pub const fn tag_for(pos: u64) -> u64 {
    pos | 1
}

/// One CPU's private cached head and consumer tail.
///
/// `head` is written by `Reader::create` once, then only inside [`push_record`] on this CPU.
/// `tail` is read inside `push_record` and written by the caller on refresh with a Release store.
#[repr(C, align(128))]
pub struct Private {
    pub head: AtomicU64,
    pub tail: AtomicU64,
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    static __rseq_offset: isize;
    static __rseq_size: u32;
}

/// Returns whether glibc registered an rseq area for this process's threads.
#[cfg(target_os = "linux")]
pub fn registered() -> bool {
    // SAFETY: glibc exports this process-wide value for the life of the process.
    unsafe { ptr::read_volatile(ptr::addr_of!(__rseq_size)) != 0 }
}

/// The calling thread's `struct rseq`. This value is neither `Send` nor `Sync`.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
pub struct ThreadArea {
    ptr: *mut u8,
}

#[cfg(target_os = "linux")]
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
        })
    }

    /// Returns a volatile read of `cpu_id_start`. The result is always a valid CPU number.
    pub fn cpu_id_start(self) -> u32 {
        // SAFETY: `ThreadArea` can only be created for the calling thread's registered area.
        unsafe { ptr::read_volatile(self.ptr.cast::<u32>()) }
    }
}

/// Outcome of one [`push_record`] attempt.
#[cfg(target_os = "linux")]
#[derive(Debug, PartialEq, Eq)]
pub enum Push {
    /// The record is committed in the ring.
    Done,
    /// The record does not fit below the cached tail plus the ring size. Nothing was written.
    Full,
    /// The attempt was cut short: the thread was not on `cpu`, or it was preempted, migrated, or
    /// signalled before the tag store. The interruption point determines whether the private head
    /// is updated and which record bytes are written. Retry.
    Aborted,
}

/// Appends one record to `cpu`'s ring inside one rseq critical section.
///
/// The record goes at `base + (pos & (ring_size - 1))`, where `pos` is the position after the
/// cached-head walk. Its 32-byte header contains the position tag, payload length, stream, and
/// sequence number, followed by `len` payload bytes and unwritten padding to a multiple of 8.
///
/// The cached-tail check relies on ordering: the in-section plain load of `private.tail` is
/// followed only by stores that depend on the room-check branch. Both supported architectures
/// order those stores after the load through this control dependency. The branch must remain
/// between that load and every store to the ring.
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
///    it is on. It walks past every committed record at the cached head and calls the resulting
///    position `pos`.
/// 5. It checks that the record and the following tag word fit below the cached tail plus the ring
///    size. It stores `pos` to the private head, then clears that following tag word.
/// 6. It writes the length, stream, sequence number, and payload, then stores the position tag.
///    The tag store is the commit and the last instruction of the section: before it the consumer
///    sees no record at this position, after it the consumer sees the whole record.
/// 7. The kernel only jumps to an abort handler preceded by the 4-byte signature glibc registered
///    (`RSEQ_SIG`).
///
/// Local labels, the same on both architectures:
///
/// | Label | Meaning |
/// |---|---|
/// | 20 | the `struct rseq_cs` descriptor (in `__rseq_cs`) |
/// | 30 | start of the critical section |
/// | 40 | end of the critical section, right after the tag store |
/// | 50 | cached-head walk loop |
/// | 51 | cached-head walk exit |
/// | 52 | aarch64 payload-copy join and commit |
/// | 53 | aarch64 32-byte copy loop and trailing overlapping copy |
/// | 54, 55, 56, 57, 58 | aarch64 copy cases for 16-31, 8-15, 4-7, 2-3, and 0-1 bytes |
/// | 60 | abort handler: result 2, `Aborted` |
/// | 70 | ring full: result 1, `Full` |
/// | 90 | common exit; success leaves result 0, `Done` |
///
/// # Safety
///
/// `private` must point to a live, aligned `Private` for `cpu`. Its head must be a multiple of 8
/// and must only be accessed by this function from threads running on `cpu`; its tail must contain
/// a value loaded from the consumer tail with Acquire and stored with Release. `base` must be
/// 8-aligned and `ring_size` must be a nonzero power of two. The destination at
/// `base + (pos & (ring_size - 1))` must be writable for [`record_total(len)`] plus eight bytes.
/// The destination record and cleared word after it must not overlap `private`. Positions and
/// their additions must not wrap. `src` must be readable for `len` bytes and must not overlap the
/// destination record or `private`.
#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)] // mirrors the asm operand list
pub unsafe fn push_record(
    area: ThreadArea,
    cpu: u32,
    private: *const Private,
    base: *mut u8,
    ring_size: u64,
    src: *const u8,
    len: u64,
    stream: u64,
    seq: u64,
) -> Push {
    #[cfg(target_arch = "x86_64")]
    #[repr(C)]
    struct X86Args {
        base: *mut u8,
        ring_size: u64,
        stream: u64,
        seq: u64,
    }

    #[cfg(target_arch = "x86_64")]
    let args = X86Args {
        base,
        ring_size,
        stream,
        seq,
    };
    let result: u64;

    // SAFETY: the caller provides the memory and per-CPU writer guarantees documented above.
    unsafe {
        // x86_64. `rep movsb` copies rcx bytes from rsi to rdi, so those three registers are fixed
        // (Rust does not allow names on fixed registers):
        //   rdi: the record address, then the destination cursor
        //   rsi: the source cursor, starts at `src`
        //   rcx: bytes left to copy, starts at `len`
        #[cfg(target_arch = "x86_64")]
        asm!(
            // Emit descriptor = { version 0, flags 0, start, length, abort handler }.
            ".pushsection __rseq_cs, \"aw\"",
            ".balign 32",
            "20:",
            ".long 0, 0",
            ".quad 30f, 40f - 30f, 60f",
            ".popsection",
            // Arm rseq->rseq_cs immediately before the start label.
            "leaq 20b(%rip), {desc}",
            "movq {desc}, 8({rseq})",
            "30:",
            // Abort unless rseq->cpu_id (offset 4) is `cpu`.
            "cmpl {cpu:e}, 4({rseq})",
            "jne 60f",
            // Step 1: start at the cached head.
            "movq ({private}), {cur}",
            // Step 2: walk past every committed record at the cached head.
            "50:",
            "movq {cur}, %rdi",
            "movq 8({args}), {calc}",
            "decq {calc}",
            "andq {calc}, %rdi",
            "addq ({args}), %rdi",
            "movq {cur}, {calc}",
            "orq $1, {calc}",
            "cmpq {calc}, (%rdi)",
            "jne 51f",
            "movq 8(%rdi), {calc}",
            "addq $39, {calc}",
            "andq $-8, {calc}",
            "addq {calc}, {cur}",
            "jmp 50b",
            "51:",
            // Step 3: full unless the record and the next tag word fit in the ring.
            "leaq 39(%rcx), {calc}",
            "andq $-8, {calc}",
            "addq {cur}, {calc}",
            "addq $8, {calc}",
            "movq 8({private}), {tag_ptr}",
            "addq 8({args}), {tag_ptr}",
            "cmpq {tag_ptr}, {calc}",
            "ja 70f",
            // Step 4: cache this record's base before writing it.
            "movq {cur}, ({private})",
            // Step 5: clear the next record's tag word.
            "subq $8, {calc}",
            "movq 8({args}), {tag_ptr}",
            "decq {tag_ptr}",
            "andq {tag_ptr}, {calc}",
            "addq ({args}), {calc}",
            "movq $0, ({calc})",
            // Step 6: find the record and write every header field except the tag.
            "movq {cur}, %rdi",
            "movq 8({args}), {calc}",
            "decq {calc}",
            "andq {calc}, %rdi",
            "addq ({args}), %rdi",
            "movq %rdi, {tag_ptr}",
            "movq %rcx, 8(%rdi)",
            "movq 16({args}), {calc}",
            "movq {calc}, 16(%rdi)",
            "movq 24({args}), {calc}",
            "movq {calc}, 24(%rdi)",
            "addq $32, %rdi",
            // The copy is interruptible and string stores stay ordered before the tag store.
            "rep movsb",
            // Step 7: commit with the position tag; last instruction of the section.
            "orq $1, {cur}",
            "movq {cur}, ({tag_ptr})",
            "40:",
            "movq $0, {result}",
            "jmp 90f",
            // RSEQ_SIG 0x53053053 is the tail of a `ud1` instruction so
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
            private = in(reg) private,
            args = in(reg) &args,
            inout("rsi") src => _,
            inout("rcx") len => _,
            out("rdi") _,
            desc = out(reg) _,
            cur = out(reg) _,
            calc = out(reg) _,
            tag_ptr = out(reg) _,
            result = lateout(reg) result,
            options(att_syntax, nostack),
        );

        // aarch64. There is no `rep movsb`, so the copy uses overlapping loads:
        //   src, dst: source and destination cursors
        //   left: bytes left to copy, starts at `len`
        //   src_end, dst_end: ends of the source and destination payloads
        //   qa, qb (NEON), ra, rb (general): values in flight
        #[cfg(target_arch = "aarch64")]
        asm!(
            // Emit descriptor = { version 0, flags 0, start, length, abort handler }.
            ".pushsection __rseq_cs, \"aw\"",
            ".balign 32",
            "20:",
            ".long 0, 0",
            ".quad 30f, 40f - 30f, 60f",
            ".popsection",
            // Arm rseq->rseq_cs immediately before the start label.
            // adrp + add reaches the descriptor wherever the linker places __rseq_cs.
            "adrp {desc}, 20b",
            "add {desc}, {desc}, :lo12:20b",
            "str {desc}, [{rseq}, #8]",
            "30:",
            // Abort unless rseq->cpu_id (offset 4) is `cpu`.
            "ldr {cpu_now:w}, [{rseq}, #4]",
            "cmp {cpu_now:w}, {cpu:w}",
            "b.ne 60f",
            // Step 1: start at the cached head.
            "ldr {cur}, [{private}]",
            // Step 2: walk past every committed record at the cached head.
            "50:",
            "sub {calc}, {ring_size}, #1",
            "and {dst}, {cur}, {calc}",
            "add {dst}, {ring_base}, {dst}",
            "orr {ra}, {cur}, #1",
            "ldr {rb}, [{dst}]",
            "cmp {rb}, {ra}",
            "b.ne 51f",
            "ldr {calc}, [{dst}, #8]",
            "add {calc}, {calc}, #39",
            "and {calc}, {calc}, #-8",
            "add {cur}, {cur}, {calc}",
            "b 50b",
            "51:",
            // Step 3: full unless the record and the next tag word fit in the ring.
            "add {calc}, {left}, #39",
            "and {calc}, {calc}, #-8",
            "add {calc}, {cur}, {calc}",
            "add {calc}, {calc}, #8",
            "ldr {ra}, [{private}, #8]",
            "add {ra}, {ra}, {ring_size}",
            "cmp {calc}, {ra}",
            "b.hi 70f",
            // Step 4: cache this record's base before writing it.
            "str {cur}, [{private}]",
            // Step 5: clear the next record's tag word.
            "sub {calc}, {calc}, #8",
            "sub {ra}, {ring_size}, #1",
            "and {dst}, {calc}, {ra}",
            "add {dst}, {ring_base}, {dst}",
            "str xzr, [{dst}]",
            // Step 6: find the record and write every header field except the tag.
            "sub {calc}, {ring_size}, #1",
            "and {dst}, {cur}, {calc}",
            "add {dst}, {ring_base}, {dst}",
            "mov {tag_ptr}, {dst}",
            "str {left}, [{dst}, #8]",
            "str {stream}, [{dst}, #16]",
            "str {seq}, [{dst}, #24]",
            "add {dst}, {dst}, #32",
            "add {src_end}, {src}, {left}",
            "add {dst_end}, {dst}, {left}",
            "cmp {left}, #32",
            "b.lo 54f",
            // Step 6: lengths of at least 32 load full remaining blocks; the trailing load ends
            // at `src_end`, so no load passes it.
            "53:",
            "ldp {qa:q}, {qb:q}, [{src}], #32",
            "stp {qa:q}, {qb:q}, [{dst}], #32",
            "sub {left}, {left}, #32",
            "cmp {left}, #32",
            "b.hs 53b",
            "cbz {left}, 52f",
            "ldp {qa:q}, {qb:q}, [{src_end}, #-32]",
            "stp {qa:q}, {qb:q}, [{dst_end}, #-32]",
            "b 52f",
            // Step 6: lengths 16 through 31 load at the start and ending at `src_end`, so no
            // load passes it.
            "54:",
            "cmp {left}, #16",
            "b.lo 55f",
            "ldr {qa:q}, [{src}]",
            "ldr {qb:q}, [{src_end}, #-16]",
            "str {qa:q}, [{dst}]",
            "str {qb:q}, [{dst_end}, #-16]",
            "b 52f",
            // Step 6: lengths 8 through 15 load at the start and ending at `src_end`, so no
            // load passes it.
            "55:",
            "cmp {left}, #8",
            "b.lo 56f",
            "ldr {ra}, [{src}]",
            "ldr {rb}, [{src_end}, #-8]",
            "str {ra}, [{dst}]",
            "str {rb}, [{dst_end}, #-8]",
            "b 52f",
            // Step 6: lengths 4 through 7 load at the start and ending at `src_end`, so no load
            // passes it.
            "56:",
            "cmp {left}, #4",
            "b.lo 57f",
            "ldr {ra:w}, [{src}]",
            "ldr {rb:w}, [{src_end}, #-4]",
            "str {ra:w}, [{dst}]",
            "str {rb:w}, [{dst_end}, #-4]",
            "b 52f",
            // Step 6: lengths 2 and 3 load at the start and ending at `src_end`, so no load
            // passes it.
            "57:",
            "cmp {left}, #2",
            "b.lo 58f",
            "ldrh {ra:w}, [{src}]",
            "ldrh {rb:w}, [{src_end}, #-2]",
            "strh {ra:w}, [{dst}]",
            "strh {rb:w}, [{dst_end}, #-2]",
            "b 52f",
            // Step 6: length 1 copies one byte; length 0 copies nothing. Neither passes
            // `src_end`. Both fall through to the commit.
            "58:",
            "cbz {left}, 52f",
            "ldrb {ra:w}, [{src}]",
            "strb {ra:w}, [{dst}]",
            // Step 7: commit. Store-release of the tag orders it after the record stores.
            "52:",
            "orr {cur}, {cur}, #1",
            "stlr {cur}, [{tag_ptr}]",
            "40:",
            "mov {result}, #0",
            "b 90f",
            // RSEQ_SIG is the `brk #0x45e0` instruction. The kernel reads the 4 bytes
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
            private = in(reg) private,
            ring_base = in(reg) base,
            ring_size = in(reg) ring_size,
            stream = in(reg) stream,
            seq = in(reg) seq,
            src = inout(reg) src => _,
            left = inout(reg) len => _,
            desc = out(reg) _,
            cpu_now = out(reg) _,
            cur = out(reg) _,
            calc = out(reg) _,
            dst = out(reg) _,
            tag_ptr = out(reg) _,
            src_end = out(reg) _,
            dst_end = out(reg) _,
            qa = out(vreg) _,
            qb = out(vreg) _,
            ra = out(reg) _,
            rb = out(reg) _,
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::mem;
    use std::slice;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    const RING_SIZE: usize = 64 * 1024;
    const AA_WORD: u64 = u64::from_ne_bytes([0xaa; 8]);

    fn pin_to(cpu: usize) {
        // SAFETY: `set` is initialized before it is passed to `sched_setaffinity`.
        unsafe {
            let mut set: libc::cpu_set_t = mem::zeroed();
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

    /// Pins the current thread and returns its registered rseq area with the matching CPU id.
    fn pinned_area() -> (ThreadArea, u32) {
        let cpu = available_cpus()[0];
        pin_to(cpu);
        let area = ThreadArea::current().expect("glibc did not register rseq");
        assert_eq!(area.cpu_id_start(), cpu as u32);
        (area, cpu as u32)
    }

    /// Returns the record header plus the payload rounded up to its eight-byte record size.
    fn total(len: usize) -> usize {
        record_total(len as u64) as usize
    }

    fn private(head: u64, tail: u64) -> Private {
        Private {
            head: AtomicU64::new(head),
            tail: AtomicU64::new(tail),
        }
    }

    fn refresh(private: &Private, tails_word: &AtomicU64) {
        private.tail.store(
            tails_word.load(Ordering::Acquire),
            Ordering::Release,
        );
    }

    struct SignalStopGuard<'a>(&'a AtomicBool);

    impl Drop for SignalStopGuard<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    /// Retries `push_record` after rseq aborts caused by preemption, which pinning does not prevent.
    ///
    /// The retry count is bounded so an implementation that aborts every attempt fails promptly.
    #[allow(clippy::too_many_arguments)]
    unsafe fn push_retrying(
        area: ThreadArea,
        cpu: u32,
        private: &Private,
        buffer: &mut [u64],
        ring_size: u64,
        src: &[u8],
        stream: u64,
        seq: u64,
    ) -> Push {
        for _ in 0..10_000 {
            // SAFETY: the caller provides storage for this record and the per-CPU private line.
            let result = unsafe {
                push_record(
                    area,
                    cpu,
                    private,
                    buffer.as_mut_ptr().cast(),
                    ring_size,
                    src.as_ptr(),
                    src.len() as u64,
                    stream,
                    seq,
                )
            };
            if result != Push::Aborted {
                return result;
            }
        }
        panic!("push aborted 10000 consecutive times");
    }

    #[test]
    /// Walks one committed record, then tries one byte too little capacity and exact capacity.
    fn full_boundaries_do_not_write() {
        let (area, cpu) = pinned_area();
        let payload = [0x35; 17];
        let needed = total(payload.len()) as u64;
        let head0 = RING_SIZE as u64 + 64;
        let head1 = head0 + needed;
        let mut buffer = vec![AA_WORD; (RING_SIZE + total(payload.len()) + 8) / 8];
        let private = private(head0, head0);

        // SAFETY: the allocation covers the record, its cleared word, and this CPU owns `private`.
        let first = unsafe {
            push_retrying(
                area,
                cpu,
                &private,
                &mut buffer,
                RING_SIZE as u64,
                &payload,
                7,
                11,
            )
        };
        assert_eq!(first, Push::Done);
        assert_eq!(private.head.load(Ordering::Relaxed), head0);

        let original = buffer.clone();
        let exact_tail = head1 + needed + 8 - RING_SIZE as u64;
        private.tail.store(exact_tail - 1, Ordering::Release);
        // SAFETY: the full check runs before the otherwise valid destination is written.
        let short = unsafe {
            push_retrying(
                area,
                cpu,
                &private,
                &mut buffer,
                RING_SIZE as u64,
                &payload,
                7,
                11,
            )
        };
        assert_eq!(short, Push::Full);
        assert_eq!(private.head.load(Ordering::Relaxed), head0);
        assert_eq!(buffer, original);

        private.tail.store(exact_tail, Ordering::Release);
        // SAFETY: the allocation covers the fitting record, its cleared word, and this CPU owns it.
        let exact = unsafe {
            push_retrying(
                area,
                cpu,
                &private,
                &mut buffer,
                RING_SIZE as u64,
                &payload,
                7,
                11,
            )
        };
        assert_eq!(exact, Push::Done);
        assert_eq!(private.head.load(Ordering::Relaxed), head1);
        let bytes = unsafe {
            slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), buffer.len() * 8)
        };
        let offset = head1 as usize & (RING_SIZE - 1);
        assert_eq!(
            u64::from_ne_bytes(bytes[offset..offset + 8].try_into().unwrap()),
            tag_for(head1),
        );
    }

    #[test]
    /// Supplies a CPU id different from the thread's pinned CPU for an otherwise valid record.
    ///
    /// It requires `Aborted` with an untouched head and buffer. This catches work done after a
    /// failed CPU check, which should abort the critical section.
    fn wrong_cpu_does_not_write() {
        let (area, cpu) = pinned_area();
        let payload = [0x71; 16];
        let mut buffer = vec![AA_WORD; (total(payload.len()) + 8) / 8];
        let original = buffer.clone();
        let private = private(0, 0);

        // SAFETY: the CPU check aborts before the otherwise valid destination is written.
        let result = unsafe {
            push_record(
                area,
                cpu + 1,
                &private,
                buffer.as_mut_ptr().cast(),
                RING_SIZE as u64,
                payload.as_ptr(),
                payload.len() as u64,
                7,
                11,
            )
        };

        assert_eq!(result, Push::Aborted);
        assert_eq!(private.head.load(Ordering::Relaxed), 0);
        assert_eq!(buffer, original);
    }

    #[test]
    /// Places payloads immediately before a protected page and copies every length through
    /// one hundred bytes.
    ///
    /// An over-read inside the section faults. The first fault enters the abort handler and the
    /// retry faults again, so the test fails, most likely by the process dying on the second fault.
    fn layout_and_no_source_over_read() {
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

            for len in 0..=100 {
                let src = guard.sub(len);
                for index in 0..len {
                    src.add(index).write(index as u8 ^ 0xa6);
                }
                let mut buffer = vec![AA_WORD; RING_SIZE / 8];
                let private = private(0, 0);
                let src = slice::from_raw_parts(src, len);
                let stream = 0x1020_3040_5060_7080;
                let seq = len as u64 + 0x1122;

                let check = |buffer: &[u64], pos: u64| {
                    // SAFETY: `buffer` is live, aligned, and viewed for exactly its byte length.
                    let buffer =
                        slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), buffer.len() * 8);
                    let offset = (pos as usize) & (RING_SIZE - 1);
                    assert_eq!(
                        u64::from_ne_bytes(
                            buffer[offset + TAG_OFFSET..offset + TAG_OFFSET + 8]
                                .try_into()
                                .unwrap(),
                        ),
                        tag_for(pos),
                    );
                    assert_eq!(
                        u64::from_ne_bytes(
                            buffer[offset + LEN_OFFSET..offset + LEN_OFFSET + 8]
                                .try_into()
                                .unwrap(),
                        ),
                        len as u64,
                    );
                    assert_eq!(
                        u64::from_ne_bytes(
                            buffer[offset + STREAM_OFFSET..offset + STREAM_OFFSET + 8]
                                .try_into()
                                .unwrap(),
                        ),
                        stream,
                    );
                    assert_eq!(
                        u64::from_ne_bytes(
                            buffer[offset + SEQ_OFFSET..offset + SEQ_OFFSET + 8]
                                .try_into()
                                .unwrap(),
                        ),
                        seq,
                    );
                    let payload = offset + HEADER_LEN as usize;
                    assert_eq!(&buffer[payload..payload + len], src);
                    assert_eq!(
                        &buffer[payload + len..offset + total(len)],
                        vec![0xaa; total(len) - HEADER_LEN as usize - len],
                    );
                    assert_eq!(private.head.load(Ordering::Relaxed), pos);
                };

                let result = push_retrying(
                    area,
                    cpu,
                    &private,
                    &mut buffer,
                    RING_SIZE as u64,
                    src,
                    stream,
                    seq,
                );
                assert_eq!(result, Push::Done);
                check(&buffer, 0);

                let second = total(len) as u64;
                let result = push_retrying(
                    area,
                    cpu,
                    &private,
                    &mut buffer,
                    RING_SIZE as u64,
                    src,
                    stream,
                    seq,
                );
                assert_eq!(result, Push::Done);
                check(&buffer, second);

                let empty = 4096_u64;
                private.head.store(empty, Ordering::Relaxed);
                let result = push_retrying(
                    area,
                    cpu,
                    &private,
                    &mut buffer,
                    RING_SIZE as u64,
                    src,
                    stream,
                    seq,
                );
                assert_eq!(result, Push::Done);
                check(&buffer, empty);
            }

            assert_eq!(libc::munmap(mapping, page_size * 2), 0);
        }
    }

    #[test]
    /// Clears a payload word that would otherwise look like a future record's tag.
    fn stale_payload_is_not_a_tag() {
        let (area, cpu) = pinned_area();
        let mut payload = [0_u8; 32];
        payload[..8].copy_from_slice(&tag_for(96).to_ne_bytes());
        let mut buffer = vec![0_u64; (64 + total(payload.len()) + 8) / 8];
        let private = private(0, 8);
        let tails = AtomicU64::new(0);

        // SAFETY: the allocation covers each record and cleared word, and this CPU owns `private`.
        let first = unsafe {
            push_retrying(area, cpu, &private, &mut buffer, 64, &payload, 7, 11)
        };
        assert_eq!(first, Push::Done);

        tails.store(64, Ordering::Release);
        refresh(&private, &tails);
        // SAFETY: the allocation covers each record and cleared word, and this CPU owns `private`.
        let second = unsafe {
            push_retrying(area, cpu, &private, &mut buffer, 64, &[], 7, 12)
        };
        assert_eq!(second, Push::Done);

        tails.store(96, Ordering::Release);
        refresh(&private, &tails);
        assert_eq!(buffer[4], 0);

        // SAFETY: the allocation covers each record and cleared word, and this CPU owns `private`.
        let third = unsafe {
            push_retrying(area, cpu, &private, &mut buffer, 64, &[], 7, 13)
        };
        assert_eq!(third, Push::Done);
        assert_eq!(private.head.load(Ordering::Relaxed), 96);
        assert_eq!(buffer[4], tag_for(96));
    }

    /// Accepts `SIGUSR1` so delivery can interrupt rseq without terminating the test process.
    extern "C" fn ignore_signal(_: libc::c_int) {}

    fn thread_id() -> libc::pid_t {
        // SAFETY: `gettid` has no pointer arguments or caller obligations.
        unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t }
    }

    fn send_signal(tid: libc::pid_t) {
        // SAFETY: `tgkill` receives the current process and a thread id obtained from `gettid`.
        unsafe {
            libc::syscall(libc::SYS_tgkill, libc::getpid(), tid, libc::SIGUSR1);
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
            *byte = ((writer as u64 * 31) ^ (index * 17) ^ (offset + 16) as u64) as u8;
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

        // SAFETY: the handler has C ABI and remains valid for the process lifetime.
        unsafe {
            let mut action: libc::sigaction = mem::zeroed();
            action.sa_sigaction = ignore_signal as *const () as usize;
            action.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut action.sa_mask);
            assert_eq!(libc::sigaction(libc::SIGUSR1, &action, ptr::null_mut()), 0);
        }
        let cpus = available_cpus();
        assert!(cpus.len() >= 2, "test requires at least two available CPUs");
        let writer_cpu = cpus[0];
        let other_cpu = cpus[1];
        let private = private(0, 0);
        let tails = AtomicU64::new(0);
        let bytes = (0..(RING_SIZE + total(8 * 1024)) / 8)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let active = AtomicUsize::new(WRITERS);
        let start = Barrier::new(WRITERS + 1);
        let (tid_tx, tid_rx) = mpsc::channel();
        let signal_stop = AtomicBool::new(false);

        thread::scope(|scope| {
            let private = &private;
            let tails = &tails;
            let bytes = &bytes;
            let active = &active;
            let start = &start;
            let signal_stop = &signal_stop;
            let _signal_stop_guard = SignalStopGuard(signal_stop);
            let mut writers = Vec::new();
            for writer_id in 0..WRITERS {
                let writer_tid = tid_tx.clone();
                writers.push(scope.spawn(move || {
                    let base = bytes.as_ptr() as *mut u8;
                    pin_to(writer_cpu);
                    let area = ThreadArea::current().expect("glibc did not register rseq");
                    assert_eq!(area.cpu_id_start(), writer_cpu as u32);
                    writer_tid.send(thread_id()).unwrap();
                    start.wait();
                    let deadline = Instant::now() + Duration::from_secs(3);
                    let mut records = 0_u64;
                    let mut aborts = 0_u64;
                    let mut record = vec![0; 8 * 1024];

                    while Instant::now() < deadline {
                        let len = record_len(writer_id, records);
                        fill_record(&mut record[..len], writer_id, records);
                        // SAFETY: the allocation covers wrapped records and rseq serializes writers.
                        let result = unsafe {
                            push_record(
                                area,
                                writer_cpu as u32,
                                private,
                                base,
                                RING_SIZE as u64,
                                record.as_ptr(),
                                len as u64,
                                writer_id as u64,
                                records,
                            )
                        };
                        match result {
                            Push::Done => records += 1,
                            Push::Full => {
                                refresh(private, tails);
                                thread::yield_now();
                            }
                            Push::Aborted => aborts += 1,
                        }
                    }

                    active.fetch_sub(1, Ordering::Release);
                    (records, aborts)
                }));
            }
            drop(tid_tx);
            let writer_tids: Vec<_> = (0..WRITERS).map(|_| tid_rx.recv().unwrap()).collect();

            let consumer = scope.spawn(move || {
                let base = bytes.as_ptr() as *mut u8;
                pin_to(other_cpu);
                let mut tail_value = 0_u64;
                let mut expected = vec![0_u64; WRITERS];

                loop {
                    let offset = (tail_value as usize) & (RING_SIZE - 1);
                    // SAFETY: every record position is eight-aligned in this allocation.
                    let tag = unsafe { &*base.add(offset).cast::<AtomicU64>() };
                    if tag.load(Ordering::Acquire) == tag_for(tail_value) {
                        // SAFETY: the acquired tag exposes a complete header at this ring position.
                        let len = unsafe {
                            ptr::read_unaligned(base.add(offset + LEN_OFFSET).cast::<u64>()) as usize
                        };
                        assert!((16..=8 * 1024).contains(&len));
                        // SAFETY: the acquired tag exposes the remaining header fields.
                        let stream = unsafe {
                            ptr::read_unaligned(base.add(offset + STREAM_OFFSET).cast::<u64>())
                        };
                        // SAFETY: the acquired tag exposes the remaining header fields.
                        let seq = unsafe {
                            ptr::read_unaligned(base.add(offset + SEQ_OFFSET).cast::<u64>())
                        };
                        assert!(stream < WRITERS as u64);
                        assert_eq!(seq, expected[stream as usize]);
                        // SAFETY: the allocation includes enough trailing bytes for a wrapped record.
                        let record = unsafe {
                            slice::from_raw_parts(
                                base.add(offset + HEADER_LEN as usize),
                                len,
                            )
                        };
                        check_record(record, &mut expected);
                        tail_value += total(len) as u64;
                        tails.store(tail_value, Ordering::Release);
                        continue;
                    }
                    if active.load(Ordering::Acquire) == 0
                        && tag.load(Ordering::Acquire) != tag_for(tail_value)
                    {
                        break;
                    }
                    std::hint::spin_loop();
                }
                expected
            });

            let signaler = scope.spawn(move || {
                pin_to(other_cpu);
                while !signal_stop.load(Ordering::Acquire) {
                    for &tid in &writer_tids {
                        send_signal(tid);
                    }
                }
            });

            start.wait();
            let results: Vec<_> = writers.into_iter().map(|writer| writer.join().unwrap()).collect();
            let consumed = consumer.join().unwrap();
            signal_stop.store(true, Ordering::Release);
            signaler.join().unwrap();

            let counts: Vec<_> = results.iter().map(|&(count, _)| count).collect();
            assert_eq!(consumed, counts);
            assert!(counts.iter().all(|&count| count >= 1000));
            assert!(results.iter().map(|&(_, aborts)| aborts).sum::<u64>() > 0);
        });
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
        let mut buffer = vec![AA_WORD; (RING_SIZE + total(payload.len()) + 8) / 8];
        let private = private(0, 0);
        let mut descriptor_address = 0_u64;

        for _ in 0..10_000 {
            private.head.store(0, Ordering::Relaxed);
            buffer[0] = AA_WORD;
            // SAFETY: the test allocations cover the record and only this CPU writes `private`.
            let result = unsafe {
                push_retrying(
                    area,
                    cpu,
                    &private,
                    &mut buffer,
                    RING_SIZE as u64,
                    &payload,
                    7,
                    11,
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
        let descriptor = unsafe { &*(descriptor_address as *const [u64; 4]) };
        assert_eq!(descriptor[0], 0);

        // SAFETY: the descriptor points into this function's emitted instructions.
        let signature = unsafe { ptr::read_unaligned((descriptor[3] - 4) as *const u32) };

        #[cfg(target_arch = "aarch64")]
        {
            // `0xd428bc00` is `brk #0x45e0`, glibc's `RSEQ_SIG`, read from `abort_ip - 4`.
            assert_eq!(signature, 0xd428bc00);
            // SAFETY: the descriptor brackets aligned AArch64 instructions in this function.
            let arming = unsafe { ptr::read_unaligned((descriptor[1] - 4) as *const u32) };
            // SAFETY: the descriptor brackets aligned AArch64 instructions in this function.
            let commit = unsafe {
                ptr::read_unaligned(
                    (descriptor[1] + descriptor[2] - 4) as *const u32,
                )
            };
            // With mask `0xFFFF_FC00`, `0xF900_0400` is a 64-bit `str` at immediate offset 8
            // with any registers. It is read before `start_ip` as the arming store to rseq_cs.
            assert_eq!(arming & 0xffff_fc00, 0xf900_0400);
            // With the same mask, `0xC89F_FC00` is a 64-bit `stlr` with any registers. It is read
            // before `start_ip + post_commit_offset` as the commit store to the tag.
            assert_eq!(commit & 0xffff_fc00, 0xc89f_fc00);
        }

        #[cfg(target_arch = "x86_64")]
        {
            // `0x53053053` is glibc's `RSEQ_SIG`, read from the four bytes before `abort_ip`.
            assert_eq!(signature, 0x53053053);
            // SAFETY: the descriptor's start follows at least the five-byte arming instruction.
            let arming = unsafe {
                slice::from_raw_parts((descriptor[1] - 5) as *const u8, 5)
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

            let post_commit = descriptor[1] + descriptor[2];
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

            let (commit_start, commit_rex, commit_modrm) = if commit_without_suffix {
                (post_commit - 3, commit[1], commit[3])
            } else {
                (post_commit - 4, commit[0], commit[2])
            };
            // SAFETY: the commit store is immediately preceded by a four-byte `orq $1, %reg`.
            let tag_or = unsafe { slice::from_raw_parts((commit_start - 4) as *const u8, 4) };
            assert_eq!(tag_or[0] & 0xf8, 0x48);
            assert_eq!(tag_or[1], 0x83);
            assert_eq!(tag_or[2] & 0xf8, 0xc8);
            assert_eq!(tag_or[3], 0x01);

            let tag_register = (tag_or[2] & 0x07) | ((tag_or[0] & 0x01) << 3);
            let store_register = ((commit_modrm >> 3) & 0x07) | ((commit_rex & 0x04) << 1);
            assert_eq!(store_register, tag_register);
        }
    }
}
