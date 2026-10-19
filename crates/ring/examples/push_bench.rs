#[cfg(not(target_os = "linux"))]
fn main() {
    println!("push_bench requires Linux");
}

#[cfg(target_os = "linux")]
mod linux {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    use ceph_ring::{Config, PushError, Reader, Writer};

    const PUSHES: u128 = 20_000_000;

    fn pin(cpu: usize) {
        // SAFETY: the zeroed set is initialized before the affinity syscall reads it.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(cpu, &mut set);
            assert_eq!(
                libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set),
                0
            );
        }
    }

    fn measure(writer: &Writer, payload: &[u8], label: &str) {
        let start = Instant::now();
        for seq in 0..PUSHES as u64 {
            loop {
                match writer.try_push(payload, 0, seq) {
                    Ok(()) => break,
                    Err(PushError::Full) => std::hint::spin_loop(),
                    Err(PushError::TooLarge) => unreachable!(),
                }
            }
        }
        println!("{label}: {} ns", start.elapsed().as_nanos() / PUSHES);
    }

    pub fn main() {
        let mut reader = Reader::create(Config {
            ring_size: 1024 * 1024,
            max_record: 4096,
        })
        .unwrap();
        let writer = Writer::attach(reader.name()).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let consumer_stop = Arc::clone(&stop);
        let consumer = std::thread::spawn(move || {
            pin(0);
            let mut ready = Vec::new();
            while !consumer_stop.load(Ordering::Acquire) {
                ready.clear();
                reader.poll(&mut ready);
                for &ring in &ready {
                    while reader.front(ring).is_some() {
                        reader.advance(ring);
                    }
                }
                reader.publish();
            }
        });
        pin(1);
        measure(&writer, &[0; 16], "push");
        measure(&writer, &[0; 4096], "push 4096");
        stop.store(true, Ordering::Release);
        consumer.join().unwrap();
    }
}

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}
