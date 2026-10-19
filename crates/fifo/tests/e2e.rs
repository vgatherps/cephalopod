#![cfg(target_os = "linux")]

use std::hint::spin_loop;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use ceph_control::{Listener, connect};
use ceph_fifo::Fifo;
use ceph_ring::{Config, PushError, Reader, Writer};

const CHILD_ENV: &str = "CEPH_E2E_CHILD";
const CHILDREN: usize = 4;
const COUNT: u64 = 2_000;

fn allowed_cpus() -> Vec<usize> {
    // SAFETY: plain syscall into a zeroed cpu_set_t.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        assert_eq!(
            libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set),
            0
        );
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&cpu| libc::CPU_ISSET(cpu, &set))
            .collect()
    }
}

fn pin(cpu: usize) {
    // SAFETY: plain syscall on the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}

fn pattern(index: usize, seq: u64) -> Vec<u8> {
    let mut payload = (index as u64).to_le_bytes().to_vec();
    payload.extend((0..seq % 200).map(|j| (index as u64 * 31 + seq + j) as u8));
    payload
}

fn run_child(spec: &str) {
    // SAFETY: prctl with integer arguments only; the child dies if the test process dies.
    unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) };
    let [socket, index, count] = spec.split(' ').collect::<Vec<_>>()[..] else {
        panic!("{spec}")
    };
    let (index, count): (usize, u64) = (index.parse().unwrap(), count.parse().unwrap());
    let cpus = allowed_cpus();
    pin(cpus[index % cpus.len()]);
    let session = connect(Path::new(socket)).unwrap();
    let writer = Writer::attach(&session.prefix).unwrap();
    let mut seq = 0;
    while seq < count {
        match writer.try_push(&pattern(index, seq), session.stream, seq) {
            Ok(()) => seq += 1,
            Err(PushError::Full) => spin_loop(),
            Err(PushError::TooLarge) => panic!("payload too large"),
        }
    }
}

fn take(fifo: &mut Fifo, seen: &mut [Vec<u64>], streams: &mut [Option<u64>]) -> bool {
    let Some(message) = fifo.next() else {
        return false;
    };
    let payload = message.payload();
    let index = u64::from_le_bytes(payload[..8].try_into().unwrap()) as usize;
    assert!(index < CHILDREN, "unknown child {index}");
    if let Some(stream) = streams[index] {
        assert_eq!(message.stream(), stream, "child {index} changed stream");
    } else {
        assert!(!streams.contains(&Some(message.stream())), "stream reused");
        streams[index] = Some(message.stream());
    }
    assert_eq!(message.seq(), seen[index].len() as u64, "child {index}");
    assert_eq!(payload, pattern(index, message.seq()), "child {index}");
    seen[index].push(message.seq());
    true
}

#[test]
fn sequencer_and_players_end_to_end() {
    if let Ok(spec) = std::env::var(CHILD_ENV) {
        return run_child(&spec);
    }
    let prefix = format!("cphe2e{}", std::process::id());
    let socket = format!("/tmp/cphe2e{}.sock", std::process::id());
    let reader = Reader::create(
        &prefix,
        Config {
            ring_size: 64 * 1024,
            max_record: 256,
        },
    )
    .unwrap();
    let (control_tx, control_rx) = mpsc::channel();
    let (kick_tx, kick_rx) = mpsc::channel();
    let (forward_tx, forward_rx) = mpsc::channel();
    let kick_count = Arc::new(AtomicUsize::new(0));
    let relay_count = Arc::clone(&kick_count);
    let relay = std::thread::spawn(move || {
        while let Ok(kick) = kick_rx.recv() {
            relay_count.fetch_add(1, Ordering::Relaxed);
            forward_tx.send(kick).unwrap();
        }
    });
    let listener = Listener::bind(Path::new(&socket), &prefix, 16, control_tx, forward_rx)
        .unwrap();
    let listener = std::thread::spawn(move || listener.run().unwrap());
    let mut fifo = Fifo::new(reader, 16, control_rx, kick_tx);
    let exe = std::env::current_exe().unwrap();
    let mut children: Vec<_> = (0..CHILDREN)
        .map(|index| {
            Some(
                Command::new(&exe)
                    .args(["sequencer_and_players_end_to_end", "--exact", "--nocapture"])
                    .env(CHILD_ENV, format!("{socket} {index} {COUNT}"))
                    .stdout(Stdio::null())
                    .spawn()
                    .unwrap(),
            )
        })
        .collect();
    let mut seen = vec![Vec::new(); CHILDREN];
    let mut streams = [None; CHILDREN];
    let mut killed = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !(seen[..3].iter().all(|items| items.len() == COUNT as usize) && killed) {
        assert!(
            Instant::now() < deadline,
            "players stalled: {:?}",
            seen.iter().map(Vec::len).collect::<Vec<_>>()
        );
        if !take(&mut fifo, &mut seen, &mut streams) {
            spin_loop();
        }
        if !killed && seen[3].len() >= 100 {
            let mut child = children[3].take().unwrap();
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success(), "killed child exited successfully");
            killed = true;
        }
    }
    for (index, child) in children.iter_mut().enumerate().take(3) {
        let status = child.take().unwrap().wait().unwrap();
        assert!(status.success(), "child {index}: {status}");
    }
    let drain_deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < drain_deadline && take(&mut fifo, &mut seen, &mut streams) {}
    for (index, items) in seen[..3].iter().enumerate() {
        assert!(items.iter().copied().eq(0..COUNT), "child {index}");
    }
    assert!(seen[3].len() >= 100);
    assert!(seen[3].iter().copied().eq(0..seen[3].len() as u64));
    drop(fifo);
    relay.join().unwrap();
    assert_eq!(kick_count.load(Ordering::Relaxed), 0);
    listener.join().unwrap();
    std::fs::remove_file(socket).unwrap();
}
