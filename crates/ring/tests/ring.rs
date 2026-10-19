#![cfg(target_os = "linux")]

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ceph_ring::{Config, PushError, Reader, Writer};

const KIB: usize = 1024;
const CHILD_ENV: &str = "CEPH_RING_CHILD";

fn prefix(test: &str) -> String {
    format!("ceph-ring-{test}-{}", std::process::id())
}

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

fn pin(cpus: &[usize]) {
    // SAFETY: plain syscall on the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &cpu in cpus {
            libc::CPU_SET(cpu, &mut set);
        }
        assert_eq!(
            libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}

fn indexed_record(i: usize) -> Vec<u8> {
    (0..i % 301).map(|j| (i * 7 + j) as u8).collect()
}

fn drain(reader: &mut Reader, ring: usize) -> Vec<(u64, u64, Vec<u8>)> {
    let mut records = Vec::new();
    while reader.front(ring).is_some() {
        let record = reader.view(ring);
        records.push((record.stream, record.seq, record.payload.to_vec()));
        reader.advance(ring);
    }
    reader.publish();
    records
}

#[test]
fn round_trip_and_wrap() {
    let p = prefix("round_trip_and_wrap");
    let mut reader = Reader::create(
        &p,
        Config {
            ring_size: 16 * KIB,
            max_record: 300,
        },
    )
    .unwrap();
    let writer = Writer::attach(&p).unwrap();
    for &k in allowed_cpus().iter().take(2) {
        pin(&[k]);
        let (mut pushed, mut next) = (0, 0);
        let mut check = |(stream, seq, payload): (u64, u64, Vec<u8>)| {
            assert_eq!(stream, k as u64, "ring {k} record {next}");
            assert_eq!(seq, next as u64, "ring {k} record {next}");
            assert_eq!(payload, indexed_record(next), "ring {k} record {next}");
            next += 1;
        };
        while pushed < 1000 {
            match writer.try_push(&indexed_record(pushed), k as u64, pushed as u64) {
                Ok(()) => pushed += 1,
                Err(PushError::Full) => {
                    let records = drain(&mut reader, k);
                    assert!(!records.is_empty());
                    records.into_iter().for_each(&mut check);
                }
                Err(e) => panic!("{e:?}"),
            }
        }
        let mut ready = Vec::new();
        reader.poll(&mut ready);
        assert_eq!(ready, [k]);
        drain(&mut reader, k).into_iter().for_each(&mut check);
        assert_eq!(next, pushed);
    }
}

#[test]
fn full_boundary_and_oversize() {
    let p = prefix("full_boundary_and_oversize");
    let max = 16_344;
    let mut reader = Reader::create(
        &p,
        Config {
            ring_size: 16 * KIB,
            max_record: max,
        },
    )
    .unwrap();
    let writer = Writer::attach(&p).unwrap();
    let cpu = allowed_cpus()[0];
    pin(&[cpu]);

    let rec = [0x5a; 1000];
    for seq in 0..15 {
        assert_eq!(writer.try_push(&rec, 7, seq), Ok(()));
    }
    assert_eq!(writer.try_push(&rec, 7, 15), Err(PushError::Full));
    let record = reader.view(cpu);
    assert_eq!((record.stream, record.seq, record.payload), (7, 0, &rec[..]));
    reader.advance(cpu);
    reader.publish();
    assert_eq!(writer.try_push(&rec, 7, 15), Ok(()));
    let records = drain(&mut reader, cpu);
    assert_eq!(records.len(), 15);
    for (stream, _, payload) in records {
        assert_eq!(stream, 7);
        assert_eq!(payload, rec);
    }

    let big: Vec<u8> = (0..max).map(|j| (j * 13) as u8).collect();
    assert_eq!(writer.try_push(&big, 8, 16), Ok(()));
    assert_eq!(writer.try_push(&[], 8, 17), Err(PushError::Full));
    assert_eq!(
        writer.try_push(&vec![0; max + 1], 8, 17),
        Err(PushError::TooLarge)
    );
    assert_eq!(drain(&mut reader, cpu), [(8, 16, big)]);
}

#[test]
fn stop_before() {
    let p = prefix("stop_before");
    let mut reader = Reader::create(
        &p,
        Config {
            ring_size: 16 * KIB,
            max_record: 64,
        },
    )
    .unwrap();
    let writer = Writer::attach(&p).unwrap();
    let cpu = allowed_cpus()[0];
    pin(&[cpu]);
    for (seq, r) in [&b"A"[..], b"BB", b"CCC"].into_iter().enumerate() {
        writer.try_push(r, 9, seq as u64).unwrap();
    }

    let mut seen = Vec::new();
    let record = reader.view(cpu);
    seen.push((record.seq, record.payload.to_vec()));
    reader.advance(cpu);
    let record = reader.view(cpu);
    seen.push((record.seq, record.payload.to_vec()));
    reader.publish();
    assert_eq!(seen, [(0, b"A".to_vec()), (1, b"BB".to_vec())]);

    seen.clear();
    for (_, seq, payload) in drain(&mut reader, cpu) {
        seen.push((seq, payload));
    }
    assert_eq!(seen, [(1, b"BB".to_vec()), (2, b"CCC".to_vec())]);
}

fn child_record(id: u32, index: u64) -> Vec<u8> {
    (0..index % 200)
        .map(|j| (id as u64 * 31 + index + j) as u8)
        .collect()
}

/// Child role of `multi_process`: writes indexes 0..`count`. All children share the first two
/// allowed CPUs, so processes interleave in the same rings and migrate.
fn run_child(spec: &str) {
    // SAFETY: prctl with integer arguments only; the child dies if the test process dies.
    unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) };
    let [p, id, count] = spec.split(' ').collect::<Vec<_>>()[..] else {
        panic!("{spec}")
    };
    let (id, count): (u32, u64) = (id.parse().unwrap(), count.parse().unwrap());
    let cpus = allowed_cpus();
    pin(&cpus[..cpus.len().min(2)]);
    let writer = Writer::attach(p).unwrap();
    let mut index = 0;
    while index < count {
        match writer.try_push(&child_record(id, index), id as u64, index) {
            Ok(()) => index += 1,
            Err(PushError::Full) => std::thread::yield_now(),
            Err(e) => panic!("{e:?}"),
        }
    }
}

fn drain_children(reader: &mut Reader, seen: &mut [Vec<u64>]) {
    for ring in 0..reader.nr_rings() {
        for (stream, index, payload) in drain(reader, ring) {
            let id = u32::try_from(stream).unwrap();
            assert_eq!(
                payload,
                child_record(id, index),
                "child {id} index {index}"
            );
            seen[id as usize].push(index);
        }
    }
}

#[test]
fn multi_process() {
    if let Ok(spec) = std::env::var(CHILD_ENV) {
        return run_child(&spec);
    }
    const CHILDREN: usize = 2;
    const COUNT: u64 = 5_000;
    let p = prefix("multi_process");
    let mut reader = Reader::create(
        &p,
        Config {
            ring_size: 64 * KIB,
            max_record: 256,
        },
    )
    .unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut children: Vec<_> = (0..CHILDREN)
        .map(|id| {
            Command::new(&exe)
                .args(["multi_process", "--exact", "--nocapture"])
                .env(CHILD_ENV, format!("{p} {id} {COUNT}"))
                .stdout(Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();

    let mut seen = vec![Vec::new(); CHILDREN];
    let deadline = Instant::now() + Duration::from_secs(60);
    while !seen.iter().all(|s| s.len() as u64 >= COUNT) {
        assert!(Instant::now() < deadline, "children stalled");
        drain_children(&mut reader, &mut seen);
    }
    for (id, child) in children.iter_mut().enumerate() {
        let status = child.wait().unwrap();
        assert!(status.success(), "child {id}: {status}");
    }
    drain_children(&mut reader, &mut seen);

    for (id, mut indexes) in seen.into_iter().enumerate() {
        indexes.sort_unstable();
        assert!(
            indexes.iter().copied().eq(0..COUNT),
            "child {id}: indexes are not 0..{COUNT}"
        );
    }
}

#[test]
fn stale_object() {
    let p = prefix("stale_object");
    let cfg = || Config {
        ring_size: 16 * KIB,
        max_record: 64,
    };
    let cpu = allowed_cpus()[0];
    pin(&[cpu]);
    let stale = Reader::create(&p, cfg()).unwrap();
    Writer::attach(&p)
        .unwrap()
        .try_push(b"stale", 1, 0)
        .unwrap();
    std::mem::forget(stale);

    let mut reader = Reader::create(&p, cfg()).unwrap();
    Writer::attach(&p)
        .unwrap()
        .try_push(b"fresh", 2, 0)
        .unwrap();
    assert_eq!(drain(&mut reader, cpu), [(2, 0, b"fresh".to_vec())]);
}
