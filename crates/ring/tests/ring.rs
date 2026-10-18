use std::os::unix::process::ExitStatusExt;
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
    for &k in allowed_cpus().iter().take(4) {
        pin(&[k]);
        let (mut pushed, mut next) = (0, 0);
        let mut check = |r: &[u8]| {
            assert_eq!(r, indexed_record(next), "ring {k} record {next}");
            next += 1;
            true
        };
        while pushed < 5000 {
            match writer.try_push(&indexed_record(pushed)) {
                Ok(()) => pushed += 1,
                Err(PushError::Full) => assert!(reader.read(k, &mut check) > 0),
                Err(e) => panic!("{e:?}"),
            }
        }
        reader.read(k, &mut check);
        assert_eq!(next, pushed);
        for other in (0..reader.nr_rings()).filter(|&r| r != k) {
            assert_eq!(
                reader.read(other, |_| true),
                0,
                "ring {other} got a record from cpu {k}"
            );
        }
    }
}

#[test]
fn full_boundary_and_oversize() {
    let p = prefix("full_boundary_and_oversize");
    let max = 16 * KIB - 8;
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
    for _ in 0..16 {
        assert_eq!(writer.try_push(&rec), Ok(()));
    }
    assert_eq!(writer.try_push(&rec), Err(PushError::Full));
    let mut first = true;
    assert_eq!(
        reader.read(cpu, |_| std::mem::replace(&mut first, false)),
        1
    );
    assert_eq!(writer.try_push(&rec), Ok(()));
    assert_eq!(writer.try_push(&rec), Err(PushError::Full));
    assert_eq!(reader.read(cpu, |r| r == rec), 16);

    let big: Vec<u8> = (0..max).map(|j| (j * 13) as u8).collect();
    assert_eq!(writer.try_push(&big), Ok(()));
    assert_eq!(writer.try_push(&[]), Err(PushError::Full));
    assert_eq!(writer.try_push(&vec![0; max + 1]), Err(PushError::TooLarge));
    assert_eq!(reader.read(cpu, |r| r == big), 1);
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
    for r in [&b"A"[..], b"BB", b"CCC"] {
        writer.try_push(r).unwrap();
    }

    let mut seen = Vec::new();
    let consumed = reader.read(cpu, |r| {
        seen.push(r.to_vec());
        r != b"BB"
    });
    assert_eq!(consumed, 1);
    assert_eq!(seen, [b"A".to_vec(), b"BB".to_vec()]);

    seen.clear();
    let consumed = reader.read(cpu, |r| {
        seen.push(r.to_vec());
        true
    });
    assert_eq!(consumed, 2);
    assert_eq!(seen, [b"BB".to_vec(), b"CCC".to_vec()]);
}

fn child_record(id: u32, index: u64) -> Vec<u8> {
    let mut r = Vec::new();
    r.extend(id.to_le_bytes());
    r.extend(index.to_le_bytes());
    r.extend((0..index % 200).map(|j| (id as u64 * 31 + index + j) as u8));
    r
}

/// Child role of `multi_process`: writes indexes 0.. until `count` (0 = until killed). All children
/// share the first two allowed CPUs, so processes interleave in the same rings and migrate.
fn run_child(spec: &str) {
    let [p, id, count] = spec.split(' ').collect::<Vec<_>>()[..] else {
        panic!("{spec}")
    };
    let (id, count): (u32, u64) = (id.parse().unwrap(), count.parse().unwrap());
    let cpus = allowed_cpus();
    pin(&cpus[..cpus.len().min(2)]);
    let writer = Writer::attach(p).unwrap();
    let mut index = 0;
    while count == 0 || index < count {
        match writer.try_push(&child_record(id, index)) {
            Ok(()) => index += 1,
            Err(PushError::Full) => std::thread::yield_now(),
            Err(e) => panic!("{e:?}"),
        }
    }
}

fn drain(reader: &mut Reader, seen: &mut [Vec<u64>]) {
    for ring in 0..reader.nr_rings() {
        reader.read(ring, |r| {
            let id = u32::from_le_bytes(r[..4].try_into().unwrap());
            let index = u64::from_le_bytes(r[4..12].try_into().unwrap());
            assert_eq!(r, child_record(id, index), "child {id} index {index}");
            seen[id as usize].push(index);
            true
        });
    }
}

#[test]
fn multi_process() {
    if let Ok(spec) = std::env::var(CHILD_ENV) {
        return run_child(&spec);
    }
    const FINISHED: usize = 3;
    const KILLED: usize = 2;
    const COUNT: u64 = 20_000;
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
    let mut children: Vec<_> = (0..FINISHED + KILLED)
        .map(|id| {
            let count = if id < FINISHED { COUNT } else { 0 };
            Command::new(&exe)
                .args(["multi_process", "--exact", "--nocapture"])
                .env(CHILD_ENV, format!("{p} {id} {count}"))
                .stdout(Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();

    let mut seen = vec![Vec::new(); FINISHED + KILLED];
    let deadline = Instant::now() + Duration::from_secs(60);
    while !(seen[..FINISHED].iter().all(|s| s.len() as u64 >= COUNT)
        && seen[FINISHED..].iter().all(|s| s.len() >= 1000))
    {
        assert!(Instant::now() < deadline, "children stalled");
        drain(&mut reader, &mut seen);
    }
    for (id, child) in children.iter_mut().enumerate() {
        if id >= FINISHED {
            child.kill().unwrap();
        }
        let status = child.wait().unwrap();
        if id < FINISHED {
            assert!(status.success(), "child {id}: {status}");
        } else {
            assert_eq!(status.signal(), Some(libc::SIGKILL), "child {id}: {status}");
        }
    }
    drain(&mut reader, &mut seen);

    for (id, mut indexes) in seen.into_iter().enumerate() {
        indexes.sort_unstable();
        let n = if id < FINISHED {
            COUNT
        } else {
            indexes.len() as u64
        };
        assert!(
            indexes.iter().copied().eq(0..n),
            "child {id}: indexes are not 0..{n}"
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
    Writer::attach(&p).unwrap().try_push(b"stale").unwrap();
    std::mem::forget(stale);

    let mut reader = Reader::create(&p, cfg()).unwrap();
    Writer::attach(&p).unwrap().try_push(b"fresh").unwrap();
    let mut seen = Vec::new();
    let consumed = reader.read(cpu, |r| {
        seen.push(r.to_vec());
        true
    });
    assert_eq!(consumed, 1);
    assert_eq!(seen, [b"fresh".to_vec()]);
}
