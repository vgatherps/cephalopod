#![cfg(target_os = "linux")]

use std::ffi::CString;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use ceph_ring::{Config, Reader, Writer};
use ceph_rseq::record_total;

const HUGETLBFS_MAGIC: libc::c_long = 0x9584_58f6;
const HUGE_PAGE: usize = 2 * 1024 * 1024;
const STAGGER: u64 = 128;

fn statfs(path: &Path) -> std::io::Result<libc::statfs> {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stat` points to writable storage for one value.
    if unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a successful `statfs` initialized `stat`.
    Ok(unsafe { stat.assume_init() })
}

fn fstatfs(path: &Path) -> std::io::Result<libc::statfs> {
    let file = std::fs::File::open(path)?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `file` is open and `stat` points to writable storage for one value.
    if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a successful `fstatfs` initialized `stat`.
    Ok(unsafe { stat.assume_init() })
}

fn huge_pages_free() -> Option<usize> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("HugePages_Free:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn allowed_cpu_past_first_two() -> Option<usize> {
    // SAFETY: `set` is writable storage for the affinity mask supplied to the syscall.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return None;
        }
        (2..libc::CPU_SETSIZE as usize).find(|&cpu| libc::CPU_ISSET(cpu, &set))
    }
}

fn pin(cpu: usize) {
    // SAFETY: `set` is initialized before the affinity syscall reads it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}

#[test]
fn huge_page_ring_wraps_and_unlinks() {
    let dir = Path::new("/dev/hugepages");
    let Ok(mount) = statfs(dir) else {
        eprintln!("skipping: /dev/hugepages is not mounted");
        return;
    };
    if mount.f_type != HUGETLBFS_MAGIC {
        eprintln!("skipping: /dev/hugepages is not hugetlbfs");
        return;
    }
    let free = huge_pages_free().expect("HugePages_Free is present in /proc/meminfo");
    if free < 20 {
        eprintln!("skipping: only {free} huge pages are free");
        return;
    }
    let cpu = allowed_cpu_past_first_two()
        .expect("huge-page wrap test requires an allowed CPU numbered at least 2");
    pin(cpu);

    let mut reader = Reader::create(Config {
        ring_size: HUGE_PAGE,
        max_record: 4096,
        dir: dir.into(),
    })
    .unwrap();
    let path = reader.name().to_owned();
    assert_eq!(fstatfs(Path::new(&path)).unwrap().f_bsize as usize, HUGE_PAGE);
    let writer = Writer::attach(&path).unwrap();

    let total = record_total(4096);
    let mut position = cpu as u64 * STAGGER;
    let mut expected = Vec::new();
    loop {
        let seq = expected.len() as u64;
        let mut payload = vec![(seq % 251) as u8; 4096];
        payload[..8].copy_from_slice(&seq.to_le_bytes());
        writer.try_push(&payload, 7, seq).unwrap();
        expected.push(payload);
        let next = position + total;
        if position < HUGE_PAGE as u64 && next > HUGE_PAGE as u64 {
            break;
        }
        position = next;
    }

    for (seq, payload) in expected.iter().enumerate() {
        let header = reader.front(cpu).expect("record is committed");
        assert_eq!((header.stream, header.seq), (7, seq as u64));
        assert_eq!(reader.view(cpu).payload, payload);
        reader.advance(cpu);
    }
    assert!(reader.front(cpu).is_none());
    reader.publish();
    drop(reader);
    assert!(!Path::new(&path).exists());

    let before = listing(dir);
    let result = std::panic::catch_unwind(|| {
        let _ = Reader::create(Config {
            ring_size: HUGE_PAGE / 2,
            max_record: 4096,
            dir: dir.into(),
        });
    });
    assert!(result.is_err());
    assert_eq!(listing(dir), before);
}

fn listing(dir: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    names
}
