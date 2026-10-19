use ceph_control::{Listener, connect};
use ceph_proto::{Control, Retire, pack};
use std::fs;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);

fn socket_path() -> PathBuf {
    let number = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!("cphc{}{}", std::process::id(), number));
    fs::create_dir(&directory).unwrap();
    directory.join("s")
}

#[test]
fn connect_opens_and_close_closes() {
    let path = socket_path();
    let (control_tx, control_rx) = mpsc::channel();
    let (kick_tx, kick_rx) = mpsc::channel();
    let listener = Listener::bind(&path, "ceph-test", 2, control_tx, kick_rx).unwrap();
    let thread = thread::spawn(move || listener.run().unwrap());

    let dropped = UnixStream::connect(&path).unwrap();
    drop(dropped);
    assert_eq!(
        control_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Control::Open { slot: 0, uid: 1 }
    );
    assert_eq!(
        control_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Control::Close { slot: 0, uid: 1 }
    );

    let first = connect(&path).unwrap();
    assert_eq!(first.stream, pack(0, 2));
    assert_eq!(first.prefix, "ceph-test");
    assert_eq!(
        control_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Control::Open { slot: 0, uid: 2 }
    );

    let second = connect(&path).unwrap();
    assert_eq!(second.stream, pack(1, 3));
    assert_eq!(
        control_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Control::Open { slot: 1, uid: 3 }
    );

    drop(first);
    assert_eq!(
        control_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Control::Close { slot: 0, uid: 2 }
    );

    let third = connect(&path).unwrap();
    assert_eq!(third.stream, pack(0, 4));
    assert_eq!(
        control_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Control::Open { slot: 0, uid: 4 }
    );

    drop(second);
    drop(third);
    drop(control_rx);
    drop(kick_tx);
    thread.join().unwrap();
    fs::remove_file(&path).unwrap();
    fs::remove_dir(path.parent().unwrap()).unwrap();
}

#[test]
fn kick_closes_the_player() {
    let path = socket_path();
    let (control_tx, control_rx) = mpsc::channel();
    let (kick_tx, kick_rx) = mpsc::channel();
    let listener = Listener::bind(&path, "ceph-test", 1, control_tx, kick_rx).unwrap();
    let thread = thread::spawn(move || listener.run().unwrap());

    let session = connect(&path).unwrap();
    assert_eq!(
        control_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Control::Open { slot: 0, uid: 1 }
    );
    kick_tx
        .send(Retire::Kicked { slot: 0, uid: 1 })
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while !session.closed() && Instant::now() < deadline {
        thread::yield_now();
    }
    assert!(session.closed());
    assert_eq!(
        control_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Control::Close { slot: 0, uid: 1 }
    );

    drop(control_rx);
    drop(kick_tx);
    thread.join().unwrap();
    fs::remove_file(&path).unwrap();
    fs::remove_dir(path.parent().unwrap()).unwrap();
}
