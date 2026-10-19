#![cfg(target_os = "linux")]

use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};

use ceph_fifo::Fifo;
use ceph_proto::{Control, Retire, pack};
use ceph_ring::{Config, PushError, Reader, Writer};

const KIB: usize = 1024;

fn prefix(test: &str) -> String {
    format!("ceph-fifo-{test}-{}", std::process::id())
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

fn setup(test: &str) -> (Writer, Fifo, Sender<Control>, Receiver<Retire>) {
    let prefix = prefix(test);
    let reader = Reader::create(
        &prefix,
        Config {
            ring_size: 64 * KIB,
            max_record: 256,
        },
    )
    .unwrap();
    let writer = Writer::attach(&prefix).unwrap();
    let (control_tx, control_rx) = mpsc::channel();
    let (retire_tx, retire_rx) = mpsc::channel();
    let fifo = Fifo::new(reader, 8, control_rx, retire_tx);
    (writer, fifo, control_tx, retire_rx)
}

fn next_message(fifo: &mut Fifo) -> Option<(u64, u64, Vec<u8>)> {
    fifo.next()
        .map(|message| (message.stream(), message.seq(), message.payload().to_vec()))
}

#[test]
fn reorders_across_rings() {
    let (writer, mut fifo, control, _kicks) = setup("reorders");
    let cpus = allowed_cpus();
    assert!(cpus.len() >= 2);
    let (a, b) = (cpus[0], cpus[1]);
    let stream = pack(1, 7);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();

    pin(&[a]);
    writer.try_push(b"one", stream, 1).unwrap();
    pin(&[b]);
    writer.try_push(b"zero", stream, 0).unwrap();

    assert_eq!(next_message(&mut fifo), Some((stream, 0, b"zero".to_vec())));
    assert_eq!(next_message(&mut fifo), Some((stream, 1, b"one".to_vec())));
    assert_eq!(next_message(&mut fifo), None);

    pin(&[a]);
    writer.try_push(b"two", stream, 2).unwrap();
    writer.try_push(b"three", stream, 3).unwrap();
    assert_eq!(next_message(&mut fifo), Some((stream, 2, b"two".to_vec())));
    assert_eq!(next_message(&mut fifo), Some((stream, 3, b"three".to_vec())));
}

#[test]
fn stall_kicks_the_stream() {
    let (writer, mut fifo, control, kicks) = setup("stall");
    let cpu = allowed_cpus()[0];
    let stream = pack(1, 7);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();
    pin(&[cpu]);
    writer.try_push(b"one", stream, 1).unwrap();

    assert_eq!(next_message(&mut fifo), None);
    assert_eq!(next_message(&mut fifo), None);
    assert_eq!(
        kicks.try_recv(),
        Ok(Retire::Kicked { slot: 1, uid: 7 })
    );

    writer.try_push(b"two", stream, 2).unwrap();
    let second = pack(2, 9);
    control.send(Control::Open { slot: 2, uid: 9 }).unwrap();
    writer.try_push(b"zero", second, 0).unwrap();
    assert_eq!(
        next_message(&mut fifo),
        Some((second, 0, b"zero".to_vec()))
    );
    assert_eq!(next_message(&mut fifo), None);
    assert_eq!(kicks.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn discard_backlog_is_not_a_stall() {
    let (writer, mut fifo, control, kicks) = setup("discard-backlog");
    let cpus = allowed_cpus();
    assert!(cpus.len() >= 2);
    let (a, b) = (cpus[0], cpus[1]);
    let discarded = pack(1, 7);
    let stream = pack(2, 9);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();
    control.send(Control::Open { slot: 2, uid: 9 }).unwrap();

    pin(&[b]);
    for seq in 0..128 {
        writer.try_push(b"discard", discarded, seq).unwrap();
    }
    writer.try_push(b"zero", stream, 0).unwrap();
    pin(&[a]);
    writer.try_push(b"one", stream, 1).unwrap();

    control.send(Control::Close { slot: 1, uid: 7 }).unwrap();

    assert_eq!(next_message(&mut fifo), Some((stream, 0, b"zero".to_vec())));
    assert_eq!(next_message(&mut fifo), Some((stream, 1, b"one".to_vec())));
    assert_eq!(next_message(&mut fifo), None);
    assert_eq!(kicks.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn held_message_blocks_publish() {
    let (writer, mut fifo, control, _kicks) = setup("held-message");
    let cpu = allowed_cpus()[0];
    let stream = pack(1, 7);
    let payload = vec![0x5a; 200];
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();
    pin(&[cpu]);

    let mut seq = 0;
    loop {
        match writer.try_push(&payload, stream, seq) {
            Ok(()) => seq += 1,
            Err(PushError::Full) => break,
            Err(error) => panic!("unexpected push error: {error:?}"),
        }
    }

    let m = fifo.next().unwrap();
    assert_eq!(writer.try_push(&payload, stream, seq), Err(PushError::Full));
    assert_eq!(m.payload(), payload);
    drop(m);

    while fifo.next().is_some() {}
    assert_eq!(writer.try_push(&payload, stream, seq), Ok(()));
}

#[test]
fn old_uid_is_discarded() {
    let (writer, mut fifo, control, _kicks) = setup("old-uid");
    let cpus = allowed_cpus();
    assert!(cpus.len() >= 2);
    let old_stream = pack(1, 7);
    let new_stream = pack(1, 8);
    let late_stream = pack(2, 9);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();
    pin(&[cpus[0]]);
    writer.try_push(b"old zero", old_stream, 0).unwrap();
    pin(&[cpus[1]]);
    writer.try_push(b"late open", late_stream, 0).unwrap();
    assert_eq!(
        next_message(&mut fifo),
        Some((old_stream, 0, b"old zero".to_vec()))
    );
    control.send(Control::Open { slot: 2, uid: 9 }).unwrap();
    assert_eq!(
        next_message(&mut fifo),
        Some((late_stream, 0, b"late open".to_vec()))
    );

    control.send(Control::Close { slot: 1, uid: 7 }).unwrap();
    control.send(Control::Open { slot: 1, uid: 8 }).unwrap();
    control.send(Control::Close { slot: 1, uid: 7 }).unwrap();
    // Control messages are drained at a sweep boundary or when a front record's uid does not
    // match its slot. The new stream's record goes first so that rule fires; the old stream's
    // record behind it must then be discarded.
    pin(&[cpus[0]]);
    writer.try_push(b"new zero", new_stream, 0).unwrap();
    writer.try_push(b"old one", old_stream, 1).unwrap();

    assert_eq!(
        next_message(&mut fifo),
        Some((new_stream, 0, b"new zero".to_vec()))
    );
    assert_eq!(next_message(&mut fifo), None);
}

#[test]
fn end_of_stream_retires() {
    let (writer, mut fifo, control, retire) = setup("end-of-stream");
    let cpu = allowed_cpus()[0];
    let stream = pack(1, 7);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();
    pin(&[cpu]);
    writer.try_push(b"zero", stream, 0).unwrap();
    writer.try_finish(stream, 1).unwrap();

    let first = fifo.next().unwrap();
    assert_eq!((first.stream(), first.seq()), (stream, 0));
    assert!(!first.end());
    assert_eq!(first.payload(), b"zero");
    drop(first);

    let last = fifo.next().unwrap();
    assert_eq!((last.stream(), last.seq()), (stream, 1));
    assert!(last.end());
    assert!(last.payload().is_empty());
    assert_eq!(retire.try_recv(), Err(TryRecvError::Empty));
    drop(last);
    assert_eq!(
        retire.try_recv(),
        Ok(Retire::Finished { slot: 1, uid: 7 })
    );
    assert!(fifo.next().is_none());

    writer.try_push(b"after", stream, 2).unwrap();
    assert!(fifo.next().is_none());
}
