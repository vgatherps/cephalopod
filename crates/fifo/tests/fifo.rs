use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};

use ceph_fifo::Fifo;
use ceph_proto::{Control, Retire, pack};
use ceph_ring::{Config, END_OF_STREAM, PushError, Reader};

const KIB: usize = 1024;

fn reader(ring_size: usize) -> Reader {
    Reader::create(Config {
        ring_size,
        max_record: 256,
    })
    .unwrap()
}

fn setup(reader: Reader, max_streams: u32) -> (Fifo, Sender<Control>, Receiver<Retire>) {
    let (control_tx, control_rx) = mpsc::channel();
    let (retire_tx, retire_rx) = mpsc::channel();
    (
        Fifo::new(reader, max_streams, control_rx, retire_tx),
        control_tx,
        retire_rx,
    )
}

fn next_message(fifo: &mut Fifo) -> Option<(u64, u64, Vec<u8>)> {
    fifo.next_message()
        .map(|message| (message.stream(), message.seq(), message.payload().to_vec()))
}

#[test]
fn reorders_across_rings() {
    let reader = reader(64 * KIB);
    let stream = pack(1, 7);
    unsafe { reader.write_unchecked(0, stream, 1, b"one") }.unwrap();
    unsafe { reader.write_unchecked(1, stream, 0, b"zero") }.unwrap();
    let (mut fifo, control, _kicks) = setup(reader, 8);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();

    assert_eq!(next_message(&mut fifo), Some((stream, 0, b"zero".to_vec())));
    assert_eq!(next_message(&mut fifo), Some((stream, 1, b"one".to_vec())));
    assert_eq!(next_message(&mut fifo), None);

    unsafe { fifo.reader().write_unchecked(0, stream, 2, b"two") }.unwrap();
    unsafe { fifo.reader().write_unchecked(0, stream, 3, b"three") }.unwrap();
    assert_eq!(next_message(&mut fifo), Some((stream, 2, b"two".to_vec())));
    assert_eq!(
        next_message(&mut fifo),
        Some((stream, 3, b"three".to_vec()))
    );
}

#[test]
fn kick_unblocks_the_ring() {
    let reader = reader(64 * KIB);
    let stuck = pack(1, 7);
    let unblocked = pack(2, 9);
    unsafe { reader.write_unchecked(0, stuck, 1, b"one") }.unwrap();
    unsafe { reader.write_unchecked(0, unblocked, 0, b"zero") }.unwrap();
    let (mut fifo, control, kicks) = setup(reader, 8);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();
    control.send(Control::Open { slot: 2, uid: 9 }).unwrap();

    assert_eq!(
        next_message(&mut fifo),
        Some((unblocked, 0, b"zero".to_vec()))
    );
    assert_eq!(kicks.try_recv(), Ok(Retire::Kicked { slot: 1, uid: 7 }));
    assert_eq!(next_message(&mut fifo), None);
    assert_eq!(kicks.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn kick_finds_the_gap() {
    let reader = reader(64 * KIB);
    let s = pack(1, 7);
    let t = pack(2, 9);
    unsafe { reader.write_unchecked(1, t, 1, b"t one") }.unwrap();
    unsafe { reader.write_unchecked(1, s, 0, b"s zero") }.unwrap();
    unsafe { reader.write_unchecked(0, s, 1, b"s one") }.unwrap();
    let (mut fifo, control, retire) = setup(reader, 8);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();
    control.send(Control::Open { slot: 2, uid: 9 }).unwrap();

    let mut messages = Vec::new();
    while let Some(message) = fifo.next_message() {
        messages.push((message.stream(), message.seq()));
    }

    assert_eq!(messages, vec![(s, 0), (s, 1)]);
    assert_eq!(retire.try_recv(), Ok(Retire::Kicked { slot: 2, uid: 9 }));
    assert_ne!(retire.try_recv(), Ok(Retire::Kicked { slot: 1, uid: 7 }));
}

#[test]
fn discard_backlog_is_not_a_stall() {
    let reader = reader(64 * KIB);
    let discarded = pack(1, 7);
    let stream = pack(2, 9);
    for seq in 0..128 {
        unsafe { reader.write_unchecked(1, discarded, seq, b"discard") }.unwrap();
    }
    unsafe { reader.write_unchecked(1, stream, 0, b"zero") }.unwrap();
    unsafe { reader.write_unchecked(0, stream, 1, b"one") }.unwrap();
    let (mut fifo, control, kicks) = setup(reader, 8);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();
    control.send(Control::Open { slot: 2, uid: 9 }).unwrap();
    control.send(Control::Close { slot: 1, uid: 7 }).unwrap();

    assert_eq!(next_message(&mut fifo), Some((stream, 0, b"zero".to_vec())));
    assert_eq!(next_message(&mut fifo), Some((stream, 1, b"one".to_vec())));
    assert_eq!(next_message(&mut fifo), None);
    assert_eq!(kicks.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn held_message_blocks_publish() {
    let reader = reader(64 * KIB);
    let stream = pack(1, 7);
    let payload = vec![0x5a; 200];
    let mut seq = 0;
    loop {
        match unsafe { reader.write_unchecked(0, stream, seq, &payload) } {
            Ok(()) => seq += 1,
            Err(PushError::Full) => break,
            Err(error) => panic!("unexpected push error: {error:?}"),
        }
    }
    let (mut fifo, control, _kicks) = setup(reader, 8);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();

    let message = fifo.next_message().unwrap();
    assert_eq!(message.payload(), payload);
    drop(message);
    assert_eq!(
        unsafe { fifo.reader().write_unchecked(0, stream, seq, &payload) },
        Err(PushError::Full)
    );

    while fifo.next_message().is_some() {}
    assert_eq!(
        unsafe { fifo.reader().write_unchecked(0, stream, seq, &payload) },
        Ok(())
    );
}

#[test]
fn old_uid_is_discarded() {
    let reader = reader(64 * KIB);
    let old_stream = pack(1, 7);
    let new_stream = pack(1, 8);
    let late_stream = pack(2, 9);
    unsafe { reader.write_unchecked(0, old_stream, 0, b"old zero") }.unwrap();
    unsafe { reader.write_unchecked(1, late_stream, 0, b"late open") }.unwrap();
    let (mut fifo, control, _kicks) = setup(reader, 8);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();

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
    // The new stream's record goes first so control is drained before the old record behind it.
    unsafe { fifo.reader().write_unchecked(0, new_stream, 0, b"new zero") }.unwrap();
    unsafe { fifo.reader().write_unchecked(0, old_stream, 1, b"old one") }.unwrap();

    assert_eq!(
        next_message(&mut fifo),
        Some((new_stream, 0, b"new zero".to_vec()))
    );
    assert_eq!(next_message(&mut fifo), None);
}

#[test]
fn end_of_stream_retires() {
    let reader = reader(64 * KIB);
    let stream = pack(1, 7);
    unsafe { reader.write_unchecked(0, stream, 0, b"zero") }.unwrap();
    unsafe { reader.write_unchecked(0, stream, 1 | END_OF_STREAM, b"") }.unwrap();
    let (mut fifo, control, retire) = setup(reader, 8);
    control.send(Control::Open { slot: 1, uid: 7 }).unwrap();

    let first = fifo.next_message().unwrap();
    assert_eq!((first.stream(), first.seq()), (stream, 0));
    assert!(!first.end());
    assert_eq!(first.payload(), b"zero");
    drop(first);

    let last = fifo.next_message().unwrap();
    assert_eq!((last.stream(), last.seq()), (stream, 1));
    assert!(last.end());
    assert!(last.payload().is_empty());
    assert_eq!(retire.try_recv(), Err(TryRecvError::Empty));
    drop(last);
    assert_eq!(retire.try_recv(), Ok(Retire::Finished { slot: 1, uid: 7 }));
    assert!(fifo.next_message().is_none());

    unsafe { fifo.reader().write_unchecked(0, stream, 2, b"after") }.unwrap();
    assert!(fifo.next_message().is_none());
}
