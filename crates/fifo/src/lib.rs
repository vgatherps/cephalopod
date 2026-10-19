//! Sequencer-side ordering, stall handling, and end-of-stream delivery.
//!
//! Dropping a [`Message`] consumes its record.

use std::sync::mpsc::{Receiver, Sender};

use ceph_proto::{Control, Retire, slot, uid};
use ceph_ring::{END_OF_STREAM, Reader};

const BATCH: usize = 64;

pub struct Message<'a> {
    stream: u64,
    seq: u64,
    payload: *const [u8],
    ring: usize,
    finish: Option<Retire>,
    reader: &'a mut Reader,
    retire: &'a Sender<Retire>,
}

impl Message<'_> {
    pub fn stream(&self) -> u64 {
        self.stream
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// This is the stream's last record. Its payload is empty.
    pub fn end(&self) -> bool {
        self.finish.is_some()
    }

    pub fn payload(&self) -> &[u8] {
        // SAFETY: the slice points into the ring mapping, which stays valid while this message
        // holds the mutable reader borrow and prevents publishing its tail.
        unsafe { &*self.payload }
    }
}

impl Drop for Message<'_> {
    fn drop(&mut self) {
        self.reader.advance(self.ring);
        if let Some(retire) = self.finish {
            let _ = self.retire.send(retire);
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Slot {
    uid: u32,
    expected: u64,
    live: bool,
    stopped_since: Option<(u64, u64)>,
}

/// Records in a ring are in commit order, and a stream's sequence numbers commit in program
/// order. Therefore the earliest committed unconsumed record is always at the front of its ring
/// and is deliverable or discardable. This is why stopping on a ring cannot deadlock and why the
/// global two-sweep rule never kicks a correct stream.
pub struct Fifo {
    reader: Reader,
    slots: Vec<Slot>,
    ready: Vec<usize>,
    pos: usize,
    batch: usize,
    stopped: bool,
    delivered: bool,
    empty_sweeps: u32,
    sweep: u64,
    // A missing predecessor can sit behind every ring's maximum number of 32-byte records,
    // drained 64 per sweep. Add two sweeps per ring for stopped-ring chaining and the global rule.
    stall_bound: u64,
    control: Receiver<Control>,
    retire: Sender<Retire>,
}

impl Fifo {
    /// `max_streams` must equal the value passed to `Listener::bind`.
    pub fn new(
        reader: Reader,
        max_streams: u32,
        control: Receiver<Control>,
        retire: Sender<Retire>,
    ) -> Fifo {
        let nr_rings = reader.nr_rings() as u64;
        let stall_bound = nr_rings * (reader.ring_size() as u64 / 32 / BATCH as u64)
            + 2 * nr_rings;
        Fifo {
            reader,
            slots: vec![Slot::default(); max_streams as usize],
            ready: Vec::new(),
            pos: 0,
            batch: 0,
            stopped: false,
            delivered: false,
            empty_sweeps: 0,
            sweep: 0,
            stall_bound,
            control,
            retire,
        }
    }

    /// Tests only: the underlying reader, for writing records into rings mid-test.
    #[cfg(feature = "test-util")]
    pub fn reader(&self) -> &Reader {
        &self.reader
    }

    /// Returns the next deliverable message, or `None` if none is available now. The message
    /// borrows the `Fifo`; dropping it consumes the record, and `publish` happens at the next
    /// sweep boundary. Up to 64 consecutive deliveries from one ring are returned before
    /// advancing to the next ring; discarded records do not count. A stalled stream is dropped
    /// after two sweeps in which nothing at all was delivered, or after it has been stuck for
    /// longer than every other ring could possibly take to drain while other streams delivered.
    pub fn next(&mut self) -> Option<Message<'_>> {
        loop {
            if self.pos == self.ready.len() {
                self.finish_sweep();
                self.start_sweep();
                if self.ready.is_empty() {
                    return None;
                }
            }

            let ring = self.ready[self.pos];
            let Some(header) = self.reader.front(ring) else {
                self.pos += 1;
                self.batch = 0;
                continue;
            };
            let index = slot(header.stream) as usize;
            if index >= self.slots.len() {
                self.reader.advance(ring);
                continue;
            }

            if uid(header.stream) != self.slots[index].uid {
                self.drain_control();
                if uid(header.stream) != self.slots[index].uid {
                    self.reader.advance(ring);
                    continue;
                }
            }

            let seq = header.seq & !END_OF_STREAM;
            let end = header.seq & END_OF_STREAM != 0;
            let state = self.slots[index];
            if !state.live || seq < state.expected {
                self.reader.advance(ring);
                continue;
            }
            if seq > state.expected {
                if self.slots[index]
                    .stopped_since
                    .map(|(_, expected)| expected)
                    != Some(state.expected)
                {
                    self.slots[index].stopped_since = Some((self.sweep, state.expected));
                }
                self.stopped = true;
                self.pos += 1;
                self.batch = 0;
                continue;
            }

            self.slots[index].expected += 1;
            self.slots[index].stopped_since = None;
            let finish = end.then(|| {
                self.slots[index].live = false;
                Retire::Finished {
                    slot: index as u32,
                    uid: state.uid,
                }
            });
            self.delivered = true;
            self.count_record();
            let (stream, payload) = {
                let record = self.reader.view(ring);
                (
                    record.stream,
                    std::ptr::slice_from_raw_parts(record.payload.as_ptr(), record.payload.len()),
                )
            };
            return Some(Message {
                stream,
                seq,
                payload,
                ring,
                finish,
                reader: &mut self.reader,
                retire: &self.retire,
            });
        }
    }

    fn count_record(&mut self) {
        self.batch += 1;
        if self.batch == BATCH {
            self.pos += 1;
            self.batch = 0;
        }
    }

    fn start_sweep(&mut self) {
        self.sweep += 1;
        self.drain_control();
        self.reader.publish();
        self.ready.clear();
        self.reader.poll(&mut self.ready);
        self.pos = 0;
        self.batch = 0;
        self.stopped = false;
        self.delivered = false;
    }

    fn finish_sweep(&mut self) {
        if self.stopped && !self.delivered {
            self.empty_sweeps += 1;
        } else {
            self.empty_sweeps = 0;
        }
        if self.empty_sweeps == 2 {
            self.kick_first_stopped();
            self.empty_sweeps = 0;
        }
        self.kick_first_expired_stopped();
    }

    fn drain_control(&mut self) {
        while let Ok(message) = self.control.try_recv() {
            match message {
                Control::Open { slot, uid } => {
                    assert!(
                        (slot as usize) < self.slots.len(),
                        "control thread's max_streams exceeds the fifo's"
                    );
                    self.slots[slot as usize] = Slot {
                        uid,
                        expected: 0,
                        live: true,
                        stopped_since: None,
                    };
                }
                Control::Close { slot, uid } => {
                    let state = &mut self.slots[slot as usize];
                    if state.uid == uid {
                        state.live = false;
                    }
                }
            }
        }
    }

    fn kick_first_stopped(&mut self) {
        for &ring in &self.ready {
            let Some(header) = self.reader.front(ring) else {
                continue;
            };
            let index = slot(header.stream) as usize;
            let Some(state) = self.slots.get(index).copied() else {
                continue;
            };
            let stream_uid = uid(header.stream);
            let seq = header.seq & !END_OF_STREAM;
            if state.live
                && state.uid == stream_uid
                && seq > state.expected
                && state
                    .stopped_since
                    .is_some_and(|(since, expected)| {
                        expected == state.expected && self.sweep - since >= 2
                    })
            {
                self.kick(index, stream_uid);
                break;
            }
        }
    }

    fn kick_first_expired_stopped(&mut self) {
        for &ring in &self.ready {
            let Some(header) = self.reader.front(ring) else {
                continue;
            };
            let index = slot(header.stream) as usize;
            let Some(state) = self.slots.get(index).copied() else {
                continue;
            };
            let stream_uid = uid(header.stream);
            let seq = header.seq & !END_OF_STREAM;
            if state.live
                && state.uid == stream_uid
                && seq > state.expected
                && state
                    .stopped_since
                    .is_some_and(|(since, expected)| {
                        expected == state.expected && self.sweep - since > self.stall_bound
                    })
            {
                self.kick(index, stream_uid);
                break;
            }
        }
    }

    fn kick(&mut self, index: usize, stream_uid: u32) {
        self.slots[index].live = false;
        let _ = self.retire.send(Retire::Kicked {
            slot: index as u32,
            uid: stream_uid,
        });
    }

    #[cfg(test)]
    fn sweep_once(&mut self, between: impl FnOnce(&Reader)) {
        self.start_sweep();
        between(&self.reader);
        while self.pos != self.ready.len() {
            let ring = self.ready[self.pos];
            let Some(header) = self.reader.front(ring) else {
                self.pos += 1;
                self.batch = 0;
                continue;
            };
            let index = slot(header.stream) as usize;
            if index >= self.slots.len() {
                self.reader.advance(ring);
                continue;
            }

            if uid(header.stream) != self.slots[index].uid {
                self.drain_control();
                if uid(header.stream) != self.slots[index].uid {
                    self.reader.advance(ring);
                    continue;
                }
            }

            let seq = header.seq & !END_OF_STREAM;
            let end = header.seq & END_OF_STREAM != 0;
            let state = self.slots[index];
            if !state.live || seq < state.expected {
                self.reader.advance(ring);
                continue;
            }
            if seq > state.expected {
                if self.slots[index]
                    .stopped_since
                    .map(|(_, expected)| expected)
                    != Some(state.expected)
                {
                    self.slots[index].stopped_since = Some((self.sweep, state.expected));
                }
                self.stopped = true;
                self.pos += 1;
                self.batch = 0;
                continue;
            }

            self.slots[index].expected += 1;
            self.slots[index].stopped_since = None;
            if end {
                self.slots[index].live = false;
                let _ = self.retire.send(Retire::Finished {
                    slot: index as u32,
                    uid: state.uid,
                });
            }
            self.delivered = true;
            self.reader.advance(ring);
            self.count_record();
        }
        self.finish_sweep();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceph_proto::pack;
    use ceph_ring::Config;
    use std::sync::mpsc;

    #[test]
    fn empty_stopped_sweeps_do_not_kick() {
        // SAFETY: sysconf reads a process-wide constant.
        let ring_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let reader = Reader::create(Config {
            ring_size,
            max_record: 16,
        })
        .unwrap();
        unsafe { reader.write_unchecked(0, pack(0, 7), 1, b"one") }.unwrap();
        let (control_tx, control_rx) = mpsc::channel();
        let (retire_tx, retire_rx) = mpsc::channel();
        let mut fifo = Fifo::new(reader, 1, control_rx, retire_tx);
        control_tx.send(Control::Open { slot: 0, uid: 7 }).unwrap();

        fifo.start_sweep();
        let header = fifo.reader.front(fifo.ready[0]).unwrap();
        assert_eq!(header.seq, 1);
        assert!(header.seq > fifo.slots[0].expected);
        fifo.stopped = true;
        fifo.slots[0].stopped_since = Some((fifo.sweep - 1, 0));
        fifo.pos = fifo.ready.len();
        fifo.finish_sweep();
        fifo.start_sweep();
        assert_eq!(fifo.empty_sweeps, 1);
        assert_eq!(retire_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

        let header = fifo.reader.front(fifo.ready[0]).unwrap();
        assert_eq!(header.seq, 1);
        assert!(header.seq > fifo.slots[0].expected);
        fifo.stopped = true;
        fifo.pos = fifo.ready.len();
        fifo.finish_sweep();
        fifo.start_sweep();
        assert_eq!(fifo.empty_sweeps, 0);
        assert_eq!(
            retire_rx.try_recv(),
            Ok(Retire::Kicked { slot: 0, uid: 7 })
        );
    }

    #[test]
    fn stall_history_is_per_stream() {
        // SAFETY: sysconf reads a process-wide constant.
        let ring_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let reader = Reader::create(Config {
            ring_size,
            max_record: 16,
        })
        .unwrap();
        let (control_tx, control_rx) = mpsc::channel();
        let (retire_tx, retire_rx) = mpsc::channel();
        let stream_a = pack(1, 7);
        let stream_b = pack(2, 9);

        control_tx.send(Control::Open { slot: 1, uid: 7 }).unwrap();
        control_tx.send(Control::Open { slot: 2, uid: 9 }).unwrap();
        unsafe { reader.write_unchecked(0, stream_a, 1, b"a one") }.unwrap();
        let mut fifo = Fifo::new(reader, 3, control_rx, retire_tx);

        fifo.sweep_once(|_| {});
        assert_eq!(fifo.empty_sweeps, 1);

        control_tx.send(Control::Close { slot: 1, uid: 7 }).unwrap();
        unsafe { fifo.reader().write_unchecked(0, stream_b, 1, b"b one") }
            .unwrap();
        fifo.sweep_once(|reader| {
            unsafe { reader.write_unchecked(1, stream_b, 0, b"b zero") }.unwrap();
        });
        assert_eq!(retire_rx.try_recv(), Err(mpsc::TryRecvError::Empty));

        fifo.sweep_once(|_| {});
        assert_eq!(fifo.slots[2].expected, 1);
        fifo.sweep_once(|_| {});
        assert_eq!(fifo.slots[2].expected, 2);
        assert_eq!(retire_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    }
}
