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
}

/// Where a sweep is between calls. Polling and the kick decision happen at the boundary and
/// never return a message, so they are steps, not phases.
#[derive(Clone, Copy)]
enum Phase {
    /// No sweep in progress; the next call drains control, publishes, polls.
    Idle,
    /// Inside a sweep over `ready`.
    Sweeping(Sweep),
}

/// Position inside a sweep. `pos < ready.len()` always holds.
#[derive(Clone, Copy)]
struct Sweep {
    pos: usize,
    /// Deliveries from `ready[pos]` so far; at `BATCH` the sweep moves on.
    batch: usize,
    /// Whether anything was delivered or discarded, or a ring newly stalled, this sweep.
    progress: bool,
}

/// What a ring's front record is for the stream it belongs to.
enum Front {
    Empty,   // nothing committed at the front
    Discard, // slot out of range, uid mismatch after a control drain, dead slot, seq < expected
    Stalled, // live stream, seq > expected
    Deliver { end: bool },
}

/// At kick time, every ring with committed data is stalled. The victim is a front stream whose
/// expected record is absent from every stalled ring's committed backlog.
pub struct Fifo {
    reader: Reader,
    slots: Vec<Slot>,
    ready: Vec<usize>,
    stalled: Vec<usize>,
    phase: Phase,
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
        Fifo {
            reader,
            slots: vec![Slot::default(); max_streams as usize],
            ready: Vec::new(),
            stalled: Vec::new(),
            phase: Phase::Idle,
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
    /// advancing to the next ring; discarded records do not count.
    pub fn next_message(&mut self) -> Option<Message<'_>> {
        loop {
            match self.phase {
                Phase::Idle => {
                    self.start_sweep();
                    if self.ready.is_empty() {
                        return None;
                    }
                    self.phase = Phase::Sweeping(Sweep {
                        pos: 0,
                        batch: 0,
                        progress: false,
                    });
                }
                Phase::Sweeping(sweep) => {
                    let ring = self.ready[sweep.pos];
                    match self.classify(ring) {
                        Front::Empty => {
                            self.unstall(ring);
                            self.next_ring();
                        }
                        Front::Discard => {
                            self.reader.advance(ring);
                            self.mark_progress();
                        }
                        Front::Stalled => {
                            self.stall(ring);
                            self.next_ring();
                        }
                        Front::Deliver { end } => {
                            self.unstall(ring);
                            self.count_delivery();
                            return Some(self.deliver(ring, end));
                        }
                    }
                }
            }
        }
    }

    fn classify(&mut self, ring: usize) -> Front {
        let Some(header) = self.reader.front(ring) else {
            return Front::Empty;
        };
        let index = slot(header.stream) as usize;
        if index >= self.slots.len() {
            return Front::Discard;
        }

        if uid(header.stream) != self.slots[index].uid {
            self.drain_control();
            if uid(header.stream) != self.slots[index].uid {
                return Front::Discard;
            }
        }

        let seq = header.seq & !END_OF_STREAM;
        let state = self.slots[index];
        if !state.live || seq < state.expected {
            return Front::Discard;
        }
        if seq > state.expected {
            return Front::Stalled;
        }

        Front::Deliver {
            end: header.seq & END_OF_STREAM != 0,
        }
    }

    fn stall(&mut self, ring: usize) {
        if !self.stalled.contains(&ring) {
            self.stalled.push(ring);
            self.mark_progress();
        }
    }

    fn sweep(&mut self) -> &mut Sweep {
        let Phase::Sweeping(sweep) = &mut self.phase else {
            unreachable!("not inside a sweep");
        };
        sweep
    }

    fn mark_progress(&mut self) {
        self.sweep().progress = true;
    }

    fn next_ring(&mut self) {
        let sweep = self.sweep();
        sweep.pos += 1;
        sweep.batch = 0;
        let sweep = *sweep;
        if sweep.pos == self.ready.len() {
            if !sweep.progress && !self.stalled.is_empty() {
                let (index, uid) = self.victim();
                self.kick(index, uid);
            }
            self.phase = Phase::Idle;
        }
    }

    fn count_delivery(&mut self) {
        let sweep = self.sweep();
        sweep.progress = true;
        sweep.batch += 1;
        if sweep.batch == BATCH {
            self.next_ring();
        }
    }

    fn deliver(&mut self, ring: usize, end: bool) -> Message<'_> {
        let (stream, seq, payload) = {
            let record = self.reader.view(ring);
            (
                record.stream,
                record.seq & !END_OF_STREAM,
                std::ptr::slice_from_raw_parts(record.payload.as_ptr(), record.payload.len()),
            )
        };
        let index = slot(stream) as usize;
        let state = self.slots[index];
        self.slots[index].expected += 1;
        let finish = end.then(|| {
            self.slots[index].live = false;
            Retire::Finished {
                slot: index as u32,
                uid: state.uid,
            }
        });
        Message {
            stream,
            seq,
            payload,
            ring,
            finish,
            reader: &mut self.reader,
            retire: &self.retire,
        }
    }

    fn start_sweep(&mut self) {
        self.drain_control();
        self.reader.publish();
        self.ready.clear();
        self.reader.poll(&mut self.ready);
    }

    /// The front stream whose expected record is in no stalled ring's committed backlog. If
    /// every expected record is present, a producer committed its own records out of order;
    /// the first stalled stream is dropped.
    fn victim(&self) -> (usize, u32) {
        let mut wanted = Vec::with_capacity(self.stalled.len());
        for &ring in &self.stalled {
            let front = self
                .reader
                .front(ring)
                .unwrap_or_else(|| unreachable!("stalled ring {ring} has no committed front"));
            wanted.push((
                front.stream,
                self.slots[slot(front.stream) as usize].expected,
                false,
            ));
        }

        for &ring in &self.stalled {
            for header in self.reader.committed(ring) {
                let seq = header.seq & !END_OF_STREAM;
                for candidate in &mut wanted {
                    if candidate.0 == header.stream && candidate.1 == seq {
                        candidate.2 = true;
                    }
                }
            }
        }

        let stream = wanted
            .iter()
            .find(|candidate| !candidate.2)
            .unwrap_or(&wanted[0])
            .0;
        (slot(stream) as usize, uid(stream))
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

    fn unstall(&mut self, ring: usize) {
        if let Some(index) = self.stalled.iter().position(|&stalled| stalled == ring) {
            self.stalled.remove(index);
        }
    }

    fn kick(&mut self, index: usize, uid: u32) {
        self.slots[index].live = false;
        let _ = self.retire.send(Retire::Kicked {
            slot: index as u32,
            uid,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceph_proto::pack;
    use ceph_ring::Config;
    use std::sync::mpsc;

    /// A sweep's poll can see a stream's second record before its first, when the first is
    /// committed to a ring the poll already passed. The stream is stalled in that sweep and
    /// must get another poll before anyone decides it has a gap.
    #[test]
    fn a_stall_seen_this_sweep_is_not_kicked_this_sweep() {
        let reader = Reader::create(Config {
            ring_size: 16 * 1024,
            max_record: 256,
            dir: "/dev/shm".into(),
        })
        .unwrap();
        let (control_tx, control_rx) = mpsc::channel();
        let (retire_tx, retire_rx) = mpsc::channel();
        let mut fifo = Fifo::new(reader, 4, control_rx, retire_tx);
        let stream = pack(1, 7);
        control_tx.send(Control::Open { slot: 1, uid: 7 }).unwrap();

        // SAFETY: no Writer is attached to these rings; each ring is written by this thread only.
        unsafe { fifo.reader.write_unchecked(1, stream, 1, b"one").unwrap() };
        fifo.start_sweep();
        assert_eq!(fifo.ready, [1]);
        fifo.phase = Phase::Sweeping(Sweep {
            pos: 0,
            batch: 0,
            progress: false,
        });
        // Committed after the poll looked at ring 0.
        // SAFETY: as above.
        unsafe { fifo.reader.write_unchecked(0, stream, 0, b"zero").unwrap() };

        let first = fifo.next_message().expect("seq 0 is deliverable after a fresh poll");
        assert_eq!((first.stream(), first.seq()), (stream, 0));
        drop(first);
        let second = fifo.next_message().expect("seq 1 follows");
        assert_eq!((second.stream(), second.seq()), (stream, 1));
        drop(second);
        assert!(retire_rx.try_recv().is_err(), "a correct stream was kicked");
    }
}
