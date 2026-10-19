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

/// At kick time, every ring with committed data is stalled. The victim is a front stream whose
/// expected record is absent from every stalled ring's committed backlog.
pub struct Fifo {
    reader: Reader,
    slots: Vec<Slot>,
    ready: Vec<usize>,
    pos: usize,
    batch: usize,
    stalled: Vec<usize>,
    wanted: Vec<(u64, u64, bool)>,
    progress: bool,
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
            pos: 0,
            batch: 0,
            stalled: Vec::new(),
            wanted: Vec::new(),
            progress: false,
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
    /// advancing to the next ring; discarded records do not count. A stream whose next record
    /// never arrives is dropped at the end of the first pass in which nothing was delivered or
    /// discarded anywhere and a fresh poll still showed nothing for it. While other streams keep
    /// delivering, it and whatever is behind it in that ring wait.
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
                self.unstall(ring);
                self.pos += 1;
                self.batch = 0;
                continue;
            };
            let index = slot(header.stream) as usize;
            if index >= self.slots.len() {
                self.reader.advance(ring);
                self.progress = true;
                continue;
            }

            if uid(header.stream) != self.slots[index].uid {
                self.drain_control();
                if uid(header.stream) != self.slots[index].uid {
                    self.reader.advance(ring);
                    self.progress = true;
                    continue;
                }
            }

            let seq = header.seq & !END_OF_STREAM;
            let end = header.seq & END_OF_STREAM != 0;
            let state = self.slots[index];
            if !state.live || seq < state.expected {
                self.reader.advance(ring);
                self.progress = true;
                continue;
            }
            if seq > state.expected {
                if !self.stalled.contains(&ring) {
                    self.stalled.push(ring);
                    self.progress = true;
                }
                self.pos += 1;
                self.batch = 0;
                continue;
            }

            self.slots[index].expected += 1;
            let finish = end.then(|| {
                self.slots[index].live = false;
                Retire::Finished {
                    slot: index as u32,
                    uid: state.uid,
                }
            });
            self.progress = true;
            self.unstall(ring);
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
        self.drain_control();
        self.reader.publish();
        self.ready.clear();
        self.reader.poll(&mut self.ready);
        self.pos = 0;
        self.batch = 0;
        self.progress = false;
    }

    fn finish_sweep(&mut self) {
        if !self.progress && !self.stalled.is_empty() {
            let ring = self.victim();
            self.kick_front(ring);
        }
    }

    /// The stalled ring whose front stream has a gap: its expected record is in no stalled ring's
    /// backlog. Falls back to the first stalled ring when every expected record is present, which
    /// takes a producer that committed its own records out of order.
    fn victim(&mut self) -> usize {
        self.wanted.clear();
        for &ring in &self.stalled {
            let front = self
                .reader
                .front(ring)
                .unwrap_or_else(|| unreachable!("stalled ring {ring} has no committed front"));
            self.wanted.push((
                front.stream,
                self.slots[slot(front.stream) as usize].expected,
                false,
            ));
        }

        for &ring in &self.stalled {
            for header in self.reader.committed(ring) {
                let seq = header.seq & !END_OF_STREAM;
                for wanted in &mut self.wanted {
                    if wanted.0 == header.stream && wanted.1 == seq {
                        wanted.2 = true;
                    }
                }
            }
        }

        self.wanted
            .iter()
            .position(|wanted| !wanted.2)
            .map_or(self.stalled[0], |index| self.stalled[index])
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

    fn kick_front(&mut self, ring: usize) {
        let header = self
            .reader
            .front(ring)
            .unwrap_or_else(|| unreachable!("stalled ring {ring} has no committed front"));
        let index = slot(header.stream) as usize;
        let state = self.slots.get(index).copied().unwrap_or_else(|| {
            unreachable!("stalled ring {ring} has out-of-range slot {index}")
        });
        let stream_uid = uid(header.stream);
        let seq = header.seq & !END_OF_STREAM;
        if !state.live || state.uid != stream_uid || seq <= state.expected {
            unreachable!("stalled ring {ring} front no longer satisfies the stall invariant");
        }
        self.slots[index].live = false;
        let _ = self.retire.send(Retire::Kicked {
            slot: index as u32,
            uid: stream_uid,
        });
    }
}
