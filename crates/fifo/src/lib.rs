use std::sync::mpsc::{Receiver, Sender};

use ceph_proto::{Control, Kick, slot, uid};
use ceph_ring::Reader;

const BATCH: usize = 64;

pub struct Message<'a> {
    stream: u64,
    seq: u64,
    payload: *const [u8],
    ring: usize,
    reader: &'a mut Reader,
}

impl Message<'_> {
    pub fn stream(&self) -> u64 {
        self.stream
    }

    pub fn seq(&self) -> u64 {
        self.seq
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
    }
}

#[derive(Clone, Copy, Default)]
struct Slot {
    uid: u32,
    expected: u64,
    live: bool,
}

pub struct Fifo {
    reader: Reader,
    slots: Vec<Slot>,
    ready: Vec<usize>,
    pos: usize,
    batch: usize,
    stopped: bool,
    delivered: bool,
    empty_sweeps: u32,
    control: Receiver<Control>,
    kicks: Sender<Kick>,
}

impl Fifo {
    pub fn new(
        reader: Reader,
        max_streams: u32,
        control: Receiver<Control>,
        kicks: Sender<Kick>,
    ) -> Fifo {
        Fifo {
            reader,
            slots: vec![Slot::default(); max_streams as usize],
            ready: Vec::new(),
            pos: 0,
            batch: 0,
            stopped: false,
            delivered: false,
            empty_sweeps: 0,
            control,
            kicks,
        }
    }

    /// Returns the next deliverable message, or `None` if none is available now. The message
    /// borrows the `Fifo`; dropping it consumes the record, and `publish` happens at the next
    /// sweep boundary. Up to 64 consecutive records are passed on one ring before advancing to
    /// the next ring.
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
                self.count_record();
                continue;
            }

            if uid(header.stream) != self.slots[index].uid {
                self.drain_control();
                if uid(header.stream) != self.slots[index].uid {
                    self.reader.advance(ring);
                    self.count_record();
                    continue;
                }
            }

            let state = self.slots[index];
            if !state.live || header.seq < state.expected {
                self.reader.advance(ring);
                self.count_record();
                continue;
            }
            if header.seq > state.expected {
                self.stopped = true;
                self.pos += 1;
                self.batch = 0;
                continue;
            }

            self.slots[index].expected += 1;
            self.delivered = true;
            self.count_record();
            let (stream, seq, payload) = {
                let record = self.reader.view(ring);
                (
                    record.stream,
                    record.seq,
                    std::ptr::slice_from_raw_parts(record.payload.as_ptr(), record.payload.len()),
                )
            };
            return Some(Message {
                stream,
                seq,
                payload,
                ring,
                reader: &mut self.reader,
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
    }

    fn drain_control(&mut self) {
        while let Ok(message) = self.control.try_recv() {
            match message {
                Control::Open { slot, uid } => {
                    if let Some(state) = self.slots.get_mut(slot as usize) {
                        *state = Slot {
                            uid,
                            expected: 0,
                            live: true,
                        };
                    }
                }
                Control::Close { slot, uid } => {
                    if let Some(state) = self.slots.get_mut(slot as usize) {
                        if state.uid == uid {
                            state.live = false;
                        }
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
            let Some(state) = self.slots.get_mut(index) else {
                continue;
            };
            let stream_uid = uid(header.stream);
            if state.live && state.uid == stream_uid && header.seq > state.expected {
                state.live = false;
                let _ = self.kicks.send(Kick {
                    slot: index as u32,
                    uid: stream_uid,
                });
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceph_ring::Config;
    use std::sync::mpsc;

    #[test]
    fn empty_stopped_sweeps_do_not_kick() {
        let prefix = format!("ceph-fifo-unit-{}", std::process::id());
        // SAFETY: sysconf reads a process-wide constant.
        let ring_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let reader = Reader::create(
            &prefix,
            Config {
                ring_size,
                max_record: 16,
            },
        )
        .unwrap();
        let (_control_tx, control_rx) = mpsc::channel();
        let (kick_tx, kick_rx) = mpsc::channel();
        let mut fifo = Fifo::new(reader, 1, control_rx, kick_tx);

        fifo.stopped = true;
        fifo.finish_sweep();
        assert_eq!(fifo.empty_sweeps, 1);
        assert_eq!(
            kick_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        );

        fifo.finish_sweep();
        assert_eq!(fifo.empty_sweeps, 0);
        assert_eq!(
            kick_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        );
    }
}
