use std::sync::mpsc::{Receiver, Sender};

use ceph_proto::{Control, Kick, slot, uid};
use ceph_ring::{Reader, Record};

const BATCH: usize = 64;

pub struct Message<'a> {
    pub stream: u64,
    pub seq: u64,
    pub payload: &'a [u8],
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

    /// Returns the next deliverable message, or `None` if none is available now. The returned
    /// payload remains valid until the next call to `next`. Up to 64 consecutive messages are
    /// delivered from one ring before advancing to the next ring.
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
                drop(self.reader.take(ring));
                continue;
            }

            if uid(header.stream) != self.slots[index].uid {
                self.drain_control();
                if uid(header.stream) != self.slots[index].uid {
                    drop(self.reader.take(ring));
                    continue;
                }
            }

            let state = self.slots[index];
            if !state.live || header.seq < state.expected {
                drop(self.reader.take(ring));
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
            self.batch += 1;
            if self.batch == BATCH {
                self.pos += 1;
                self.batch = 0;
            }
            let Record {
                stream,
                seq,
                payload,
            } = self.reader.take(ring);
            return Some(Message {
                stream,
                seq,
                payload,
            });
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
