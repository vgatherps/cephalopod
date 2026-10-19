//! Definitions shared by the sequencer's polling thread (`ceph-fifo`) and its control thread
//! (`ceph-control`).
//!
//! Stream ids keep the slot in the high 32 bits and the unique id in the low 32 bits.

/// A stream id as carried in every record: the slot index in the high 32 bits, the unique id
/// in the low 32 bits.
pub fn pack(slot: u32, uid: u32) -> u64 {
    (slot as u64) << 32 | uid as u64
}

pub fn slot(stream: u64) -> u32 {
    (stream >> 32) as u32
}

pub fn uid(stream: u64) -> u32 {
    stream as u32
}

/// Control thread to polling thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    Open { slot: u32, uid: u32 },
    Close { slot: u32, uid: u32 },
}

/// Polling thread to control thread. The control thread closes the socket and frees the slot in
/// both cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retire {
    Kicked { slot: u32, uid: u32 },
    Finished { slot: u32, uid: u32 },
}
