use std::hint::spin_loop;
use std::path::Path;
use std::sync::mpsc;

use ceph_control::Listener;
use ceph_fifo::Fifo;
use ceph_ring::{Config, Reader};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let [_, socket, dir] = args.as_slice() else {
        panic!("usage: sequencer <socket path> <shared-file directory>");
    };
    let reader = Reader::create(Config {
        ring_size: 2 << 20,
        max_record: 4096,
        dir: dir.into(),
    })
    .unwrap();
    let (control_tx, control_rx) = mpsc::channel();
    let (retire_tx, retire_rx) = mpsc::channel();
    let listener =
        Listener::bind(Path::new(socket), reader.name(), 1024, control_tx, retire_rx).unwrap();
    std::thread::spawn(move || listener.run().unwrap());
    let mut fifo = Fifo::new(reader, 1024, control_rx, retire_tx);
    loop {
        match fifo.next_message() {
            Some(message) if message.end() => println!("end"),
            Some(message) => println!(
                "stream={} seq={} len={}",
                message.stream(),
                message.seq(),
                message.payload().len()
            ),
            None => spin_loop(),
        }
    }
}
