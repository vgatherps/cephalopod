use std::hint::spin_loop;
use std::path::Path;

use ceph_control::connect;
use ceph_ring::{PushError, Writer};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let [_, socket, count] = args.as_slice() else {
        panic!("usage: player <socket path> <count>");
    };
    let Ok(count) = count.parse::<u64>() else {
        panic!("usage: player <socket path> <count>");
    };
    let session = connect(Path::new(socket)).unwrap();
    let writer = Writer::attach(&session.prefix).unwrap();
    for seq in 0..count {
        let payload = seq.to_string();
        loop {
            match writer.try_push(payload.as_bytes(), session.stream, seq) {
                Ok(()) => break,
                Err(PushError::Full) => spin_loop(),
                Err(PushError::TooLarge) => panic!("payload too large"),
            }
        }
    }
    loop {
        match writer.try_finish(session.stream, count) {
            Ok(()) => break,
            Err(PushError::Full) => spin_loop(),
            Err(PushError::TooLarge) => unreachable!(),
        }
    }
    while !session.closed() {
        spin_loop();
    }
    println!("done");
}
