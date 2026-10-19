# cephalopod

FIFO messaging from many processes to one consumer over shared memory.

Producers write into a queue local to the CPU they are running on, so they never contend with
each other. The consumer reads all the per-CPU queues and delivers each producer's messages in
order. Cost scales with the number of cores, not the number of producers.

## How to use it

Producer:

```rust
let session = connect(Path::new("/tmp/myqueue.sock"))?;
let writer = Writer::attach(&session.prefix)?;
let mut seq = 0;
for msg in messages {
    loop {
        match writer.try_push(msg, session.stream, seq) {
            Ok(()) => break,
            Err(PushError::Full) => std::hint::spin_loop(),
            Err(PushError::TooLarge) => panic!("message larger than max_record"),
        }
    }
    seq += 1;
}
while writer.try_finish(session.stream, seq) == Err(PushError::Full) {}
while !session.closed() {}
```

Consumer:

```rust
let reader = Reader::create(Config { ring_size: 1 << 20, max_record: 4096 })?;
let (control_tx, control_rx) = mpsc::channel();
let (retire_tx, retire_rx) = mpsc::channel();
let listener = Listener::bind(Path::new("/tmp/myqueue.sock"), reader.name(), 1024, control_tx, retire_rx)?;
std::thread::spawn(move || listener.run().expect("control thread failed"));
let mut fifo = Fifo::new(reader, 1024, control_rx, retire_tx);
loop {
    if let Some(m) = fifo.next() {
        handle(m.stream(), m.seq(), m.payload(), m.end());
    }
}
```

`crates/fifo/examples/sequencer.rs` and `crates/fifo/examples/player.rs` are the runnable
versions.

## How it works

Each push goes into the ring of the CPU the producer is on, inside an rseq critical section,
so one ring has one writer at a time and nothing is locked. A record is a 32-byte header
(position tag, length, stream, sequence number) and the payload; the tag is written last and
is the commit.

The consumer sweeps the rings and hands out messages. Messages from one producer come out in
sequence order even when the producer moved between CPUs.

## Crates

| Crate | |
|---|---|
| `ceph-rseq` | the rseq critical section that writes one record |
| `ceph-ring` | shared-memory layout, `Reader`, `Writer` |
| `ceph-proto` | stream ids and control messages |
| `ceph-fifo` | `Fifo::next()`, in-order delivery |
| `ceph-control` | hands out stream ids |

## Tests

```
scripts/test-linux.sh
```

## Numbers

M4 Pro, 14-CPU Linux VM:

| | |
|---|---|
| push, 16-byte payload | 4.6 ns |
| consume, per message, 8 producers | 1.6 to 1.9 ns |
| idle sweep over 14 rings | 9.5 ns |
