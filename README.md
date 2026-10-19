# cephalopod

FIFO messaging from many processes to one consumer over shared memory.

Each producer gets its own stream. The consumer receives every stream in order. Producers
write to a queue local to the CPU they are on, so they never contend with each other, and the
consumer's work grows with the number of cores, not the number of producers.

## Usage

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
    if let Some(m) = fifo.next_message() {
        handle(m.stream(), m.seq(), m.payload(), m.end());
    }
}
```

`try_push` returns `Full` when the queue has no room; retry. `try_finish` ends the stream;
`session.closed()` turns true once the consumer has handled the last message. On the consumer
side, dropping a message consumes it.

Runnable versions: `crates/fifo/examples/sequencer.rs` and `crates/fifo/examples/player.rs`.

## Performance

M4 Pro, 14-CPU Linux VM:

| | |
|---|---|
| push, 16-byte payload | 4.6 ns |
| consume, per message, 8 producers | 1.6 to 1.9 ns |
| idle sweep over 14 rings | 9.5 ns |

## Tests

```
make test        # in Docker
make e2e
make bench
make clippy
```

## How it works

One ring per CPU. A producer pushes into the ring of the CPU it is running on, inside an rseq
critical section: if the kernel preempts or migrates it mid-push, the push is abandoned and
retried. So each ring has exactly one writer at any instant and one reader, with no locks and
no atomic read-modify-writes. A push is a few plain stores plus one release store of the
record's tag, which is the commit.

The consumer sweeps the rings from one core, reading tags to find committed records, and
publishes its read positions once per sweep. Its per-message cost is a couple of nanoseconds;
the fixed cost of a sweep is one cache line per ring.
