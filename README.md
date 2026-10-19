# cephalopod

A shared-memory queue. One sequencer process receives messages from many player processes on
the same Linux machine.

There is one ring per CPU. A player writes into the ring of the CPU it is running on, inside
an rseq critical section. That is what keeps two writers out of the same ring without any
atomic read-modify-write. The sequencer polls every ring from one core and hands messages to
the application in per-stream order.

## How it works

A message is a 32-byte header and a payload:

| Offset | Field |
|---|---|
| 0 | tag: the record's absolute byte position in the ring, with bit 0 set |
| 8 | payload length |
| 16 | stream id |
| 24 | sequence number |
| 32 | payload, padded to 8 bytes |

The tag is written last, with release ordering, and that store is the commit. Before writing a
record the producer zeroes the tag word at the next record position, so a record start always
holds a real tag or 0. There is no shared head word. The consumer finds the end of committed
data by reading tags.

Each CPU has a private cache line with its ring's write position and a copy of the consumer's
read position. The consumer publishes read positions once per sweep.

A stream id is a 64-bit number handed out over a Unix socket: a slot index in the high 32 bits,
a per-process unique id in the low 32 bits. A player numbers its messages from 0 and advances
only after a successful push. A player can migrate between CPUs; its messages then land in
different rings, and the sequencer still delivers them in sequence order.

A stream whose next record never arrives is dropped once nothing else is left to deliver, and
its socket is closed. The shared-memory name is random per sequencer run and handed out over
the socket.

A player ends a stream with `try_finish` and keeps its socket open. The sequencer closes it
after the application has consumed the last record. A player whose socket closes before that
is treated as gone and its remaining messages are dropped.

## Crates

| Crate | What it holds |
|---|---|
| `ceph-rseq` | the rseq critical section that writes one record; x86_64 and aarch64 inline asm |
| `ceph-ring` | the shared-memory layout, `Reader` and `Writer` |
| `ceph-proto` | stream ids and the two message types between the sequencer's threads |
| `ceph-fifo` | `Fifo::next()`: per-stream ordering, stall handling, end of stream |
| `ceph-control` | the Unix socket that hands out stream ids; `connect` for players |

## Using it

Sequencer:

```rust
let reader = Reader::create(Config { ring_size: 1 << 20, max_record: 4096 })?;
let (control_tx, control_rx) = mpsc::channel();
let (retire_tx, retire_rx) = mpsc::channel();
let listener = Listener::bind(
    Path::new("/tmp/myqueue.sock"),
    reader.name(),
    1024,
    control_tx,
    retire_rx,
)?;
std::thread::spawn(move || listener.run().expect("control thread failed"));
let mut fifo = Fifo::new(reader, 1024, control_rx, retire_tx);
loop {
    if let Some(m) = fifo.next() {
        handle(m.stream(), m.seq(), m.payload(), m.end());
    }   // dropping `m` consumes the record
}
```

Player:

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
    seq += 1;   // only after a successful push
}
while writer.try_finish(session.stream, seq) == Err(PushError::Full) {}   // an empty record cannot be too large
while !session.closed() {}   // the sequencer closes the socket once everything is consumed
```

`crates/fifo/examples/sequencer.rs` and `crates/fifo/examples/player.rs` are runnable versions
of the above.

## Tests

The tests need Linux and run in Docker:

```
scripts/test-linux.sh                         # the whole workspace
scripts/test-linux.sh test -p ceph-fifo --test e2e   # one sequencer process, four player processes
```

`cargo test` on macOS runs the control-socket tests and one smoke test of the fallback path.

## Numbers

M4 Pro, 14-CPU Linux VM:

| | |
|---|---|
| push, 16-byte payload, consumer polling | 4.6 ns |
| consumer, per message, 8 writers saturating | 1.6 to 1.9 ns |
| idle sweep over 14 rings | 9.5 ns |
