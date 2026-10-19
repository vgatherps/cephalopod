# cephalopod

A shared-memory queue for one sequencer process receiving messages from many player
processes on the same Linux machine.

Each CPU has one ring. A player writes into the ring of the CPU it is running on, inside an
rseq critical section, so a ring never has two writers at once and no atomic read-modify-write
is needed. The sequencer polls every ring from one core and hands messages to the application
in per-stream order.

Linux with glibc 2.35 or newer, kernel 6 or newer. aarch64 is tested. x86_64 compiles but has
not been run. On macOS the crates build and run with one ring and a spinlock, for developing
the code around them; that path is not for production.

## How it works

A message is a 32-byte header and a payload:

| Offset | Field |
|---|---|
| 0 | tag: the record's absolute byte position in the ring, with bit 0 set |
| 8 | payload length |
| 16 | stream id |
| 24 | sequence number |
| 32 | payload, padded to 8 bytes |

The tag is written last, with release ordering. That store is the commit. Before writing a
record the producer zeroes the tag word of the next record position, so a record start always
holds either a real tag or 0. There is no shared head word; the consumer finds the end of
committed data by reading tags.

Each CPU has a private cache line holding its ring's write position and a copy of the
consumer's read position. The consumer publishes read positions once per sweep.

Streams are identified by a 64-bit id handed out over a Unix socket: a slot index in the high
32 bits and a per-process unique id in the low 32 bits. A player numbers its messages from 0
and advances only after a successful push. The sequencer delivers a stream's messages in
sequence order even when they land in different rings because the player migrated between
CPUs. If a stream's next message never arrives, the sequencer drops that stream after two
sweeps with no progress and closes its socket.

A player ends a stream with `try_finish`. The sequencer delivers everything up to that record,
and closes the socket after the application has consumed it. A player whose socket closes
without that record is treated as gone and its undelivered messages are dropped.

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
let reader = Reader::create("myqueue", Config { ring_size: 1 << 20, max_record: 4096 })?;
let (control_tx, control_rx) = mpsc::channel();
let (retire_tx, retire_rx) = mpsc::channel();
let listener = Listener::bind(Path::new("/tmp/myqueue.sock"), "myqueue", 1024, control_tx, retire_rx)?;
std::thread::spawn(move || listener.run());
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
    while writer.try_push(msg, session.stream, seq) == Err(PushError::Full) {}
    seq += 1;
}
while writer.try_finish(session.stream, seq) == Err(PushError::Full) {}
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

Measured on an M4 Pro inside a 14-CPU Linux VM. Treat them as relative.

| | |
|---|---|
| push, 16-byte payload, consumer polling | 4.6 ns |
| consumer, per message, 8 writers saturating | 1.6 to 1.9 ns |
| idle sweep over 14 rings | 9.5 ns |

## Not done

x86_64 has never executed. Huge pages are not used. Rings are sized at creation and every
player maps all of them. There is no C API.
