use ceph_ring::{Config, Reader, Writer};

#[test]
fn push_and_read_back() {
    // macOS limits shm names to 31 bytes including the slash.
    let prefix = format!("cph{}", std::process::id());
    let mut reader = Reader::create(
        &prefix,
        Config {
            ring_size: 64 * 1024,
            max_record: 100,
        },
    )
    .unwrap();
    let writer = Writer::attach(&prefix).unwrap();
    let records: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; i as usize * 3]).collect();
    for (seq, r) in records.iter().enumerate() {
        writer.try_push(r, 11, seq as u64).unwrap();
    }
    // Sweep the way a consumer does: poll, then read exactly the rings it named. A Linux thread
    // can migrate between pushes, so order holds within each ring only.
    let mut ready = Vec::new();
    reader.poll(&mut ready);
    assert!(!ready.is_empty());
    let mut seen = Vec::new();
    for &ring in &ready {
        let mut in_ring = Vec::new();
        reader.read(ring, |r| {
            in_ring.push((r.seq, r.stream, r.payload.to_vec()));
            true
        });
        assert!(
            in_ring.windows(2).all(|w| w[0].0 < w[1].0),
            "ring {ring} out of order"
        );
        seen.extend(in_ring);
    }
    seen.sort();
    let expected: Vec<_> = records
        .into_iter()
        .enumerate()
        .map(|(seq, payload)| (seq as u64, 11, payload))
        .collect();
    assert_eq!(seen, expected);
}
