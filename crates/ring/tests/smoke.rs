use ceph_ring::{Config, Reader, Writer};

fn drain(reader: &mut Reader, ring: usize) -> Vec<(u64, u64, Vec<u8>)> {
    let mut records = Vec::new();
    while reader.front(ring).is_some() {
        let record = reader.view(ring);
        records.push((record.stream, record.seq, record.payload.to_vec()));
        reader.advance(ring);
    }
    reader.publish();
    records
}

#[test]
fn push_and_read_back() {
    let mut reader = Reader::create(Config {
        ring_size: 64 * 1024,
        max_record: 100,
    })
    .unwrap();
    assert_eq!(reader.name().len(), 19);
    assert!(reader.name().starts_with("cph"));
    assert!(
        reader.name()[3..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    let writer = Writer::attach(reader.name()).unwrap();
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
        in_ring.extend(
            drain(&mut reader, ring)
                .into_iter()
                .map(|(stream, seq, payload)| (seq, stream, payload)),
        );
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
