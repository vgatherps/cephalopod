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
    for r in &records {
        writer.try_push(r).unwrap();
    }
    // A Linux thread can migrate between pushes, so order holds within each ring only.
    let mut seen = Vec::new();
    for ring in 0..reader.nr_rings() {
        let mut in_ring = Vec::new();
        reader.read(ring, |r| {
            in_ring.push(r.to_vec());
            true
        });
        assert!(in_ring.is_sorted(), "ring {ring} out of order");
        seen.extend(in_ring);
    }
    seen.sort();
    assert_eq!(seen, records);
}
