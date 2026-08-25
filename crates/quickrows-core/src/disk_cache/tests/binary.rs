use super::super::store::validate_cache_payload;
use super::super::*;
use std::io::{Seek, SeekFrom, Write};

#[test]
fn offsets_cache_round_trip() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");

    let key = cache_key(&file_path, None).expect("cache key");
    let offsets_path = offsets_cache_path(dir.path(), key);
    let offsets = vec![0u64, 12, 16];

    write_offsets_cache(&offsets_path, key, &offsets).expect("write offsets");
    let loaded = read_offsets_cache(&offsets_path, key)
        .expect("read offsets")
        .expect("offsets");
    assert_eq!(loaded, offsets);
}

#[test]
fn truncated_binary_cache_headers_are_treated_as_cache_misses() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"col1,col2\n1,2\n").expect("write csv");
    let key = cache_key(&file_path, None).expect("cache key");

    let offsets_path = offsets_cache_path(dir.path(), key);
    std::fs::write(&offsets_path, OFFSETS_MAGIC).unwrap();
    assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());

    let order_path = order_cache_path(dir.path(), key, 0, true);
    std::fs::write(&order_path, ORDER_MAGIC).unwrap();
    assert!(
        read_order_cache(&order_path, key, 0, true)
            .unwrap()
            .is_none()
    );
}

#[test]
fn offsets_cache_rejects_modified_truncated_and_extended_payloads() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");
    let key = cache_key(&file_path, None).expect("cache key");
    let offsets_path = offsets_cache_path(dir.path(), key);
    let offsets = vec![0u64, 12, 16];
    write_offsets_cache(&offsets_path, key, &offsets).expect("write offsets");
    let valid = std::fs::read(&offsets_path).unwrap();

    let mut modified = valid.clone();
    modified[64] ^= 0xff;
    std::fs::write(&offsets_path, modified).unwrap();
    assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());

    std::fs::write(&offsets_path, &valid[..valid.len() - 1]).unwrap();
    assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());

    let mut extended = valid;
    extended.push(0);
    std::fs::write(&offsets_path, extended).unwrap();
    assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());
}

#[test]
fn zeroed_offset_count_cannot_turn_a_populated_csv_into_an_empty_cache() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");
    let key = cache_key(&file_path, None).expect("cache key");
    let offsets_path = offsets_cache_path(dir.path(), key);
    write_offsets_cache(&offsets_path, key, &[12, 16]).expect("write offsets");
    let mut bytes = std::fs::read(&offsets_path).unwrap();
    bytes[56..64].copy_from_slice(&0u64.to_le_bytes());
    std::fs::write(&offsets_path, bytes).unwrap();

    assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());
}

#[test]
fn buffered_cache_round_trip_crosses_multiple_io_buffers() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"col1\n1\n").expect("write csv");
    let mut key = cache_key(&file_path, None).expect("cache key");
    key.len = u64::MAX;
    let offsets_path = offsets_cache_path(dir.path(), key);
    let offsets = (0..200_000u64).collect::<Vec<_>>();

    write_offsets_cache(&offsets_path, key, &offsets).expect("write offsets");
    let loaded = read_offsets_cache(&offsets_path, key)
        .expect("read offsets")
        .expect("offsets");
    assert_eq!(loaded, offsets);
}

#[test]
fn oversized_cache_write_removes_an_existing_unusable_entry() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"a\n").unwrap();
    let mut key = cache_key(&file_path, None).unwrap();
    key.len = u64::MAX;
    let path = offsets_cache_path(dir.path(), key);
    std::fs::write(&path, b"stale cache").unwrap();
    let oversized_count = (MAX_CACHE_ALLOCATION_BYTES / 8 + 1) as usize;

    assert!(validate_cache_payload(&path, key, oversized_count, "Offset").is_err());
    assert!(!path.exists());
}

#[test]
fn corrupt_counts_are_rejected_before_allocation() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"a,b\n1,2\n").unwrap();
    let key = cache_key(&file_path, None).unwrap();
    let offsets_path = offsets_cache_path(dir.path(), key);
    write_offsets_cache(&offsets_path, key, &[4]).unwrap();
    let mut bytes = std::fs::read(&offsets_path).unwrap();
    bytes[56..64].copy_from_slice(&u64::MAX.to_le_bytes());
    std::fs::write(&offsets_path, bytes).unwrap();
    assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());
}

#[test]
fn oversized_offset_allocation_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"a\n1\n").unwrap();
    let mut key = cache_key(&file_path, None).unwrap();
    key.len = u64::MAX;
    let offsets_path = offsets_cache_path(dir.path(), key);
    write_offsets_cache(&offsets_path, key, &[]).unwrap();
    let count = MAX_CACHE_ALLOCATION_BYTES / 8 + 1;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&offsets_path)
        .unwrap();
    file.seek(SeekFrom::Start(56)).unwrap();
    file.write_all(&count.to_le_bytes()).unwrap();
    file.set_len(64 + count * 8).unwrap();

    assert!(read_offsets_cache(&offsets_path, key).unwrap().is_none());
}

#[test]
fn order_cache_round_trip_and_mismatch() {
    let dir = tempfile::tempdir().expect("temp dir");
    let file_path = dir.path().join("data.csv");
    std::fs::write(&file_path, b"col1,col2\n1,2\n3,4\n").expect("write csv");

    let key = cache_key(&file_path, None).expect("cache key");
    let order_path = order_cache_path(dir.path(), key, 2, true);
    let order = vec![2usize, 0, 1];

    write_order_cache(&order_path, key, 2, true, &order).expect("write order");
    let loaded = read_order_cache(&order_path, key, 2, true)
        .expect("read order")
        .expect("order");
    assert_eq!(loaded, order);

    let wrong_column = read_order_cache(&order_path, key, 1, true).expect("read order");
    assert!(wrong_column.is_none());

    let wrong_direction = read_order_cache(&order_path, key, 2, false).expect("read order");
    assert!(wrong_direction.is_none());

    let mut invalid_direction = std::fs::read(&order_path).unwrap();
    invalid_direction[60] = 2;
    std::fs::write(&order_path, invalid_direction).unwrap();
    assert!(
        read_order_cache(&order_path, key, 2, false)
            .expect("read invalid direction")
            .is_none()
    );

    write_order_cache(&order_path, key, 2, true, &order).unwrap();
    let mut corrupted = std::fs::read(&order_path).unwrap();
    corrupted[69] ^= 0xff;
    std::fs::write(&order_path, corrupted).unwrap();
    assert!(
        read_order_cache(&order_path, key, 2, true)
            .expect("read corrupted order")
            .is_none()
    );
}
