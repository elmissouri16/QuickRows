use super::*;

fn document(contents: &str) -> (tempfile::TempDir, CsvDocument) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.csv");
    std::fs::write(&path, contents).unwrap();
    let doc = CsvDocument::open(path, None, None).unwrap();
    (dir, doc)
}

fn document_with_headers(contents: &str) -> (tempfile::TempDir, CsvDocument) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.csv");
    std::fs::write(&path, contents).unwrap();
    let doc = CsvDocument::open(
        path,
        Some(ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        }),
        None,
    )
    .unwrap();
    (dir, doc)
}

fn utf16_bytes(text: &str, little_endian: bool, bom: bool) -> Vec<u8> {
    let mut bytes = if bom {
        if little_endian {
            vec![0xff, 0xfe]
        } else {
            vec![0xfe, 0xff]
        }
    } else {
        Vec::new()
    };
    for unit in text.encode_utf16() {
        let encoded = if little_endian {
            unit.to_le_bytes()
        } else {
            unit.to_be_bytes()
        };
        bytes.extend_from_slice(&encoded);
    }
    bytes
}

include!("parsing.rs");
include!("operations.rs");
include!("save.rs");
include!("indexing.rs");
include!("source_safety.rs");
include!("cache.rs");
