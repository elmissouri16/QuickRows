use super::super::*;

#[test]
fn open_file_identity_rejects_replaced_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("candidate.csv");
    std::fs::write(&path, "a,b\n1,2\n").unwrap();
    let original = std::fs::File::open(&path).unwrap();
    verify_path_references_open_file(&path, &original).unwrap();

    let replacement = dir.path().join("replacement.csv");
    std::fs::write(&replacement, "a,b\n3,4\n").unwrap();

    let error = verify_path_references_open_file(&replacement, &original).unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::DestinationChanged);
    assert!(error.contains("replaced"));
}

#[test]
fn open_file_state_rejects_a_same_length_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("candidate.csv");
    std::fs::write(&path, "a,b\n1,2\n").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let state = capture_open_file_state(&file).unwrap();

    std::fs::write(&path, "a,b\n3,4\n").unwrap();

    let error = verify_open_file_state(&file, state).unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::DestinationChanged);
    assert!(error.contains("changed"));
}
