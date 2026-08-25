use super::*;

#[test]
fn settings_round_trip_and_normalize() {
    let dir = tempfile::tempdir().unwrap();
    let store = SettingsStore::new(dir.path().join("settings.json"));
    let mut settings = AppSettings {
        column_width: 20.0,
        ..AppSettings::default()
    };
    settings.remember_file(PathBuf::from("/tmp/example.csv"));
    store.save(&settings).unwrap();
    settings.column_width = 180.0;
    store.save(&settings).unwrap();

    let loaded = store.load().unwrap();
    assert_eq!(loaded.column_width, 180.0);
    assert_eq!(loaded.recent_files, vec![PathBuf::from("/tmp/example.csv")]);
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_recent_paths_round_trip() {
    use std::os::unix::ffi::OsStringExt;

    let dir = tempfile::tempdir().unwrap();
    let store = SettingsStore::new(dir.path().join("settings.json"));
    let mut bytes = b"/tmp/rows-".to_vec();
    bytes.push(0xff);
    bytes.extend_from_slice(b".csv");
    let path = PathBuf::from(std::ffi::OsString::from_vec(bytes));
    let mut settings = AppSettings::default();
    settings.remember_file(path.clone());

    store.save(&settings).unwrap();
    let loaded = store.load().unwrap();

    assert_eq!(loaded.recent_files, vec![path.clone()]);
    assert_eq!(loaded.last_open_dir, path.parent().map(Path::to_path_buf));
}

#[test]
fn parse_overrides_round_trip_with_extended_dialects() {
    let dir = tempfile::tempdir().unwrap();
    let store = SettingsStore::new(dir.path().join("settings.json"));
    let settings = AppSettings {
        parse_overrides: ParseOverrides {
            delimiter: Some("§".to_string()),
            quote: Some("«".to_string()),
            escape: Some("※".to_string()),
            comment: Some("#".to_string()),
            excel_sep: Some(false),
            line_ending: Some("crlf".to_string()),
            encoding: Some("utf-16le".to_string()),
            has_headers: Some(false),
            malformed: Some("strict".to_string()),
            max_field_size: Some(1024),
            max_record_size: Some(4096),
        },
        ..AppSettings::default()
    };
    store.save(&settings).unwrap();

    let loaded = store.load().unwrap();
    let parse = loaded.parse_overrides;
    assert_eq!(parse.delimiter.as_deref(), Some("§"));
    assert_eq!(parse.quote.as_deref(), Some("«"));
    assert_eq!(parse.escape.as_deref(), Some("※"));
    assert_eq!(parse.comment.as_deref(), Some("#"));
    assert_eq!(parse.excel_sep, Some(false));
    assert_eq!(parse.encoding.as_deref(), Some("utf-16le"));
    assert_eq!(parse.max_record_size, Some(4096));
}

#[test]
fn older_settings_without_extended_parse_fields_still_load() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("settings.json");
    std::fs::write(
        &path,
        r#"{
                "version": 1,
                "parse_overrides": {
                    "delimiter": "semicolon",
                    "has_headers": true
                }
            }"#,
    )
    .unwrap();
    let loaded = SettingsStore::new(path).load().unwrap();
    assert_eq!(
        loaded.parse_overrides.delimiter.as_deref(),
        Some("semicolon")
    );
    assert_eq!(loaded.parse_overrides.comment, None);
    assert_eq!(loaded.parse_overrides.excel_sep, None);
}

#[test]
fn missing_settings_use_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let store = SettingsStore::new(dir.path().join("missing.json"));
    assert_eq!(store.load().unwrap().row_density, RowDensity::Default);
}

#[test]
fn malformed_settings_are_classified() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("settings.json");
    std::fs::write(&path, "{not json").unwrap();
    let error = SettingsStore::new(path).load().unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::InvalidSettings);
}
