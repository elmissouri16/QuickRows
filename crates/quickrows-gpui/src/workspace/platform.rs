// Native paths, open-target encoding, and single-instance coordination.
fn settings_path() -> PathBuf {
    ProjectDirs::from("com", "el", "csv-viewer")
        .map(|dirs| dirs.config_dir().join("settings.json"))
        .unwrap_or_else(|| PathBuf::from("quickrows-settings.json"))
}

fn migrate_legacy_settings() {
    let target = settings_path();
    if target.exists() {
        return;
    }
    let Some(legacy) = ProjectDirs::from("com", "el", "QuickRows")
        .map(|dirs| dirs.config_dir().join("settings.json"))
        .filter(|path| path.is_file())
    else {
        return;
    };
    if let Some(parent) = target.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::copy(legacy, target);
}

fn diagnostics_path() -> PathBuf {
    ProjectDirs::from("com", "el", "csv-viewer")
        .map(|dirs| dirs.data_dir().join("logs"))
        .unwrap_or_else(|| PathBuf::from("quickrows-logs"))
}

fn cache_path() -> PathBuf {
    ProjectDirs::from("com", "el", "csv-viewer")
        .map(|dirs| dirs.cache_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("quickrows-cache"))
}

fn document_file_fingerprint(document: &CsvDocument) -> FileFingerprint {
    document.source_fingerprint()
}

fn file_metadata_matches(path: &Path, expected: Option<FileFingerprint>) -> bool {
    let Some(expected) = expected else {
        return !path.exists();
    };
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    metadata.len() == expected.len && modified == expected.modified
}

fn file_fingerprint(path: &Path) -> Option<FileFingerprint> {
    quickrows_core::file_fingerprint(path).ok()
}

fn display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| path.to_str().unwrap_or("CSV"))
        .to_string()
}

fn parse_fragment(value: &str) -> Option<CsvFragment> {
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .ok()?
        .parse()
        .ok()
}

fn open_target_from_value(value: &str) -> Option<OpenTarget> {
    if let Ok(url) = url::Url::parse(value)
        && url.scheme() == "file"
    {
        let fragment = url.fragment().and_then(parse_fragment);
        let path = url.to_file_path().ok().filter(|path| path.is_file())?;
        return Some(OpenTarget { path, fragment });
    }
    let path = PathBuf::from(value);
    if path.is_file() {
        return Some(path.into());
    }
    let (path, fragment) = value.rsplit_once('#')?;
    let path = PathBuf::from(path);
    path.is_file().then(|| OpenTarget {
        path,
        fragment: parse_fragment(fragment),
    })
}

fn open_target_from_os_value(value: &OsStr) -> Option<OpenTarget> {
    let path = PathBuf::from(value);
    if path.is_file() {
        return Some(path.into());
    }
    value.to_str().and_then(open_target_from_value)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    fn digit(value: u8) -> Option<u8> {
        match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            b'A'..=b'F' => Some(value - b'A' + 10),
            _ => None,
        }
    }
    let chunks = value.as_bytes().chunks_exact(2);
    if !chunks.remainder().is_empty() {
        return None;
    }
    chunks
        .map(|pair| Some((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
}

#[cfg(unix)]
fn encode_os_path(path: &Path) -> String {
    use std::os::unix::ffi::OsStrExt;
    encode_hex(path.as_os_str().as_bytes())
}

#[cfg(unix)]
fn decode_os_path(value: &str) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    Some(PathBuf::from(OsString::from_vec(decode_hex(value)?)))
}

#[cfg(windows)]
fn encode_os_path(path: &Path) -> String {
    use std::os::windows::ffi::OsStrExt;
    let bytes = path
        .as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    encode_hex(&bytes)
}

#[cfg(windows)]
fn decode_os_path(value: &str) -> Option<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    let bytes = decode_hex(value)?;
    let chunks = bytes.chunks_exact(2);
    if !chunks.remainder().is_empty() {
        return None;
    }
    let wide = chunks
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect::<Vec<_>>();
    Some(PathBuf::from(OsString::from_wide(&wide)))
}

#[cfg(not(any(unix, windows)))]
fn encode_os_path(path: &Path) -> String {
    encode_hex(path.to_string_lossy().as_bytes())
}

#[cfg(not(any(unix, windows)))]
fn decode_os_path(value: &str) -> Option<PathBuf> {
    String::from_utf8(decode_hex(value)?)
        .ok()
        .map(PathBuf::from)
}

fn encode_open_target(target: &OpenTarget) -> String {
    let fragment = target
        .fragment
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    format!("{}|{fragment}", encode_os_path(&target.path))
}

fn decode_open_target(value: &str) -> Option<OpenTarget> {
    let (path, fragment) = value.split_once('|').unwrap_or((value, ""));
    let path = decode_os_path(path)?;
    path.is_file().then(|| OpenTarget {
        path,
        fragment: (!fragment.is_empty())
            .then(|| parse_fragment(fragment))
            .flatten(),
    })
}

fn initial_paths() -> Vec<OpenTarget> {
    std::env::args_os()
        .skip(1)
        .filter_map(|value| open_target_from_os_value(&value))
        .collect()
}

#[cfg(test)]
fn path_from_open_value(value: &str) -> Option<PathBuf> {
    open_target_from_value(value).map(|target| target.path)
}

fn coordinate_instance(
    paths: &[OpenTarget],
) -> Result<Option<Arc<Mutex<VecDeque<RuntimeRequest>>>>, String> {
    let address = SocketAddr::from(([127, 0, 0, 1], INSTANCE_PORT));
    match TcpListener::bind(address) {
        Ok(listener) => {
            let requests = Arc::new(Mutex::new(VecDeque::new()));
            let listener_requests = requests.clone();
            std::thread::Builder::new()
                .name("quickrows-instance-listener".to_string())
                .spawn(move || {
                    for stream in listener.incoming() {
                        let Ok(mut stream) = stream else { continue };
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                        let mut bytes = Vec::new();
                        if Read::take(&mut stream, 4 * 1024 * 1024)
                            .read_to_end(&mut bytes)
                            .is_err()
                        {
                            continue;
                        }
                        let parts = bytes
                            .split(|byte| *byte == 0)
                            .filter_map(|part| std::str::from_utf8(part).ok())
                            .collect::<Vec<_>>();
                        if parts.first().copied() != Some(INSTANCE_MAGIC) {
                            continue;
                        }
                        if let Ok(mut requests) = listener_requests.lock() {
                            for part in parts.into_iter().skip(1) {
                                if part == "A" {
                                    requests.push_back(RuntimeRequest::Activate);
                                } else if let Some(target) =
                                    part.strip_prefix('H').and_then(decode_open_target).or_else(
                                        || part.strip_prefix('P').and_then(open_target_from_value),
                                    )
                                {
                                    requests.push_back(RuntimeRequest::Open(target));
                                }
                            }
                        }
                        let _ = stream.write_all(b"OK");
                    }
                })
                .map_err(|error| error.to_string())?;
            Ok(Some(requests))
        }
        Err(bind_error) if bind_error.kind() == std::io::ErrorKind::AddrInUse => {
            let mut stream =
                TcpStream::connect_timeout(&address, Duration::from_secs(2)).map_err(|error| {
                    format!("QuickRows is already running, but forwarding failed: {error}")
                })?;
            stream
                .write_all(INSTANCE_MAGIC.as_bytes())
                .and_then(|_| stream.write_all(&[0]))
                .map_err(|error| error.to_string())?;
            if paths.is_empty() {
                stream
                    .write_all(b"A\0")
                    .map_err(|error| error.to_string())?;
            } else {
                for target in paths {
                    stream.write_all(b"H").map_err(|error| error.to_string())?;
                    let value = encode_open_target(target);
                    stream
                        .write_all(value.as_bytes())
                        .and_then(|_| stream.write_all(&[0]))
                        .map_err(|error| error.to_string())?;
                }
            }
            stream
                .shutdown(Shutdown::Write)
                .map_err(|error| error.to_string())?;
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut response = [0u8; 2];
            stream
                .read_exact(&mut response)
                .map_err(|error| format!("QuickRows forwarding acknowledgement failed: {error}"))?;
            if &response != b"OK" {
                return Err("Another process is using the QuickRows instance channel".to_string());
            }
            Ok(None)
        }
        Err(error) => Err(format!(
            "Unable to initialize single-instance channel: {error}"
        )),
    }
}
