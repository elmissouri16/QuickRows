use super::*;

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum StoredPath {
    Utf8(String),
    Encoded { encoding: String, data: String },
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

fn stored_path(path: &Path) -> StoredPath {
    if let Some(path) = path.to_str() {
        return StoredPath::Utf8(path.to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        return StoredPath::Encoded {
            encoding: "unix-bytes".to_string(),
            data: encode_hex(path.as_os_str().as_bytes()),
        };
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let bytes = path
            .as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        return StoredPath::Encoded {
            encoding: "windows-wide".to_string(),
            data: encode_hex(&bytes),
        };
    }
    #[allow(unreachable_code)]
    StoredPath::Utf8(path.to_string_lossy().into_owned())
}

fn restore_path(path: StoredPath) -> Result<PathBuf, String> {
    match path {
        StoredPath::Utf8(path) => Ok(PathBuf::from(path)),
        StoredPath::Encoded { encoding, data } => {
            let bytes =
                decode_hex(&data).ok_or_else(|| "Invalid encoded settings path".to_string())?;
            #[cfg(unix)]
            if encoding == "unix-bytes" {
                use std::os::unix::ffi::OsStringExt;
                return Ok(PathBuf::from(std::ffi::OsString::from_vec(bytes)));
            }
            #[cfg(windows)]
            if encoding == "windows-wide" {
                use std::os::windows::ffi::OsStringExt;
                let chunks = bytes.chunks_exact(2);
                if !chunks.remainder().is_empty() {
                    return Err("Invalid Windows settings path".to_string());
                }
                let wide = chunks
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                    .collect::<Vec<_>>();
                return Ok(PathBuf::from(std::ffi::OsString::from_wide(&wide)));
            }
            Err(format!("Unsupported settings path encoding: {encoding}"))
        }
    }
}

pub(super) mod optional_path {
    use super::{PathBuf, StoredPath, restore_path, stored_path};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(path: &Option<PathBuf>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        path.as_deref().map(stored_path).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<StoredPath>::deserialize(deserializer)?
            .map(restore_path)
            .transpose()
            .map_err(serde::de::Error::custom)
    }
}

pub(super) mod path_vec {
    use super::{PathBuf, StoredPath, restore_path, stored_path};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(paths: &[PathBuf], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        paths
            .iter()
            .map(|path| stored_path(path))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<StoredPath>::deserialize(deserializer)?
            .into_iter()
            .map(restore_path)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::de::Error::custom)
    }
}
