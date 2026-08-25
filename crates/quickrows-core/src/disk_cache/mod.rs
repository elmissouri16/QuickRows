//! Disposable on-disk indexes and diagnostic caches.

use crate::error::{QuickRowsError, QuickRowsResult};
use std::io::{Read, Write};

const CACHE_VERSION: u32 = 4;
const HASH_BUFFER_BYTES: usize = 1024 * 1024;
const CACHE_IO_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_CACHE_ALLOCATION_BYTES: u64 = 128 * 1024 * 1024;
const MAX_WARNING_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const OFFSETS_HEADER_BYTES: u64 = 64;
const ORDER_HEADER_BYTES: u64 = 69;
const PAYLOAD_CHECKSUM_BYTES: u64 = 32;

const OFFSETS_MAGIC: &[u8; 4] = b"CVOF";
const ORDER_MAGIC: &[u8; 4] = b"CVSO";

mod binary;
mod checksum;
mod fingerprint;
mod store;
mod warnings;

pub use binary::{read_offsets_cache, read_order_cache, write_offsets_cache, write_order_cache};
pub use fingerprint::{FileFingerprint, file_fingerprint};
pub use store::{
    CacheKey, cache_key, cache_key_from_fingerprint, ensure_cache_dir, offsets_cache_path,
    order_cache_path, prune_cache_dir, warnings_cache_path,
};
pub use warnings::{read_warnings_cache, write_warnings_cache};

#[cfg(test)]
mod tests;
