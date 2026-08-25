use crate::error::{QuickRowsError, QuickRowsResult};
use chardetng::EncodingDetector;
use csv::{ByteRecord, Position, ReaderBuilder, StringRecord, Terminator};
use encoding_rs::Encoding;
use rayon::prelude::*;
use rayon::slice::ParallelSliceMut;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

const SAMPLE_SIZE: usize = 64 * 1024;
pub const MAX_WARNING_COUNT: usize = 200;

fn map_csv_error(error: csv::Error) -> QuickRowsError {
    if error.is_io_error() {
        QuickRowsError::io(error.to_string())
    } else {
        QuickRowsError::invalid_csv(error.to_string())
    }
}

fn map_boxed_csv_error(error: Box<dyn std::error::Error>) -> QuickRowsError {
    match error.downcast::<csv::Error>() {
        Ok(error) => map_csv_error(*error),
        Err(error) => match error.downcast::<std::io::Error>() {
            Ok(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                QuickRowsError::invalid_csv(error.to_string())
            }
            Ok(error) => QuickRowsError::from(*error),
            Err(error) => QuickRowsError::invalid_csv(error.to_string()),
        },
    }
}

// Keep the CSV façade and its established public paths while grouping the
// implementation by responsibility. Included files share this module's private helpers.
include!("dialect.rs");
include!("canonical.rs");
include!("records.rs");
include!("offsets.rs");
include!("reads.rs");
include!("query.rs");

#[cfg(test)]
mod tests;
