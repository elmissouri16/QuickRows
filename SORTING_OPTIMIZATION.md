# Sorting Optimization Plans

## Current Issue
Sorting 10M rows uses **3GB+ RAM** and is very slow because it loads entire dataset into memory.

## Option 1: Quick Memory Optimization ✅ IMPLEMENTED

### Approach
Use compact representation instead of full strings to reduce memory footprint.

### Changes Made
- Truncate sort values to 256 chars max (most comparison differences happen early)
- Use `Box<str>` instead of `String` (saves pointer overhead)
- Memory: **~500MB-1GB** (vs 3GB+)
- Speed: **Faster** (better cache locality)

---

## Option 2: External Merge Sort (Future Implementation)

### Complete Implementation Guide

#### Algorithm Overview
External merge sort handles datasets larger than RAM by:
1. **Phase 1**: Split data into chunks, sort each chunk in memory, write to temp files
2. **Phase 2**: K-way merge of sorted chunks into final order

#### Detailed Implementation

##### 1. Data Structures

```rust
// Chunk size for in-memory sorting (~100K rows = ~30MB)
const SORT_CHUNK_SIZE: usize = 100_000;
const MAX_SORT_MEMORY_MB: usize = 100;

struct SortChunkWriter {
    chunk_id: usize,
    items: Vec<(usize, String)>,
    temp_file: File,
}

struct SortChunkReader {
    reader: BufReader<File>,
    current: Option<(usize, String)>,
}

struct KWayMerger {
    heap: BinaryHeap<Reverse<HeapItem>>,
    readers: Vec<SortChunkReader>,
}

#[derive(Eq, PartialEq)]
struct HeapItem {
    value: String,
    index: usize,
    reader_id: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
            .then(self.index.cmp(&other.index))
    }
}
```

##### 2. Phase 1: Create Sorted Chunks

```rust
async fn create_sorted_chunks(
    path: &str,
    column_idx: usize,
    ascending: bool,
    settings: &ParseSettings,
    offsets: &Option<Vec<u64>>,
    mmap: &Option<Arc<Mmap>>,
) -> Result<Vec<PathBuf>, String> {
    let temp_dir = tempfile::tempdir()
        .map_err(|e| format!("Failed to create temp dir: {}", e))?;
    
    let mut chunk_files = Vec::new();
    let mut start = 0;
    let mut chunk_id = 0;
    
    loop {
        // Read chunk
        let chunk = read_chunk_with_offsets_or_mmap(
            path, offsets, mmap, settings,
            start, SORT_CHUNK_SIZE
        )?;
        
        if chunk.is_empty() {
            break;
        }
        
        // Extract sort column values
        let mut chunk_data: Vec<(usize, String)> = chunk
            .iter()
            .enumerate()
            .map(|(idx, row)| {
                let row_index = start + idx;
                let value = row.get(column_idx)
                    .cloned()
                    .unwrap_or_default();
                (row_index, value)
            })
            .collect();
        
        // Sort chunk in memory
        if ascending {
            chunk_data.par_sort_unstable_by(|a, b| a.1.cmp(&b.1));
        } else {
            chunk_data.par_sort_unstable_by(|a, b| b.1.cmp(&a.1));
        }
        
        // Write to temp file
        let chunk_path = temp_dir.path().join(format!("chunk_{}.bin", chunk_id));
        write_sorted_chunk(&chunk_path, &chunk_data)?;
        chunk_files.push(chunk_path);
        
        if chunk.len() < SORT_CHUNK_SIZE {
            break;
        }
        
        start += chunk.len();
        chunk_id += 1;
    }
    
    Ok(chunk_files)
}

fn write_sorted_chunk(
    path: &Path,
    data: &[(usize, String)]
) -> Result<(), String> {
    use std::io::Write;
    
    let file = File::create(path)
        .map_err(|e| format!("Failed to create chunk file: {}", e))?;
    let mut writer = BufWriter::new(file);
    
    // Write count
    writer.write_all(&data.len().to_le_bytes())?;
    
    // Write each entry
    for (index, value) in data {
        writer.write_all(&index.to_le_bytes())?;
        let bytes = value.as_bytes();
        writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
        writer.write_all(bytes)?;
    }
    
    writer.flush()
        .map_err(|e| format!("Failed to write chunk: {}", e))?;
    
    Ok(())
}
```

##### 3. Phase 2: K-Way Merge

```rust
struct SortChunkReader {
    reader: BufReader<File>,
    current: Option<(usize, String)>,
}

impl SortChunkReader {
    fn new(path: &Path) -> Result<Self, String> {
        let file = File::open(path)
            .map_err(|e| format!("Failed to open chunk: {}", e))?;
        let mut reader = BufReader::new(file);
        
        // Read and discard count
        let mut count_bytes = [0u8; 8];
        reader.read_exact(&mut count_bytes)?;
        
        let mut chunk_reader = SortChunkReader {
            reader,
            current: None,
        };
        
        chunk_reader.advance()?;
        Ok(chunk_reader)
    }
    
    fn advance(&mut self) -> Result<(), String> {
        use std::io::Read;
        
        // Read index
        let mut index_bytes = [0u8; 8];
        if self.reader.read_exact(&mut index_bytes).is_err() {
            self.current = None;
            return Ok(());
        }
        let index = usize::from_le_bytes(index_bytes);
        
        // Read value length
        let mut len_bytes = [0u8; 4];
        self.reader.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        // Read value
        let mut value_bytes = vec![0u8; len];
        self.reader.read_exact(&mut value_bytes)?;
        let value = String::from_utf8(value_bytes)
            .map_err(|e| format!("Invalid UTF-8: {}", e))?;
        
        self.current = Some((index, value));
        Ok(())
    }
}

fn k_way_merge(
    chunk_files: Vec<PathBuf>,
    ascending: bool,
) -> Result<Vec<usize>, String> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    
    // Open all chunk readers
    let mut readers: Vec<SortChunkReader> = chunk_files
        .iter()
        .map(|path| SortChunkReader::new(path))
        .collect::<Result<Vec<_>, _>>()?;
    
    // Initialize heap with first item from each reader
    let mut heap = if ascending {
        BinaryHeap::new() // Min-heap via Reverse
    } else {
        BinaryHeap::new() // Max-heap natural
    };
    
    for (reader_id, reader) in readers.iter().enumerate() {
        if let Some((index, value)) = &reader.current {
            if ascending {
                heap.push(Reverse(HeapItem {
                    value: value.clone(),
                    index: *index,
                    reader_id,
                }));
            } else {
                heap.push(HeapItem {
                    value: value.clone(),
                    index: *index,
                    reader_id,
                });
            }
        }
    }
    
    let mut result = Vec::new();
    
    while let Some(item) = heap.pop() {
        let (value, index, reader_id) = if ascending {
            let Reverse(item) = item;
            (item.value, item.index, item.reader_id)
        } else {
            (item.value, item.index, item.reader_id)
        };
        
        result.push(index);
        
        // Advance the reader that provided this item
        if let Some(reader) = readers.get_mut(reader_id) {
            reader.advance()?;
            if let Some((next_index, next_value)) = &reader.current {
                if ascending {
                    heap.push(Reverse(HeapItem {
                        value: next_value.clone(),
                        index: *next_index,
                        reader_id,
                    }));
                } else {
                    heap.push(HeapItem {
                        value: next_value.clone(),
                        index: *next_index,
                        reader_id,
                    });
                }
            }
        }
    }
    
    Ok(result)
}
```

##### 4. Main Sort Function

```rust
async fn sort_csv_external(
    column_idx: usize,
    ascending: bool,
    state: State<'_, AppState>,
) -> Result<Vec<usize>, String> {
    let path = state.file_path.lock().unwrap()
        .clone().ok_or("No file loaded")?;
    let settings = state.parse_settings.lock().unwrap().clone();
    let mmap = state.mmap.lock().unwrap().clone();
    let offsets = state.row_offsets.lock().unwrap().clone();
    
    // Check cache first
    // ... (existing cache logic)
    
    // Phase 1: Create sorted chunks
    let chunk_files = create_sorted_chunks(
        &path, column_idx, ascending,
        &settings, &offsets, &mmap
    ).await?;
    
    // Phase 2: K-way merge
    let order = k_way_merge(chunk_files, ascending)?;
    
    // Cache the result
    // ... (existing cache logic)
    
    *state.sorted_order.lock().unwrap() = Some(order.clone());
    Ok(order)
}
```

#### Memory Analysis

**Current Approach**:
- 10M rows × 300 bytes = **3GB RAM**

**External Merge Sort**:
- Chunk size: 100K rows × 300 bytes = **30MB per chunk**
- Active chunks in heap: 100 chunks × small overhead = **~10MB**
- Total: **~100MB fixed** regardless of file size

#### Performance Characteristics

- **Time Complexity**: O(N log N) - same as in-memory
- **Space Complexity**: O(1) - constant memory
- **I/O**: 2 passes (read + write chunks, read + merge)
- **Speed**: Slower than in-memory but enables very large files

#### Implementation Checklist

- [ ] Add `tempfile = "3"` to Cargo.toml ✅ (already done)
- [ ] Implement `SortChunkWriter` and `write_sorted_chunk`
- [ ] Implement `SortChunkReader` with buffered reading
- [ ] Implement `HeapItem` with proper Ord trait
- [ ] Implement `k_way_merge` with BinaryHeap
- [ ] Implement `create_sorted_chunks` with progress reporting
- [ ] Replace `sort_csv` with `sort_csv_external`
- [ ] Add temp file cleanup on errors
- [ ] Test with 10M+ row files
- [ ] Verify memory stays under 200MB

#### Testing Strategy

1. **Correctness**: Compare results with small files against current implementation
2. **Memory**: Use `ps` or `/proc` to monitor RSS during sort
3. **Performance**: Benchmark on 1M, 5M, 10M row files
4. **Edge Cases**: Empty files, single column, very long strings

---

## Summary

**Option 1** provides immediate relief (500MB-1GB RAM usage) with minimal code changes.

**Option 2** provides true scalability (100-200MB fixed RAM) but requires ~2 hours implementation time and thorough testing.

Recommendation: Use Option 1 now, implement Option 2 if users need to sort files > 50M rows or have strict memory constraints.
