# High-Performance CSV Viewer for Tauri (500k+ Rows)

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Backend Optimizations (Rust)](#backend-optimizations-rust)
3. [Frontend Optimizations](#frontend-optimizations)
4. [Advanced Techniques](#advanced-techniques)
5. [Implementation Checklist](#implementation-checklist)

---

## Architecture Overview

### Key Principles
- **Lazy Loading**: Never load all data at once
- **Virtual Scrolling**: Only render visible rows
- **Backend Processing**: Heavy operations in Rust
- **Streaming**: Process data in chunks
- **Caching**: Smart data caching strategy

### Data Flow
```
CSV File → Rust Parser → Chunked Data → Frontend Cache → Virtual Scroller → DOM
```

---

## Backend Optimizations (Rust)

### 1. Streaming CSV Parser

**Why**: Loading 500k rows into memory crashes the app. Streaming reads one row at a time.

**Implementation**:

```rust
// Cargo.toml
[dependencies]
csv = "1.3"
serde = { version = "1.0", features = ["derive"] }
rayon = "1.8"  # For parallel processing
memmap2 = "0.9"  # For memory-mapped files
```

```rust
// src-tauri/src/csv_handler.rs
use csv::ReaderBuilder;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};

pub fn count_rows_fast(path: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);
    
    // Fast counting without parsing
    let count = rdr.records().count();
    Ok(count)
}

pub fn get_headers(path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    
    let headers = rdr.headers()?
        .iter()
        .map(|h| h.to_string())
        .collect();
    
    Ok(headers)
}

pub fn read_chunk(
    path: &str,
    start: usize,
    count: usize
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    
    let rows: Vec<Vec<String>> = rdr.records()
        .skip(start)
        .take(count)
        .filter_map(|r| r.ok())
        .map(|record| record.iter().map(|f| f.to_string()).collect())
        .collect();
    
    Ok(rows)
}
```

### 2. Memory-Mapped Files (For Very Large Files)

**Why**: For files >1GB, memory mapping lets the OS handle paging.

**Implementation**:

```rust
// src-tauri/src/csv_mmap.rs
use memmap2::Mmap;
use std::fs::File;

pub struct MmapCsvReader {
    mmap: Mmap,
    line_offsets: Vec<usize>,  // Cache line start positions
}

impl MmapCsvReader {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        
        // Build index of line offsets
        let mut line_offsets = vec![0];
        for (i, &byte) in mmap.iter().enumerate() {
            if byte == b'\n' {
                line_offsets.push(i + 1);
            }
        }
        
        Ok(Self { mmap, line_offsets })
    }
    
    pub fn get_line(&self, index: usize) -> Option<&str> {
        if index >= self.line_offsets.len() - 1 {
            return None;
        }
        
        let start = self.line_offsets[index];
        let end = self.line_offsets[index + 1].saturating_sub(1);
        
        std::str::from_utf8(&self.mmap[start..end]).ok()
    }
    
    pub fn total_lines(&self) -> usize {
        self.line_offsets.len().saturating_sub(1)
    }
}
```

### 3. Parallel Processing with Rayon

**Why**: Use all CPU cores for sorting, filtering, searching.

**Implementation**:

```rust
// src-tauri/src/csv_operations.rs
use rayon::prelude::*;

pub fn search_parallel(
    data: &[Vec<String>],
    column_idx: usize,
    query: &str
) -> Vec<usize> {
    data.par_iter()
        .enumerate()
        .filter_map(|(idx, row)| {
            if row.get(column_idx)?.contains(query) {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}

pub fn sort_by_column(
    data: &mut [Vec<String>],
    column_idx: usize,
    ascending: bool
) {
    data.par_sort_by(|a, b| {
        let cmp = a[column_idx].cmp(&b[column_idx]);
        if ascending { cmp } else { cmp.reverse() }
    });
}
```

### 4. Smart Caching with LRU

**Why**: Keep recently accessed chunks in memory.

**Implementation**:

```rust
// Add to Cargo.toml
// lru = "0.12"

use lru::LruCache;
use std::sync::Mutex;
use std::num::NonZeroUsize;

pub struct CsvCache {
    cache: Mutex<LruCache<(usize, usize), Vec<Vec<String>>>>,
}

impl CsvCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())),
        }
    }
    
    pub fn get(&self, start: usize, count: usize) -> Option<Vec<Vec<String>>> {
        self.cache.lock().unwrap().get(&(start, count)).cloned()
    }
    
    pub fn put(&self, start: usize, count: usize, data: Vec<Vec<String>>) {
        self.cache.lock().unwrap().put((start, count), data);
    }
}
```

### 5. Complete Tauri Command Setup

```rust
// src-tauri/src/main.rs
use std::sync::Mutex;
use tauri::State;

struct AppState {
    file_path: Mutex<Option<String>>,
    total_rows: Mutex<usize>,
    headers: Mutex<Vec<String>>,
    cache: CsvCache,
}

#[tauri::command]
async fn load_csv_metadata(
    path: String,
    state: State<'_, AppState>
) -> Result<(Vec<String>, usize), String> {
    let headers = get_headers(&path).map_err(|e| e.to_string())?;
    let count = count_rows_fast(&path).map_err(|e| e.to_string())?;
    
    *state.file_path.lock().unwrap() = Some(path);
    *state.total_rows.lock().unwrap() = count;
    *state.headers.lock().unwrap() = headers.clone();
    
    Ok((headers, count))
}

#[tauri::command]
async fn get_csv_chunk(
    start: usize,
    count: usize,
    state: State<'_, AppState>
) -> Result<Vec<Vec<String>>, String> {
    // Check cache first
    if let Some(cached) = state.cache.get(start, count) {
        return Ok(cached);
    }
    
    let path = state.file_path.lock().unwrap().clone()
        .ok_or("No file loaded")?;
    
    let data = read_chunk(&path, start, count).map_err(|e| e.to_string())?;
    
    // Cache the result
    state.cache.put(start, count, data.clone());
    
    Ok(data)
}

#[tauri::command]
async fn search_csv(
    column_idx: usize,
    query: String,
    state: State<'_, AppState>
) -> Result<Vec<usize>, String> {
    let path = state.file_path.lock().unwrap().clone()
        .ok_or("No file loaded")?;
    
    // Load all data for search (could be optimized further)
    let total = *state.total_rows.lock().unwrap();
    let data = read_chunk(&path, 0, total).map_err(|e| e.to_string())?;
    
    Ok(search_parallel(&data, column_idx, &query))
}

fn main() {
    tauri::Builder::default()
        .manage(AppState {
            file_path: Mutex::new(None),
            total_rows: Mutex::new(0),
            headers: Mutex::new(Vec::new()),
            cache: CsvCache::new(50), // Cache 50 chunks
        })
        .invoke_handler(tauri::generate_handler![
            load_csv_metadata,
            get_csv_chunk,
            search_csv
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

---

## Frontend Optimizations

### 1. Virtual Scrolling with TanStack Virtual

**Why**: Rendering 500k DOM nodes freezes the browser. Virtual scrolling renders only ~50 visible rows.

**Installation**:
```bash
npm install @tanstack/react-virtual
```

**Implementation**:

```typescript
// src/components/CsvTable.tsx
import { useVirtualizer } from '@tanstack/react-virtual';
import { useRef, useState, useEffect } from 'react';
import { invoke } from '@tauri-apps/api/tauri';

interface CsvTableProps {
  filePath: string;
  headers: string[];
  totalRows: number;
}

export function CsvTable({ headers, totalRows }: CsvTableProps) {
  const parentRef = useRef<HTMLDivElement>(null);
  const [data, setData] = useState<Map<number, string[]>>(new Map());
  const [loading, setLoading] = useState(false);

  const rowVirtualizer = useVirtualizer({
    count: totalRows,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 35, // Estimated row height
    overscan: 20, // Render 20 extra rows above/below viewport
  });

  // Load data as user scrolls
  useEffect(() => {
    const virtualItems = rowVirtualizer.getVirtualItems();
    if (virtualItems.length === 0) return;

    const startIndex = Math.max(0, virtualItems[0].index - 10);
    const endIndex = Math.min(
      totalRows,
      virtualItems[virtualItems.length - 1].index + 10
    );

    // Check if we need to load data
    const needsLoading = virtualItems.some(
      item => !data.has(item.index)
    );

    if (needsLoading && !loading) {
      setLoading(true);
      invoke<string[][]>('get_csv_chunk', {
        start: startIndex,
        count: endIndex - startIndex,
      }).then(chunk => {
        setData(prev => {
          const newData = new Map(prev);
          chunk.forEach((row, idx) => {
            newData.set(startIndex + idx, row);
          });
          return newData;
        });
        setLoading(false);
      });
    }
  }, [rowVirtualizer.range, totalRows, loading]);

  return (
    <div
      ref={parentRef}
      style={{
        height: '600px',
        overflow: 'auto',
        border: '1px solid #ddd',
      }}
    >
      <div
        style={{
          height: `${rowVirtualizer.getTotalSize()}px`,
          width: '100%',
          position: 'relative',
        }}
      >
        {/* Header Row */}
        <div
          style={{
            position: 'sticky',
            top: 0,
            backgroundColor: '#f0f0f0',
            zIndex: 1,
            display: 'flex',
            borderBottom: '2px solid #ddd',
          }}
        >
          {headers.map((header, idx) => (
            <div
              key={idx}
              style={{
                minWidth: '150px',
                padding: '8px',
                fontWeight: 'bold',
                borderRight: '1px solid #ddd',
              }}
            >
              {header}
            </div>
          ))}
        </div>

        {/* Virtual Rows */}
        {rowVirtualizer.getVirtualItems().map(virtualRow => {
          const rowData = data.get(virtualRow.index);
          
          return (
            <div
              key={virtualRow.index}
              style={{
                position: 'absolute',
                top: 0,
                left: 0,
                width: '100%',
                height: `${virtualRow.size}px`,
                transform: `translateY(${virtualRow.start}px)`,
                display: 'flex',
                borderBottom: '1px solid #eee',
              }}
            >
              {rowData ? (
                rowData.map((cell, cellIdx) => (
                  <div
                    key={cellIdx}
                    style={{
                      minWidth: '150px',
                      padding: '8px',
                      borderRight: '1px solid #eee',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {cell}
                  </div>
                ))
              ) : (
                <div style={{ padding: '8px' }}>Loading...</div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
```

### 2. Debounced Search

**Why**: Prevent excessive backend calls while typing.

```typescript
// src/hooks/useDebounce.ts
import { useEffect, useState } from 'react';

export function useDebounce<T>(value: T, delay: number): T {
  const [debouncedValue, setDebouncedValue] = useState(value);

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedValue(value);
    }, delay);

    return () => clearTimeout(handler);
  }, [value, delay]);

  return debouncedValue;
}

// Usage in Search Component
function SearchBar() {
  const [searchTerm, setSearchTerm] = useState('');
  const debouncedSearch = useDebounce(searchTerm, 500);

  useEffect(() => {
    if (debouncedSearch) {
      invoke('search_csv', {
        columnIdx: 0,
        query: debouncedSearch,
      }).then(results => {
        console.log('Search results:', results);
      });
    }
  }, [debouncedSearch]);

  return (
    <input
      type="text"
      value={searchTerm}
      onChange={e => setSearchTerm(e.target.value)}
      placeholder="Search..."
    />
  );
}
```

### 3. Progressive Loading Indicator

**Why**: Give user feedback during loading.

```typescript
// src/components/LoadingProgress.tsx
import { useState, useEffect } from 'react';

export function LoadingProgress({ total }: { total: number }) {
  const [loaded, setLoaded] = useState(0);
  const [speed, setSpeed] = useState(0);

  const percentage = (loaded / total) * 100;

  return (
    <div style={{ padding: '20px' }}>
      <div style={{ marginBottom: '10px' }}>
        Loading: {loaded.toLocaleString()} / {total.toLocaleString()} rows
        ({percentage.toFixed(1)}%)
      </div>
      <div style={{
        width: '100%',
        height: '20px',
        backgroundColor: '#e0e0e0',
        borderRadius: '10px',
        overflow: 'hidden',
      }}>
        <div style={{
          width: `${percentage}%`,
          height: '100%',
          backgroundColor: '#4caf50',
          transition: 'width 0.3s ease',
        }} />
      </div>
      <div style={{ marginTop: '5px', fontSize: '12px', color: '#666' }}>
        {speed > 0 && `${speed.toLocaleString()} rows/sec`}
      </div>
    </div>
  );
}
```

### 4. Web Workers for Heavy Frontend Processing

**Why**: Keep UI responsive during filtering/sorting.

```typescript
// src/workers/csv.worker.ts
self.addEventListener('message', (e) => {
  const { type, data } = e.data;

  switch (type) {
    case 'FILTER':
      const filtered = data.rows.filter((row: string[]) =>
        row.some(cell => cell.toLowerCase().includes(data.query.toLowerCase()))
      );
      self.postMessage({ type: 'FILTER_RESULT', data: filtered });
      break;

    case 'SORT':
      const sorted = [...data.rows].sort((a, b) => {
        const aVal = a[data.columnIdx];
        const bVal = b[data.columnIdx];
        return data.ascending
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal);
      });
      self.postMessage({ type: 'SORT_RESULT', data: sorted });
      break;
  }
});

// Usage in component
const worker = new Worker(new URL('./workers/csv.worker.ts', import.meta.url));

worker.postMessage({
  type: 'FILTER',
  data: { rows: allRows, query: searchTerm },
});

worker.addEventListener('message', (e) => {
  if (e.data.type === 'FILTER_RESULT') {
    setFilteredRows(e.data.data);
  }
});
```

---

## Advanced Techniques

### 1. Column-Oriented Storage

**Why**: Faster column operations (filtering, aggregations).

```rust
// src-tauri/src/columnar.rs
pub struct ColumnStore {
    columns: Vec<Vec<String>>,
    row_count: usize,
}

impl ColumnStore {
    pub fn from_csv(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
        
        let headers = rdr.headers()?.len();
        let mut columns: Vec<Vec<String>> = vec![Vec::new(); headers];
        let mut row_count = 0;
        
        for result in rdr.records() {
            let record = result?;
            for (idx, field) in record.iter().enumerate() {
                columns[idx].push(field.to_string());
            }
            row_count += 1;
        }
        
        Ok(Self { columns, row_count })
    }
    
    pub fn filter_column(&self, col_idx: usize, predicate: &str) -> Vec<usize> {
        self.columns[col_idx]
            .par_iter()
            .enumerate()
            .filter_map(|(idx, val)| {
                if val.contains(predicate) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect()
    }
    
    pub fn get_column_stats(&self, col_idx: usize) -> ColumnStats {
        let col = &self.columns[col_idx];
        ColumnStats {
            unique_values: col.iter().collect::<std::collections::HashSet<_>>().len(),
            min: col.iter().min().cloned(),
            max: col.iter().max().cloned(),
        }
    }
}
```

### 2. Incremental Indexing

**Why**: Build search index in background without blocking.

```rust
// src-tauri/src/indexer.rs
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct SearchIndex {
    index: Arc<RwLock<HashMap<String, Vec<usize>>>>,
}

impl SearchIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn build_incremental(
        &self,
        path: String,
        column_idx: usize,
        chunk_size: usize,
    ) {
        let file = File::open(&path).unwrap();
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
        
        let mut row_idx = 0;
        let mut buffer = Vec::new();
        
        for result in rdr.records() {
            let record = result.unwrap();
            if let Some(value) = record.get(column_idx) {
                buffer.push((value.to_string(), row_idx));
            }
            row_idx += 1;
            
            // Process in chunks
            if buffer.len() >= chunk_size {
                self.process_chunk(&buffer).await;
                buffer.clear();
            }
        }
        
        // Process remaining
        if !buffer.is_empty() {
            self.process_chunk(&buffer).await;
        }
    }
    
    async fn process_chunk(&self, chunk: &[(String, usize)]) {
        let mut index = self.index.write().await;
        for (value, row_idx) in chunk {
            index.entry(value.clone())
                .or_insert_with(Vec::new)
                .push(*row_idx);
        }
    }
    
    pub async fn search(&self, query: &str) -> Vec<usize> {
        let index = self.index.read().await;
        index.get(query).cloned().unwrap_or_default()
    }
}
```

### 3. Compression for Network Transfer

**Why**: Reduce data transfer between Rust and JS.

```rust
// Add to Cargo.toml
// flate2 = "1.0"

use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;

#[tauri::command]
async fn get_csv_chunk_compressed(
    start: usize,
    count: usize,
    state: State<'_, AppState>
) -> Result<Vec<u8>, String> {
    let data = get_csv_chunk(start, count, state).await?;
    let json = serde_json::to_string(&data).map_err(|e| e.to_string())?;
    
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(json.as_bytes()).map_err(|e| e.to_string())?;
    let compressed = encoder.finish().map_err(|e| e.to_string())?;
    
    Ok(compressed)
}
```

### 4. Database Backend for Complex Queries

**Why**: SQLite can handle complex filtering, sorting, joins efficiently.

```rust
// Add to Cargo.toml
// rusqlite = { version = "0.30", features = ["bundled"] }

use rusqlite::{Connection, params};

pub fn import_to_sqlite(csv_path: &str, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open(db_path)?;
    
    // Create table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY,
            row_data TEXT
        )",
        [],
    )?;
    
    // Import CSV
    let file = File::open(csv_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    
    let mut stmt = conn.prepare("INSERT INTO data (row_data) VALUES (?1)")?;
    
    for result in rdr.records() {
        let record = result?;
        let json = serde_json::to_string(&record)?;
        stmt.execute(params![json])?;
    }
    
    Ok(())
}

#[tauri::command]
async fn query_sqlite(
    db_path: String,
    query: String,
) -> Result<Vec<Vec<String>>, String> {
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    let mut stmt = conn.prepare(&query).map_err(|e| e.to_string())?;
    
    let rows = stmt.query_map([], |row| {
        let json: String = row.get(0)?;
        Ok(serde_json::from_str(&json).unwrap())
    }).map_err(|e| e.to_string())?;
    
    let results: Vec<Vec<String>> = rows.filter_map(|r| r.ok()).collect();
    Ok(results)
}
```

---

## Implementation Checklist

### Phase 1: Basic Setup (Day 1)
- [ ] Set up Tauri project with Rust backend
- [ ] Install dependencies: `csv`, `serde`, `rayon`
- [ ] Implement basic CSV streaming parser
- [ ] Create Tauri commands for metadata and chunks
- [ ] Set up React with TanStack Virtual
- [ ] Implement basic virtual scrolling table

### Phase 2: Performance (Day 2-3)
- [ ] Add LRU caching on backend
- [ ] Implement frontend data caching
- [ ] Add debounced search
- [ ] Implement parallel search with Rayon
- [ ] Add loading indicators and progress bars
- [ ] Optimize chunk size (test with 1000, 5000, 10000 rows)

### Phase 3: Advanced Features (Day 4-5)
- [ ] Add memory-mapped file support for huge files
- [ ] Implement column-oriented storage option
- [ ] Add incremental indexing
- [ ] Implement sorting with backend
- [ ] Add filtering with multiple conditions
- [ ] Create export functionality

### Phase 4: Polish (Day 6-7)
- [ ] Add keyboard navigation
- [ ] Implement column resizing
- [ ] Add theme support
- [ ] Optimize bundle size
- [ ] Add error handling and recovery
- [ ] Performance testing with real datasets
- [ ] Memory profiling

### Performance Benchmarks to Aim For
- **Initial load**: < 500ms for metadata
- **Chunk loading**: < 100ms for 5000 rows
- **Search**: < 2s for 500k rows
- **Sorting**: < 3s for 500k rows
- **Memory usage**: < 500MB for 500k rows
- **Smooth scrolling**: 60 FPS

### Testing Strategy
```bash
# Generate test CSV
# Create 500k row CSV for testing
head -n 1 sample.csv > test_500k.csv
for i in {1..500000}; do
  echo "Row$i,Value$i,Data$i" >> test_500k.csv
done

# Monitor memory
# Use Activity Monitor (Mac) or Task Manager (Windows)
# Or install process monitoring:
cargo install bottom  # Modern system monitor
btm  # Run while testing
```

### Common Pitfalls to Avoid

1. **Don't**: Load entire file into memory
   **Do**: Use streaming and chunks

2. **Don't**: Render all rows in DOM
   **Do**: Use virtual scrolling

3. **Don't**: Parse CSV on frontend
   **Do**: Parse in Rust backend

4. **Don't**: Block UI thread
   **Do**: Use async operations and workers

5. **Don't**: Re-parse for every operation
   **Do**: Cache parsed data intelligently

6. **Don't**: Use synchronous file I/O
   **Do**: Use async Tokio runtime

7. **Don't**: Send huge payloads over IPC
   **Do**: Use compression or pagination

8. **Don't**: Ignore memory leaks
   **Do**: Profile and clean up regularly

---

## Example tauri.conf.json Optimization

```json
{
  "build": {
    "beforeBuildCommand": "npm run build",
    "beforeDevCommand": "npm run dev",
    "devPath": "http://localhost:5173",
    "distDir": "../dist"
  },
  "tauri": {
    "allowlist": {
      "all": false,
      "fs": {
        "all": false,
        "readFile": true,
        "scope": ["$APPDATA/*", "$RESOURCE/*"]
      },
      "dialog": {
        "all": false,
        "open": true
      }
    },
    "bundle": {
      "active": true,
      "targets": "all",
      "identifier": "com.csvviewer.app",
      "icon": [
        "icons/icon.png"
      ]
    },
    "security": {
      "csp": null
    },
    "windows": [
      {
        "fullscreen": false,
        "height": 800,
        "resizable": true,
        "title": "High-Performance CSV Viewer",
        "width": 1200,
        "minWidth": 800,
        "minHeight": 600
      }
    ]
  }
}
```

---

## Final Tips

1. **Profile First**: Use `cargo flamegraph` to find bottlenecks
2. **Test with Real Data**: Use actual 500k+ row files from production
3. **Incremental Optimization**: Don't optimize prematurely, measure first
4. **User Feedback**: Add loading states, progress bars, cancellation
5. **Error Handling**: Handle corrupted CSVs, encoding issues gracefully
6. **Documentation**: Document chunk sizes, cache limits for future tuning

This guide should get you to a production-ready CSV viewer handling 500k+ rows smoothly!
