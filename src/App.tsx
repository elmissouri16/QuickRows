import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import type { CSSProperties } from "react";
import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useDebounce } from "./hooks/useDebounce";
import "./App.css";

const DEFAULT_ROW_HEIGHT = 36;
const PREFETCH = 20;
const CHUNK_SIZE = 800;
const COLUMN_WIDTH_MIN = 120;
const SETTINGS_KEY = "csv-viewer.settings";
const ROW_HEIGHT_OPTIONS = new Set([28, 36, 44]);

type SortDirection = "asc" | "desc";
type SortState = { column: number; direction: SortDirection };
type SortedRow = { index: number; row: string[] };

const clampColumnWidth = (value: number) => Math.max(COLUMN_WIDTH_MIN, value);

function App() {
  const [filePath, setFilePath] = useState<string | null>(null);
  const [checkingInitialOpen, setCheckingInitialOpen] = useState(true);
  const [headers, setHeaders] = useState<string[]>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [data, setData] = useState<Map<number, string[]>>(new Map());
  const [loadingRows, setLoadingRows] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [searchColumn, setSearchColumn] = useState(0);
  const [searchResults, setSearchResults] = useState<number[] | null>(null);
  const [searching, setSearching] = useState(false);
  const [showFind, setShowFind] = useState(false);
  const [currentMatch, setCurrentMatch] = useState(0);
  const [showIndex, setShowIndex] = useState(false);
  const [sortState, setSortState] = useState<SortState | null>(null);
  const [sortLoading, setSortLoading] = useState(false);
  const [rowIndexMap, setRowIndexMap] = useState<Map<number, number>>(
    new Map(),
  );
  const [sortedIndexLookup, setSortedIndexLookup] = useState<number[] | null>(
    null,
  );
  const [rowHeight, setRowHeight] = useState(DEFAULT_ROW_HEIGHT);
  const [columnWidth, setColumnWidth] = useState(160);
  const [columnWidths, setColumnWidths] = useState<number[]>([]);

  const debouncedSearch = useDebounce(searchTerm, 450);
  const parentRef = useRef<HTMLDivElement>(null);
  const headerRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const resizeRef = useRef<{
    columnIndex: number;
    startX: number;
    startWidth: number;
  } | null>(null);

  const rowVirtualizer = useVirtualizer({
    count: totalRows,
    getScrollElement: () => parentRef.current,
    estimateSize: () => rowHeight,
    overscan: 12,
  });

  const headerLabels = useMemo(
    () =>
      headers.map((header, idx) => {
        const trimmed = header.trim();
        return trimmed.length ? trimmed : `Column ${idx + 1}`;
      }),
    [headers],
  );

  const virtualItems = rowVirtualizer.getVirtualItems();
  const searchSet = useMemo(() => {
    if (!searchResults) return null;
    return new Set(searchResults);
  }, [searchResults]);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(SETTINGS_KEY);
      if (!raw) {
        return;
      }
      const parsed = JSON.parse(raw) as {
        showIndex?: boolean;
        rowHeight?: number;
        columnWidth?: number;
        columnWidths?: number[];
      };
      if (typeof parsed.showIndex === "boolean") {
        setShowIndex(parsed.showIndex);
      }
      if (typeof parsed.rowHeight === "number") {
        if (ROW_HEIGHT_OPTIONS.has(parsed.rowHeight)) {
          setRowHeight(parsed.rowHeight);
        }
      }
      if (typeof parsed.columnWidth === "number") {
        const nextWidth = clampColumnWidth(parsed.columnWidth);
        setColumnWidth(nextWidth);
      }
      if (Array.isArray(parsed.columnWidths)) {
        const nextWidths = parsed.columnWidths
          .filter((value) => typeof value === "number")
          .map((value) => clampColumnWidth(value));
        if (nextWidths.length) {
          setColumnWidths(nextWidths);
        }
      }
    } catch {
      // Ignore malformed settings.
    }
  }, []);

  useEffect(() => {
    const payload = {
      showIndex,
      rowHeight,
      columnWidth,
      columnWidths,
    };
    try {
      localStorage.setItem(SETTINGS_KEY, JSON.stringify(payload));
    } catch {
      // Ignore storage failures.
    }
  }, [showIndex, rowHeight, columnWidth, columnWidths]);

  useEffect(() => {
    invoke("set_show_index_checked", { checked: showIndex }).catch(() => {});
  }, [showIndex]);

  useEffect(() => {
    if (!headers.length) {
      setColumnWidths([]);
      return;
    }

    setColumnWidths((prev) => {
      const next = prev.slice(0, headers.length);
      while (next.length < headers.length) {
        next.push(columnWidth);
      }
      return next;
    });
  }, [columnWidth, headers]);

  const resetForNewFile = useCallback(() => {
    setError(null);
    setSortState(null);
    setSortLoading(false);
    setSortedIndexLookup(null);
    setRowIndexMap(new Map());
    setShowFind(false);
    setSearchTerm("");
    setSearchResults(null);
    setCurrentMatch(0);
    setData(new Map());
    invoke("clear_sort").catch(() => {});
  }, []);

  const handleOpenPath = useCallback(
    async (path: string) => {
      resetForNewFile();
      setFilePath(path);
      setHeaders([]);
      setTotalRows(0);

      try {
        const [csvHeaders, rowCount] = await invoke<[string[], number]>(
          "load_csv_metadata",
          { path },
        );
        setHeaders(csvHeaders);
        setTotalRows(rowCount);
        setSearchColumn(0);
      } catch (err) {
        setError(
          typeof err === "string" ? err : "Unable to load CSV metadata.",
        );
        setFilePath(null);
        setHeaders([]);
        setTotalRows(0);
      }
    },
    [resetForNewFile],
  );

  const handlePickFile = useCallback(async () => {
    const selected = await open({
      multiple: false,
      filters: [{ name: "CSV", extensions: ["csv"] }],
    });

    if (!selected || Array.isArray(selected)) {
      return;
    }

    handleOpenPath(selected);
  }, [handleOpenPath]);

  const handleClearFile = useCallback(() => {
    setFilePath(null);
    setHeaders([]);
    setTotalRows(0);
    setData(new Map());
    setSearchTerm("");
    setSearchResults(null);
    setError(null);
    setShowFind(false);
    setSortState(null);
    setSortLoading(false);
    setSortedIndexLookup(null);
    setRowIndexMap(new Map());
    invoke("clear_sort").catch(() => {});
  }, []);

  useEffect(() => {
    if (!filePath || totalRows === 0 || loadingRows || sortLoading) {
      return;
    }
    if (virtualItems.length === 0) {
      return;
    }

    const startIndex = Math.max(0, virtualItems[0].index - PREFETCH);
    const endIndex = Math.min(
      totalRows,
      virtualItems[virtualItems.length - 1].index + PREFETCH + 1,
    );
    const rangeCount = endIndex - startIndex;

    if (rangeCount <= 0) {
      return;
    }

    const needsLoading = virtualItems.some((item) => !data.has(item.index));

    if (!needsLoading) {
      return;
    }

    setLoadingRows(true);
    const sortKey = sortState
      ? `${sortState.column}:${sortState.direction}`
      : null;
    const command = sortState ? "get_sorted_chunk" : "get_csv_chunk";
    const requestedCount = Math.min(
      Math.max(rangeCount, CHUNK_SIZE),
      totalRows - startIndex,
    );
    invoke<string[][] | SortedRow[]>(command, {
      start: startIndex,
      count: requestedCount,
    })
      .then((chunk) => {
        const currentSortKey = sortState
          ? `${sortState.column}:${sortState.direction}`
          : null;
        if (sortKey !== currentSortKey) {
          return;
        }
        if (sortState) {
          const sortedChunk = chunk as SortedRow[];
          setData((prev) => {
            const next = new Map(prev);
            sortedChunk.forEach((row, idx) => {
              next.set(startIndex + idx, row.row);
            });
            return next;
          });
          setRowIndexMap((prev) => {
            const next = new Map(prev);
            sortedChunk.forEach((row, idx) => {
              next.set(startIndex + idx, row.index);
            });
            return next;
          });
        } else {
          const rows = chunk as string[][];
          setData((prev) => {
            const next = new Map(prev);
            rows.forEach((row, idx) => {
              next.set(startIndex + idx, row);
            });
            return next;
          });
        }
      })
      .catch((err) => {
        setError(typeof err === "string" ? err : "Failed to load CSV rows.");
      })
      .finally(() => setLoadingRows(false));
  }, [
    data,
    filePath,
    loadingRows,
    sortLoading,
    sortState,
    totalRows,
    virtualItems,
  ]);

  useEffect(() => {
    rowVirtualizer.measure();
  }, [rowHeight, rowVirtualizer]);

  useEffect(() => {
    if (showFind) {
      searchInputRef.current?.focus();
      searchInputRef.current?.select();
    }
  }, [showFind]);

  useEffect(() => {
    const scrollTarget = parentRef.current;
    const header = headerRef.current;
    if (!scrollTarget || !header) {
      return;
    }

    const handleScroll = () => {
      header.scrollLeft = scrollTarget.scrollLeft;
    };

    scrollTarget.addEventListener("scroll", handleScroll, { passive: true });
    return () => {
      scrollTarget.removeEventListener("scroll", handleScroll);
    };
  }, [filePath]);

  useEffect(() => {
    if (!filePath) {
      return;
    }
    if (!debouncedSearch) {
      setSearchResults(null);
      return;
    }

    setSearching(true);
    invoke<number[]>("search_csv", {
      columnIdx: searchColumn,
      query: debouncedSearch,
    })
      .then((results) => setSearchResults(results))
      .catch((err) => {
        setError(typeof err === "string" ? err : "Search failed to complete.");
      })
      .finally(() => setSearching(false));
  }, [debouncedSearch, filePath, searchColumn]);

  const scrollToMatch = useCallback(
    (matchIndex: number) => {
      if (!searchResults?.length) {
        return;
      }
      const originalIndex = searchResults[matchIndex];
      const displayIndex = sortState
        ? sortedIndexLookup?.[originalIndex]
        : originalIndex;
      if (displayIndex === undefined) {
        return;
      }
      rowVirtualizer.scrollToIndex(displayIndex, { align: "center" });
    },
    [rowVirtualizer, searchResults, sortState, sortedIndexLookup],
  );

  useEffect(() => {
    if (!searchResults?.length) {
      setCurrentMatch(0);
      return;
    }
    setCurrentMatch(0);
    scrollToMatch(0);
  }, [scrollToMatch, searchResults]);

  const goToNextMatch = useCallback(() => {
    if (!searchResults?.length) {
      return;
    }
    setCurrentMatch((prev) => {
      const next = (prev + 1) % searchResults.length;
      scrollToMatch(next);
      return next;
    });
  }, [scrollToMatch, searchResults]);

  const goToPrevMatch = useCallback(() => {
    if (!searchResults?.length) {
      return;
    }
    setCurrentMatch((prev) => {
      const next = (prev - 1 + searchResults.length) % searchResults.length;
      scrollToMatch(next);
      return next;
    });
  }, [scrollToMatch, searchResults]);

  useEffect(() => {
    if (!filePath) {
      return;
    }

    if (!sortState) {
      setSortedIndexLookup(null);
      setRowIndexMap(new Map());
      setData(new Map());
      invoke("clear_sort").catch(() => {});
      return;
    }

    const sortKey = `${sortState.column}:${sortState.direction}`;
    setSortLoading(true);
    setRowIndexMap(new Map());
    setData(new Map());
    invoke<number[]>("sort_csv", {
      columnIdx: sortState.column,
      ascending: sortState.direction === "asc",
    })
      .then((order) => {
        const currentKey = sortState
          ? `${sortState.column}:${sortState.direction}`
          : null;
        if (currentKey !== sortKey) {
          return;
        }
        const lookup = new Array(order.length);
        order.forEach((original, displayIndex) => {
          lookup[original] = displayIndex;
        });
        setSortedIndexLookup(lookup);
        rowVirtualizer.scrollToIndex(0);
      })
      .catch((err) => {
        setError(typeof err === "string" ? err : "Failed to sort CSV.");
        setSortState(null);
      })
      .finally(() => setSortLoading(false));
  }, [filePath, rowVirtualizer, sortState]);

  useEffect(() => {
    let active = true;
    let unlistenFns: Array<() => void> = [];

    const setupMenuListeners = async () => {
      const fns = await Promise.all([
        listen("menu-open", () => {
          handlePickFile();
        }),
        listen("menu-clear", () => {
          handleClearFile();
        }),
        listen("menu-find", () => {
          setShowFind(true);
        }),
        listen("menu-clear-search", () => {
          setSearchTerm("");
          setSearchResults(null);
          setCurrentMatch(0);
        }),
        listen<number>("menu-row-height", (event) => {
          setRowHeight(event.payload);
        }),
        listen<number>("menu-column-width", (event) => {
          const nextWidth = clampColumnWidth(event.payload);
          setColumnWidth(nextWidth);
          setColumnWidths((prev) => prev.map(() => nextWidth));
        }),
        listen<boolean>("menu-show-index", (event) => {
          setShowIndex(event.payload);
        }),
        listen("menu-next-match", () => {
          goToNextMatch();
        }),
        listen("menu-prev-match", () => {
          goToPrevMatch();
        }),
        listen("menu-close-find", () => {
          setShowFind(false);
        }),
      ]);

      if (!active) {
        fns.forEach((fn) => fn());
        return;
      }

      unlistenFns = fns;
    };

    setupMenuListeners();

    return () => {
      active = false;
      unlistenFns.forEach((fn) => fn());
    };
  }, [goToNextMatch, goToPrevMatch, handleClearFile, handlePickFile]);

  useEffect(() => {
    let active = true;

    invoke<string | null>("take_pending_open")
      .then((path) => {
        if (!active) {
          return;
        }
        if (path) {
          return handleOpenPath(path);
        }
      })
      .catch(() => {})
      .finally(() => {
        if (active) {
          setCheckingInitialOpen(false);
        }
      });

    return () => {
      active = false;
    };
  }, [handleOpenPath]);

  useEffect(() => {
    const handleKeydown = (event: KeyboardEvent) => {
      if (!event.ctrlKey && !event.metaKey) {
        return;
      }
      const key = event.key.toLowerCase();
      if (key === "f") {
        event.preventDefault();
        setShowFind(true);
        return;
      }
      if (key === "o" || key === "r") {
        event.preventDefault();
      }
    };

    window.addEventListener("keydown", handleKeydown);
    return () => {
      window.removeEventListener("keydown", handleKeydown);
    };
  }, []);

  const defaultColumnStyle = useMemo(() => {
    const width = `${columnWidth}px`;
    return {
      width,
      minWidth: width,
      maxWidth: width,
      flex: `0 0 ${width}`,
    } as CSSProperties;
  }, [columnWidth]);

  const columnStyles = useMemo(
    () =>
      headerLabels.map((_, idx) => {
        const widthValue = columnWidths[idx] ?? columnWidth;
        const width = `${widthValue}px`;
        return {
          width,
          minWidth: width,
          maxWidth: width,
          flex: `0 0 ${width}`,
        } as CSSProperties;
      }),
    [columnWidth, columnWidths, headerLabels],
  );

  const handleResizeStart = useCallback(
    (event: React.MouseEvent<HTMLDivElement>, columnIndex: number) => {
      if (event.button !== 0) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();

      const startWidth = columnWidths[columnIndex] ?? columnWidth;
      resizeRef.current = {
        columnIndex,
        startX: event.clientX,
        startWidth,
      };

      const handleMove = (moveEvent: MouseEvent) => {
        const current = resizeRef.current;
        if (!current) {
          return;
        }
        const delta = moveEvent.clientX - current.startX;
        const nextWidth = clampColumnWidth(current.startWidth + delta);
        setColumnWidths((prev) => {
          const next = prev.length
            ? [...prev]
            : headerLabels.map(() => columnWidth);
          next[current.columnIndex] = nextWidth;
          return next;
        });
      };

      const handleUp = () => {
        resizeRef.current = null;
        window.removeEventListener("mousemove", handleMove);
        window.removeEventListener("mouseup", handleUp);
      };

      window.addEventListener("mousemove", handleMove);
      window.addEventListener("mouseup", handleUp);
    },
    [columnWidth, columnWidths, headerLabels],
  );

  const handleHeaderClick = (columnIndex: number) => {
    if (sortLoading) {
      return;
    }
    setSortState((prev) => {
      if (!prev || prev.column !== columnIndex) {
        return { column: columnIndex, direction: "asc" };
      }
      if (prev.direction === "asc") {
        return { column: columnIndex, direction: "desc" };
      }
      return null;
    });
  };

  return (
    <main
      className="app"
      style={
        {
          "--cell-min": `${columnWidth}px`,
          "--row-height": `${rowHeight}px`,
        } as CSSProperties
      }
    >
      {error ? <div className="status">{error}</div> : null}

      <section className="table-shell">
        {showFind ? (
          <div className={`find-panel${showIndex ? " with-index" : ""}`}>
            <div className="find-controls">
              <select
                value={searchColumn}
                onChange={(event) =>
                  setSearchColumn(Number(event.target.value))
                }
                disabled={!headers.length}
              >
                {headerLabels.map((header, idx) => (
                  <option value={idx} key={`${header}-${idx}`}>
                    {header}
                  </option>
                ))}
              </select>
              <input
                type="text"
                value={searchTerm}
                onChange={(event) => setSearchTerm(event.target.value)}
                placeholder="Search selected column"
                disabled={!filePath}
                ref={searchInputRef}
                onKeyDown={(event) => {
                  if (event.key === "Escape") {
                    setShowFind(false);
                  }
                  if (event.key === "Enter") {
                    event.preventDefault();
                    if (event.shiftKey) {
                      goToPrevMatch();
                    } else {
                      goToNextMatch();
                    }
                  }
                }}
              />
              <button
                className="btn subtle"
                onClick={() => {
                  setSearchTerm("");
                  setSearchResults(null);
                  setCurrentMatch(0);
                }}
                disabled={!searchTerm}
              >
                Clear
              </button>
              <button
                className="btn subtle"
                onClick={goToPrevMatch}
                disabled={!searchResults?.length}
              >
                Prev
              </button>
              <button
                className="btn subtle"
                onClick={goToNextMatch}
                disabled={!searchResults?.length}
              >
                Next
              </button>
              <span className="find-count">
                {searchResults?.length
                  ? `${currentMatch + 1}/${searchResults.length}`
                  : "0/0"}
              </span>
              <button className="btn subtle" onClick={() => setShowFind(false)}>
                Close
              </button>
            </div>
            <div className="find-meta">
              {searching
                ? "Searching..."
                : searchResults
                  ? `${searchResults.length.toLocaleString()} matches`
                  : filePath
                    ? "Ready"
                    : "No file loaded"}
            </div>
          </div>
        ) : null}
        {!filePath ? (
          checkingInitialOpen ? (
            <div className="empty-state loading">
              <h2>Opening file...</h2>
              <p>Preparing the CSV viewer.</p>
            </div>
          ) : (
            <div className="empty-state">
              <h2>Open a CSV to start</h2>
              <p>Streaming keeps the table responsive for large files.</p>
              <button className="btn primary" onClick={handlePickFile}>
                Open CSV
              </button>
            </div>
          )
        ) : (
          <div className="table-wrapper">
            <div className="table-header" ref={headerRef}>
              {showIndex ? (
                <div className="table-cell header index">#</div>
              ) : null}
              {headerLabels.map((header, idx) => (
                <div
                  key={`${header}-${idx}`}
                  className={`table-cell header sortable${sortState?.column === idx ? " active" : ""}`}
                  onClick={() => handleHeaderClick(idx)}
                  role="button"
                  tabIndex={0}
                  style={columnStyles[idx] ?? defaultColumnStyle}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      handleHeaderClick(idx);
                    }
                  }}
                >
                  <span className="header-label">{header}</span>
                  {sortState?.column === idx ? (
                    <span className="sort-indicator">
                      {sortState.direction === "asc" ? "▲" : "▼"}
                    </span>
                  ) : null}
                  <div
                    className="col-resizer"
                    onMouseDown={(event) => handleResizeStart(event, idx)}
                    onClick={(event) => event.stopPropagation()}
                  />
                </div>
              ))}
            </div>
            <div ref={parentRef} className="table-body">
              <div
                className="table-spacer"
                style={{
                  height: `${rowVirtualizer.getTotalSize()}px`,
                }}
              >
                {virtualItems.map((virtualRow) => {
                  const rowData = data.get(virtualRow.index);
                  const originalIndex = sortState
                    ? rowIndexMap.get(virtualRow.index)
                    : virtualRow.index;
                  const rowNumber = (originalIndex ?? virtualRow.index) + 1;
                  const currentIndex = searchResults?.[currentMatch];
                  const isMatch =
                    originalIndex !== undefined
                      ? searchSet?.has(originalIndex)
                      : false;
                  const isCurrent =
                    originalIndex !== undefined &&
                    currentIndex !== undefined &&
                    originalIndex === currentIndex;
                  const isEven = virtualRow.index % 2 === 0;

                  return (
                    <div
                      key={virtualRow.index}
                      className={`table-row${isMatch ? " match" : ""}${isCurrent ? " current" : ""}${isEven ? " even" : " odd"}`}
                      style={{
                        height: `${virtualRow.size}px`,
                        transform: `translateY(${virtualRow.start}px)`,
                      }}
                    >
                      {showIndex ? (
                        <div className="table-cell index">{rowNumber}</div>
                      ) : null}
                      {rowData ? (
                        rowData.map((cell, cellIdx) => (
                          <div
                            key={cellIdx}
                            className="table-cell"
                            style={columnStyles[cellIdx] ?? defaultColumnStyle}
                            title={cell}
                          >
                            {cell}
                          </div>
                        ))
                      ) : (
                        <div className="table-cell loading">Loading...</div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
            {loadingRows ? (
              <div className="loading-strip">Fetching more rows...</div>
            ) : null}
          </div>
        )}
      </section>
    </main>
  );
}

export default App;
