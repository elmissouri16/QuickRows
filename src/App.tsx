import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import type { CSSProperties } from "react";
import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useDebounce } from "./hooks/useDebounce";
import "./App.css";

const BASE_TITLE = "csv-viewer";
const DEFAULT_ROW_HEIGHT = 36;
const PREFETCH = 12;
const CHUNK_SIZE = 800;
const MAX_CACHED_ROWS = CHUNK_SIZE * 12;
const ROW_COUNT_POLL_INTERVAL = 250;
const ROW_COUNT_POLL_MAX = 60;
const COLUMN_WIDTH_MIN = 120;
const SETTINGS_KEY = "csv-viewer.settings";
const MAX_RECENT_FILES = 6;
const ROW_HEIGHT_OPTIONS = new Set([28, 36, 44]);

type SortDirection = "asc" | "desc";
type SortState = { column: number; direction: SortDirection };
type SortedRow = { index: number; row: string[] };
type ThemeMode = "light" | "dark";
type ThemePreference = ThemeMode | "system";
type ContextMenuState = {
  x: number;
  y: number;
  cellText: string | null;
  rowText: string;
};

const clampColumnWidth = (value: number) => Math.max(COLUMN_WIDTH_MIN, value);
const getSystemTheme = (): ThemeMode => {
  if (typeof window !== "undefined") {
    if (window.matchMedia?.("(prefers-color-scheme: dark)").matches) {
      return "dark";
    }
  }
  return "light";
};
const getDirFromPath = (path: string) => {
  const slashIndex = path.lastIndexOf("/");
  const backslashIndex = path.lastIndexOf("\\");
  const lastIndex = Math.max(slashIndex, backslashIndex);
  if (lastIndex < 0) {
    return null;
  }
  if (lastIndex === 0) {
    return path.slice(0, 1);
  }
  if (backslashIndex === lastIndex && lastIndex === 2 && path[1] === ":") {
    return path.slice(0, lastIndex + 1);
  }
  return path.slice(0, lastIndex);
};
const getFileNameFromPath = (path: string) => {
  const slashIndex = path.lastIndexOf("/");
  const backslashIndex = path.lastIndexOf("\\");
  const lastIndex = Math.max(slashIndex, backslashIndex);
  if (lastIndex < 0 || lastIndex === path.length - 1) {
    return path;
  }
  return path.slice(lastIndex + 1);
};
const setWindowTitle = (path: string | null) => {
  const name = path ? getFileNameFromPath(path) : "";
  const title = name ? `${name} - ${BASE_TITLE}` : BASE_TITLE;
  getCurrentWindow()
    .setTitle(title)
    .catch(() => {});
};
const formatCsvCell = (value: string) => {
  if (!/[",\n\r]/.test(value)) {
    return value;
  }
  return `"${value.replace(/"/g, '""')}"`;
};
const formatCsvRow = (row: string[]) =>
  row.map((cell) => formatCsvCell(cell)).join(",");
const copyToClipboard = async (value: string) => {
  if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value);
      return true;
    } catch {
      // Fall through to the legacy path.
    }
  }
  if (typeof document === "undefined") {
    return false;
  }
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "absolute";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  textarea.setSelectionRange(0, textarea.value.length);
  const ok = document.execCommand("copy");
  document.body.removeChild(textarea);
  return ok;
};

function App() {
  const [filePath, setFilePath] = useState<string | null>(null);
  const [checkingInitialOpen, setCheckingInitialOpen] = useState(true);
  const [headers, setHeaders] = useState<string[]>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [rowCountReady, setRowCountReady] = useState(false);
  const [loadingRows, setLoadingRows] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [searchColumn, setSearchColumn] = useState<number | null>(0);
  const [searchResults, setSearchResults] = useState<number[] | null>(null);
  const [searching, setSearching] = useState(false);
  const [showFind, setShowFind] = useState(false);
  const [currentMatch, setCurrentMatch] = useState(0);
  const [showDuplicates, setShowDuplicates] = useState(false);
  const [duplicateColumn, setDuplicateColumn] = useState<number | null>(null);
  const [duplicateResults, setDuplicateResults] = useState<number[] | null>(
    null,
  );
  const [duplicateChecking, setDuplicateChecking] = useState(false);
  const [currentDuplicateMatch, setCurrentDuplicateMatch] = useState(0);
  const [activeHighlight, setActiveHighlight] = useState<
    "search" | "duplicates" | null
  >(null);
  const [showIndex, setShowIndex] = useState(false);
  const [sortState, setSortState] = useState<SortState | null>(null);
  const [sortLoading, setSortLoading] = useState(false);
  const [sortedIndexLookup, setSortedIndexLookup] = useState<number[] | null>(
    null,
  );
  const [rowHeight, setRowHeight] = useState(DEFAULT_ROW_HEIGHT);
  const [columnWidth, setColumnWidth] = useState(160);
  const [columnWidths, setColumnWidths] = useState<number[]>([]);
  const [themePreference, setThemePreference] =
    useState<ThemePreference>("system");
  const [resolvedTheme, setResolvedTheme] = useState<ThemeMode>(getSystemTheme);
  const [lastOpenDir, setLastOpenDir] = useState<string | null>(null);
  const [recentFiles, setRecentFiles] = useState<string[]>([]);
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const dataRef = useRef<Map<number, string[]>>(new Map());
  const rowIndexMapRef = useRef<Map<number, number>>(new Map());
  const [, setDataVersion] = useState(0);
  const [, setRowIndexVersion] = useState(0);

  const debouncedSearch = useDebounce(searchTerm, 450);
  const parentRef = useRef<HTMLDivElement>(null);
  const headerRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const resizeRef = useRef<{
    columnIndex: number;
    startX: number;
    startWidth: number;
  } | null>(null);
  const resizeFrameRef = useRef<number | null>(null);
  const pendingWidthRef = useRef<number | null>(null);

  const rowVirtualizer = useVirtualizer({
    count: totalRows,
    getScrollElement: () => parentRef.current,
    estimateSize: () => rowHeight,
    overscan: 8,
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
  const activeResults = useMemo(() => {
    if (activeHighlight === "search") {
      return searchResults;
    }
    if (activeHighlight === "duplicates") {
      return duplicateResults;
    }
    return null;
  }, [activeHighlight, duplicateResults, searchResults]);
  const activeMatchSet = useMemo(() => {
    if (!activeResults) return null;
    return new Set(activeResults);
  }, [activeResults]);
  const searchQueryLower = useMemo(
    () => debouncedSearch.trim().toLowerCase(),
    [debouncedSearch],
  );
  const searchHighlightActive =
    activeHighlight === "search" && searchQueryLower.length > 0;
  const activeCurrentMatch =
    activeHighlight === "search"
      ? currentMatch
      : activeHighlight === "duplicates"
        ? currentDuplicateMatch
        : 0;

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
        theme?: ThemePreference;
        lastOpenDir?: string;
        recentFiles?: string[];
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
      if (
        parsed.theme === "light" ||
        parsed.theme === "dark" ||
        parsed.theme === "system"
      ) {
        setThemePreference(parsed.theme);
      }
      if (typeof parsed.lastOpenDir === "string") {
        setLastOpenDir(parsed.lastOpenDir);
      }
      if (Array.isArray(parsed.recentFiles)) {
        const seen = new Set<string>();
        const nextRecent = parsed.recentFiles
          .filter((value): value is string => typeof value === "string")
          .filter((value) => {
            if (seen.has(value)) {
              return false;
            }
            seen.add(value);
            return true;
          })
          .slice(0, MAX_RECENT_FILES);
        if (nextRecent.length) {
          setRecentFiles(nextRecent);
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
      theme: themePreference,
      lastOpenDir,
      recentFiles,
    };
    try {
      localStorage.setItem(SETTINGS_KEY, JSON.stringify(payload));
    } catch {
      // Ignore storage failures.
    }
  }, [
    showIndex,
    rowHeight,
    columnWidth,
    columnWidths,
    themePreference,
    lastOpenDir,
    recentFiles,
  ]);

  useEffect(() => {
    invoke("set_show_index_checked", { checked: showIndex }).catch(() => {});
  }, [showIndex]);

  useEffect(() => {
    if (themePreference !== "system") {
      setResolvedTheme(themePreference);
      return;
    }
    const media = window.matchMedia?.("(prefers-color-scheme: dark)");
    const update = () => setResolvedTheme(media?.matches ? "dark" : "light");
    update();
    if (!media) {
      return;
    }
    if (media.addEventListener) {
      media.addEventListener("change", update);
      return () => media.removeEventListener("change", update);
    }
    media.addListener(update);
    return () => media.removeListener(update);
  }, [themePreference]);

  useEffect(() => {
    document.documentElement.dataset.theme = resolvedTheme;
  }, [resolvedTheme]);

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

  useEffect(() => {
    if (duplicateColumn === null) {
      return;
    }
    if (duplicateColumn >= headers.length) {
      setDuplicateColumn(null);
    }
  }, [duplicateColumn, headers.length]);

  const resetForNewFile = useCallback(() => {
    setError(null);
    setSortState(null);
    setSortLoading(false);
    setSortedIndexLookup(null);
    rowIndexMapRef.current = new Map();
    setRowIndexVersion((prev) => prev + 1);
    setShowFind(false);
    setShowDuplicates(false);
    setSearchTerm("");
    setSearchResults(null);
    setCurrentMatch(0);
    setDuplicateResults(null);
    setDuplicateChecking(false);
    setCurrentDuplicateMatch(0);
    setDuplicateColumn(null);
    setActiveHighlight(null);
    setContextMenu(null);
    dataRef.current = new Map();
    setDataVersion((prev) => prev + 1);
    setRowCountReady(false);
    invoke("clear_sort").catch(() => {});
  }, []);

  const toggleTheme = useCallback(() => {
    setThemePreference((prev) => {
      const current = prev === "system" ? getSystemTheme() : prev;
      return current === "dark" ? "light" : "dark";
    });
  }, []);

  const handleOpenPath = useCallback(
    async (path: string) => {
      resetForNewFile();
      setFilePath(path);
      setWindowTitle(path);
      setHeaders([]);
      setTotalRows(0);
      setRowCountReady(false);

      try {
        const csvHeaders = await invoke<string[]>("load_csv_metadata", {
          path,
        });
        setHeaders(csvHeaders);
        setTotalRows(CHUNK_SIZE);
        setSearchColumn(0);
        const nextDir = getDirFromPath(path);
        if (nextDir) {
          setLastOpenDir(nextDir);
        }
        setRecentFiles((prev) => {
          const next = [path, ...prev.filter((item) => item !== path)];
          return next.slice(0, MAX_RECENT_FILES);
        });
      } catch (err) {
        setError(
          typeof err === "string" ? err : "Unable to load CSV metadata.",
        );
        setFilePath(null);
        setHeaders([]);
        setTotalRows(0);
        setRowCountReady(false);
        setWindowTitle(null);
      }
    },
    [resetForNewFile],
  );

  const handlePickFile = useCallback(async () => {
    const selected = await open({
      multiple: false,
      filters: [{ name: "CSV", extensions: ["csv"] }],
      defaultPath: lastOpenDir ?? undefined,
    });

    if (!selected || Array.isArray(selected)) {
      return;
    }

    handleOpenPath(selected);
  }, [handleOpenPath, lastOpenDir]);

  const handleClearFile = useCallback(() => {
    setFilePath(null);
    setWindowTitle(null);
    setHeaders([]);
    setTotalRows(0);
    dataRef.current = new Map();
    setDataVersion((prev) => prev + 1);
    setSearchTerm("");
    setSearchResults(null);
    setError(null);
    setShowFind(false);
    setShowDuplicates(false);
    setSortState(null);
    setSortLoading(false);
    setSortedIndexLookup(null);
    rowIndexMapRef.current = new Map();
    setRowIndexVersion((prev) => prev + 1);
    setRowCountReady(false);
    setDuplicateResults(null);
    setDuplicateChecking(false);
    setCurrentDuplicateMatch(0);
    setDuplicateColumn(null);
    setActiveHighlight(null);
    setContextMenu(null);
    invoke("clear_sort").catch(() => {});
  }, []);

  useEffect(() => {
    setWindowTitle(filePath);
  }, [filePath]);

  const handleCheckDuplicates = useCallback(() => {
    if (!filePath) {
      return;
    }
    setDuplicateChecking(true);
    invoke<number[]>("find_duplicates", {
      columnIdx: duplicateColumn,
    })
      .then((results) => {
        setError(null);
        setDuplicateResults(results);
        setActiveHighlight("duplicates");
      })
      .catch((err) => {
        setError(
          typeof err === "string" ? err : "Duplicate check failed to complete.",
        );
      })
      .finally(() => setDuplicateChecking(false));
  }, [duplicateColumn, filePath]);

  useEffect(() => {
    if (!filePath || loadingRows || sortLoading || totalRows === 0) {
      return;
    }
    if (virtualItems.length === 0) {
      return;
    }
    if (sortState && !sortedIndexLookup) {
      return;
    }

    if (!rowCountReady) {
      const lastIndex = virtualItems[virtualItems.length - 1].index;
      if (lastIndex >= totalRows - PREFETCH - 1) {
        setTotalRows((prev) => prev + CHUNK_SIZE);
      }
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

    const needsLoading = virtualItems.some(
      (item) => !dataRef.current.has(item.index),
    );

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
        setError(null);
        if (sortState) {
          const sortedChunk = chunk as SortedRow[];
          const dataMap = dataRef.current;
          const indexMap = rowIndexMapRef.current;
          sortedChunk.forEach((row, idx) => {
            dataMap.set(startIndex + idx, row.row);
            indexMap.set(startIndex + idx, row.index);
          });
          setDataVersion((prev) => prev + 1);
          setRowIndexVersion((prev) => prev + 1);
          if (!rowCountReady && sortedChunk.length < requestedCount) {
            setTotalRows(startIndex + sortedChunk.length);
            setRowCountReady(true);
          }
        } else {
          const rows = chunk as string[][];
          const dataMap = dataRef.current;
          rows.forEach((row, idx) => {
            dataMap.set(startIndex + idx, row);
          });
          setDataVersion((prev) => prev + 1);
          if (!rowCountReady && rows.length < requestedCount) {
            setTotalRows(startIndex + rows.length);
            setRowCountReady(true);
          }
        }
      })
      .catch((err) => {
        const currentSortKey = sortState
          ? `${sortState.column}:${sortState.direction}`
          : null;
        if (sortKey !== currentSortKey) {
          return;
        }
        setError(typeof err === "string" ? err : "Failed to load CSV rows.");
      })
      .finally(() => setLoadingRows(false));
  }, [
    filePath,
    loadingRows,
    rowCountReady,
    sortLoading,
    sortState,
    sortedIndexLookup,
    totalRows,
    virtualItems,
  ]);

  useEffect(() => {
    rowVirtualizer.measure();
  }, [rowHeight, rowVirtualizer]);

  useEffect(() => {
    if (virtualItems.length === 0) {
      return;
    }
    const dataMap = dataRef.current;
    if (dataMap.size <= MAX_CACHED_ROWS) {
      return;
    }

    const viewStart = virtualItems[0].index;
    const viewEnd = virtualItems[virtualItems.length - 1].index;
    const center = Math.floor((viewStart + viewEnd) / 2);
    const halfWindow = Math.floor(MAX_CACHED_ROWS / 2);

    let windowStart = Math.max(0, center - halfWindow);
    let windowEnd = windowStart + MAX_CACHED_ROWS;

    if (rowCountReady && windowEnd > totalRows) {
      windowEnd = totalRows;
      windowStart = Math.max(0, windowEnd - MAX_CACHED_ROWS);
    }

    let removed = false;
    for (const key of dataMap.keys()) {
      if (key < windowStart || key >= windowEnd) {
        dataMap.delete(key);
        removed = true;
      }
    }

    if (sortState) {
      const indexMap = rowIndexMapRef.current;
      for (const key of indexMap.keys()) {
        if (key < windowStart || key >= windowEnd) {
          indexMap.delete(key);
          removed = true;
        }
      }
    }

    if (removed) {
      setDataVersion((prev) => prev + 1);
      if (sortState) {
        setRowIndexVersion((prev) => prev + 1);
      }
    }
  }, [rowCountReady, sortState, totalRows, virtualItems]);

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
    if (!filePath || !rowCountReady) {
      setSearching(false);
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
  }, [debouncedSearch, filePath, rowCountReady, searchColumn]);

  const getDisplayIndex = useCallback(
    (originalIndex: number) => {
      if (!sortState) {
        return originalIndex;
      }
      return sortedIndexLookup?.[originalIndex];
    },
    [sortState, sortedIndexLookup],
  );

  const scrollToMatch = useCallback(
    (matches: number[] | null, matchIndex: number) => {
      if (!matches?.length) {
        return;
      }
      const originalIndex = matches[matchIndex];
      const displayIndex = getDisplayIndex(originalIndex);
      if (displayIndex === undefined) {
        return;
      }
      rowVirtualizer.scrollToIndex(displayIndex, { align: "center" });
    },
    [getDisplayIndex, rowVirtualizer],
  );

  useEffect(() => {
    if (searchResults === null) {
      setCurrentMatch(0);
      if (activeHighlight === "search") {
        setActiveHighlight(null);
      }
      return;
    }
    if (!searchResults.length) {
      setCurrentMatch(0);
      return;
    }
    setCurrentMatch(0);
    if (activeHighlight === "search") {
      scrollToMatch(searchResults, 0);
    }
  }, [activeHighlight, scrollToMatch, searchResults]);

  useEffect(() => {
    if (duplicateResults === null) {
      setCurrentDuplicateMatch(0);
      if (activeHighlight === "duplicates") {
        setActiveHighlight(null);
      }
      return;
    }
    if (!duplicateResults.length) {
      setCurrentDuplicateMatch(0);
      return;
    }
    setCurrentDuplicateMatch(0);
    if (activeHighlight === "duplicates") {
      scrollToMatch(duplicateResults, 0);
    }
  }, [activeHighlight, duplicateResults, scrollToMatch]);

  const goToNextMatch = useCallback(() => {
    if (!searchResults?.length) {
      return;
    }
    setActiveHighlight("search");
    setCurrentMatch((prev) => {
      const next = (prev + 1) % searchResults.length;
      scrollToMatch(searchResults, next);
      return next;
    });
  }, [scrollToMatch, searchResults]);

  const goToPrevMatch = useCallback(() => {
    if (!searchResults?.length) {
      return;
    }
    setActiveHighlight("search");
    setCurrentMatch((prev) => {
      const next = (prev - 1 + searchResults.length) % searchResults.length;
      scrollToMatch(searchResults, next);
      return next;
    });
  }, [scrollToMatch, searchResults]);

  const goToNextDuplicate = useCallback(() => {
    if (!duplicateResults?.length) {
      return;
    }
    setActiveHighlight("duplicates");
    setCurrentDuplicateMatch((prev) => {
      const next = (prev + 1) % duplicateResults.length;
      scrollToMatch(duplicateResults, next);
      return next;
    });
  }, [duplicateResults, scrollToMatch]);

  const goToPrevDuplicate = useCallback(() => {
    if (!duplicateResults?.length) {
      return;
    }
    setActiveHighlight("duplicates");
    setCurrentDuplicateMatch((prev) => {
      const next =
        (prev - 1 + duplicateResults.length) % duplicateResults.length;
      scrollToMatch(duplicateResults, next);
      return next;
    });
  }, [duplicateResults, scrollToMatch]);

  useEffect(() => {
    if (!filePath) {
      return;
    }

    if (!sortState) {
      setSortedIndexLookup(null);
      rowIndexMapRef.current = new Map();
      setRowIndexVersion((prev) => prev + 1);
      dataRef.current = new Map();
      setDataVersion((prev) => prev + 1);
      invoke("clear_sort").catch(() => {});
      return;
    }

    const sortKey = `${sortState.column}:${sortState.direction}`;
    setSortLoading(true);
    rowIndexMapRef.current = new Map();
    setRowIndexVersion((prev) => prev + 1);
    dataRef.current = new Map();
    setDataVersion((prev) => prev + 1);
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
          setShowDuplicates(false);
          setActiveHighlight("search");
        }),
        listen("menu-clear-search", () => {
          setSearchTerm("");
          setSearchResults(null);
          setCurrentMatch(0);
          setActiveHighlight((prev) => (prev === "search" ? null : prev));
        }),
        listen("menu-check-duplicates", () => {
          setShowDuplicates(true);
          setShowFind(false);
          setActiveHighlight("duplicates");
        }),
        listen<number>("menu-row-height", (event) => {
          setRowHeight(event.payload);
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
        listen("menu-toggle-theme", () => {
          toggleTheme();
        }),
        listen<number>("row-count", (event) => {
          setTotalRows(event.payload);
          setRowCountReady(true);
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
  }, [
    goToNextMatch,
    goToPrevMatch,
    handleClearFile,
    handlePickFile,
    toggleTheme,
  ]);

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
    if (!filePath || rowCountReady) {
      return;
    }

    let active = true;
    let attempts = 0;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const poll = async () => {
      try {
        const count = await invoke<number>("get_row_count");
        if (!active) {
          return;
        }
        if (count > 0) {
          setTotalRows(count);
          setRowCountReady(true);
          return;
        }
      } catch {
        // Ignore row count polling failures.
      }

      attempts += 1;
      if (!active || attempts >= ROW_COUNT_POLL_MAX) {
        return;
      }
      timer = setTimeout(poll, ROW_COUNT_POLL_INTERVAL);
    };

    poll();

    return () => {
      active = false;
      if (timer) {
        clearTimeout(timer);
      }
    };
  }, [filePath, rowCountReady]);

  useEffect(() => {
    if (!contextMenu) {
      return;
    }
    const handleClick = () => setContextMenu(null);
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setContextMenu(null);
      }
    };
    const handleScroll = () => setContextMenu(null);
    const scrollTarget = parentRef.current;
    window.addEventListener("click", handleClick);
    window.addEventListener("contextmenu", handleClick);
    window.addEventListener("keydown", handleKey);
    window.addEventListener("resize", handleScroll);
    scrollTarget?.addEventListener("scroll", handleScroll);
    return () => {
      window.removeEventListener("click", handleClick);
      window.removeEventListener("contextmenu", handleClick);
      window.removeEventListener("keydown", handleKey);
      window.removeEventListener("resize", handleScroll);
      scrollTarget?.removeEventListener("scroll", handleScroll);
    };
  }, [contextMenu]);

  useEffect(() => {
    const handleKeydown = (event: KeyboardEvent) => {
      if (!event.ctrlKey && !event.metaKey) {
        return;
      }
      const key = event.key.toLowerCase();
      if (key === "f") {
        event.preventDefault();
        setShowFind(true);
        setShowDuplicates(false);
        setActiveHighlight("search");
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

  const indexWidth = useMemo(() => {
    const digits = Math.max(1, String(Math.max(totalRows, 1)).length);
    return Math.max(64, digits * 10 + 28);
  }, [totalRows]);
  const duplicateSelectionValue =
    duplicateColumn === null ? "row" : String(duplicateColumn);
  const searchSelectionValue =
    searchColumn === null ? "row" : String(searchColumn);

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
      pendingWidthRef.current = startWidth;

      const handleMove = (moveEvent: MouseEvent) => {
        const current = resizeRef.current;
        if (!current) {
          return;
        }
        const delta = moveEvent.clientX - current.startX;
        const nextWidth = clampColumnWidth(current.startWidth + delta);
        pendingWidthRef.current = nextWidth;
        if (resizeFrameRef.current !== null) {
          return;
        }
        resizeFrameRef.current = window.requestAnimationFrame(() => {
          resizeFrameRef.current = null;
          const latest = resizeRef.current;
          const pending = pendingWidthRef.current;
          if (!latest || pending === null) {
            return;
          }
          setColumnWidths((prev) => {
            const next = prev.length
              ? [...prev]
              : headerLabels.map(() => columnWidth);
            next[latest.columnIndex] = pending;
            return next;
          });
        });
      };

      const handleUp = () => {
        if (resizeFrameRef.current !== null) {
          cancelAnimationFrame(resizeFrameRef.current);
          resizeFrameRef.current = null;
        }
        const latest = resizeRef.current;
        const pending = pendingWidthRef.current;
        if (latest && pending !== null) {
          setColumnWidths((prev) => {
            const next = prev.length
              ? [...prev]
              : headerLabels.map(() => columnWidth);
            next[latest.columnIndex] = pending;
            return next;
          });
        }
        resizeRef.current = null;
        pendingWidthRef.current = null;
        window.removeEventListener("mousemove", handleMove);
        window.removeEventListener("mouseup", handleUp);
      };

      window.addEventListener("mousemove", handleMove);
      window.addEventListener("mouseup", handleUp);
    },
    [columnWidth, columnWidths, headerLabels],
  );

  const handleHeaderClick = (columnIndex: number) => {
    if (sortLoading || !rowCountReady) {
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
  const openContextMenu = useCallback(
    (event: React.MouseEvent, cellText: string | null, rowText: string) => {
      event.preventDefault();
      event.stopPropagation();
      const menuWidth = 200;
      const menuHeight = cellText === null ? 56 : 88;
      const padding = 12;
      const maxX = window.innerWidth - menuWidth - padding;
      const maxY = window.innerHeight - menuHeight - padding;
      const x = Math.max(padding, Math.min(event.clientX, maxX));
      const y = Math.max(padding, Math.min(event.clientY, maxY));
      setContextMenu({ x, y, cellText, rowText });
    },
    [],
  );
  const handleCopy = useCallback(async (value: string) => {
    await copyToClipboard(value);
    setContextMenu(null);
  }, []);

  return (
    <main
      className="app"
      style={
        {
          "--cell-min": `${columnWidth}px`,
          "--row-height": `${rowHeight}px`,
          "--index-width": `${indexWidth}px`,
        } as CSSProperties
      }
    >
      {error ? <div className="status">{error}</div> : null}

      <section className={`table-shell${filePath ? "" : " is-empty"}`}>
        {showFind && filePath ? (
          <div className={`find-panel${showIndex ? " with-index" : ""}`}>
            <div className="find-controls">
              <select
                value={searchSelectionValue}
                onChange={(event) => {
                  const value = event.target.value;
                  setSearchColumn(value === "row" ? null : Number(value));
                }}
                disabled={!headers.length || !rowCountReady}
              >
                <option value="row">Entire row</option>
                {headerLabels.map((header, idx) => (
                  <option value={idx} key={`${header}-${idx}`}>
                    {header}
                  </option>
                ))}
              </select>
              <input
                type="text"
                value={searchTerm}
                onChange={(event) => {
                  setSearchTerm(event.target.value);
                  setActiveHighlight("search");
                }}
                placeholder="Search column or row"
                disabled={!filePath || !rowCountReady}
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
                  if (activeHighlight === "search") {
                    setActiveHighlight(null);
                  }
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
                : !rowCountReady && filePath
                  ? "Counting rows..."
                  : searchResults
                    ? `${searchResults.length.toLocaleString()} matches`
                    : null}
            </div>
          </div>
        ) : null}
        {showDuplicates && filePath ? (
          <div className={`find-panel${showIndex ? " with-index" : ""}`}>
            <div className="find-controls">
              <span className="find-label">Match on</span>
              <select
                value={duplicateSelectionValue}
                onChange={(event) => {
                  const value = event.target.value;
                  setDuplicateColumn(value === "row" ? null : Number(value));
                }}
                disabled={!filePath}
              >
                <option value="row">Entire row</option>
                {headerLabels.map((header, idx) => (
                  <option value={idx} key={`duplicates-${header}-${idx}`}>
                    {header}
                  </option>
                ))}
              </select>
              <button
                className="btn subtle"
                onClick={handleCheckDuplicates}
                disabled={!filePath || duplicateChecking}
              >
                {duplicateChecking ? "Checking..." : "Check"}
              </button>
              <button
                className="btn subtle"
                onClick={() => {
                  setDuplicateResults(null);
                  setCurrentDuplicateMatch(0);
                  setActiveHighlight((prev) =>
                    prev === "duplicates" ? null : prev,
                  );
                }}
                disabled={duplicateResults === null}
              >
                Clear
              </button>
              <button
                className="btn subtle"
                onClick={goToPrevDuplicate}
                disabled={!duplicateResults?.length}
              >
                Prev
              </button>
              <button
                className="btn subtle"
                onClick={goToNextDuplicate}
                disabled={!duplicateResults?.length}
              >
                Next
              </button>
              <span className="find-count">
                {duplicateResults?.length
                  ? `${currentDuplicateMatch + 1}/${duplicateResults.length}`
                  : "0/0"}
              </span>
              <button
                className="btn subtle"
                onClick={() => setShowDuplicates(false)}
              >
                Close
              </button>
            </div>
            <div className="find-meta">
              {duplicateChecking
                ? "Checking duplicates..."
                : duplicateResults
                  ? `${duplicateResults.length.toLocaleString()} duplicate rows`
                  : null}
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
              <button
                className="btn primary empty-cta"
                onClick={handlePickFile}
              >
                Open CSV
              </button>
              {recentFiles.length ? (
                <div className="empty-recent">
                  <div className="recent-title">Recent files</div>
                  <div className="recent-list">
                    {recentFiles.map((path) => {
                      const name = getFileNameFromPath(path);
                      const dir = getDirFromPath(path);
                      return (
                        <button
                          key={path}
                          type="button"
                          className="recent-item"
                          onClick={() => handleOpenPath(path)}
                          title={path}
                        >
                          <span className="recent-name">{name}</span>
                          <span className="recent-path">{dir ?? path}</span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              ) : null}
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
                  const rowData = dataRef.current.get(virtualRow.index);
                  const originalIndex = sortState
                    ? rowIndexMapRef.current.get(virtualRow.index)
                    : virtualRow.index;
                  const rowNumber = (originalIndex ?? virtualRow.index) + 1;
                  const rowText = rowData ? formatCsvRow(rowData) : "";
                  const currentIndex = activeResults?.[activeCurrentMatch];
                  const isMatch =
                    originalIndex !== undefined
                      ? activeMatchSet?.has(originalIndex)
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
                        <div
                          className="table-cell index"
                          onContextMenu={(event) => {
                            if (!rowData) {
                              return;
                            }
                            openContextMenu(event, null, rowText);
                          }}
                        >
                          {rowNumber}
                        </div>
                      ) : null}
                      {rowData ? (
                        rowData.map((cell, cellIdx) => {
                          const cellValue = cell ?? "";
                          let isCellMatch = false;
                          if (
                            searchHighlightActive &&
                            (searchColumn === null || cellIdx === searchColumn)
                          ) {
                            isCellMatch = cellValue
                              .toLowerCase()
                              .includes(searchQueryLower);
                          }
                          return (
                            <div
                              key={cellIdx}
                              className={`table-cell${isCellMatch ? " cell-match" : ""}`}
                              style={
                                columnStyles[cellIdx] ?? defaultColumnStyle
                              }
                              title={cellValue}
                              onContextMenu={(event) =>
                                openContextMenu(event, cellValue, rowText)
                              }
                            >
                              {cellValue}
                            </div>
                          );
                        })
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
      {contextMenu ? (
        <div
          className="context-menu"
          style={{ left: `${contextMenu.x}px`, top: `${contextMenu.y}px` }}
          onClick={(event) => event.stopPropagation()}
          onContextMenu={(event) => event.preventDefault()}
        >
          <button
            type="button"
            className="context-menu-item"
            onClick={() => {
              if (contextMenu.cellText === null) {
                return;
              }
              void handleCopy(contextMenu.cellText);
            }}
            disabled={contextMenu.cellText === null}
          >
            Copy cell
          </button>
          <button
            type="button"
            className="context-menu-item"
            onClick={() => {
              void handleCopy(contextMenu.rowText);
            }}
          >
            Copy row
          </button>
        </div>
      ) : null}
    </main>
  );
}

export default App;
