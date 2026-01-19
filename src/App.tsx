import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import type { CSSProperties } from "react";
import { invoke } from "@tauri-apps/api/core";
import { open as openDialog, save } from "@tauri-apps/plugin-dialog";
import { openPath } from "@tauri-apps/plugin-opener";
import { appDataDir, join } from "@tauri-apps/api/path";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useDebounce } from "./hooks/useDebounce";
import "./App.css";

const BASE_TITLE = "QuickRows";
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
const MAX_PARSE_WARNINGS = 200;

type SortDirection = "asc" | "desc";
type SortState = { column: number; direction: SortDirection };
type SortedRow = { index: number; row: string[] };
type SortLookup = Uint32Array | number[];
type SortWorkerRequest = {
  type: "BUILD_SORT_LOOKUP";
  requestId: number;
  order: number[];
};
type SortWorkerResponse = {
  type: "SORT_LOOKUP_RESULT";
  requestId: number;
  lookup: Uint32Array;
};
type MatchesChunkPayload = {
  requestId: number;
  matches: number[];
};
type MatchesCompletePayload = {
  requestId: number;
  total: number;
};
type ThemeMode = "light" | "dark";
type ThemePreference = ThemeMode | "system";
type ContextMenuState = {
  x: number;
  y: number;
  cellText: string | null;
  rowText: string;
  rowIndex: number | null;
  columnIndex: number | null;
};
type ParseInfo = {
  delimiter: string;
  quote: string;
  escape: string | null;
  line_ending: string;
  encoding: string;
  has_headers: boolean;
  malformed: string;
  max_field_size: number;
  max_record_size: number;
};
type ParseWarning = {
  record?: number;
  line?: number;
  byte?: number;
  field?: number;
  kind: string;
  message: string;
  expected_len?: number;
  len?: number;
};
type CsvMetadata = {
  headers: string[];
  detected: ParseInfo;
  effective: ParseInfo;
  warnings: ParseWarning[];
  estimated_count?: number;
};
type EditingCell = {
  displayRow: number;
  column: number;
  originalRow: number;
  value: string;
  initialValue: string;
  originalValue: string;
};
type ParseOverridesState = {
  delimiter:
    | "auto"
    | "comma"
    | "tab"
    | "semicolon"
    | "pipe"
    | "space"
    | "custom";
  delimiterCustom: string;
  quote: "auto" | "double" | "single";
  escape: "auto" | "none" | "backslash";
  lineEnding: "auto" | "lf" | "crlf" | "cr";
  encoding:
    | "auto"
    | "utf-8"
    | "windows-1252"
    | "iso-8859-1"
    | "utf-16le"
    | "utf-16be";
  hasHeaders: "auto" | "yes" | "no";
  malformed: "strict" | "skip" | "repair";
  maxFieldSize: number;
  maxRecordSize: number;
};

type SelectionRange = { start: number; end: number };

const clampColumnWidth = (value: number) => Math.max(COLUMN_WIDTH_MIN, value);
const normalizeSelectionRanges = (ranges: SelectionRange[]) => {
  const sorted = ranges
    .map((range) => ({
      start: Math.min(range.start, range.end),
      end: Math.max(range.start, range.end),
    }))
    .filter(
      (range) => Number.isFinite(range.start) && Number.isFinite(range.end),
    )
    .sort((a, b) => a.start - b.start);

  const merged: SelectionRange[] = [];
  for (const range of sorted) {
    const last = merged[merged.length - 1];
    if (!last || range.start > last.end + 1) {
      merged.push({ ...range });
      continue;
    }
    last.end = Math.max(last.end, range.end);
  }
  return merged;
};
const isIndexInSelectionRanges = (ranges: SelectionRange[], index: number) => {
  let lo = 0;
  let hi = ranges.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const range = ranges[mid];
    if (index < range.start) {
      hi = mid - 1;
      continue;
    }
    if (index > range.end) {
      lo = mid + 1;
      continue;
    }
    return true;
  }
  return false;
};
const toggleSelectionIndex = (ranges: SelectionRange[], index: number) => {
  const next: SelectionRange[] = [];
  for (const range of ranges) {
    if (index < range.start || index > range.end) {
      next.push(range);
      continue;
    }
    if (range.start === range.end) {
      continue;
    }
    if (index === range.start) {
      next.push({ start: range.start + 1, end: range.end });
      continue;
    }
    if (index === range.end) {
      next.push({ start: range.start, end: range.end - 1 });
      continue;
    }
    next.push({ start: range.start, end: index - 1 });
    next.push({ start: index + 1, end: range.end });
  }
  return next;
};
const addSelectionRange = (ranges: SelectionRange[], range: SelectionRange) =>
  normalizeSelectionRanges([...ranges, range]);
const isTextInputTarget = (target: EventTarget | null) => {
  if (!target || !(target instanceof HTMLElement)) {
    return false;
  }
  const tag = target.tagName;
  return (
    tag === "INPUT" ||
    tag === "TEXTAREA" ||
    tag === "SELECT" ||
    target.isContentEditable
  );
};
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
const setWindowTitle = (path: string | null, dirty: boolean = false) => {
  const name = path ? getFileNameFromPath(path) : "";
  const suffix = dirty ? " *" : "";
  const title = name ? `${name}${suffix} - ${BASE_TITLE}` : BASE_TITLE;
  getCurrentWindow()
    .setTitle(title)
    .catch(() => {});
};
const formatCsvCell = (
  value: string,
  delimiter: string = ",",
  quote: string = '"',
) => {
  if (
    value &&
    (value.includes(delimiter) ||
      value.includes("\n") ||
      value.includes("\r") ||
      value.includes(quote))
  ) {
    const escaped = value.split(quote).join(`${quote}${quote}`);
    return `${quote}${escaped}${quote}`;
  }
  return value;
};
const formatCsvRow = (
  row: string[],
  delimiter: string = ",",
  quote: string = '"',
) => row.map((cell) => formatCsvCell(cell, delimiter, quote)).join(delimiter);

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
const DEFAULT_PARSE_OVERRIDES: ParseOverridesState = {
  delimiter: "auto",
  delimiterCustom: ",",
  quote: "auto",
  escape: "auto",
  lineEnding: "auto",
  encoding: "auto",
  hasHeaders: "auto",
  malformed: "skip",
  maxFieldSize: 256 * 1024,
  maxRecordSize: 2 * 1024 * 1024,
};

function App() {
  const [filePath, setFilePath] = useState<string | null>(null);
  const [checkingInitialOpen, setCheckingInitialOpen] = useState(true);
  const [headers, setHeaders] = useState<string[]>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [rowCountReady, setRowCountReady] = useState(false);
  const [loadingRows, setLoadingRows] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [searchColumn, setSearchColumn] = useState<number | null>(null);
  const [searchMatchCase, setSearchMatchCase] = useState(false);
  const [searchWholeWord, setSearchWholeWord] = useState(false);
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
  const [sortedIndexLookup, setSortedIndexLookup] = useState<SortLookup | null>(
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
  const [parseDetected, setParseDetected] = useState<ParseInfo | null>(null);
  const [parseEffective, setParseEffective] = useState<ParseInfo | null>(null);
  const [parseWarnings, setParseWarnings] = useState<ParseWarning[]>([]);
  /* const [showParseSettings, setShowParseSettings] = useState(false); */

  const [showHeaderPrompt, setShowHeaderPrompt] = useState(false);
  const [parseOverrides, setParseOverrides] = useState<ParseOverridesState>(
    DEFAULT_PARSE_OVERRIDES,
  );
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [editingCell, setEditingCell] = useState<EditingCell | null>(null);
  const [hasEdits, setHasEdits] = useState(false);
  const [saving, setSaving] = useState(false);
  const [searchStale, setSearchStale] = useState(false);
  const [duplicateStale, setDuplicateStale] = useState(false);
  const [savePath, setSavePath] = useState<string | null>(null);
  const [deletedRowsVersion, setDeletedRowsVersion] = useState(0);
  const [loadingProgress, setLoadingProgress] = useState<number | null>(null);
  const [showSettings, setShowSettings] = useState(false);
  const [debugLogging, setDebugLogging] = useState(false);
  const [debugLogPath, setDebugLogPath] = useState<string | null>(null);
  const [crashLogPath, setCrashLogPath] = useState<string | null>(null);
  const [selectedRanges, setSelectedRanges] = useState<SelectionRange[]>([]);
  const [selectionAnchor, setSelectionAnchor] = useState<number | null>(null);
  const [focusedRow, setFocusedRow] = useState<number | null>(null);
  const [enableIndexing, setEnableIndexing] = useState(() => {
    const saved = localStorage.getItem("csv-viewer-enable-indexing");
    return saved !== null ? saved === "true" : true; // Default enabled
  });
  const debugLoggingRef = useRef(debugLogging);
  debugLoggingRef.current = debugLogging;
  const dataRef = useRef<Map<number, string[]>>(new Map());
  const rowIndexMapRef = useRef<Map<number, number>>(new Map());
  const searchRequestIdRef = useRef(0);
  const duplicateRequestIdRef = useRef(0);
  const searchResultsCountRef = useRef(0);
  const duplicateResultsCountRef = useRef(0);
  const sortWorkerRef = useRef<Worker | null>(null);
  const sortWorkerRequestIdRef = useRef(0);
  const pendingSortOrderRef = useRef<{
    requestId: number;
    order: number[];
  } | null>(null);
  const sortWorkerTimeoutRef = useRef<number | null>(null);
  const editsRef = useRef<Map<number, Map<number, string>>>(new Map());
  const deletedRowsRef = useRef<Set<number>>(new Set());
  const [, setDataVersion] = useState(0);
  const [, setRowIndexVersion] = useState(0);
  const [searchRefreshToken, setSearchRefreshToken] = useState(0);

  const debouncedSearch = useDebounce(searchTerm, 450);
  const parentRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const headerRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const editInputRef = useRef<HTMLInputElement>(null);
  const resizeRef = useRef<{
    columnIndex: number;
    startX: number;
    startWidth: number;
  } | null>(null);
  const resizeFrameRef = useRef<number | null>(null);
  const pendingWidthRef = useRef<number | null>(null);

  const selectedCount = useMemo(() => {
    let total = 0;
    for (const range of selectedRanges) {
      total += range.end - range.start + 1;
    }
    return total;
  }, [selectedRanges]);

  const isDirty = editsRef.current.size > 0 || deletedRowsRef.current.size > 0;

  const clearSelection = useCallback(() => {
    setSelectedRanges([]);
    setSelectionAnchor(null);
    setFocusedRow(null);
  }, []);

  const selectDisplayRow = useCallback(
    (displayIndex: number, opts: { shiftKey: boolean; toggleKey: boolean }) => {
      if (!Number.isFinite(displayIndex) || displayIndex < 0) {
        return;
      }
      if (totalRows && displayIndex > totalRows - 1) {
        return;
      }

      if (opts.shiftKey) {
        const anchor = selectionAnchor ?? displayIndex;
        const range = {
          start: Math.min(anchor, displayIndex),
          end: Math.max(anchor, displayIndex),
        };
        setSelectedRanges((prev) => {
          const normalized = normalizeSelectionRanges(prev);
          return opts.toggleKey
            ? addSelectionRange(normalized, range)
            : [range];
        });
        setSelectionAnchor((prev) => prev ?? displayIndex);
        setFocusedRow(displayIndex);
        return;
      }

      if (opts.toggleKey) {
        setSelectedRanges((prev) => {
          const normalized = normalizeSelectionRanges(prev);
          if (isIndexInSelectionRanges(normalized, displayIndex)) {
            return toggleSelectionIndex(normalized, displayIndex);
          }
          return addSelectionRange(normalized, {
            start: displayIndex,
            end: displayIndex,
          });
        });
        setSelectionAnchor(displayIndex);
        setFocusedRow(displayIndex);
        return;
      }

      setSelectedRanges([{ start: displayIndex, end: displayIndex }]);
      setSelectionAnchor(displayIndex);
      setFocusedRow(displayIndex);
    },
    [selectionAnchor, totalRows],
  );

  useEffect(() => {
    if (error) {
      const timer = setTimeout(() => setError(null), 3000);
      return () => clearTimeout(timer);
    }
  }, [error]);

  useEffect(() => {
    if (notice) {
      const timer = setTimeout(() => setNotice(null), 2200);
      return () => clearTimeout(timer);
    }
  }, [notice]);

  const editingKey = useMemo(
    () =>
      editingCell ? `${editingCell.displayRow}:${editingCell.column}` : null,
    [editingCell?.displayRow, editingCell?.column],
  );

  useEffect(() => {
    if (!editingKey) {
      return;
    }
    editInputRef.current?.focus();
    editInputRef.current?.select();
  }, [editingKey]);

  useEffect(() => {
    localStorage.setItem("csv-viewer-enable-indexing", String(enableIndexing));
    invoke("set_enable_indexing", { enabled: enableIndexing }).catch(() => {});
  }, [enableIndexing]);

  useEffect(() => {
    if (!totalRows) {
      setSelectedRanges([]);
      setSelectionAnchor(null);
      setFocusedRow(null);
      return;
    }
    setSelectedRanges((prev) => {
      const maxIndex = totalRows - 1;
      const next = prev
        .map((range) => ({
          start: Math.max(0, Math.min(range.start, maxIndex)),
          end: Math.max(0, Math.min(range.end, maxIndex)),
        }))
        .filter((range) => range.start <= range.end);
      return normalizeSelectionRanges(next);
    });
    setSelectionAnchor((prev) =>
      prev === null ? null : Math.min(prev, totalRows - 1),
    );
    setFocusedRow((prev) =>
      prev === null ? null : Math.min(prev, totalRows - 1),
    );
  }, [totalRows]);

  const MAX_SCROLL_PIXELS = 15_000_000;
  const totalSize = totalRows * rowHeight;
  const scaleFactor =
    totalSize > MAX_SCROLL_PIXELS ? totalSize / MAX_SCROLL_PIXELS : 1;

  const scaleFactorRef = useRef(scaleFactor);
  scaleFactorRef.current = scaleFactor;

  const rowVirtualizer = useVirtualizer({
    count: totalRows,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => rowHeight,
    overscan: 8,
    observeElementOffset: (instance, cb) => {
      const el = instance.scrollElement;
      if (!el) {
        return () => {};
      }
      const onScroll = () => {
        cb(el.scrollTop * scaleFactorRef.current, true);
      };
      el.addEventListener("scroll", onScroll, { passive: true });
      return () => {
        el.removeEventListener("scroll", onScroll);
      };
    },
    scrollToFn: (offset, _options, instance) => {
      instance.scrollElement?.scrollTo(0, offset / scaleFactorRef.current);
    },
  });

  const appendDebugLog = useCallback((message: string) => {
    if (!debugLoggingRef.current) {
      return;
    }
    invoke("append_debug_log", { message }).catch(() => {});
  }, []);

  const buildSortLookupChunked = useCallback(
    (order: number[], requestId: number) => {
      pendingSortOrderRef.current = null;
      if (sortWorkerTimeoutRef.current !== null) {
        window.clearTimeout(sortWorkerTimeoutRef.current);
        sortWorkerTimeoutRef.current = null;
      }

      const lookup = new Uint32Array(order.length);
      let i = 0;
      const chunkSize = 250_000;

      const step = () => {
        if (requestId !== sortWorkerRequestIdRef.current) {
          return;
        }
        const end = Math.min(order.length, i + chunkSize);
        for (; i < end; i += 1) {
          lookup[order[i]] = i;
        }
        if (i < order.length) {
          window.setTimeout(step, 0);
          return;
        }
        setSortedIndexLookup(lookup);
        rowVirtualizer.scrollToIndex(0);
        setSortLoading(false);
      };

      step();
    },
    [rowVirtualizer],
  );

  useEffect(() => {
    if (typeof Worker === "undefined") {
      return;
    }
    let worker: Worker | null = null;
    try {
      worker = new Worker(new URL("./workers/csv.worker.ts", import.meta.url), {
        type: "module",
      });
    } catch (err) {
      appendDebugLog(`Sort worker init failed: ${String(err)}`);
      sortWorkerRef.current = null;
      return;
    }
    sortWorkerRef.current = worker;

    const handleMessage = (event: MessageEvent<SortWorkerResponse>) => {
      const data = event.data;
      if (!data || data.type !== "SORT_LOOKUP_RESULT") {
        return;
      }
      if (data.requestId !== sortWorkerRequestIdRef.current) {
        return;
      }
      pendingSortOrderRef.current = null;
      if (sortWorkerTimeoutRef.current !== null) {
        window.clearTimeout(sortWorkerTimeoutRef.current);
        sortWorkerTimeoutRef.current = null;
      }
      setSortedIndexLookup(data.lookup);
      rowVirtualizer.scrollToIndex(0);
      setSortLoading(false);
    };

    const handleWorkerError = (event: ErrorEvent) => {
      appendDebugLog(
        `Sort worker error: ${event.message || "Unknown worker error"}`,
      );
      const pending = pendingSortOrderRef.current;
      pendingSortOrderRef.current = null;
      if (sortWorkerTimeoutRef.current !== null) {
        window.clearTimeout(sortWorkerTimeoutRef.current);
        sortWorkerTimeoutRef.current = null;
      }
      try {
        worker?.terminate();
      } catch {
        // ignore
      }
      sortWorkerRef.current = null;
      if (pending && pending.requestId === sortWorkerRequestIdRef.current) {
        buildSortLookupChunked(pending.order, pending.requestId);
      }
    };

    const handleWorkerMessageError = () => {
      appendDebugLog("Sort worker message deserialization failed.");
    };

    worker.addEventListener("message", handleMessage);
    worker.addEventListener("error", handleWorkerError);
    worker.addEventListener("messageerror", handleWorkerMessageError);
    return () => {
      worker.removeEventListener("message", handleMessage);
      worker.removeEventListener("error", handleWorkerError);
      worker.removeEventListener("messageerror", handleWorkerMessageError);
      worker.terminate();
      sortWorkerRef.current = null;
    };
  }, [appendDebugLog, buildSortLookupChunked, rowVirtualizer]);

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
    if (activeHighlight === "search" && !searchStale) {
      return searchResults;
    }
    if (activeHighlight === "duplicates" && !duplicateStale) {
      return duplicateResults;
    }
    return null;
  }, [
    activeHighlight,
    duplicateResults,
    searchResults,
    searchStale,
    duplicateStale,
  ]);
  const activeMatchSet = useMemo(() => {
    if (!activeResults) return null;
    return new Set(activeResults);
  }, [activeResults]);
  const searchQueryLower = useMemo(
    () => debouncedSearch.trim().toLowerCase(),
    [debouncedSearch],
  );
  const searchHighlightActive =
    activeHighlight === "search" && !searchStale && searchQueryLower.length > 0;
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
        debugLogging?: boolean;
        parseOverrides?: Partial<ParseOverridesState>;
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
      if (typeof parsed.debugLogging === "boolean") {
        setDebugLogging(parsed.debugLogging);
      }
      if (parsed.parseOverrides) {
        setParseOverrides((prev) => ({
          ...prev,
          ...parsed.parseOverrides,
        }));
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
      debugLogging,
      parseOverrides,
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
    debugLogging,
    parseOverrides,
  ]);

  useEffect(() => {
    invoke("set_show_index_checked", { checked: showIndex }).catch(() => {});
  }, [showIndex]);

  useEffect(() => {
    invoke<string>("get_debug_log_path")
      .then((path) => setDebugLogPath(path))
      .catch(() => {});
    invoke<string>("get_crash_log_path")
      .then((path) => setCrashLogPath(path))
      .catch(() => {});
  }, []);

  useEffect(() => {
    invoke<string>("set_debug_logging", { enabled: debugLogging })
      .then((path) => setDebugLogPath(path))
      .catch(() => {});
  }, [debugLogging]);

  useEffect(() => {
    if (!debugLogging) {
      return;
    }

    const onError = (event: ErrorEvent) => {
      appendDebugLog(
        `window.error: ${event.message || "Unknown error"} @ ${event.filename}:${event.lineno}:${event.colno}`,
      );
    };
    const onRejection = (event: PromiseRejectionEvent) => {
      const reason = (() => {
        try {
          if (typeof event.reason === "string") {
            return event.reason;
          }
          return JSON.stringify(event.reason);
        } catch {
          return String(event.reason);
        }
      })();
      appendDebugLog(`unhandledrejection: ${reason}`);
    };

    window.addEventListener("error", onError);
    window.addEventListener("unhandledrejection", onRejection);
    return () => {
      window.removeEventListener("error", onError);
      window.removeEventListener("unhandledrejection", onRejection);
    };
  }, [appendDebugLog, debugLogging]);

  useEffect(() => {
    if (!filePath) {
      return;
    }
    clearSelection();
  }, [clearSelection, filePath, sortState?.column, sortState?.direction]);

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

  useEffect(() => {
    if (!filePath || !parseDetected) {
      setShowHeaderPrompt(false);
      return;
    }
    if (!parseDetected.has_headers && parseOverrides.hasHeaders === "auto") {
      setShowHeaderPrompt(true);
    } else {
      setShowHeaderPrompt(false);
    }
  }, [filePath, parseDetected, parseOverrides.hasHeaders]);

  const resetForNewFile = useCallback(() => {
    searchRequestIdRef.current += 1;
    duplicateRequestIdRef.current += 1;
    setError(null);
    setSelectedRanges([]);
    setSelectionAnchor(null);
    setFocusedRow(null);
    setSortState(null);
    setSortLoading(false);
    setSortedIndexLookup(null);
    rowIndexMapRef.current = new Map();
    setRowIndexVersion((prev) => prev + 1);
    setShowFind(false);
    setShowDuplicates(false);
    setSearchTerm("");
    setSearchResults(null);
    setSearching(false);
    setCurrentMatch(0);
    setDuplicateResults(null);
    setDuplicateChecking(false);
    setCurrentDuplicateMatch(0);
    setDuplicateColumn(null);
    setActiveHighlight(null);
    setParseDetected(null);
    setParseEffective(null);
    setParseWarnings([]);
    setShowHeaderPrompt(false);
    setContextMenu(null);
    setEditingCell(null);
    setHasEdits(false);
    setSaving(false);
    setSearchStale(false);
    setDuplicateStale(false);
    setSavePath(null);
    editsRef.current = new Map();
    deletedRowsRef.current = new Set();
    setDeletedRowsVersion((prev) => prev + 1);
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

  const buildParseOverrides = useCallback(
    (overridesState: ParseOverridesState = parseOverrides) => {
      const overrides: {
        delimiter?: string;
        quote?: string;
        escape?: string;
        line_ending?: string;
        encoding?: string;
        has_headers?: boolean;
        malformed?: "strict" | "skip" | "repair";
        max_field_size?: number;
        max_record_size?: number;
      } = {
        malformed: overridesState.malformed,
        max_field_size: overridesState.maxFieldSize,
        max_record_size: overridesState.maxRecordSize,
      };

      if (overridesState.delimiter !== "auto") {
        if (overridesState.delimiter === "custom") {
          if (overridesState.delimiterCustom.trim().length) {
            overrides.delimiter = overridesState.delimiterCustom;
          }
        } else {
          overrides.delimiter = overridesState.delimiter;
        }
      }
      if (overridesState.quote !== "auto") {
        overrides.quote = overridesState.quote;
      }
      if (overridesState.escape !== "auto") {
        overrides.escape = overridesState.escape;
      }
      if (overridesState.lineEnding !== "auto") {
        overrides.line_ending = overridesState.lineEnding;
      }
      if (overridesState.encoding !== "auto") {
        overrides.encoding = overridesState.encoding;
      }
      if (overridesState.hasHeaders !== "auto") {
        overrides.has_headers = overridesState.hasHeaders === "yes";
      }

      return overrides;
    },
    [parseOverrides],
  );

  const appendParseWarnings = useCallback((next: ParseWarning[]) => {
    if (!next.length) {
      return;
    }
    setParseWarnings((prev) => {
      const merged = [...prev, ...next];
      if (merged.length <= MAX_PARSE_WARNINGS) {
        return merged;
      }
      return merged.slice(-MAX_PARSE_WARNINGS);
    });
  }, []);

  const refreshParseWarnings = useCallback(() => {
    invoke<ParseWarning[]>("get_parse_warnings", { clear: true })
      .then((next) => {
        appendParseWarnings(next);
      })
      .catch(() => {});
  }, [appendParseWarnings]);

  const csvFormat = useMemo(() => {
    const delimiter = parseEffective?.delimiter ?? ",";
    const quote = parseEffective?.quote ?? '"';
    const lineEndingValue = parseEffective?.line_ending ?? "lf";
    const lineEnding =
      lineEndingValue === "crlf"
        ? "\r\n"
        : lineEndingValue === "cr"
          ? "\r"
          : "\n";
    return {
      delimiter,
      quote,
      lineEnding,
      hasHeaders: parseEffective?.has_headers ?? false,
    };
  }, [parseEffective]);

  const applyEditsToRow = useCallback(
    (row: string[], originalIndex: number | undefined) => {
      if (originalIndex === undefined) {
        return row;
      }
      const rowEdits = editsRef.current.get(originalIndex);
      if (!rowEdits || rowEdits.size === 0) {
        return row;
      }
      const next = row.slice();
      rowEdits.forEach((value, colIndex) => {
        next[colIndex] = value;
      });
      return next;
    },
    [],
  );

  const getOriginalRowIndex = useCallback(
    (displayRow: number) => {
      if (!sortState) {
        return displayRow;
      }
      return rowIndexMapRef.current.get(displayRow);
    },
    [sortState],
  );

  const isRowDeleted = useCallback(
    (originalIndex: number | undefined) => {
      if (originalIndex === undefined) {
        return false;
      }
      return deletedRowsRef.current.has(originalIndex);
    },
    [deletedRowsVersion],
  );

  const startEditCell = useCallback(
    (
      displayRow: number,
      column: number,
      value: string,
      originalValue: string,
    ) => {
      if (!filePath) {
        return;
      }
      const originalRow = getOriginalRowIndex(displayRow);
      if (originalRow === undefined) {
        return;
      }
      if (isRowDeleted(originalRow)) {
        return;
      }
      setEditingCell({
        displayRow,
        column,
        originalRow,
        value,
        initialValue: value,
        originalValue,
      });
    },
    [filePath, getOriginalRowIndex, isRowDeleted],
  );

  const deleteRow = useCallback(
    (displayRow: number) => {
      const originalRow = getOriginalRowIndex(displayRow);
      if (originalRow === undefined) {
        return;
      }
      if (deletedRowsRef.current.has(originalRow)) {
        return;
      }
      deletedRowsRef.current.add(originalRow);
      setEditingCell((prev) =>
        prev && prev.originalRow === originalRow ? null : prev,
      );
      setDeletedRowsVersion((prev) => prev + 1);
      setHasEdits(true);
      setSearchStale(true);
      setDuplicateStale(true);
    },
    [getOriginalRowIndex],
  );

  const deleteSelectedRows = useCallback(async () => {
    if (!filePath) {
      return;
    }
    const normalized = normalizeSelectionRanges(selectedRanges);
    if (!normalized.length) {
      return;
    }

    const selectionSize = selectedCount;
    if (selectionSize >= 1000) {
      const ok = window.confirm(
        `Delete ${selectionSize.toLocaleString()} selected rows?`,
      );
      if (!ok) {
        return;
      }
    }

    try {
      let changed = false;
      if (!sortState) {
        for (const range of normalized) {
          for (
            let displayRow = range.start;
            displayRow <= range.end;
            displayRow += 1
          ) {
            if (displayRow < 0 || displayRow >= totalRows) {
              continue;
            }
            if (deletedRowsRef.current.has(displayRow)) {
              continue;
            }
            deletedRowsRef.current.add(displayRow);
            changed = true;
          }
        }
      } else {
        for (const range of normalized) {
          let start = Math.max(0, range.start);
          const end = Math.min(range.end, totalRows - 1);
          while (start <= end) {
            const count = Math.min(CHUNK_SIZE, end - start + 1);
            const originalIndices = await invoke<number[]>(
              "get_sorted_indices",
              {
                start,
                count,
              },
            );
            originalIndices.forEach((originalRow) => {
              if (deletedRowsRef.current.has(originalRow)) {
                return;
              }
              deletedRowsRef.current.add(originalRow);
              changed = true;
            });
            start += count;
            if (originalIndices.length < count) {
              break;
            }
          }
        }
      }

      if (!changed) {
        return;
      }

      setEditingCell((prev) =>
        prev && deletedRowsRef.current.has(prev.originalRow) ? null : prev,
      );
      setDeletedRowsVersion((prev) => prev + 1);
      setHasEdits(true);
      setSearchStale(true);
      setDuplicateStale(true);
    } catch (err) {
      setError(
        typeof err === "string" ? err : "Failed to delete selected rows.",
      );
    }
  }, [filePath, selectedCount, selectedRanges, sortState, totalRows]);

  const copySelectedRows = useCallback(async () => {
    if (!filePath) {
      return;
    }
    if (sortLoading) {
      setError("Wait for sorting to finish before copying.");
      return;
    }
    const normalized = normalizeSelectionRanges(selectedRanges);
    if (!normalized.length) {
      return;
    }

    const selectionSize = selectedCount;
    if (selectionSize >= 5000) {
      const ok = window.confirm(
        `Copy ${selectionSize.toLocaleString()} selected rows?`,
      );
      if (!ok) {
        return;
      }
    }

    try {
      const lines: string[] = [];
      if (!sortState) {
        for (const range of normalized) {
          let start = Math.max(0, range.start);
          const end = Math.min(range.end, totalRows - 1);
          while (start <= end) {
            const count = Math.min(CHUNK_SIZE, end - start + 1);
            const chunk = await invoke<string[][]>("get_csv_chunk", {
              start,
              count,
            });
            chunk.forEach((row, idx) => {
              const originalIndex = start + idx;
              if (deletedRowsRef.current.has(originalIndex)) {
                return;
              }
              const nextRow = applyEditsToRow(row, originalIndex);
              lines.push(
                formatCsvRow(nextRow, csvFormat.delimiter, csvFormat.quote),
              );
            });
            start += chunk.length;
            if (chunk.length < count) {
              break;
            }
          }
        }
      } else {
        for (const range of normalized) {
          let start = Math.max(0, range.start);
          const end = Math.min(range.end, totalRows - 1);
          while (start <= end) {
            const count = Math.min(CHUNK_SIZE, end - start + 1);
            const chunk = await invoke<SortedRow[]>("get_sorted_chunk", {
              start,
              count,
            });
            chunk.forEach((item) => {
              if (deletedRowsRef.current.has(item.index)) {
                return;
              }
              const nextRow = applyEditsToRow(item.row, item.index);
              lines.push(
                formatCsvRow(nextRow, csvFormat.delimiter, csvFormat.quote),
              );
            });
            start += chunk.length;
            if (chunk.length < count) {
              break;
            }
          }
        }
      }

      if (!lines.length) {
        return;
      }

      const ok = await copyToClipboard(lines.join(csvFormat.lineEnding));
      if (!ok) {
        setError("Copy failed (clipboard access may be disabled).");
        return;
      }
      setNotice(`Copied ${lines.length.toLocaleString()} row(s).`);
    } catch (err) {
      setError(typeof err === "string" ? err : "Failed to copy selected rows.");
    }
  }, [
    applyEditsToRow,
    csvFormat.delimiter,
    csvFormat.lineEnding,
    csvFormat.quote,
    filePath,
    selectedCount,
    selectedRanges,
    sortLoading,
    sortState,
    totalRows,
  ]);

  const restoreRow = useCallback(
    (displayRow: number) => {
      const originalRow = getOriginalRowIndex(displayRow);
      if (originalRow === undefined) {
        return;
      }
      if (!deletedRowsRef.current.has(originalRow)) {
        return;
      }
      deletedRowsRef.current.delete(originalRow);
      setDeletedRowsVersion((prev) => prev + 1);
      setHasEdits(editsRef.current.size > 0 || deletedRowsRef.current.size > 0);
      setSearchStale(true);
      setDuplicateStale(true);
    },
    [getOriginalRowIndex],
  );

  const restoreSelectedRows = useCallback(async () => {
    if (!filePath) {
      return;
    }
    const normalized = normalizeSelectionRanges(selectedRanges);
    if (!normalized.length) {
      return;
    }

    try {
      let changed = false;
      if (!sortState) {
        for (const range of normalized) {
          for (
            let displayRow = range.start;
            displayRow <= range.end;
            displayRow += 1
          ) {
            if (displayRow < 0 || displayRow >= totalRows) {
              continue;
            }
            if (!deletedRowsRef.current.has(displayRow)) {
              continue;
            }
            deletedRowsRef.current.delete(displayRow);
            changed = true;
          }
        }
      } else {
        for (const range of normalized) {
          let start = Math.max(0, range.start);
          const end = Math.min(range.end, totalRows - 1);
          while (start <= end) {
            const count = Math.min(CHUNK_SIZE, end - start + 1);
            const originalIndices = await invoke<number[]>(
              "get_sorted_indices",
              {
                start,
                count,
              },
            );
            originalIndices.forEach((originalRow) => {
              if (!deletedRowsRef.current.has(originalRow)) {
                return;
              }
              deletedRowsRef.current.delete(originalRow);
              changed = true;
            });
            start += count;
            if (originalIndices.length < count) {
              break;
            }
          }
        }
      }

      if (!changed) {
        return;
      }

      setDeletedRowsVersion((prev) => prev + 1);
      setHasEdits(editsRef.current.size > 0 || deletedRowsRef.current.size > 0);
      setSearchStale(true);
      setDuplicateStale(true);
    } catch (err) {
      setError(
        typeof err === "string" ? err : "Failed to restore selected rows.",
      );
    }
  }, [filePath, selectedRanges, sortState, totalRows]);

  const commitEdit = useCallback(() => {
    if (!editingCell) {
      return;
    }
    const { column, originalRow, value, initialValue, originalValue } =
      editingCell;
    setEditingCell(null);

    if (value === initialValue) {
      return;
    }

    const edits = editsRef.current;
    const rowEdits = edits.get(originalRow);
    if (value === originalValue) {
      if (rowEdits) {
        rowEdits.delete(column);
        if (rowEdits.size === 0) {
          edits.delete(originalRow);
        }
      }
    } else {
      const nextRowEdits = rowEdits ?? new Map<number, string>();
      nextRowEdits.set(column, value);
      edits.set(originalRow, nextRowEdits);
    }

    setHasEdits(editsRef.current.size > 0 || deletedRowsRef.current.size > 0);
    setSearchStale(true);
    setDuplicateStale(true);
  }, [editingCell]);

  const cancelEdit = useCallback(() => {
    setEditingCell(null);
  }, []);

  const handleOpenPath = useCallback(
    async (path: string, overrides?: Record<string, unknown>) => {
      try {
        const csvMetadata = await invoke<CsvMetadata>("load_csv_metadata", {
          path,
          overrides: overrides ?? buildParseOverrides(),
        });
        resetForNewFile();
        setFilePath(path);
        setWindowTitle(path, false);
        setHeaders(csvMetadata.headers);
        setParseDetected(csvMetadata.detected);
        setParseEffective(csvMetadata.effective);
        setParseWarnings(csvMetadata.warnings ?? []);
        invoke("get_parse_warnings", { clear: true }).catch(() => {});
        setLoadingProgress(0);
        setTotalRows(csvMetadata.estimated_count ?? CHUNK_SIZE);
        setRowCountReady(false);
        setSearchColumn(null);
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
        // Do not change filePath or clear state if load failed,
        // effectively keeping user on previous screen or file with an error message.
      }
    },
    [buildParseOverrides, resetForNewFile],
  );

  const removeRecent = useCallback((path: string) => {
    setRecentFiles((prev) => prev.filter((item) => item !== path));
  }, []);

  const applyParseOverrides = useCallback(
    (nextOverrides: ParseOverridesState) => {
      setParseOverrides(nextOverrides);
      if (filePath) {
        handleOpenPath(filePath, buildParseOverrides(nextOverrides));
      }
    },
    [buildParseOverrides, filePath, handleOpenPath],
  );

  const handleApplyParse = useCallback(() => {
    if (!filePath) {
      return;
    }
    handleOpenPath(filePath, buildParseOverrides());
  }, [buildParseOverrides, filePath, handleOpenPath]);

  const handleHeaderPromptChoice = useCallback(
    (useHeaders: boolean) => {
      const nextOverrides = {
        ...parseOverrides,
        hasHeaders: (useHeaders
          ? "yes"
          : "no") as ParseOverridesState["hasHeaders"],
      };
      setShowHeaderPrompt(false);
      applyParseOverrides(nextOverrides);
    },
    [applyParseOverrides, parseOverrides],
  );

  const handlePickFile = useCallback(async () => {
    const selected = await openDialog({
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
    searchRequestIdRef.current += 1;
    duplicateRequestIdRef.current += 1;
    setFilePath(null);
    setWindowTitle(null);
    setHeaders([]);
    setTotalRows(0);
    setSelectedRanges([]);
    setSelectionAnchor(null);
    setFocusedRow(null);
    dataRef.current = new Map();
    setDataVersion((prev) => prev + 1);
    setSearchTerm("");
    setSearchResults(null);
    setSearching(false);
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
    setParseDetected(null);
    setParseEffective(null);
    setParseWarnings([]);
    setShowHeaderPrompt(false);
    setContextMenu(null);
    setEditingCell(null);
    setHasEdits(false);
    setSaving(false);
    setSearchStale(false);
    setDuplicateStale(false);
    setSavePath(null);
    editsRef.current = new Map();
    deletedRowsRef.current = new Set();
    setDeletedRowsVersion((prev) => prev + 1);
    invoke("clear_sort").catch(() => {});
  }, []);

  useEffect(() => {
    setWindowTitle(filePath, isDirty);
  }, [filePath, isDirty]);

  const handleCheckDuplicates = useCallback(() => {
    if (!filePath) {
      return;
    }
    const requestId = duplicateRequestIdRef.current + 1;
    duplicateRequestIdRef.current = requestId;
    setError(null);
    setDuplicateChecking(true);
    setDuplicateResults([]);
    setActiveHighlight("duplicates");
    setDuplicateStale(false);
    invoke("find_duplicates_stream", {
      columnIdx: duplicateColumn,
      requestId,
    }).catch((err) => {
      if (requestId !== duplicateRequestIdRef.current) {
        return;
      }
      setError(
        typeof err === "string" ? err : "Duplicate check failed to complete.",
      );
      setDuplicateChecking(false);
      setDuplicateResults(null);
    });
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
          refreshParseWarnings();
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
          refreshParseWarnings();
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
    refreshParseWarnings,
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
    if (!filePath) {
      return;
    }
    if (!totalRows) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (showSettings || contextMenu || editingKey) {
        return;
      }
      if (isTextInputTarget(event.target)) {
        return;
      }

      const toggleKey = event.metaKey || event.ctrlKey;
      const key = event.key;

      if (toggleKey && (key === "a" || key === "A")) {
        event.preventDefault();
        const normalized = normalizeSelectionRanges(selectedRanges);
        const fallback =
          focusedRow ??
          selectionAnchor ??
          (normalized.length ? normalized[0].start : 0);
        const anchor = Math.max(0, Math.min(fallback, totalRows - 1));
        setSelectedRanges([{ start: 0, end: totalRows - 1 }]);
        setSelectionAnchor(anchor);
        setFocusedRow(anchor);
        return;
      }

      if (key === "Escape") {
        if (selectedRanges.length) {
          event.preventDefault();
          clearSelection();
        }
        return;
      }

      if (key === "Delete" || key === "Backspace") {
        if (selectedRanges.length) {
          event.preventDefault();
          void deleteSelectedRows();
        }
        return;
      }

      if (toggleKey && (key === "c" || key === "C")) {
        if (selectedCount > 1) {
          event.preventDefault();
          void copySelectedRows();
        }
        return;
      }

      const isNavKey =
        key === "ArrowDown" ||
        key === "ArrowUp" ||
        key === "Home" ||
        key === "End" ||
        key === "PageDown" ||
        key === "PageUp";
      if (!isNavKey) {
        return;
      }

      const normalized = normalizeSelectionRanges(selectedRanges);
      const current =
        focusedRow ??
        selectionAnchor ??
        (normalized.length ? normalized[0].start : 0);

      let nextIndex = current;
      const pageSize = Math.max(
        1,
        Math.floor((parentRef.current?.clientHeight ?? 0) / rowHeight),
      );

      if (key === "ArrowDown") nextIndex = Math.min(totalRows - 1, current + 1);
      if (key === "ArrowUp") nextIndex = Math.max(0, current - 1);
      if (key === "Home") nextIndex = 0;
      if (key === "End") nextIndex = totalRows - 1;
      if (key === "PageDown")
        nextIndex = Math.min(totalRows - 1, current + pageSize);
      if (key === "PageUp") nextIndex = Math.max(0, current - pageSize);

      if (nextIndex === current) {
        return;
      }

      event.preventDefault();
      rowVirtualizer.scrollToIndex(nextIndex, { align: "center" });
      setFocusedRow(nextIndex);

      if (event.shiftKey) {
        const anchor = selectionAnchor ?? current;
        const range = {
          start: Math.min(anchor, nextIndex),
          end: Math.max(anchor, nextIndex),
        };
        setSelectedRanges([range]);
        setSelectionAnchor(anchor);
        return;
      }

      if (!toggleKey) {
        setSelectedRanges([{ start: nextIndex, end: nextIndex }]);
        setSelectionAnchor(nextIndex);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [
    clearSelection,
    copySelectedRows,
    contextMenu,
    deleteSelectedRows,
    editingKey,
    filePath,
    focusedRow,
    rowHeight,
    rowVirtualizer,
    selectedRanges,
    selectionAnchor,
    showSettings,
    totalRows,
  ]);

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
      searchRequestIdRef.current += 1;
      setSearching(false);
      setSearchStale(false);
      return;
    }
    if (!debouncedSearch) {
      searchRequestIdRef.current += 1;
      setSearchResults(null);
      setSearching(false);
      setSearchStale(false);
      return;
    }

    const requestId = searchRequestIdRef.current + 1;
    searchRequestIdRef.current = requestId;
    setError(null);
    setSearching(true);
    setSearchResults([]);
    setSearchStale(false);
    invoke("search_csv_stream", {
      columnIdx: searchColumn,
      query: debouncedSearch,
      matchCase: searchMatchCase,
      wholeWord: searchWholeWord,
      requestId,
    }).catch((err) => {
      if (requestId !== searchRequestIdRef.current) {
        return;
      }
      setError(typeof err === "string" ? err : "Search failed to complete.");
      setSearching(false);
      setSearchResults(null);
    });
  }, [
    debouncedSearch,
    filePath,
    rowCountReady,
    searchColumn,
    searchMatchCase,
    searchWholeWord,
    searchRefreshToken,
  ]);

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
      searchResultsCountRef.current = 0;
      setCurrentMatch(0);
      if (activeHighlight === "search") {
        setActiveHighlight(null);
      }
      return;
    }
    const prevCount = searchResultsCountRef.current;
    const nextCount = searchResults.length;
    searchResultsCountRef.current = nextCount;
    if (!nextCount) {
      setCurrentMatch(0);
      return;
    }
    if (prevCount === 0) {
      setCurrentMatch(0);
      if (activeHighlight === "search") {
        scrollToMatch(searchResults, 0);
      }
    }
  }, [activeHighlight, scrollToMatch, searchResults]);

  useEffect(() => {
    if (duplicateResults === null) {
      duplicateResultsCountRef.current = 0;
      setCurrentDuplicateMatch(0);
      if (activeHighlight === "duplicates") {
        setActiveHighlight(null);
      }
      return;
    }
    const prevCount = duplicateResultsCountRef.current;
    const nextCount = duplicateResults.length;
    duplicateResultsCountRef.current = nextCount;
    if (!nextCount) {
      setCurrentDuplicateMatch(0);
      return;
    }
    if (prevCount === 0) {
      setCurrentDuplicateMatch(0);
      if (activeHighlight === "duplicates") {
        scrollToMatch(duplicateResults, 0);
      }
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

  const handleSave = useCallback(
    async (forceSaveAs: boolean) => {
      if (!filePath || saving) {
        return;
      }
      if (!hasEdits) {
        return;
      }
      setError(null);
      setSaving(true);

      try {
        let targetPath = savePath ?? filePath;
        if (forceSaveAs || !targetPath) {
          const selected = await save({
            filters: [{ name: "CSV", extensions: ["csv"] }],
            defaultPath: targetPath ?? lastOpenDir ?? undefined,
          });
          if (!selected) {
            setSaving(false);
            return;
          }
          targetPath = selected;
          setSavePath(targetPath);
          const nextDir = getDirFromPath(targetPath);
          if (nextDir) {
            setLastOpenDir(nextDir);
          }
        }

        const rows: string[] = [];
        if (csvFormat.hasHeaders && headers.length) {
          rows.push(
            formatCsvRow(headers, csvFormat.delimiter, csvFormat.quote),
          );
        }

        let start = 0;
        while (true) {
          const chunk = await invoke<string[][]>("get_csv_chunk", {
            start,
            count: CHUNK_SIZE,
          });
          if (chunk.length === 0) {
            break;
          }
          chunk.forEach((row, idx) => {
            const originalIndex = start + idx;
            if (deletedRowsRef.current.has(originalIndex)) {
              return;
            }
            const nextRow = applyEditsToRow(row, originalIndex);
            rows.push(
              formatCsvRow(nextRow, csvFormat.delimiter, csvFormat.quote),
            );
          });
          if (chunk.length < CHUNK_SIZE) {
            break;
          }
          start += chunk.length;
        }

        const contents = rows.join(csvFormat.lineEnding);
        await invoke("write_csv_file", {
          path: targetPath,
          contents,
        });

        setHasEdits(false);
        if (targetPath === filePath) {
          editsRef.current = new Map();
          await handleOpenPath(filePath);
        } else {
          setSavePath(targetPath);
        }
      } catch (err) {
        setError(typeof err === "string" ? err : "Failed to save CSV file.");
      } finally {
        setSaving(false);
      }
    },
    [
      applyEditsToRow,
      csvFormat,
      filePath,
      handleOpenPath,
      hasEdits,
      headers,
      lastOpenDir,
      savePath,
      saving,
    ],
  );

  useEffect(() => {
    if (!filePath) {
      setSortLoading(false);
      return;
    }

    if (!sortState) {
      sortWorkerRequestIdRef.current += 1;
      setSortedIndexLookup(null);
      rowIndexMapRef.current = new Map();
      setRowIndexVersion((prev) => prev + 1);
      dataRef.current = new Map();
      setDataVersion((prev) => prev + 1);
      invoke("clear_sort").catch(() => {});
      setSortLoading(false);
      return;
    }

    const requestId = sortWorkerRequestIdRef.current + 1;
    sortWorkerRequestIdRef.current = requestId;
    setSortLoading(true);
    setSortedIndexLookup(null);
    rowIndexMapRef.current = new Map();
    setRowIndexVersion((prev) => prev + 1);
    dataRef.current = new Map();
    setDataVersion((prev) => prev + 1);
    invoke<number[]>("sort_csv", {
      columnIdx: sortState.column,
      ascending: sortState.direction === "asc",
    })
      .then((order) => {
        if (requestId !== sortWorkerRequestIdRef.current) {
          return;
        }
        const worker = sortWorkerRef.current;
        if (worker) {
          const message: SortWorkerRequest = {
            type: "BUILD_SORT_LOOKUP",
            requestId,
            order,
          };
          pendingSortOrderRef.current = { requestId, order };
          if (sortWorkerTimeoutRef.current !== null) {
            window.clearTimeout(sortWorkerTimeoutRef.current);
          }
          sortWorkerTimeoutRef.current = window.setTimeout(() => {
            if (requestId !== sortWorkerRequestIdRef.current) {
              return;
            }
            appendDebugLog("Sort worker timeout; falling back to main thread.");
            try {
              sortWorkerRef.current?.terminate();
            } catch {
              // ignore
            }
            sortWorkerRef.current = null;
            buildSortLookupChunked(order, requestId);
          }, 15_000);
          try {
            worker.postMessage(message);
            return;
          } catch (err) {
            appendDebugLog(`Sort worker postMessage failed: ${String(err)}`);
            sortWorkerRef.current = null;
          }
        }
        buildSortLookupChunked(order, requestId);
      })
      .catch((err) => {
        if (requestId !== sortWorkerRequestIdRef.current) {
          return;
        }
        appendDebugLog(`sort_csv invoke failed: ${String(err)}`);
        setError(typeof err === "string" ? err : "Failed to sort CSV.");
        setSortState(null);
        setSortLoading(false);
      });
  }, [
    appendDebugLog,
    buildSortLookupChunked,
    filePath,
    rowVirtualizer,
    sortState,
  ]);

  useEffect(() => {
    let active = true;
    let unlistenFns: Array<() => void> = [];

    const setupMenuListeners = async () => {
      const fns = await Promise.all([
        listen("menu-open", () => {
          handlePickFile();
        }),
        listen("menu-save", () => {
          handleSave(false);
        }),
        listen("menu-save-as", () => {
          handleSave(true);
        }),
        listen("menu-clear", () => {
          handleClearFile();
        }),
        listen("open-settings", () => {
          setShowSettings(true);
        }),
        listen("menu-find", () => {
          setShowFind(true);
          setShowDuplicates(false);
          setActiveHighlight("search");
        }),
        listen("menu-clear-search", () => {
          searchRequestIdRef.current += 1;
          setSearchTerm("");
          setSearchResults(null);
          setSearching(false);
          setCurrentMatch(0);
          setActiveHighlight((prev) => (prev === "search" ? null : prev));
          setSearchStale(false);
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
        listen("menu-parse-settings", () => {
          setShowSettings(true);
        }),
        listen<number>("row-count", (event) => {
          setTotalRows(event.payload);
          setRowCountReady(true);
          setLoadingProgress(null);
        }),
        listen<number>("parse-progress", (event) => {
          setLoadingProgress(event.payload);
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
    handleSave,
    toggleTheme,
  ]);

  useEffect(() => {
    let active = true;
    let unlistenFns: Array<() => void> = [];

    const setupStreamListeners = async () => {
      const fns = await Promise.all([
        listen<MatchesChunkPayload>("search-chunk", (event) => {
          const payload = event.payload;
          if (payload.requestId !== searchRequestIdRef.current) {
            return;
          }
          setSearchResults((prev) =>
            prev ? prev.concat(payload.matches) : [...payload.matches],
          );
        }),
        listen<MatchesCompletePayload>("search-complete", (event) => {
          if (event.payload.requestId !== searchRequestIdRef.current) {
            return;
          }
          setSearching(false);
        }),
        listen<MatchesChunkPayload>("duplicates-chunk", (event) => {
          const payload = event.payload;
          if (payload.requestId !== duplicateRequestIdRef.current) {
            return;
          }
          setDuplicateResults((prev) =>
            prev ? prev.concat(payload.matches) : [...payload.matches],
          );
        }),
        listen<MatchesCompletePayload>("duplicates-complete", (event) => {
          if (event.payload.requestId !== duplicateRequestIdRef.current) {
            return;
          }
          setDuplicateChecking(false);
        }),
      ]);

      if (!active) {
        fns.forEach((fn) => fn());
        return;
      }

      unlistenFns = fns;
    };

    setupStreamListeners();

    return () => {
      active = false;
      unlistenFns.forEach((fn) => fn());
    };
  }, []);

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
          setLoadingProgress(null);
          return;
        }
      } catch {
        // Ignore row count polling failures.
      }

      attempts += 1;
      if (!active || attempts >= ROW_COUNT_POLL_MAX) {
        if (active) {
          setLoadingProgress(null);
        }
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
    if (rowCountReady && loadingProgress !== null) {
      setLoadingProgress(null);
    }
  }, [loadingProgress, rowCountReady]);

  useEffect(() => {
    if (!filePath || !rowCountReady) {
      return;
    }
    refreshParseWarnings();
  }, [filePath, refreshParseWarnings, rowCountReady]);

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
  // const duplicateSelectionValue =
  //   duplicateColumn === null ? "row" : String(duplicateColumn);
  // const searchSelectionValue =
  //   searchColumn === null ? "row" : String(searchColumn);
  const parseWarningCount = parseWarnings.length;
  const showFloatingToolbar =
    !!filePath && (parseWarningCount > 0 || isDirty || selectedCount > 1);

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
    (
      event: React.MouseEvent,
      cellText: string | null,
      rowText: string,
      rowIndex: number | null,
      columnIndex: number | null,
    ) => {
      event.preventDefault();
      event.stopPropagation();
      if (rowIndex !== null && rowIndex !== undefined) {
        setSelectedRanges((prev) => {
          const normalized = normalizeSelectionRanges(prev);
          if (isIndexInSelectionRanges(normalized, rowIndex)) {
            return normalized;
          }
          return [{ start: rowIndex, end: rowIndex }];
        });
        setSelectionAnchor(rowIndex);
        setFocusedRow(rowIndex);
      }
      const menuWidth = 200;
      const menuHeight = cellText === null ? 136 : 168;
      const padding = 12;
      const maxX = window.innerWidth - menuWidth - padding;
      const maxY = window.innerHeight - menuHeight - padding;
      const x = Math.max(padding, Math.min(event.clientX, maxX));
      const y = Math.max(padding, Math.min(event.clientY, maxY));
      setContextMenu({ x, y, cellText, rowText, rowIndex, columnIndex });
    },
    [],
  );
  const handleCopy = useCallback(async (value: string) => {
    const ok = await copyToClipboard(value);
    if (!ok) {
      setError("Copy failed (clipboard access may be disabled).");
    } else {
      setNotice("Copied to clipboard.");
    }
    setContextMenu(null);
  }, []);

  const handleSearchFromCell = useCallback((value: string) => {
    setSearchTerm(value);
    setShowFind(true);
    setActiveHighlight("search");
    setContextMenu(null);
    // Focus search input after a short delay
    setTimeout(() => {
      searchInputRef.current?.focus();
    }, 10);
  }, []);

  const contextMenuOriginalRow =
    contextMenu?.rowIndex !== null && contextMenu?.rowIndex !== undefined
      ? getOriginalRowIndex(contextMenu.rowIndex)
      : undefined;
  const contextMenuRowDeleted = isRowDeleted(contextMenuOriginalRow);
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
      {notice ? (
        <div
          className={`notice${showFloatingToolbar ? " with-floating-toolbar" : ""}`}
        >
          {notice}
        </div>
      ) : null}

      <section className={`table-shell${filePath ? "" : " is-empty"}`}>
        {loadingProgress !== null ? (
          <div className="loading-banner">
            <div className="loading-banner-icon">
              <div className="spinner-ring" />
            </div>
            <div className="loading-banner-text">
              Loading {getFileNameFromPath(filePath || "")} (
              {loadingProgress.toLocaleString()} rows)...
            </div>
            {/* Optional: Cancel button if supported */}
          </div>
        ) : null}
        {showHeaderPrompt && filePath ? (
          <div className={`parse-banner${showIndex ? " with-index" : ""}`}>
            <div className="parse-banner-text">
              No header row detected. How should the first row be treated?
            </div>
            <div className="parse-banner-actions">
              <button
                className="btn subtle"
                onClick={() => handleHeaderPromptChoice(true)}
              >
                Use as headers
              </button>
              <button
                className="btn subtle"
                onClick={() => handleHeaderPromptChoice(false)}
              >
                Treat as data
              </button>
              <button
                className="btn subtle"
                onClick={() => setShowHeaderPrompt(false)}
              >
                Dismiss
              </button>
            </div>
          </div>
        ) : null}

        {showFind && filePath ? (
          <div className="find-widget">
            <div className="find-scope-select-wrapper">
              <span className="find-scope-icon">
                {searchColumn === null ? (
                  <svg
                    width="14"
                    height="14"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <path d="M3 3h18v18H3zM21 9H3M21 15H3M12 3v18" />
                  </svg>
                ) : (
                  <svg
                    width="14"
                    height="14"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <path d="M12 3v18" />
                  </svg>
                )}
              </span>
              <select
                className="find-scope-select"
                value={searchColumn === null ? "row" : String(searchColumn)}
                onChange={(e) => {
                  const val = e.target.value;
                  setSearchColumn(val === "row" ? null : Number(val));
                }}
                title="Select search scope"
              >
                <option value="row">Entire Row</option>
                {headerLabels.map((lbl, idx) => (
                  <option key={idx} value={String(idx)}>
                    {lbl}
                  </option>
                ))}
              </select>
              <span className="find-scope-arrow">▼</span>
            </div>

            <div className="find-input-container">
              <span
                className={`find-search-icon${searching ? " spinning" : ""}`}
              >
                {searching ? (
                  <svg
                    width="14"
                    height="14"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83" />
                  </svg>
                ) : (
                  <svg
                    width="14"
                    height="14"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <circle cx="11" cy="11" r="8"></circle>
                    <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
                  </svg>
                )}
              </span>
              <input
                type="text"
                value={searchTerm}
                onChange={(event) => {
                  setSearchTerm(event.target.value);
                  setActiveHighlight("search");
                }}
                placeholder="Find"
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
              <div className="find-input-actions">
                <button
                  className={`find-toggle-btn${searchMatchCase ? " active" : ""}`}
                  onClick={() => setSearchMatchCase((prev) => !prev)}
                  title="Match Case"
                >
                  Aa
                </button>
                <button
                  className={`find-toggle-btn${searchWholeWord ? " active" : ""}`}
                  onClick={() => setSearchWholeWord((prev) => !prev)}
                  title="Match Whole Word"
                >
                  ab
                </button>
              </div>
            </div>

            <span className="find-results-count">
              {searchResults?.length
                ? `${currentMatch + 1} of ${searchResults.length}`
                : "No results"}
            </span>
            {searchStale ? (
              <div className="find-stale">
                <span className="find-results-stale">Results outdated</span>
                <button
                  className="find-rerun-btn"
                  onClick={() => {
                    setSearchRefreshToken((prev) => prev + 1);
                    setActiveHighlight("search");
                  }}
                >
                  Re-run
                </button>
              </div>
            ) : null}

            <button
              className="find-icon-btn"
              onClick={goToPrevMatch}
              disabled={!searchResults?.length}
              title="Previous Match"
            >
              ↑
            </button>
            <button
              className="find-icon-btn"
              onClick={goToNextMatch}
              disabled={!searchResults?.length}
              title="Next Match"
            >
              ↓
            </button>
            <button
              className="find-icon-btn"
              onClick={() => setShowFind(false)}
              title="Close"
            >
              ✕
            </button>
          </div>
        ) : null}
        {showDuplicates && filePath ? (
          <div className="find-widget">
            <div className="find-scope-select-wrapper">
              <span className="find-scope-icon">
                {duplicateColumn === null ? (
                  <svg
                    width="14"
                    height="14"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <path d="M3 3h18v18H3zM21 9H3M21 15H3M12 3v18" />
                  </svg>
                ) : (
                  <svg
                    width="14"
                    height="14"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <path d="M12 3v18" />
                  </svg>
                )}
              </span>
              <select
                className="find-scope-select"
                value={
                  duplicateColumn === null ? "row" : String(duplicateColumn)
                }
                onChange={(e) => {
                  const val = e.target.value;
                  setDuplicateColumn(val === "row" ? null : Number(val));
                }}
                title="Select duplication scope"
              >
                <option value="row">Entire Row</option>
                {headerLabels.map((lbl, idx) => (
                  <option key={idx} value={String(idx)}>
                    {lbl}
                  </option>
                ))}
              </select>
              <span className="find-scope-arrow">▼</span>
            </div>

            <button
              className="find-toggle-btn"
              style={{
                marginLeft: 6,
                height: 26,
                alignSelf: "center",
                border: "1px solid var(--border)",
                borderRadius: 4,
                padding: "0 8px",
              }}
              onClick={handleCheckDuplicates}
              disabled={!filePath || duplicateChecking}
            >
              {duplicateChecking ? "Checking..." : "Check"}
            </button>

            <button
              className="find-toggle-btn"
              style={{
                marginLeft: 4,
                height: 26,
                alignSelf: "center",
                border: "1px solid var(--border)",
                borderRadius: 4,
                padding: "0 8px",
              }}
              onClick={() => {
                setDuplicateResults(null);
                setCurrentDuplicateMatch(0);
                setActiveHighlight((prev) =>
                  prev === "duplicates" ? null : prev,
                );
                setDuplicateStale(false);
              }}
              disabled={duplicateResults === null}
            >
              Clear
            </button>

            <span
              className="find-results-count"
              style={{ flexGrow: 1, justifyContent: "flex-end" }}
            >
              {duplicateResults?.length
                ? `${currentDuplicateMatch + 1} of ${duplicateResults.length}`
                : "No duplicates"}
            </span>
            {duplicateStale ? (
              <div className="find-stale">
                <span className="find-results-stale">Results outdated</span>
                <button
                  className="find-rerun-btn"
                  onClick={handleCheckDuplicates}
                >
                  Re-run
                </button>
              </div>
            ) : null}

            <button
              className="find-icon-btn"
              onClick={goToPrevDuplicate}
              disabled={!duplicateResults?.length}
              title="Previous Match"
            >
              ↑
            </button>
            <button
              className="find-icon-btn"
              onClick={goToNextDuplicate}
              disabled={!duplicateResults?.length}
              title="Next Match"
            >
              ↓
            </button>
            <button
              className="find-icon-btn"
              onClick={() => setShowDuplicates(false)}
              title="Close"
            >
              ✕
            </button>
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
              <button
                className="btn primary empty-cta"
                onClick={handlePickFile}
              >
                Open CSV File
              </button>
              {recentFiles.length ? (
                <div className="empty-recent">
                  <div className="recent-title">Recent files</div>
                  <div className="recent-list">
                    {recentFiles.map((path) => {
                      const name = getFileNameFromPath(path);
                      const dir = getDirFromPath(path);
                      return (
                        <div key={path} className="recent-row">
                          <button
                            type="button"
                            className="recent-item"
                            onClick={() => handleOpenPath(path)}
                            title={path}
                          >
                            <span className="recent-name">{name}</span>
                            <span className="recent-path">{dir ?? path}</span>
                          </button>
                          <button
                            type="button"
                            className="recent-remove"
                            onClick={(e) => {
                              e.stopPropagation();
                              removeRecent(path);
                            }}
                            title="Remove from recent"
                          >
                            ×
                          </button>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ) : null}
            </div>
          )
        ) : (
          <div className="table-wrapper">
            <div
              className="table-header"
              ref={headerRef}
              style={{
                paddingRight: scaleFactor > 1 ? 14 : 0,
              }}
            >
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
            <div
              ref={parentRef}
              className="table-body"
              style={{ overflow: "hidden" }}
              onWheel={(e) => {
                if (scrollRef.current) {
                  scrollRef.current.scrollTop += e.deltaY / scaleFactor;
                }
              }}
            >
              <div
                className="table-spacer"
                style={{
                  height: "100%",
                  position: "relative",
                }}
              >
                <div
                  ref={scrollRef}
                  className="custom-scrollbar"
                  style={{
                    position: "absolute",
                    right: 0,
                    top: 0,
                    bottom: 0,
                    width: 14,
                    overflowX: "hidden",
                    overflowY: "auto",
                    zIndex: 10,
                  }}
                  onScroll={(e) => {
                    e.stopPropagation();
                  }}
                >
                  <div style={{ height: totalSize / scaleFactor }}></div>
                </div>
                {virtualItems.map((virtualRow) => {
                  const rowData = dataRef.current.get(virtualRow.index);
                  const originalIndex = sortState
                    ? rowIndexMapRef.current.get(virtualRow.index)
                    : virtualRow.index;
                  const isDeleted = isRowDeleted(originalIndex);
                  const isSelected = isIndexInSelectionRanges(
                    selectedRanges,
                    virtualRow.index,
                  );
                  const displayRow = rowData
                    ? applyEditsToRow(rowData, originalIndex)
                    : null;
                  const rowNumber = (originalIndex ?? virtualRow.index) + 1;
                  const rowText = displayRow
                    ? formatCsvRow(
                        displayRow,
                        csvFormat.delimiter,
                        csvFormat.quote,
                      )
                    : "";
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
                      className={`table-row${isMatch ? " match" : ""}${isCurrent ? " current" : ""}${isEven ? " even" : " odd"}${isDeleted ? " deleted" : ""}${isSelected ? " selected" : ""}`}
                      style={{
                        height: `${virtualRow.size}px`,
                        transform: `translateY(${virtualRow.start - (rowVirtualizer.scrollOffset ?? 0)}px)`,
                      }}
                      onMouseDown={(event) => {
                        if (event.button !== 0) {
                          return;
                        }
                        selectDisplayRow(virtualRow.index, {
                          shiftKey: event.shiftKey,
                          toggleKey: event.metaKey || event.ctrlKey,
                        });
                      }}
                    >
                      {showIndex ? (
                        <div
                          className="table-cell index"
                          onContextMenu={(event) => {
                            if (!rowData) {
                              return;
                            }
                            openContextMenu(
                              event,
                              null,
                              rowText,
                              virtualRow.index,
                              null,
                            );
                          }}
                        >
                          {rowNumber}
                        </div>
                      ) : null}
                      {displayRow ? (
                        displayRow.map((cell, cellIdx) => {
                          const cellValue = cell ?? "";
                          let isCellMatch = false;
                          if (
                            searchHighlightActive &&
                            (searchColumn === null || cellIdx === searchColumn)
                          ) {
                            if (!searchMatchCase) {
                              const scLower = cellValue.toLowerCase();
                              if (searchWholeWord) {
                                isCellMatch = scLower === searchQueryLower;
                              } else {
                                isCellMatch =
                                  scLower.includes(searchQueryLower);
                              }
                            } else {
                              // Use raw debouncedSearch without trimming? Or trimmed but raw case?
                              // searchQueryLower was computed as trimmed lower.
                              // I should use `debouncedSearch.trim()`.
                              const queryRaw = debouncedSearch.trim();
                              if (searchWholeWord) {
                                isCellMatch = cellValue === queryRaw;
                              } else {
                                isCellMatch = cellValue.includes(queryRaw);
                              }
                            }
                          }
                          return (
                            <div
                              key={cellIdx}
                              className={`table-cell${isCellMatch ? " cell-match" : ""}${
                                editingCell?.displayRow === virtualRow.index &&
                                editingCell.column === cellIdx
                                  ? " editing"
                                  : ""
                              }${isDeleted ? " deleted" : ""}`}
                              style={
                                columnStyles[cellIdx] ?? defaultColumnStyle
                              }
                              title={cellValue}
                              onContextMenu={(event) =>
                                openContextMenu(
                                  event,
                                  cellValue,
                                  rowText,
                                  virtualRow.index,
                                  cellIdx,
                                )
                              }
                              onDoubleClick={() =>
                                startEditCell(
                                  virtualRow.index,
                                  cellIdx,
                                  cellValue,
                                  rowData?.[cellIdx] ?? "",
                                )
                              }
                              onKeyDown={(event) => {
                                if (event.key === "Enter") {
                                  event.preventDefault();
                                  startEditCell(
                                    virtualRow.index,
                                    cellIdx,
                                    cellValue,
                                    rowData?.[cellIdx] ?? "",
                                  );
                                }
                              }}
                              tabIndex={0}
                            >
                              {editingCell?.displayRow === virtualRow.index &&
                              editingCell.column === cellIdx ? (
                                <input
                                  ref={editInputRef}
                                  className="cell-editor"
                                  value={editingCell.value}
                                  onChange={(event) =>
                                    setEditingCell((prev) =>
                                      prev
                                        ? { ...prev, value: event.target.value }
                                        : prev,
                                    )
                                  }
                                  onBlur={commitEdit}
                                  onKeyDown={(event) => {
                                    event.stopPropagation();
                                    if (event.key === "Enter") {
                                      event.preventDefault();
                                      commitEdit();
                                    }
                                    if (event.key === "Escape") {
                                      event.preventDefault();
                                      cancelEdit();
                                    }
                                  }}
                                />
                              ) : (
                                cellValue
                              )}
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
            {showFloatingToolbar ? (
              <div
                className={`floating-toolbar${loadingRows ? " with-loading" : ""}`}
              >
                {parseWarningCount > 0 || selectedCount > 1 ? (
                  <div className="floating-toolbar-surface">
                    {parseWarningCount > 0 ? (
                      <span className="parse-warning-pill">
                        {parseWarningCount} warning
                        {parseWarningCount === 1 ? "" : "s"}
                      </span>
                    ) : null}
                    {selectedCount > 1 ? (
                      <span className="selection-pill">
                        {selectedCount.toLocaleString()} selected
                      </span>
                    ) : null}
                  </div>
                ) : (
                  <span />
                )}
                {selectedCount > 1 || isDirty ? (
                  <div className="floating-toolbar-surface floating-toolbar-actions">
                    {selectedCount > 1 ? (
                      <>
                        <button
                          className="btn subtle"
                          onClick={() => void copySelectedRows()}
                          title="Copy selected rows (Ctrl/Cmd+C)"
                        >
                          Copy selected
                        </button>
                        <button
                          className="btn subtle"
                          onClick={() => void deleteSelectedRows()}
                          title="Delete selected rows (Delete)"
                        >
                          Delete selected
                        </button>
                        <button
                          className="btn subtle"
                          onClick={() => void restoreSelectedRows()}
                          title="Restore selected rows"
                        >
                          Restore selected
                        </button>
                      </>
                    ) : null}
                    {isDirty ? (
                      <>
                        <span className="table-toolbar-meta">
                          Unsaved edits
                        </span>
                        <button
                          className="btn subtle"
                          onClick={() => handleSave(false)}
                          disabled={saving}
                        >
                          {saving ? "Saving..." : "Save"}
                        </button>
                        <button
                          className="btn subtle"
                          onClick={() => handleSave(true)}
                          disabled={saving}
                        >
                          Save As
                        </button>
                      </>
                    ) : null}
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
        )}
      </section>
      {showSettings ? (
        <div className="modal-overlay" onClick={() => setShowSettings(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Settings</h2>
              <button
                className="close-button"
                onClick={() => setShowSettings(false)}
              >
                &times;
              </button>
            </div>
            <div className="modal-body">
              <div className="setting-group">
                <h3>Appearance</h3>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Theme</span>
                    <select
                      className="setting-select"
                      value={themePreference}
                      onChange={(e) => {
                        const val = e.target.value as ThemePreference;
                        setThemePreference(val);
                      }}
                    >
                      <option value="system">System</option>
                      <option value="light">Light</option>
                      <option value="dark">Dark</option>
                    </select>
                  </div>
                </div>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Row Height</span>
                    <select
                      className="setting-select"
                      value={
                        rowHeight === DEFAULT_ROW_HEIGHT
                          ? "default"
                          : rowHeight < DEFAULT_ROW_HEIGHT
                            ? "compact"
                            : "spacious"
                      }
                      onChange={(e) => {
                        const val = e.target.value;
                        if (val === "compact") setRowHeight(24);
                        else if (val === "spacious") setRowHeight(48);
                        else setRowHeight(DEFAULT_ROW_HEIGHT);
                      }}
                    >
                      <option value="compact">Compact</option>
                      <option value="default">Default</option>
                      <option value="spacious">Spacious</option>
                    </select>
                  </div>
                </div>
              </div>

              <div className="setting-group">
                <h3>View</h3>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Show Line Numbers</span>
                    <label className="toggle-switch">
                      <input
                        type="checkbox"
                        checked={showIndex}
                        onChange={(e) => {
                          const checked = e.target.checked;
                          setShowIndex(checked);
                          invoke("set_show_index_checked", {
                            checked,
                          }).catch(console.error);
                        }}
                      />
                      <span className="slider"></span>
                    </label>
                  </div>
                </div>
              </div>

              <div className="setting-group">
                <h3>Search & Parsing</h3>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">
                      Enable Search Indexing
                    </span>
                    <label className="toggle-switch">
                      <input
                        type="checkbox"
                        checked={enableIndexing}
                        onChange={(e) => {
                          setEnableIndexing(e.target.checked);
                        }}
                      />
                      <span className="slider"></span>
                    </label>
                  </div>
                  <p className="setting-description">
                    Speeds up search by building an in-memory index. Uses
                    significantly more RAM (~500MB per 10M rows).
                    <br />
                    <em>Change requires reopening the file.</em>
                  </p>
                </div>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Delimiter</span>
                    <select
                      className="setting-select"
                      value={parseOverrides.delimiter}
                      onChange={(event) => {
                        const value = event.target
                          .value as ParseOverridesState["delimiter"];
                        setParseOverrides((prev) => ({
                          ...prev,
                          delimiter: value,
                        }));
                      }}
                    >
                      <option value="auto">Auto</option>
                      <option value="comma">Comma</option>
                      <option value="tab">Tab</option>
                      <option value="semicolon">Semicolon</option>
                      <option value="pipe">Pipe</option>
                      <option value="space">Space</option>
                      <option value="custom">Custom</option>
                    </select>
                  </div>
                  {parseOverrides.delimiter === "custom" ? (
                    <div
                      className="setting-item-row"
                      style={{ marginTop: 8, justifyContent: "flex-end" }}
                    >
                      <input
                        type="text"
                        className="setting-select"
                        style={{ width: 120 }}
                        value={parseOverrides.delimiterCustom}
                        onChange={(event) =>
                          setParseOverrides((prev) => ({
                            ...prev,
                            delimiterCustom: event.target.value,
                          }))
                        }
                        placeholder="Char"
                      />
                    </div>
                  ) : null}
                </div>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Quote Char</span>
                    <select
                      className="setting-select"
                      value={parseOverrides.quote}
                      onChange={(event) => {
                        const value = event.target
                          .value as ParseOverridesState["quote"];
                        setParseOverrides((prev) => ({
                          ...prev,
                          quote: value,
                        }));
                      }}
                    >
                      <option value="auto">Auto</option>
                      <option value="double">Double</option>
                      <option value="single">Single</option>
                    </select>
                  </div>
                </div>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Escape Char</span>
                    <select
                      className="setting-select"
                      value={parseOverrides.escape}
                      onChange={(event) => {
                        const value = event.target
                          .value as ParseOverridesState["escape"];
                        setParseOverrides((prev) => ({
                          ...prev,
                          escape: value,
                        }));
                      }}
                    >
                      <option value="auto">Auto</option>
                      <option value="none">None</option>
                      <option value="backslash">Backslash</option>
                    </select>
                  </div>
                </div>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Encoding</span>
                    <select
                      className="setting-select"
                      value={parseOverrides.encoding}
                      onChange={(event) => {
                        const value = event.target
                          .value as ParseOverridesState["encoding"];
                        setParseOverrides((prev) => ({
                          ...prev,
                          encoding: value,
                        }));
                      }}
                    >
                      <option value="auto">Auto</option>
                      <option value="utf-8">UTF-8</option>
                      <option value="windows-1252">Windows-1252</option>
                      <option value="iso-8859-1">ISO-8859-1</option>
                      <option value="utf-16le">UTF-16 LE</option>
                      <option value="utf-16be">UTF-16 BE</option>
                    </select>
                  </div>
                </div>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Headers</span>
                    <select
                      className="setting-select"
                      value={parseOverrides.hasHeaders}
                      onChange={(event) => {
                        const value = event.target
                          .value as ParseOverridesState["hasHeaders"];
                        setParseOverrides((prev) => ({
                          ...prev,
                          hasHeaders: value,
                        }));
                        setShowHeaderPrompt(false);
                      }}
                    >
                      <option value="auto">Auto</option>
                      <option value="yes">Use first row</option>
                      <option value="no">No headers</option>
                    </select>
                  </div>
                </div>
                <div className="setting-item setting-actions">
                  <button
                    className="btn subtle"
                    onClick={() => applyParseOverrides(DEFAULT_PARSE_OVERRIDES)}
                  >
                    Reset Defaults
                  </button>
                  <button className="btn secondary" onClick={handleApplyParse}>
                    Reload File
                  </button>
                </div>
              </div>
              <div className="setting-group">
                <h3>Diagnostics</h3>
                <div className="setting-item">
                  <div className="setting-item-row">
                    <span className="setting-label">Enable Debug Logging</span>
                    <label className="toggle-switch">
                      <input
                        type="checkbox"
                        checked={debugLogging}
                        onChange={(e) => setDebugLogging(e.target.checked)}
                      />
                      <span className="slider"></span>
                    </label>
                  </div>
                  <p className="setting-description">
                    When enabled, QuickRows writes extra logs to help diagnose
                    hangs and crashes.
                  </p>
                  <div
                    className="setting-item-row"
                    style={{ justifyContent: "flex-end", gap: 8 }}
                  >
                    <button
                      className="btn secondary small"
                      type="button"
                      disabled={!debugLogPath && !crashLogPath}
                      onClick={async () => {
                        try {
                          const baseDir = await appDataDir();
                          const logsDir = await join(
                            baseDir,
                            "csv-index-cache",
                          );
                          await openPath(logsDir);
                          return;
                        } catch (err) {
                          const path = debugLogPath || crashLogPath;
                          if (path) {
                            try {
                              await openPath(path);
                              return;
                            } catch (openErr) {
                              console.error(openErr);
                            }
                          }
                          console.error(err);
                          setError(
                            "Failed to open log folder. Open it manually if needed.",
                          );
                        }
                      }}
                    >
                      Open Log Folder
                    </button>
                  </div>
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button
                className="btn primary"
                onClick={() => setShowSettings(false)}
              >
                Done
              </button>
            </div>
          </div>
        </div>
      ) : null}
      {contextMenu ? (
        <div
          className="context-menu"
          style={{ left: `${contextMenu.x}px`, top: `${contextMenu.y}px` }}
          onClick={(event) => event.stopPropagation()}
          onContextMenu={(event) => event.preventDefault()}
        >
          {selectedCount > 1 ? (
            <>
              <button
                type="button"
                className="context-menu-item"
                onClick={() => {
                  setContextMenu(null);
                  void copySelectedRows();
                }}
              >
                Copy selected rows
              </button>
              <button
                type="button"
                className="context-menu-item"
                onClick={() => {
                  setContextMenu(null);
                  void deleteSelectedRows();
                }}
              >
                Delete selected
              </button>
              <button
                type="button"
                className="context-menu-item"
                onClick={() => {
                  setContextMenu(null);
                  void restoreSelectedRows();
                }}
              >
                Restore selected
              </button>
            </>
          ) : (
            <>
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
                  if (
                    contextMenu.cellText === null ||
                    contextMenu.rowIndex === null ||
                    contextMenu.columnIndex === null
                  ) {
                    return;
                  }
                  setContextMenu(null);
                  startEditCell(
                    contextMenu.rowIndex,
                    contextMenu.columnIndex,
                    contextMenu.cellText,
                    dataRef.current.get(contextMenu.rowIndex)?.[
                      contextMenu.columnIndex
                    ] ?? contextMenu.cellText,
                  );
                }}
                disabled={
                  contextMenu.cellText === null ||
                  contextMenu.rowIndex === null ||
                  contextMenu.columnIndex === null ||
                  contextMenuRowDeleted
                }
              >
                Edit cell
              </button>
              <button
                type="button"
                className="context-menu-item"
                onClick={() => {
                  if (contextMenu.rowIndex === null) {
                    return;
                  }
                  setContextMenu(null);
                  if (contextMenuRowDeleted) {
                    restoreRow(contextMenu.rowIndex);
                    return;
                  }
                  deleteRow(contextMenu.rowIndex);
                }}
                disabled={contextMenu.rowIndex === null}
              >
                {contextMenuRowDeleted ? "Restore row" : "Delete row"}
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
              <button
                type="button"
                className="context-menu-item"
                onClick={() => {
                  if (contextMenu.cellText === null) {
                    return;
                  }
                  handleSearchFromCell(contextMenu.cellText);
                }}
                disabled={contextMenu.cellText === null}
              >
                Search for this
              </button>
            </>
          )}
        </div>
      ) : null}
    </main>
  );
}

export default App;
