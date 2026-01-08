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
const MAX_PARSE_WARNINGS = 200;

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
    .catch(() => { });
};
const formatCsvCell = (value: string) => {
  if (!/[",\n\r]/.test(value)) {
    return value;
  }
  return `"${value.replace(/"/g, '""')}"`;
};
const formatCsvRow = (row: string[]) =>
  row.map((cell) => formatCsvCell(cell)).join(",");
const formatDelimiterLabel = (value: string) => {
  if (value === "\t") return "Tab";
  if (value === " ") return "Space";
  if (value === ",") return "Comma";
  if (value === ";") return "Semicolon";
  if (value === "|") return "Pipe";
  return value;
};
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
  const [parseDetected, setParseDetected] = useState<ParseInfo | null>(null);
  const [parseEffective, setParseEffective] = useState<ParseInfo | null>(null);
  const [parseWarnings, setParseWarnings] = useState<ParseWarning[]>([]);
  const [showParseSettings, setShowParseSettings] = useState(false);
  const [showHeaderPrompt, setShowHeaderPrompt] = useState(false);
  const [parseOverrides, setParseOverrides] = useState<ParseOverridesState>(
    DEFAULT_PARSE_OVERRIDES,
  );
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [loadingProgress, setLoadingProgress] = useState<number | null>(null);
  const dataRef = useRef<Map<number, string[]>>(new Map());
  const rowIndexMapRef = useRef<Map<number, number>>(new Map());
  const [, setDataVersion] = useState(0);
  const [, setRowIndexVersion] = useState(0);

  const debouncedSearch = useDebounce(searchTerm, 450);
  const parentRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const headerRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const resizeRef = useRef<{
    columnIndex: number;
    startX: number;
    startWidth: number;
  } | null>(null);
  const resizeFrameRef = useRef<number | null>(null);
  const pendingWidthRef = useRef<number | null>(null);

  useEffect(() => {
    if (error) {
      const timer = setTimeout(() => setError(null), 3000);
      return () => clearTimeout(timer);
    }
  }, [error]);

  const MAX_SCROLL_PIXELS = 15_000_000;
  const totalSize = totalRows * rowHeight;
  const scaleFactor = totalSize > MAX_SCROLL_PIXELS ? totalSize / MAX_SCROLL_PIXELS : 1;

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
        return () => { };
      }
      const onScroll = () => {
        cb(el.scrollTop * scaleFactorRef.current, true);
      };
      el.addEventListener('scroll', onScroll, { passive: true });
      return () => {
        el.removeEventListener('scroll', onScroll);
      };
    },
    scrollToFn: (offset, _options, instance) => {
      instance.scrollElement?.scrollTo(0, offset / scaleFactorRef.current);
    }
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
    parseOverrides,
  ]);

  useEffect(() => {
    invoke("set_show_index_checked", { checked: showIndex }).catch(() => { });
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
    setParseDetected(null);
    setParseEffective(null);
    setParseWarnings([]);
    setShowParseSettings(false);
    setShowHeaderPrompt(false);
    setContextMenu(null);
    dataRef.current = new Map();
    setDataVersion((prev) => prev + 1);
    setRowCountReady(false);
    invoke("clear_sort").catch(() => { });
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
      .catch(() => { });
  }, [appendParseWarnings]);

  const handleOpenPath = useCallback(
    async (path: string, overrides?: Record<string, unknown>) => {
      try {
        const csvMetadata = await invoke<CsvMetadata>("load_csv_metadata", {
          path,
          overrides: overrides ?? buildParseOverrides(),
        });
        resetForNewFile();
        setFilePath(path);
        setWindowTitle(path);
        setHeaders(csvMetadata.headers);
        setParseDetected(csvMetadata.detected);
        setParseEffective(csvMetadata.effective);
        setParseWarnings(csvMetadata.warnings ?? []);
        invoke("get_parse_warnings", { clear: true }).catch(() => { });
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
        hasHeaders: (useHeaders ? "yes" : "no") as ParseOverridesState["hasHeaders"],
      };
      setShowHeaderPrompt(false);
      applyParseOverrides(nextOverrides);
    },
    [applyParseOverrides, parseOverrides],
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
    setParseDetected(null);
    setParseEffective(null);
    setParseWarnings([]);
    setShowParseSettings(false);
    setShowHeaderPrompt(false);
    setContextMenu(null);
    invoke("clear_sort").catch(() => { });
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
      matchCase: searchMatchCase,
      wholeWord: searchWholeWord,
    })
      .then((results) => setSearchResults(results))
      .catch((err) => {
        setError(typeof err === "string" ? err : "Search failed to complete.");
      })
      .finally(() => setSearching(false));
  }, [debouncedSearch, filePath, rowCountReady, searchColumn, searchMatchCase, searchWholeWord]);

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
      invoke("clear_sort").catch(() => { });
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
        listen("menu-parse-settings", () => {
          setShowParseSettings(true);
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
      .catch(() => { })
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
        {loadingProgress !== null ? (
          <div className="loading-banner">
            <div className="loading-banner-icon">
              <div className="spinner-ring" />
            </div>
            <div className="loading-banner-text">
              Loading {getFileNameFromPath(filePath || "")} ({loadingProgress.toLocaleString()} rows)...
            </div>
            {/* Optional: Cancel button if supported */}
          </div>
        ) : null}
        {filePath && parseWarningCount > 0 ? (
          <div className={`table-toolbar${showIndex ? " with-index" : ""}`}>
            <span className="parse-warning-pill">
              {parseWarningCount} warning
              {parseWarningCount === 1 ? "" : "s"}
            </span>
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
        {showParseSettings && filePath ? (
          <div className={`parse-panel${showIndex ? " with-index" : ""}`}>
            <div className="parse-grid">
              <label className="parse-field">
                <span>Delimiter</span>
                <select
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
                {parseOverrides.delimiter === "custom" ? (
                  <input
                    type="text"
                    value={parseOverrides.delimiterCustom}
                    onChange={(event) =>
                      setParseOverrides((prev) => ({
                        ...prev,
                        delimiterCustom: event.target.value,
                      }))
                    }
                    placeholder="Delimiter"
                  />
                ) : null}
              </label>
              <label className="parse-field">
                <span>Quote</span>
                <select
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
              </label>
              <label className="parse-field">
                <span>Escape</span>
                <select
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
              </label>
              <label className="parse-field">
                <span>Line ending</span>
                <select
                  value={parseOverrides.lineEnding}
                  onChange={(event) => {
                    const value = event.target
                      .value as ParseOverridesState["lineEnding"];
                    setParseOverrides((prev) => ({
                      ...prev,
                      lineEnding: value,
                    }));
                  }}
                >
                  <option value="auto">Auto</option>
                  <option value="lf">LF</option>
                  <option value="crlf">CRLF</option>
                  <option value="cr">CR</option>
                </select>
              </label>
              <label className="parse-field">
                <span>Encoding</span>
                <select
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
              </label>
            </div>
            <div className="parse-grid">
              <label className="parse-field">
                <span>Headers</span>
                <select
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
              </label>
              <label className="parse-field">
                <span>Malformed rows</span>
                <select
                  value={parseOverrides.malformed}
                  onChange={(event) => {
                    const value = event.target
                      .value as ParseOverridesState["malformed"];
                    setParseOverrides((prev) => ({
                      ...prev,
                      malformed: value,
                    }));
                  }}
                >
                  <option value="strict">Strict</option>
                  <option value="skip">Skip</option>
                  <option value="repair">Repair</option>
                </select>
              </label>
              <label className="parse-field">
                <span>Max field (bytes)</span>
                <input
                  type="number"
                  min={1024}
                  value={parseOverrides.maxFieldSize}
                  onChange={(event) => {
                    const value = Number(event.target.value);
                    if (!Number.isFinite(value)) {
                      return;
                    }
                    setParseOverrides((prev) => ({
                      ...prev,
                      maxFieldSize: value,
                    }));
                  }}
                />
              </label>
              <label className="parse-field">
                <span>Max row (bytes)</span>
                <input
                  type="number"
                  min={4096}
                  value={parseOverrides.maxRecordSize}
                  onChange={(event) => {
                    const value = Number(event.target.value);
                    if (!Number.isFinite(value)) {
                      return;
                    }
                    setParseOverrides((prev) => ({
                      ...prev,
                      maxRecordSize: value,
                    }));
                  }}
                />
              </label>
            </div>
            <div className="parse-actions">
              <div className="parse-meta">
                {parseDetected ? (
                  <>
                    Detected: {formatDelimiterLabel(parseDetected.delimiter)}{" "}
                    delimiter, {parseDetected.encoding} encoding,{" "}
                    {parseDetected.line_ending.toUpperCase()} line ending
                    {parseEffective ? (
                      <>
                        {" "}
                        · Effective:{" "}
                        {formatDelimiterLabel(parseEffective.delimiter)}{" "}
                        delimiter, {parseEffective.encoding} encoding
                      </>
                    ) : null}
                  </>
                ) : (
                  "No detection info yet."
                )}
              </div>
              <div className="parse-buttons">
                <button className="btn subtle" onClick={handleApplyParse}>
                  Apply
                </button>
                <button
                  className="btn subtle"
                  onClick={() => applyParseOverrides(DEFAULT_PARSE_OVERRIDES)}
                >
                  Reset
                </button>
                <button
                  className="btn subtle"
                  onClick={() => setShowParseSettings(false)}
                >
                  Close
                </button>
              </div>
            </div>
            {parseWarnings.length ? (
              <div className="parse-warnings">
                <div className="parse-warnings-title">Latest warnings</div>
                <div className="parse-warnings-list">
                  {parseWarnings.slice(-12).map((warning, idx) => (
                    <div
                      className="parse-warning"
                      key={`${warning.kind}-${idx}`}
                    >
                      <span className="parse-warning-id">
                        Row{" "}
                        {warning.record !== undefined
                          ? warning.record + 1
                          : "?"}
                      </span>
                      <span className="parse-warning-text">
                        {warning.message}
                        {warning.field !== undefined
                          ? ` (col ${warning.field + 1})`
                          : ""}
                        {warning.expected_len !== undefined &&
                          warning.len !== undefined
                          ? ` (${warning.len}/${warning.expected_len} fields)`
                          : ""}
                      </span>
                    </div>
                  ))}
                </div>
                {parseWarnings.length > 12 ? (
                  <div className="parse-warnings-meta">
                    Showing 12 of {parseWarnings.length} warnings.
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
        ) : null}
        {showFind && filePath ? (
          <div className="find-widget">
            <div className="find-scope-select-wrapper">
              <span className="find-scope-icon">
                {searchColumn === null ? (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 3h18v18H3zM21 9H3M21 15H3M12 3v18" /></svg>
                ) : (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 3v18" /></svg>
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
                  <option key={idx} value={String(idx)}>{lbl}</option>
                ))}
              </select>
              <span className="find-scope-arrow">▼</span>
            </div>

            <div className="find-input-container">
              <span className={`find-search-icon${searching ? " spinning" : ""}`}>
                {searching ? (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83" /></svg>
                ) : (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
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
                  onClick={() => setSearchMatchCase(prev => !prev)}
                  title="Match Case"
                >
                  Aa
                </button>
                <button
                  className={`find-toggle-btn${searchWholeWord ? " active" : ""}`}
                  onClick={() => setSearchWholeWord(prev => !prev)}
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

            <button className="find-icon-btn" onClick={goToPrevMatch} disabled={!searchResults?.length} title="Previous Match">
              ↑
            </button>
            <button className="find-icon-btn" onClick={goToNextMatch} disabled={!searchResults?.length} title="Next Match">
              ↓
            </button>
            <button className="find-icon-btn" onClick={() => setShowFind(false)} title="Close">
              ✕
            </button>
          </div>
        ) : null}
        {showDuplicates && filePath ? (
          <div className="find-widget">
            <div className="find-scope-select-wrapper">
              <span className="find-scope-icon">
                {duplicateColumn === null ? (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 3h18v18H3zM21 9H3M21 15H3M12 3v18" /></svg>
                ) : (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 3v18" /></svg>
                )}
              </span>
              <select
                className="find-scope-select"
                value={duplicateColumn === null ? "row" : String(duplicateColumn)}
                onChange={(e) => {
                  const val = e.target.value;
                  setDuplicateColumn(val === "row" ? null : Number(val));
                }}
                title="Select duplication scope"
              >
                <option value="row">Entire Row</option>
                {headerLabels.map((lbl, idx) => (
                  <option key={idx} value={String(idx)}>{lbl}</option>
                ))}
              </select>
              <span className="find-scope-arrow">▼</span>
            </div>

            <button
              className="find-toggle-btn"
              style={{ marginLeft: 6, height: 26, alignSelf: "center", border: "1px solid var(--border)", borderRadius: 4, padding: "0 8px" }}
              onClick={handleCheckDuplicates}
              disabled={!filePath || duplicateChecking}
            >
              {duplicateChecking ? "Checking..." : "Check"}
            </button>

            <button
              className="find-toggle-btn"
              style={{ marginLeft: 4, height: 26, alignSelf: "center", border: "1px solid var(--border)", borderRadius: 4, padding: "0 8px" }}
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

            <span className="find-results-count" style={{ flexGrow: 1, justifyContent: "flex-end" }}>
              {duplicateResults?.length
                ? `${currentDuplicateMatch + 1} of ${duplicateResults.length}`
                : "No duplicates"}
            </span>

            <button className="find-icon-btn" onClick={goToPrevDuplicate} disabled={!duplicateResults?.length} title="Previous Match">
              ↑
            </button>
            <button className="find-icon-btn" onClick={goToNextDuplicate} disabled={!duplicateResults?.length} title="Next Match">
              ↓
            </button>
            <button className="find-icon-btn" onClick={() => setShowDuplicates(false)} title="Close">
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
                paddingRight: scaleFactor > 1 ? 14 : 0
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
              style={{ overflow: 'hidden' }}
              onWheel={(e) => {
                if (scrollRef.current) {
                  scrollRef.current.scrollTop += e.deltaY / scaleFactor;
                }
              }}
            >
              <div
                className="table-spacer"
                style={{
                  height: '100%',
                  position: 'relative'
                }}
              >
                <div
                  ref={scrollRef}
                  className="custom-scrollbar"
                  style={{
                    position: 'absolute',
                    right: 0,
                    top: 0,
                    bottom: 0,
                    width: 14,
                    overflowX: 'hidden',
                    overflowY: 'auto',
                    zIndex: 10
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
                        transform: `translateY(${virtualRow.start - (rowVirtualizer.scrollOffset ?? 0)}px)`,
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
                            if (!searchMatchCase) {
                              const scLower = cellValue.toLowerCase();
                              if (searchWholeWord) {
                                isCellMatch = scLower === searchQueryLower;
                              } else {
                                isCellMatch = scLower.includes(searchQueryLower);
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
        </div>
      ) : null}
    </main>
  );
}

export default App;
