/**
 * Google Apps Script adapter implementing the ISheetAdapter interface.
 *
 * Wraps one sheet tab of a Spreadsheet and translates 0-based data indexes
 * used by the ORM into sheet coordinates (row 1 = header, data from row 2).
 *
 * Two execution modes:
 *  - **Sheets API mode** (preferred): when the Sheets advanced service is
 *    bound, every mutation is issued through Sheets.Spreadsheets API calls —
 *    `Values.batchUpdate` for writes (multiple disjoint ranges collapse into
 *    a single HTTP request) and `Spreadsheets.batchUpdate` for structural
 *    changes.  This cuts both per-call latency and call count dramatically.
 *  - **SpreadsheetApp fallback**: the classic Range/setValues path, used when
 *    the service is unavailable or an individual API request fails.
 *
 * Invariants maintained across both modes:
 *  - `knownLastRow`/`knownLastCol`/`knownHeaders` track dimensions so
 *    getRowCount()/getHeaders()/appendRows() avoid extra RPCs.
 *  - `gridCache` holds the A1-anchored raw grid and is kept consistent on
 *    writes, so interleaved reads cost zero additional RPCs.
 *  - `name` (the tab title) is memoised — getName() is itself an RPC.
 *
 * @module GoogleSheetAdapter
 */

import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";
import {
  adjustSheetMetaRows,
  a1Quote,
  bumpSheetMetaRows,
  colToLetter,
  drainPendingWrites,
  enqueueValuesWrite,
  flushPendingWrites,
  flushRawWrites,
  getSheetMeta,
  invalidateSheetMeta,
  isGridLimitError,
  isQuotaError,
  letterToCol,
  markRawWrite,
  markSheetsAvailable,
  markSheetsUnavailable,
  peekSheetMeta,
  readsReady,
  registerSheetMeta,
  requeuePendingWrites,
  sheetsService,
  sheetsServiceUnchecked,
  sleepMs,
  toRowData,
  writesReady,
} from "./SheetsRpc.js";
import type { PendingValuesWrite, SheetMeta } from "./SheetsRpc.js";

/** Optional seed values describing a freshly created (empty) sheet. */
export interface SheetDimensions {
  lastRow?: number;
  lastCol?: number;
  headers?: string[];
  /** Known tab title — memoises getName() without an extra RPC. */
  title?: string;
  /**
   * A1-anchored full-grid contents when already known at construction
   * (e.g. the sheet was just created by a fused addSheet+updateCells
   * request).  Seeds `gridCache` so later reads cost zero RPCs.
   */
  grid?: unknown[][];
}

/** Context that enables Sheets API fast-paths without a raw Sheet handle. */
export interface SheetApiContext {
  /** Owner spreadsheet (lazy raw-sheet resolution for fallback paths). */
  spreadsheet: GoogleAppsScript.Spreadsheet.Spreadsheet;
  /** Spreadsheet ID for API calls. */
  ssId: string;
  /** Numeric sheet ID (grid ranges, deleteDimension, updateCells). */
  sheetId: number;
  /** Current tab title (for A1 ranges). */
  title: string;
}

/**
 * Production implementation of {@link ISheetAdapter} for Google Apps Script.
 * Delegates to the Sheets API when bound, SpreadsheetApp otherwise.
 */
export class GoogleSheetAdapter implements ISheetAdapter {
  /** The wrapped GAS Sheet object (null in pure API mode until lazily resolved). */
  private sheet: GoogleAppsScript.Spreadsheet.Sheet | null;

  /** Memoised sheet name — sheet.getName() is itself a GAS API call. */
  private name: string | null = null;

  /** Last row with content (1-based, includes header row). null = unknown. */
  private knownLastRow: number | null = null;
  /** Last column with content (1-based). null = unknown. */
  private knownLastCol: number | null = null;
  /** Memoised header row values. null = unknown. */
  private knownHeaders: string[] | null = null;

  /**
   * Raw grid cache: the full A1-anchored data matrix (row 0 = headers).
   * Populated by any full-grid read; invalidated/updated by every write.
   */
  private gridCache: unknown[][] | null = null;

  /** Sheets API context (present when constructed via the API adapter path). */
  private api: SheetApiContext | null = null;

  /** Memoised spreadsheet ID for raw-mode adapters (getId() is an RPC). */
  private ssIdCache: string | null = null;

  /**
   * @param sheet - A GAS Sheet object (from getSheetByName/insertSheet), or
   *                null in pure API mode.
   * @param dims  - Optional pre-known dimensions (a fresh insertSheet is
   *                always empty: lastRow=0, lastCol=0, headers=[]).
   * @param api   - Optional Sheets API context enabling fast paths.
   */
  constructor(
    sheet: GoogleAppsScript.Spreadsheet.Sheet | null,
    dims?: SheetDimensions,
    api?: SheetApiContext,
  ) {
    this.sheet = sheet;
    this.api = api ?? null;
    if (api) this.name = api.title;
    else if (dims?.title) this.name = dims.title;
    if (dims) {
      if (dims.lastRow !== undefined) this.knownLastRow = dims.lastRow;
      if (dims.lastCol !== undefined) this.knownLastCol = dims.lastCol;
      if (dims.headers !== undefined) this.knownHeaders = dims.headers;
      if (dims.grid !== undefined) {
        this.gridCache = dims.grid;
      } else if (dims.lastRow === 0 && dims.lastCol === 0) {
        this.gridCache = [];
      }
    }
  }

  /** Lazily resolve the raw Sheet handle — only needed for fallback paths. */
  private rawSheet(): GoogleAppsScript.Spreadsheet.Sheet {
    // Queued API writes must commit before any raw read/write so the raw
    // view is consistent (and issue order is preserved).
    flushPendingWrites();
    if (!this.sheet) {
      const title = this.api?.title ?? this.name ?? "";
      let found = this.api ? this.api.spreadsheet.getSheetByName(title) : null;
      if (!found && this.api) {
        // The bound Spreadsheet object can hold a stale tab list that misses
        // sheets created via the Sheets API or still-buffered SpreadsheetApp
        // inserts — flush pending writes, then reopen fresh by ID.
        flushRawWrites();
        try {
          found = SpreadsheetApp.openById(this.api.ssId).getSheetByName(title);
        } catch {
          found = null;
        }
      }
      if (!found) {
        throw new Error(`Sheet "${title}" not found for fallback operation`);
      }
      this.sheet = found;
    }
    return this.sheet;
  }

  /** rawSheet() that returns null instead of throwing — for fallback probes. */
  private resolveRawOrNull(): GoogleAppsScript.Spreadsheet.Sheet | null {
    try {
      return this.rawSheet();
    } catch {
      return null;
    }
  }

  /** Lazily resolve the numeric sheetId via the meta cache or raw sheet. */
  private rawSheetId(): number | null {
    if (this.api) return this.api.sheetId;
    const svc = sheetsService("read");
    if (svc) {
      const ssId = this.spreadsheetId();
      const title = this.name;
      if (ssId && title !== null) {
        const meta = getSheetMeta(svc, ssId);
        const m = meta?.get(title);
        if (m) {
          // Upgrade to API context so later ops skip resolution entirely
          if (!this.api && this.sheet) {
            this.api = {
              spreadsheet: this.sheet.getParent(),
              ssId,
              sheetId: m.sheetId,
              title,
            };
          }
          return m.sheetId;
        }
      }
    }
    return this.sheet ? this.sheet.getSheetId() : null;
  }

  /** Spreadsheet ID (API mode: known; raw mode: via owning spreadsheet). */
  private spreadsheetId(): string | null {
    if (this.api) return this.api.ssId;
    if (this.ssIdCache) return this.ssIdCache;
    if (!this.sheet) return null;
    this.ssIdCache = this.sheet.getParent().getId();
    return this.ssIdCache;
  }

  /** Memoised tab title — null when not yet known (never pays an RPC here). */
  private knownTitle(): string | null {
    return this.api?.title ?? this.name;
  }

  /**
   * The sheet's PHYSICAL grid row count (may exceed the last content row).
   * Structural `updateCells`/`deleteDimension` requests are bounded by it —
   * unlike Values writes or setValues, they refuse to expand the grid.
   * Peeked from the meta cache first; one meta fetch when cold.
   */
  private gridRowCount(): number | null {
    const ssId = this.api?.ssId ?? this.spreadsheetId();
    const title = this.knownTitle();
    if (!ssId || !title) return null;
    const peek = peekSheetMeta(ssId)?.get(title);
    if (peek) return peek.rowCount;
    const svc = sheetsService("read");
    return svc ? (getSheetMeta(svc, ssId)?.get(title)?.rowCount ?? null) : null;
  }

  /**
   * Grow the grid so it holds at least `minRows` rows — required before any
   * `updateCells`/`Values` range reaching that high.  No-op when the cached
   * meta already reports enough rows.  NEVER issues updateSheetProperties
   * with an unknown current size: it would SHRINK an oversized grid and
   * truncate data below `minRows`, so a cold meta is force-fetched first.
   */
  private ensureGridRows(minRows: number): boolean {
    let known = this.gridRowCount();
    if (known === null) {
      // Meta cold (or read window saturated) — one forced fetch, bypassing
      // the read window, because growing blind is unsafe.
      const ssId = this.api?.ssId ?? this.spreadsheetId();
      const title = this.knownTitle();
      const svc = sheetsServiceUnchecked();
      if (ssId && title && svc) {
        known = getSheetMeta(svc, ssId)?.get(title)?.rowCount ?? null;
      }
    }
    if (known === null || known >= minRows) return known !== null;
    const sheetId = this.api?.sheetId ?? this.rawSheetId();
    if (sheetId === null) return false;
    const ok = this.apiBatchUpdate([
      {
        updateSheetProperties: {
          properties: { sheetId, gridProperties: { rowCount: minRows } },
          fields: "gridProperties.rowCount",
        },
      },
    ]);
    if (!ok) return false;
    const ssId = this.api?.ssId ?? this.spreadsheetId();
    const title = this.knownTitle();
    if (ssId && title) {
      const peek = peekSheetMeta(ssId)?.get(title);
      if (peek) peek.rowCount = Math.max(peek.rowCount, minRows);
      else
        registerSheetMeta(ssId, {
          sheetId,
          title,
          rowCount: minRows,
          columnCount: 26,
          hidden: false,
        });
    }
    return true;
  }

  /**
   * Best-effort grid bookkeeping after writes that silently expand the grid
   * (raw setValues / Values writes): keeps the meta cache from drifting low.
   */
  private bumpMetaRows(endRow: number): void {
    const ssId = this.api?.ssId ?? this.ssIdCache;
    const title = this.knownTitle();
    if (ssId && title) bumpSheetMetaRows(ssId, title, endRow);
  }

  /** Shift the cached grid rowCount after physical row deletions. */
  private shrinkMetaRows(count: number): void {
    const ssId = this.api?.ssId ?? this.ssIdCache;
    const title = this.knownTitle();
    if (ssId && title) adjustSheetMetaRows(ssId, title, -count);
  }

  /** Return the name of the underlying sheet tab (memoised). */
  getName(): string {
    if (this.name === null) {
      this.name = this.rawSheet().getName();
    }
    return this.name;
  }

  // ─── Reads ────────────────────────────────────────────────────────────────

  /**
   * Read the whole grid as ONE RPC and refresh the grid cache.
   * API mode uses Values.get over an explicit A1-anchored range; the response
   * is trailing-trimmed so ragged rows are padded back to uniform width.
   * Fallback uses getDataRange().getValues() with anchor-aware padding.
   */
  private readGrid(): unknown[][] {
    const ssId = this.api?.ssId ?? this.spreadsheetId();
    const title = this.api?.title ?? this.name;
    // API-only adapters cannot fall back to SpreadsheetApp reliably (their
    // tab is invisible to it right after creation), so retry API reads a
    // few times before attempting the raw path.
    const apiOnly = this.sheet === null && this.api !== null;
    const attempts = apiOnly ? 4 : 1;
    for (let i = 0; i < attempts; i++) {
      // When the read-rate window is saturated, an apiOnly adapter prefers
      // the raw handle (if the sheet has propagated) over another API call.
      let svc: ReturnType<typeof sheetsServiceUnchecked> | null;
      if (apiOnly) {
        svc = readsReady()
          ? sheetsServiceUnchecked()
          : this.resolveRawOrNull()
            ? null
            : sheetsServiceUnchecked();
      } else {
        svc = sheetsService("read");
      }
      if (svc && ssId && title !== null) {
        try {
          // Bare-title range selects the whole sheet — A1-anchored, trailing
          // empty rows/columns trimmed by the API.  (Explicit bounds beyond the
          // grid, like A1:ZZ1048576 on a 26-col sheet, would throw.)
          const res = svc.Spreadsheets.Values.get(ssId, a1Quote(title), {
            valueRenderOption: "UNFORMATTED_VALUE",
            dateTimeRenderOption: "SERIAL_NUMBER",
          });
          markSheetsAvailable("read");
          const values = res.values ?? [];
          // res.range reports the actually-returned bounds (e.g. "'T'!A2:E9") —
          // when leading rows/cols are empty the anchor shifts, so parse it to
          // restore A1 anchoring.
          let rowOffset = 0;
          let colOffset = 0;
          const m = /!([A-Z]+)(\d+)/.exec(res.range ?? "");
          if (m) {
            colOffset = letterToCol(m[1]) - 1;
            rowOffset = Number(m[2]) - 1;
          }
          return this.normaliseGrid(values, rowOffset, colOffset);
        } catch (e) {
          markSheetsUnavailable(e, "read");
          if (apiOnly) {
            // The sheet may have propagated to SpreadsheetApp by now —
            // if so the raw getDataRange() path below is viable.
            if (this.resolveRawOrNull()) break;
            if (isQuotaError(e) && i + 1 < attempts) {
              sleepMs(Math.min(700 * (i + 1), 2500));
              continue;
            }
            // No viable raw sheet — surface the real API error.
            throw e;
          }
          break;
        }
      }
    }
    const range = this.rawSheet().getDataRange();
    const grid = range.getValues() as unknown[][];
    // Range.getRow()/getColumn()/getNumRows() are local getters on the range
    // handle — no extra RPC needed to learn the anchor position.
    const firstRow = range.getRow();
    const firstCol = range.getColumn();
    const numRows = range.getNumRows();
    const numCols = range.getNumColumns();

    const isEmptyGrid =
      numRows === 0 ||
      numCols === 0 ||
      (numRows === 1 && numCols === 1 && (grid[0][0] === "" || grid[0][0] === null));

    if (isEmptyGrid) {
      this.gridCache = [];
      this.knownLastRow = 0;
      this.knownLastCol = 0;
      this.knownHeaders = [];
      return this.gridCache;
    }

    this.knownLastRow = firstRow + numRows - 1;
    this.knownLastCol = firstCol + numCols - 1;

    // Pad so that grid[r][c] corresponds to sheet row (r+1), column (c+1)
    if (firstCol > 1) {
      const pad = new Array(firstCol - 1).fill("");
      for (let i = 0; i < grid.length; i++) {
        grid[i] = [...pad, ...grid[i]];
      }
    }
    if (firstRow > 1) {
      const emptyRow = new Array(this.knownLastCol).fill("");
      for (let r = firstRow - 1; r > 0; r--) {
        grid.unshift(emptyRow);
      }
    }

    this.gridCache = grid;
    this.knownHeaders = grid[0].map((v) => String(v));
    return grid;
  }

  /**
   * Normalise a value matrix from the Sheets API into the A1-anchored grid
   * shape used internally: leading rows/columns padded when the returned
   * range was trimmed, uniform column count, dimension tracking set.
   */
  private normaliseGrid(values: unknown[][], rowOffset: number, colOffset: number): unknown[][] {
    const rowCount = values.length + rowOffset;
    const maxCols = values.reduce((mx, r) => Math.max(mx, r.length), 0) + colOffset;
    if (rowCount === 0 || maxCols === 0) {
      this.gridCache = [];
      this.knownLastRow = 0;
      this.knownLastCol = 0;
      this.knownHeaders = [];
      return this.gridCache;
    }
    const grid: unknown[][] = new Array(rowCount);
    for (let i = 0; i < rowOffset; i++) {
      grid[i] = new Array(maxCols).fill("");
    }
    for (let i = 0; i < values.length; i++) {
      let row = values[i];
      if (colOffset > 0) row = [...new Array(colOffset).fill(""), ...row];
      if (row.length < maxCols) row = [...row, ...new Array(maxCols - row.length).fill("")];
      grid[i + rowOffset] = row;
    }
    this.gridCache = grid;
    this.knownLastRow = rowCount;
    this.knownLastCol = maxCols;
    this.knownHeaders = grid[0].map((v) => String(v));
    return grid;
  }

  /** Return the A1-anchored grid, from cache when available. */
  private grid(): unknown[][] {
    return this.gridCache ?? this.readGrid();
  }

  /** True when the grid cache is populated (reads are zero-RPC). */
  isGridWarm(): boolean {
    return this.gridCache !== null;
  }

  /**
   * Seed the grid cache from a Values API valueRange (batchGet prefetch).
   * The range's reported bounds restore the A1 anchor when leading
   * rows/columns were trimmed by the API.
   */
  ingestValueRange(range: string | undefined, values: unknown[][]): void {
    let rowOffset = 0;
    let colOffset = 0;
    const m = /!([A-Z]+)(\d+)/.exec(range ?? "");
    if (m) {
      colOffset = letterToCol(m[1]) - 1;
      rowOffset = Number(m[2]) - 1;
    }
    this.normaliseGrid(values ?? [], rowOffset, colOffset);
  }

  /**
   * Splice freshly-written values into the grid cache (when populated) so
   * later reads stay zero-RPC.  Pads gaps and short rows to keep the
   * A1-anchored invariant: grid[r] ↔ sheet row r+1.
   */
  private patchGrid(startSheetRow: number, rows: unknown[][]): void {
    if (this.gridCache === null) return;
    const width = Math.max(this.knownLastCol ?? 0, ...rows.map((r) => r.length));
    while (this.gridCache.length < startSheetRow - 1) {
      this.gridCache.push(new Array(width).fill(""));
    }
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      this.gridCache[startSheetRow - 1 + i] =
        row.length >= width ? [...row] : [...row, ...new Array(width - row.length).fill("")];
    }
  }

  /**
   * Read the header row (row 1) and return the column names as strings.
   * Returns an empty array if the sheet has no columns.
   */
  getHeaders(): string[] {
    if (this.knownHeaders !== null) {
      return [...this.knownHeaders];
    }
    if (this.gridCache !== null) {
      return this.gridCache.length > 0 ? this.gridCache[0].map((v) => String(v)) : [];
    }
    if (this.knownLastCol !== null) {
      if (this.knownLastCol === 0) return [];
      const row = this.rawSheet().getRange(1, 1, 1, this.knownLastCol).getValues()[0];
      this.knownHeaders = row.map((v) => String(v));
      return [...this.knownHeaders];
    }
    const grid = this.readGrid();
    return grid.length > 0 ? grid[0].map((v) => String(v)) : [];
  }

  /**
   * Overwrite the header row (row 1) with the given column names.
   * No-op if the headers array is empty.
   */
  setHeaders(headers: string[]): void {
    if (headers.length === 0) return;
    if (!this.apiWriteBatch([{ range: `A1:${colToLetter(headers.length)}1`, values: [headers] }])) {
      this.rawSheet().getRange(1, 1, 1, headers.length).setValues([headers]);
      markRawWrite();
    }
    this.knownHeaders = [...headers];
    this.knownLastCol = Math.max(this.knownLastCol ?? 0, headers.length);
    this.knownLastRow = Math.max(this.knownLastRow ?? 0, 1);
    if (this.gridCache !== null) {
      this.gridCache[0] = [...headers];
    }
  }

  /**
   * Read all data rows (row 2+) as a 2D array — single RPC.
   * Returns an empty array if the sheet has no data rows.
   */
  getAllData(): unknown[][] {
    // Fast path: known to be header-only or empty
    if (this.knownLastRow !== null && this.knownLastRow <= 1) {
      return [];
    }
    const grid = this.grid();
    const data = grid.length > 1 ? grid.slice(1) : [];
    SheetOrmLogger.log(`[Sheet:${this.getName()}] getAllData → ${data.length} rows`);
    return data;
  }

  /**
   * Return the number of data rows (excludes the header row).
   * Computed as `lastRow - 1`, where lastRow includes the header.
   */
  getRowCount(): number {
    if (this.knownLastRow === null) {
      if (sheetsService("read")) {
        // A single grid read seeds lastRow + lastCol + headers + cache.
        this.readGrid();
      } else {
        const raw = this.rawSheet();
        this.knownLastRow = raw.getLastRow();
        if (this.knownLastCol === null) {
          this.knownLastCol = raw.getLastColumn();
        }
      }
    }
    const count = Math.max(0, (this.knownLastRow ?? 1) - 1); // exclude header row
    SheetOrmLogger.log(`[Sheet:${this.getName()}] getRowCount → ${count}`);
    return count;
  }

  // ─── API write helpers ────────────────────────────────────────────────────

  /**
   * Queue value matrices for disjoint A1 ranges as DEFERRED writes — they
   * commit as one shared Values.batchUpdate on the next API call (or an
   * explicit flush), letting a whole ORM mutation collapse into a single
   * write request.  Returns true when the write was accepted (queued); false
   * when the write channel is unavailable and the caller must run the
   * SpreadsheetApp fallback immediately.
   * Ranges are column-1-anchored and expressed in absolute sheet rows.
   */
  private apiWriteBatch(data: Array<{ range: string; values: unknown[][] }>): boolean {
    const ssId = this.api?.ssId ?? this.spreadsheetId();
    const title = this.api?.title ?? this.name;
    if (!ssId || !title) return false;
    const apiOnly = this.sheet === null && this.api !== null;
    if (apiOnly) {
      // API-only adapters have no SpreadsheetApp handle — the deferred
      // fallback retries the API call (quota windows free continuously).
      if (!sheetsServiceUnchecked()) return false;
    } else if (!writesReady()) {
      return false;
    }
    // Values writes cannot address rows outside the physical grid.  Check
    // the cached meta: raw-capable adapters bail to setValues (auto-expands);
    // apiOnly adapters grow the grid via updateSheetProperties first.
    const maxEnd = Math.max(...data.map((d) => this.a1EndRow(d.range)));
    const gridRows = this.gridRowCount();
    if (gridRows !== null && maxEnd > gridRows) {
      if (!apiOnly) return false;
      this.ensureGridRows(maxEnd);
    }
    for (const d of data) {
      const range = d.range;
      const values = d.values;
      enqueueValuesWrite({
        spreadsheetId: ssId,
        range: `${a1Quote(title)}!${range}`,
        values,
        fallback: () => this.writeDeferredFallback(range, values, apiOnly),
      });
    }
    // If the queued write extends the grid, keep cached meta in step.
    if (maxEnd > 0) bumpSheetMetaRows(ssId, title, maxEnd);
    return true;
  }

  /** End row (1-based) of an `A{r}:{COL}{r2}` range string. */
  private a1EndRow(range: string): number {
    const m = /:([A-Z]+)(\d+)$/.exec(range);
    return m ? Number(m[2]) : 0;
  }

  /**
   * Fallback for a deferred write whose batchUpdate failed: API-only sheets
   * first try the raw handle (the sheet may have propagated to SpreadsheetApp
   * — a raw write consumes no API quota), then retry the API with sleeps;
   * raw-capable adapters write via setValues directly.
   */
  private writeDeferredFallback(range: string, values: unknown[][], apiOnly: boolean): void {
    if (apiOnly && this.sheet === null) {
      if (this.resolveRawOrNull()) {
        this.rawWriteA1(range, values);
        return;
      }
      // The write may target rows beyond the grid (meta can be cold here) —
      // grow first so the API retry isn't doomed by grid limits.
      const endRow = this.a1EndRow(range);
      if (endRow > 0) this.ensureGridRows(endRow);
      if (this.apiWriteImmediate([{ range, values }])) return;
      // Sheet may have propagated to SpreadsheetApp meanwhile.
      this.rawWriteA1(range, values);
      return;
    }
    this.rawWriteA1(range, values);
  }

  /** SpreadsheetApp write for an `A{r}:{COL}{r2}` range + values matrix. */
  private rawWriteA1(range: string, values: unknown[][]): void {
    const m = /^A(\d+):[A-Z]+\d+$/.exec(range);
    if (!m) throw new Error(`Cannot map range "${range}" to raw coordinates`);
    this.rawSheet()
      .getRange(Number(m[1]), 1, values.length, Math.max(1, values[0]?.length ?? 1))
      .setValues(values);
    markRawWrite();
    // setValues auto-expands the grid — keep the cached meta in step.
    this.bumpMetaRows(Number(m[1]) + values.length - 1);
  }

  /**
   * Immediate Values.batchUpdate — used when the deferred path is bypassed
   * (API-only fallback retries).  Returns true on success.
   */
  private apiWriteImmediate(data: Array<{ range: string; values: unknown[][] }>): boolean {
    const ssId = this.api?.ssId ?? this.spreadsheetId();
    const title = this.api?.title ?? this.name;
    if (!ssId || !title) return false;
    // API-only adapters have no SpreadsheetApp handle — Sheets API writes to
    // their sheet are invisible to SpreadsheetApp for a while (replication
    // lag), so retrying the API call is the ONLY viable write path.  Quota
    // windows free continuously, so short sleeps reliably succeed.
    const apiOnly = this.sheet === null && this.api !== null;
    const attempts = apiOnly ? 6 : 1;
    let lastErr: unknown = null;
    for (let i = 0; i < attempts; i++) {
      const svc = apiOnly ? sheetsServiceUnchecked() : sheetsService("write");
      if (!svc) return false;
      try {
        svc.Spreadsheets.Values.batchUpdate(
          {
            valueInputOption: "RAW",
            data: data.map((d) => ({ range: `${a1Quote(title)}!${d.range}`, values: d.values })),
          },
          ssId,
        );
        markSheetsAvailable("write");
        return true;
      } catch (e) {
        lastErr = e;
        markSheetsUnavailable(e, "write");
        // apiOnly adapters can ONLY talk to the API — unless the sheet has
        // propagated to SpreadsheetApp by now (then the raw path is viable
        // and free of API quota).  Retry transient quota errors; give up
        // immediately on hard errors.
        if (apiOnly && isGridLimitError(e) && i + 1 < attempts) {
          // Cached grid size was stale (just invalidated) — fetch the real
          // size, grow if needed, then retry the value write.
          if (this.resolveRawOrNull()) break; // → caller uses SPA fallback
          const maxEnd = Math.max(...data.map((d) => this.a1EndRow(d.range)));
          if (maxEnd > 0) this.ensureGridRows(maxEnd);
          continue;
        }
        if (apiOnly && isQuotaError(e) && i + 1 < attempts) {
          if (this.resolveRawOrNull()) break; // → caller uses SPA fallback
          sleepMs(Math.min(900 * (i + 1), 3000));
          continue;
        }
        break;
      }
    }
    if (apiOnly && this.sheet === null) throw lastErr; // Surface the real error
    return false;
  }

  /** Run a structural batchUpdate (updateCells/deleteDimension/...). */
  private apiBatchUpdate(requests: Array<Record<string, unknown>>): boolean {
    const apiOnly = this.sheet === null && this.api !== null;
    const attempts = apiOnly ? 6 : 1;
    let lastErr: unknown = null;
    const ssId = this.api?.ssId ?? this.spreadsheetId();
    if (!ssId) return false;
    // Fold queued value writes into THIS request — updateCells entries ride
    // inside the same batchUpdate, saving a separate Values.batchUpdate call
    // (write volume is capped at ~55/min, so every saved call matters).
    // Skipped when the write channel is unavailable — the queue then keeps
    // its writes for the normal Values/fallback path.
    const channelReady = apiOnly ? sheetsServiceUnchecked() !== null : writesReady();
    const drained = channelReady ? drainPendingWrites(ssId) : [];
    const merged = drained.length > 0 ? this.foldPendingInto(ssId, drained, requests) : requests;
    const restoreQueue = () => {
      if (drained.length > 0) requeuePendingWrites(drained);
    };
    for (let i = 0; i < attempts; i++) {
      const svc = apiOnly ? sheetsServiceUnchecked() : sheetsService("write");
      if (!svc) {
        restoreQueue();
        return false;
      }
      try {
        svc.Spreadsheets.batchUpdate({ requests: merged }, ssId);
        markSheetsAvailable("write");
        return true;
      } catch (e) {
        lastErr = e;
        markSheetsUnavailable(e, "write");
        // Grid-boundary failure → cached meta rowCount is stale; refetch.
        if (isGridLimitError(e)) {
          invalidateSheetMeta(ssId);
          // apiOnly: regrow the grid to fit the payload and retry — the
          // structural request can then succeed on the next attempt.
          if (i + 1 < attempts && !this.resolveRawOrNull()) {
            const maxEnd = Math.max(
              ...merged.map((r) => {
                const uc = (r as { updateCells?: { range?: { endRowIndex?: number } } }).updateCells;
                return uc?.range?.endRowIndex ?? 0;
              }),
            );
            if (maxEnd > 0) this.ensureGridRows(maxEnd);
          }
          continue;
        }
        if (apiOnly && isQuotaError(e) && i + 1 < attempts) {
          if (this.resolveRawOrNull()) break;
          sleepMs(Math.min(900 * (i + 1), 3000));
          continue;
        }
        break;
      }
    }
    if (drained.length > 0) {
      if (apiOnly && this.sheet === null) {
        // The drained writes are re-queued so a later op can retry them
        // before the hard failure propagates.
        requeuePendingWrites(drained);
        throw lastErr;
      }
      requeuePendingWrites(drained); // idempotent re-application is safe
    } else if (apiOnly && this.sheet === null) {
      throw lastErr;
    }
    return false;
  }

  /**
   * Convert each drained value write into `updateCells` requests prepended
   * to `requests` (they were issued first, so they must apply first).
   * Writes whose target sheet is not in the shared metadata map — or whose
   * range doesn't match the canonical `'Title'!A{r}:{COL}{r2}` shape — are
   * re-queued immediately so the Values.batchUpdate flush inside
   * `sheetsService("write")` still applies them, ordered before this batch.
   * (On failure the caller re-queues the whole drained set — writes are
   * idempotent, so a possible re-application of a leftover is harmless.)
   */
  private foldPendingInto(
    ssId: string,
    drained: PendingValuesWrite[],
    requests: Array<Record<string, unknown>>,
  ): Array<Record<string, unknown>> {
    const meta = peekSheetMeta(ssId);
    const converts: Array<Record<string, unknown>> = [];
    const leftovers: PendingValuesWrite[] = [];
    for (const w of drained) {
      const reqs = meta ? this.pendingAsUpdateCells(w, meta) : null;
      if (reqs) converts.push(...reqs);
      else leftovers.push(w);
    }
    if (leftovers.length > 0) requeuePendingWrites(leftovers);
    return [...converts, ...requests];
  }

  /**
   * Convert one queued `'Title'!A{r}:{COL}{r2}` write into updateCells
   * requests, fusing an updateSheetProperties grow when the target grid is
   * too small.  Returns null when the sheet isn't in `meta` (caller keeps
   * the write on the Values path).
   */
  private pendingAsUpdateCells(
    w: PendingValuesWrite,
    meta: Map<string, SheetMeta>,
  ): Array<Record<string, unknown>> | null {
    const m = /^(?:'((?:[^']|'')+)'|([^'!]+))!A(\d+):([A-Z]+)(\d+)$/.exec(w.range);
    if (!m) return null;
    const title = (m[1] ?? m[2]).replace(/''/g, "'");
    const sm = meta.get(title);
    if (!sm) return null;
    const startRow = Number(m[3]);
    const endRow = Number(m[5]);
    const endCol = letterToCol(m[4]);
    const out: Array<Record<string, unknown>> = [];
    if (sm.rowCount < endRow) {
      // updateCells cannot write outside the grid — fuse the grow.
      out.push({
        updateSheetProperties: {
          properties: { sheetId: sm.sheetId, gridProperties: { rowCount: endRow } },
          fields: "gridProperties.rowCount",
        },
      });
      sm.rowCount = endRow; // meta is the shared cached map — keep it honest
    }
    out.push({
      updateCells: {
        range: {
          sheetId: sm.sheetId,
          startRowIndex: startRow - 1,
          endRowIndex: endRow,
          startColumnIndex: 0,
          endColumnIndex: endCol,
        },
        rows: w.values.map(toRowData),
        fields: "userEnteredValue",
      },
    });
    return out;
  }

  // ─── Writes ───────────────────────────────────────────────────────────────

  /**
   * Append a single row after the last occupied row.
   * Uses the tracked row count to write via setValues() — same cost as
   * appendRow() but keeps dimension tracking accurate.
   */
  appendRow(values: unknown[]): void {
    this.appendRows([values]);
  }

  /**
   * Append multiple rows in a single API call.
   * When the last row is already known, skips the dimension-discovery RPC.
   */
  appendRows(rows: unknown[][]): void {
    if (rows.length === 0) return;
    const numCols = rows[0].length;
    let startRow: number;
    if (this.knownLastRow !== null) {
      startRow = Math.max(this.knownLastRow, 1) + 1;
    } else if (sheetsService("read")) {
      this.readGrid();
      startRow = Math.max(this.knownLastRow ?? 0, 1) + 1;
    } else {
      const raw = this.rawSheet();
      startRow = raw.getLastRow() + 1;
      this.knownLastRow = startRow - 1;
    }
    SheetOrmLogger.log(
      `[Sheet:${this.getName()}] appendRows ${rows.length} rows × ${numCols} cols at sheetRow=${startRow}`,
    );
    const endRow = startRow + rows.length - 1;
    const range = `A${startRow}:${colToLetter(numCols)}${endRow}`;
    if (!this.apiWriteBatch([{ range, values: rows }])) {
      this.rawSheet().getRange(startRow, 1, rows.length, numCols).setValues(rows);
      markRawWrite();
      this.bumpMetaRows(endRow);
    }
    this.knownLastRow = endRow;
    this.knownLastCol = Math.max(this.knownLastCol ?? 0, numCols);
    this.patchGrid(startRow, rows);
  }

  /**
   * Write rows starting at the given 0-based data index.
   * Overwrites existing cells — used for batch-update operations.
   *
   * @param startRowIndex - 0-based data index (sheet row = index + 2).
   * @param rows          - 2D array of values.
   */
  writeRowsAt(startRowIndex: number, rows: unknown[][]): void {
    if (rows.length === 0) return;
    // Convert 0-based data index to 1-based sheet row (header is row 1)
    const sheetRow = startRowIndex + 2;
    const numCols = rows[0].length;
    SheetOrmLogger.log(
      `[Sheet:${this.getName()}] writeRowsAt dataIdx=${startRowIndex} sheetRow=${sheetRow} rows=${rows.length} cols=${numCols}`,
    );
    const range = `A${sheetRow}:${colToLetter(numCols)}${sheetRow + rows.length - 1}`;
    if (!this.apiWriteBatch([{ range, values: rows }])) {
      this.rawSheet().getRange(sheetRow, 1, rows.length, numCols).setValues(rows);
      markRawWrite();
      this.bumpMetaRows(sheetRow + rows.length - 1);
    }
    this.knownLastRow = Math.max(this.knownLastRow ?? 0, sheetRow + rows.length - 1);
    this.knownLastCol = Math.max(this.knownLastCol ?? 0, numCols);
    this.patchGrid(sheetRow, rows);
  }

  /**
   * Overwrite a single data row.
   *
   * @param rowIndex - 0-based data index (sheet row = index + 2).
   * @param values   - Column values.
   */
  updateRow(rowIndex: number, values: unknown[]): void {
    const sheetRow = rowIndex + 2; // +2: 1-based + header offset
    SheetOrmLogger.log(`[Sheet:${this.getName()}] updateRow dataIdx=${rowIndex} sheetRow=${sheetRow}`);
    const range = `A${sheetRow}:${colToLetter(values.length)}${sheetRow}`;
    if (!this.apiWriteBatch([{ range, values: [values] }])) {
      this.rawSheet().getRange(sheetRow, 1, 1, values.length).setValues([values]);
      markRawWrite();
      this.bumpMetaRows(sheetRow);
    }
    this.knownLastRow = Math.max(this.knownLastRow ?? 0, sheetRow);
    this.knownLastCol = Math.max(this.knownLastCol ?? 0, values.length);
    this.patchGrid(sheetRow, [values]);
  }

  /**
   * Overwrite multiple rows in ONE API call regardless of contiguity:
   * contiguous groups collapse into a single range entry, and every span is
   * shipped in the same Values.batchUpdate request — so scattered updates
   * cost the same as one contiguous write.
   */
  updateRows(updates: Array<{ rowIndex: number; values: unknown[] }>): void {
    if (updates.length === 0) return;

    // Sort by rowIndex so we can detect contiguous sequences
    const sorted = [...updates].sort((a, b) => a.rowIndex - b.rowIndex);

    // Build A1 range entries per contiguous group
    const ranges: Array<{ range: string; values: unknown[][] }> = [];
    let groupStart = sorted[0].rowIndex;
    let groupRows: unknown[][] = [sorted[0].values];
    let maxRow = sorted[0].rowIndex;
    let numCols = sorted[0].values.length;

    for (let i = 1; i < sorted.length; i++) {
      if (sorted[i].rowIndex === sorted[i - 1].rowIndex + 1) {
        groupRows.push(sorted[i].values);
      } else {
        const endRow = groupStart + 2 + groupRows.length - 1;
        ranges.push({
          range: `A${groupStart + 2}:${colToLetter(groupRows[0].length)}${endRow}`,
          values: groupRows,
        });
        groupStart = sorted[i].rowIndex;
        groupRows = [sorted[i].values];
      }
      maxRow = Math.max(maxRow, sorted[i].rowIndex);
      numCols = Math.max(numCols, sorted[i].values.length);
    }
    const endRow = groupStart + 2 + groupRows.length - 1;
    ranges.push({
      range: `A${groupStart + 2}:${colToLetter(groupRows[0].length)}${endRow}`,
      values: groupRows,
    });

    SheetOrmLogger.log(
      `[Sheet:${this.getName()}] updateRows ${updates.length} rows in ${ranges.length} range(s)`,
    );
    if (!this.apiWriteBatch(ranges)) {
      // Fallback: one setValues call per contiguous group
      for (const r of ranges) {
        const m = /^A(\d+):[A-Z]+(\d+)$/.exec(r.range)!;
        const rowStart = Number(m[1]);
        const rowEnd = Number(m[2]);
        this.rawSheet()
          .getRange(rowStart, 1, rowEnd - rowStart + 1, r.values[0].length)
          .setValues(r.values);
        this.bumpMetaRows(rowEnd);
      }
      markRawWrite();
    }
    this.knownLastRow = Math.max(this.knownLastRow ?? 0, maxRow + 2);
    this.knownLastCol = Math.max(this.knownLastCol ?? 0, numCols);
    // Write-through each updated row into the grid cache (0 RPCs).
    for (const u of updates) this.patchGrid(u.rowIndex + 2, [u.values]);
  }

  /**
   * Delete a single data row. GAS deleteRow() shifts all rows below up by one.
   * @param rowIndex - 0-based data index.
   */
  deleteRow(rowIndex: number): void {
    this.deleteRows([rowIndex]);
  }

  /**
   * Delete multiple data rows by index.
   * Rows are deleted from bottom to top so that earlier indexes remain valid.
   * In API mode all deleteDimension requests ship in a single batchUpdate.
   */
  deleteRows(rowIndexes: number[]): void {
    if (rowIndexes.length === 0) return;
    // Sort descending to avoid index shift issues
    const sorted = [...rowIndexes].sort((a, b) => b - a);
    const sheetId = this.rawSheetId();
    if (sheetId !== null) {
      const requests = sorted.map((idx) => ({
        deleteDimension: {
          range: {
            sheetId,
            dimension: "ROWS",
            startIndex: idx + 1, // 0-based row index incl. header
            endIndex: idx + 2,
          },
        },
      }));
      if (this.apiBatchUpdate(requests)) {
        // deleteDimension physically removes grid rows — keep meta in step.
        this.shrinkMetaRows(sorted.length);
        if (this.knownLastRow !== null) {
          // Only deletions at-or-below the last content row shrink it
          const within = sorted.filter((idx) => idx + 2 <= this.knownLastRow!).length;
          this.knownLastRow -= within;
        }
        if (this.gridCache !== null) {
          for (const idx of sorted) {
            if (idx + 1 < this.gridCache.length) this.gridCache.splice(idx + 1, 1);
          }
        }
        return;
      }
    }
    for (const idx of sorted) {
      const sheetRow = idx + 2;
      this.rawSheet().deleteRow(sheetRow);
      if (this.knownLastRow !== null && sheetRow <= this.knownLastRow) {
        this.knownLastRow--;
      }
      if (this.gridCache !== null && idx + 1 < this.gridCache.length) {
        this.gridCache.splice(idx + 1, 1);
      }
    }
    this.shrinkMetaRows(sorted.length);
    markRawWrite();
  }

  /**
   * Read a single data row.
   * @param rowIndex - 0-based data index.
   * @returns Array of cell values.
   */
  getRow(rowIndex: number): unknown[] {
    // One grid read (cached) costs a single RPC and additionally seeds
    // dimensions + headers — strictly better than a targeted getRange().
    const grid = this.grid();
    const row = grid[rowIndex + 1];
    return row ? [...row] : [];
  }

  /**
   * Read several scattered data rows in ONE `Values.batchGet` call.
   * Used by index-narrowed lookups: instead of a full-grid read, only the
   * candidate rows cross the wire (payload ∝ hits, not table size).
   * Read rows are spliced into the grid cache so follow-up full reads stay
   * coherent.
   */
  readRowsAt(rowIndexes: number[]): Array<unknown[] | null> {
    if (rowIndexes.length === 0) return [];
    const svc = sheetsService("read");
    const ssId = this.spreadsheetId();
    const title = this.knownTitle();
    if (svc && ssId && title) {
      try {
        // Full-row ranges ('Title'!{row}:{row}) — the API returns only the
        // non-empty cell prefix per row, so payloads stay tight.
        const res = svc.Spreadsheets.Values.batchGet(ssId, {
          ranges: rowIndexes.map((i) => `${a1Quote(title)}!${i + 2}:${i + 2}`),
          valueRenderOption: "UNFORMATTED_VALUE",
        });
        markSheetsAvailable("read");
        const ranges = (res as { valueRanges?: Array<{ values?: unknown[][] }> }).valueRanges ?? [];
        const out: Array<unknown[] | null> = rowIndexes.map((_, i) => ranges[i]?.values?.[0] ?? null);
        // Write-through into the grid cache (when warm) — keeps subsequent
        // full-grid reads coherent without re-fetching.
        for (let i = 0; i < rowIndexes.length; i++) {
          const row = out[i];
          if (row) this.patchGrid(rowIndexes[i] + 2, [row]);
        }
        return out;
      } catch (e) {
        markSheetsUnavailable(e, "read");
      }
    }
    // Fallback: full cached-grid read (single RPC, seeds everything).
    const grid = this.grid();
    return rowIndexes.map((i) => {
      const row = grid[i + 1];
      return row ? [...row] : null;
    });
  }

  /**
   * Read one column across all data rows via a single `Values.get` on
   * `{L}2:{L}` — the narrow-payload way to rebuild id→rowIndex maps without
   * pulling the whole grid.
   */
  readIdsColumn(colIndex: number): unknown[] | null {
    const svc = sheetsService("read");
    const ssId = this.spreadsheetId();
    const title = this.knownTitle();
    const letter = colToLetter(colIndex + 1);
    if (svc && ssId && title) {
      try {
        const res = svc.Spreadsheets.Values.get(ssId, `${a1Quote(title)}!${letter}2:${letter}`, {
          valueRenderOption: "UNFORMATTED_VALUE",
        });
        markSheetsAvailable("read");
        const vals = (res as { values?: unknown[][] }).values ?? [];
        return vals.map((r) => r[0]);
      } catch (e) {
        markSheetsUnavailable(e, "read");
      }
    }
    // Fallback: derive from the (cached) full grid.
    const grid = this.grid();
    const out: unknown[] = [];
    for (let i = 1; i < grid.length; i++) out.push(grid[i][colIndex]);
    return out;
  }

  /**
   * Sparse update — writes only the given cells of a data row.
   * Adjacent columns are merged into span ranges, so the whole diff lands
   * inside ONE deferred Values.batchUpdate (or folds into the next
   * structural batchUpdate as updateCells).
   */
  updateRowSparse(rowIndex: number, cells: Array<readonly [number, unknown]>): void {
    if (cells.length === 0) return;
    const sheetRow = rowIndex + 2;
    const writes: Array<{ range: string; values: unknown[][] }> = [];
    let spanStart = cells[0][0];
    let span: unknown[] = [cells[0][1]];
    for (let i = 1; i < cells.length; i++) {
      const [col, val] = cells[i];
      if (col === spanStart + span.length) {
        span.push(val);
        continue;
      }
      writes.push({
        range: `${colToLetter(spanStart + 1)}${sheetRow}:${colToLetter(spanStart + span.length)}${sheetRow}`,
        values: [span],
      });
      spanStart = col;
      span = [val];
    }
    writes.push({
      range: `${colToLetter(spanStart + 1)}${sheetRow}:${colToLetter(spanStart + span.length)}${sheetRow}`,
      values: [span],
    });
    SheetOrmLogger.log(
      `[Sheet:${this.getName()}] updateRowSparse dataIdx=${rowIndex} cells=${cells.length} spans=${writes.length}`,
    );
    if (!this.apiWriteBatch(writes)) {
      // Raw fallback — one setValue per cell (sparse counts are small).
      const raw = this.rawSheet();
      for (const [col, val] of cells) raw.getRange(sheetRow, col + 1).setValue(val);
      markRawWrite();
    }
    this.knownLastRow = Math.max(this.knownLastRow ?? 0, sheetRow);
    // Write-through into the grid cache (cell granularity).
    if (this.gridCache !== null) {
      const gi = sheetRow - 1;
      const width = Math.max(this.knownLastCol ?? 0, cells[cells.length - 1][0] + 1);
      while (this.gridCache.length <= gi) this.gridCache.push(new Array(width).fill(""));
      const row = this.gridCache[gi] as unknown[];
      for (const [col, val] of cells) row[col] = val;
    }
  }

  /** Public numeric sheetId accessor (schema-fingerprint identity checks). */
  getSheetId(): number | null {
    return this.rawSheetId();
  }

  /**
   * Replace all data rows (row 2+) with the provided 2D array.
   *
   * If the new data has fewer rows than the old data, surplus old rows
   * are cleared. If the new data is narrower (fewer columns), surplus
   * columns in the written region are also cleared.
   *
   * API mode ships write + clear requests in a single batchUpdate.
   */
  replaceAllData(rows: unknown[][]): void {
    // Resolve current dimensions without extra RPCs when tracked
    let lastRow = this.knownLastRow;
    let lastCol = this.knownLastCol;
    if (lastRow === null || lastCol === null) {
      this.readGrid();
      lastRow = this.knownLastRow!;
      lastCol = this.knownLastCol!;
    }
    const oldDataRows = Math.max(0, lastRow - 1);
    const newCols = rows.length > 0 ? rows[0].length : 0;
    const clearCols = Math.max(lastCol, newCols);

    const sheetId = this.rawSheetId();
    const requests: Array<Record<string, unknown>> = [];
    if (sheetId !== null) {
      // updateCells is bounded by the physical grid — if the payload or the
      // clear region reaches past it, grow the grid first (same batch, so
      // this costs no extra request).  NEVER grow when the current size is
      // unknown: updateSheetProperties would SHRINK a larger grid and
      // truncate data — a cold meta is resolved by ensureGridRows instead.
      const needRows = 1 + Math.max(rows.length, oldDataRows);
      const gridRows = this.gridRowCount();
      if (gridRows !== null && needRows > gridRows) {
        requests.push({
          updateSheetProperties: {
            properties: {
              sheetId,
              gridProperties: { rowCount: needRows },
            },
            fields: "gridProperties.rowCount",
          },
        });
      } else if (gridRows === null) {
        this.ensureGridRows(needRows);
      }
      if (rows.length > 0) {
        requests.push({
          updateCells: {
            range: {
              sheetId,
              startRowIndex: 1,
              endRowIndex: 1 + rows.length,
              startColumnIndex: 0,
              endColumnIndex: newCols,
            },
            rows: rows.map(toRowData),
            fields: "userEnteredValue",
          },
        });
      }
      // Clear surplus columns in written rows if new data is narrower
      if (rows.length > 0 && newCols < lastCol) {
        requests.push({
          updateCells: {
            range: {
              sheetId,
              startRowIndex: 1,
              endRowIndex: 1 + rows.length,
              startColumnIndex: newCols,
              endColumnIndex: lastCol,
            },
            fields: "userEnteredValue",
          },
        });
      }
      // Clear surplus old rows below the new data
      if (oldDataRows > rows.length && clearCols > 0) {
        requests.push({
          updateCells: {
            range: {
              sheetId,
              startRowIndex: rows.length + 1,
              endRowIndex: 1 + oldDataRows,
              startColumnIndex: 0,
              endColumnIndex: clearCols,
            },
            fields: "userEnteredValue",
          },
        });
      }
      if (this.apiBatchUpdate(requests)) {
        this.bumpMetaRows(1 + Math.max(rows.length, oldDataRows));
        this.finishReplace(rows, clearCols);
        return;
      }
    }

    // ── SpreadsheetApp fallback (3 separate calls) ──
    const sheet = this.rawSheet();
    if (rows.length > 0) {
      sheet.getRange(2, 1, rows.length, newCols).setValues(rows);
      this.bumpMetaRows(rows.length + 1);
    }
    if (rows.length > 0 && newCols < lastCol) {
      sheet.getRange(2, newCols + 1, rows.length, lastCol - newCols).clearContent();
    }
    if (oldDataRows > rows.length && clearCols > 0) {
      sheet.getRange(rows.length + 2, 1, oldDataRows - rows.length, clearCols).clearContent();
    }
    markRawWrite();
    this.finishReplace(rows, clearCols);
  }

  /** Shared bookkeeping after a successful replaceAllData write. */
  private finishReplace(rows: unknown[][], clearCols: number): void {
    this.knownLastRow = Math.max(1, rows.length + 1);
    this.knownLastCol = clearCols;
    // Keep grid cache coherent: header row + new rows (cleared tail omitted)
    if (this.gridCache !== null) {
      const header = this.gridCache[0] ?? (this.knownHeaders !== null ? [...this.knownHeaders] : []);
      this.gridCache = [header, ...rows];
    }
  }

  /** Clear the entire sheet (headers + data). */
  clear(): void {
    const sheetId = this.rawSheetId();
    let done = false;
    if (sheetId !== null) {
      done = this.apiBatchUpdate([
        {
          updateCells: {
            range: { sheetId },
            fields: "userEnteredValue",
          },
        },
      ]);
    }
    if (!done) {
      this.rawSheet().clear();
      markRawWrite();
    }
    this.knownLastRow = 0;
    this.knownLastCol = 0;
    this.knownHeaders = null;
    this.gridCache = [];
  }

  /** Force-flush pending changes to the spreadsheet. */
  flush(): void {
    flushPendingWrites();
    SpreadsheetApp.flush();
  }

  /**
   * Write the header row and all data rows in a single API call.
   * Row 1 = headers, row 2+ = data.
   *
   * Used for newly-created sheets to avoid a separate setHeaders() round-trip.
   */
  writeAllRowsWithHeaders(headers: string[], rows: unknown[][]): void {
    const numCols = headers.length;
    // Combine headers and data into one contiguous 2D array
    const allRows: unknown[][] = [headers, ...rows];
    SheetOrmLogger.log(
      `[Sheet:${this.getName()}] writeAllRowsWithHeaders ${rows.length} data rows + header (${allRows.length} total)`,
    );
    const range = `A1:${colToLetter(numCols)}${allRows.length}`;
    if (!this.apiWriteBatch([{ range, values: allRows }])) {
      this.rawSheet().getRange(1, 1, allRows.length, numCols).setValues(allRows);
      markRawWrite();
    }
    this.knownHeaders = [...headers];
    this.knownLastRow = allRows.length;
    this.knownLastCol = numCols;
    this.gridCache = [headers, ...rows];
  }
}
