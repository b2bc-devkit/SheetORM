/**
 * Thin wrapper around the Google Sheets API advanced service (`Sheets`).
 *
 * The advanced service collapses what would be N SpreadsheetApp RPC
 * round-trips (~300-1400 ms each) into single batched HTTP calls:
 * `Sheets.Spreadsheets.batchUpdate` accepts an arbitrary list of
 * addSheet/deleteSheet/updateSheetProperties/updateCells/deleteDimension
 * requests executed as one call.
 *
 * The service is only bound when `enabledAdvancedServices` declares it in
 * appsscript.json AND the backing GCP project has the Sheets API enabled.
 * Every caller must check {@link sheetsService()} for null and fall back to
 * SpreadsheetApp calls.
 *
 * @module SheetsRpc
 */

/** Minimal structural type for the Sheets advanced service surface we use. */
interface SheetProperties {
  sheetId?: number;
  title?: string;
  hidden?: boolean;
  gridProperties?: { rowCount?: number; columnCount?: number };
}

interface SheetsSpreadsheetMeta {
  sheets?: Array<{ properties?: SheetProperties }>;
}

interface BatchUpdateReply {
  addSheet?: { properties?: SheetProperties };
}

export interface SheetsService {
  Spreadsheets: {
    get(spreadsheetId: string, optionalArgs?: Record<string, unknown>): SheetsSpreadsheetMeta;
    batchUpdate(
      resource: { requests: Array<Record<string, unknown>> },
      spreadsheetId: string,
    ): { replies?: BatchUpdateReply[] };
    Values: {
      get(
        spreadsheetId: string,
        range: string,
        optionalArgs?: Record<string, unknown>,
      ): { values?: unknown[][]; range?: string };
      /**
       * Multi-range read — ONE RPC returns a `valueRanges` array in the same
       * order as the `ranges` request arg (each entry reports its true
       * A1-bounded `range` plus `values`).
       */
      batchGet(
        spreadsheetId: string,
        optionalArgs?: Record<string, unknown>,
      ): { valueRanges?: Array<{ range?: string; values?: unknown[][] }> };
      batchUpdate(
        resource: {
          valueInputOption?: string;
          data: Array<{ range: string; values: unknown[][] }>;
        },
        spreadsheetId: string,
      ): unknown;
    };
  };
}

/** Kind of Sheets API traffic — read and write quotas are independent. */
export type SheetsTraffic = "read" | "write";

let readUnavailableUntil = 0;
let writeUnavailableUntil = 0;
let readFailStreak = 0;
let writeFailStreak = 0;

/**
 * Consistency flag: SpreadsheetApp buffers writes in a per-execution cache
 * and commits them lazily.  The Sheets API (a separate service) cannot see
 * uncommitted SpreadsheetApp mutations — reading via Values.get right after
 * a setValues() fallback returns stale/empty data.  Any raw SpreadsheetApp
 * mutation must call {@link markRawWrite}; the next API call then issues a
 * SpreadsheetApp.flush() first so the API sees a consistent view.
 */
let rawWritePending = false;

/** Mark that SpreadsheetApp was used for a mutation (needs pre-API flush). */
export function markRawWrite(): void {
  // Queued API writes were issued before this raw mutation — committing them
  // first preserves issue order for same-range collisions.
  flushPendingWrites();
  rawWritePending = true;
}

/** Flush pending SpreadsheetApp writes so the Sheets API sees them. */
export function flushRawWrites(): void {
  if (!rawWritePending) return;
  try {
    SpreadsheetApp.flush();
    rawWritePending = false;
  } catch {
    // Flush itself failed — keep the flag set so the next API call retries.
  }
}

/**
 * Return the Sheets advanced service object, or null when it is not bound
 * in this GAS project or is in a failure cooldown for the given traffic
 * kind.  Read and write cooldowns are tracked separately — the Sheets API
 * meters "Read requests" and "Write requests" per-minute independently, so
 * a blown write quota must not disable fast API reads.
 */
export function sheetsService(kind: SheetsTraffic = "read"): SheetsService | null {
  if (Date.now() < (kind === "write" ? writeUnavailableUntil : readUnavailableUntil)) return null;
  const svc = (globalThis as Record<string, unknown>)["Sheets"] as SheetsService | undefined;
  if (svc && svc.Spreadsheets && typeof svc.Spreadsheets.batchUpdate === "function") {
    // When a rate window is saturated, report "unavailable" so the caller
    // falls back to SpreadsheetApp instead of tripping the quota.
    if (kind === "write" ? writeWindowFull() : readWindowFull()) return null;
    // Mixed-mode consistency: commit queued value writes, then any raw
    // SpreadsheetApp mutation, before the Sheets API is used again so the
    // API observes a consistent view.
    flushPendingWrites();
    flushRawWrites();
    if (kind === "write") recordWriteCall();
    else recordReadCall();
    return svc;
  }
  // Global not bound at all — unlikely to appear mid-execution; probe again
  // after a while anyway so a late-binding service still gets used.
  readUnavailableUntil = Date.now() + 60_000;
  writeUnavailableUntil = readUnavailableUntil;
  return null;
}

/**
 * Deterministic client-side request error (range outside grid limits,
 * invalid request shape, bad arguments).  Unlike quota/5xx failures these
 * never heal by waiting — penalising the channel only slows the suite.
 */
function isClientRequestError(error: unknown): boolean {
  const msg = String((error as { message?: unknown })?.message ?? error ?? "");
  return /grid limits|beyond the last|Attempting to write|Invalid data|Invalid requests|INVALID_ARGUMENT|Max rows|not within/i.test(
    msg,
  );
}

/**
 * True when the error reports a write/read outside the sheet's physical
 * grid — the cached SheetMeta rowCount is then stale (auto-shrink or
 * expansion happened elsewhere), so callers invalidate it.
 */
export function isGridLimitError(error: unknown): boolean {
  const msg = String((error as { message?: unknown })?.message ?? error ?? "");
  return /grid limits|beyond the last|Attempting to write|Max rows/i.test(msg);
}

/**
 * Called when an API request throws.  The error message decides which
 * traffic bucket is penalised: "Write requests" quota errors only cool
 * writes, "Read requests" only reads; generic quota/5xx errors cool the
 * kind that failed.  Cooldowns back off exponentially per bucket.
 * Deterministic client errors are logged but apply NO cooldown — the
 * service is healthy, the request itself was invalid.
 */
export function markSheetsUnavailable(error: unknown, kind: SheetsTraffic = "write"): void {
  const msg = String((error as { message?: unknown })?.message ?? error ?? "");
  if (isClientRequestError(error)) {
    if (typeof Logger !== "undefined") {
      Logger.log(`[SheetORM] Sheets API request error (${kind}, no cooldown): ${msg.slice(0, 300)}`);
    }
    return;
  }
  const isReadQuota = /read requests/i.test(msg);
  const isWriteQuota = /write requests/i.test(msg);
  const quotaish =
    isReadQuota ||
    isWriteQuota ||
    /quota|rate.?limit|RESOURCE_EXHAUSTED|429|502|503|500|internal error|UNAVAILABLE|deadline/i.test(msg);
  const base = quotaish ? 15_000 : 2_000;

  const buckets: SheetsTraffic[] =
    isReadQuota && !isWriteQuota ? ["read"] : isWriteQuota && !isReadQuota ? ["write"] : [kind];
  for (const b of buckets) {
    if (b === "read") {
      readFailStreak++;
      readUnavailableUntil = Date.now() + Math.min(base * (1 << Math.min(readFailStreak - 1, 3)), 60_000);
    } else {
      writeFailStreak++;
      writeUnavailableUntil = Date.now() + Math.min(base * (1 << Math.min(writeFailStreak - 1, 3)), 60_000);
    }
  }
  // Logged unconditionally — diagnosing transient API failures in GAS is
  // otherwise impossible; one line per failure is negligible output.
  if (typeof Logger !== "undefined") {
    const cool = Math.max(readUnavailableUntil - Date.now(), writeUnavailableUntil - Date.now());
    Logger.log(
      `[SheetORM] Sheets API failure (${buckets.join("+")}, cooldown~${cool}ms): ${msg.slice(0, 300)}`,
    );
  }
}

/** Called after a successful API request — resets that bucket's backoff. */
export function markSheetsAvailable(kind: SheetsTraffic = "read"): void {
  if (kind === "read") readFailStreak = 0;
  else writeFailStreak = 0;
}

/** True when the error looks like a transient quota/rate-limit condition. */
export function isQuotaError(error: unknown): boolean {
  const msg = String((error as { message?: unknown })?.message ?? error ?? "");
  return /quota|rate.?limit|RESOURCE_EXHAUSTED|429|too many requests/i.test(msg);
}

/**
 * Return the raw Sheets service global ignoring cooldowns — used by
 * API-only sheet adapters that have NO SpreadsheetApp fallback handle
 * (their sheet is invisible to SpreadsheetApp due to cross-service
 * replication lag), so retrying the API is the only option.
 */
export function sheetsServiceUnchecked(): SheetsService | null {
  const svc = (globalThis as Record<string, unknown>)["Sheets"] as SheetsService | undefined;
  return svc && svc.Spreadsheets && typeof svc.Spreadsheets.batchUpdate === "function" ? svc : null;
}

// ─── Deferred values writes ─────────────────────────────────────────────────
// A value write (appendRow/updateRow/writeRowsAt/setHeaders/…) is enqueued
// instead of executed immediately; the next Sheets API call of any kind —
// or the next raw SpreadsheetApp mutation — first commits the whole queue as
// ONE Values.batchUpdate per spreadsheet.  Consecutive ORM mutations
// (entity row + index rows, several saves in a row) therefore fuse into a
// single write request, keeping total write volume under the
// 'Write requests per minute per user' quota.

export interface PendingValuesWrite {
  spreadsheetId: string;
  /** Fully-qualified A1 range INCLUDING the quoted sheet title. */
  range: string;
  values: unknown[][];
  /**
   * SpreadsheetApp/last-resort fallback — executed if the API batch fails.
   * Must perform the equivalent write through SpreadsheetApp (calling
   * markRawWrite() afterwards) or rethrow for API-only sheets.
   */
  fallback(): void;
}

const pendingWrites: PendingValuesWrite[] = [];
let flushingPendingWrites = false;

/** Queue a values write; committed by the next API call or explicit flush. */
export function enqueueValuesWrite(w: PendingValuesWrite): void {
  pendingWrites.push(w);
}

/**
 * Commit every queued values write — one Values.batchUpdate per spreadsheet.
 * On API failure each write's own fallback runs in issue order.
 */
export function flushPendingWrites(): void {
  if (flushingPendingWrites || pendingWrites.length === 0) return;
  flushingPendingWrites = true;
  try {
    const queue = pendingWrites.splice(0, pendingWrites.length);
    const bySs = new Map<string, PendingValuesWrite[]>();
    for (const w of queue) {
      const arr = bySs.get(w.spreadsheetId);
      if (arr) arr.push(w);
      else bySs.set(w.spreadsheetId, [w]);
    }
    for (const [ssId, items] of bySs) {
      const svc = Date.now() >= writeUnavailableUntil && !writeWindowFull() ? sheetsServiceUnchecked() : null;
      if (svc) {
        try {
          recordWriteCall();
          svc.Spreadsheets.Values.batchUpdate(
            {
              valueInputOption: "RAW",
              data: items.map((i) => ({ range: i.range, values: i.values })),
            },
            ssId,
          );
          markSheetsAvailable("write");
          continue;
        } catch (e) {
          markSheetsUnavailable(e, "write");
          // The cached grid size is stale (auto-shrink/expansion elsewhere)
          // — drop it so the next ensureGridRows works with real numbers.
          if (isGridLimitError(e)) invalidateSheetMeta(ssId);
        }
      }
      for (const item of items) item.fallback();
    }
  } finally {
    flushingPendingWrites = false;
  }
}

/**
 * Remove and return all queued values writes for `spreadsheetId`.  Structural
 * batchUpdate paths drain the queue and fold the writes into their own
 * request as updateCells — one API call instead of a separate
 * Values.batchUpdate.  Unconvertible items go back via
 * {@link requeuePendingWrites}.
 */
export function drainPendingWrites(spreadsheetId: string): PendingValuesWrite[] {
  const out: PendingValuesWrite[] = [];
  for (let i = pendingWrites.length - 1; i >= 0; i--) {
    if (pendingWrites[i].spreadsheetId === spreadsheetId) {
      out.unshift(pendingWrites[i]);
      pendingWrites.splice(i, 1);
    }
  }
  return out;
}

/** Re-append drained writes that could not be folded into a structural batch. */
export function requeuePendingWrites(items: PendingValuesWrite[]): void {
  pendingWrites.push(...items);
}

// ─── Parallel REST calls (UrlFetchApp.fetchAll) ────────────────

/** One REST request spec for {@link parallelFetch}. */
export interface ParallelRequestSpec {
  /** Full URL (e.g. https://sheets.googleapis.com/v4/spreadsheets/{id}). */
  url: string;
  /** HTTP method — defaults to "get". */
  method?: "get" | "post" | "put" | "delete";
  /** JSON body (serialised automatically). */
  payload?: unknown;
}

const SHEETS_REST_BASE = "https://sheets.googleapis.com/v4/spreadsheets";

/** Sheets REST URL helpers — same endpoints the Advanced Service proxies. */
export function restMetaUrl(spreadsheetId: string, fields: string): string {
  return `${SHEETS_REST_BASE}/${spreadsheetId}?fields=${encodeURIComponent(fields)}`;
}
export function restBatchGetUrl(spreadsheetId: string): string {
  return `${SHEETS_REST_BASE}/${spreadsheetId}/values:batchGet`;
}

/**
 * Fire several independent REST calls **concurrently** via
 * `UrlFetchApp.fetchAll` — the only true parallelism primitive in GAS
 * (Advanced Service calls are synchronous-serial, ~150 ms each on the wire).
 *
 * Requires the `script.external_request` + `spreadsheets` OAuth scopes
 * (present in appsscript.json).  Hits the same API quota as Advanced Service
 * calls, so callers stay inside the existing sliding windows.
 *
 * @returns Parsed JSON responses aligned with `specs` (null per failed entry),
 *          or `null` when fetchAll is unavailable — callers must fall back to
 *          the serial Advanced-Service path.
 */
export function parallelFetch(specs: ParallelRequestSpec[]): unknown[] | null {
  if (specs.length < 2) return null;
  if (
    typeof UrlFetchApp === "undefined" ||
    typeof ScriptApp === "undefined" ||
    typeof ScriptApp.getOAuthToken !== "function"
  ) {
    return null;
  }
  try {
    const token = ScriptApp.getOAuthToken();
    const reqs = specs.map((s) => ({
      url: s.url,
      method: (s.method ?? "get") as GoogleAppsScript.URL_Fetch.HttpMethod,
      contentType: "application/json",
      headers: { Authorization: `Bearer ${token}` },
      muteHttpExceptions: true,
      followRedirects: true,
      ...(s.payload !== undefined ? { payload: JSON.stringify(s.payload) } : {}),
    }));
    const responses = UrlFetchApp.fetchAll(reqs);
    return responses.map((r) => {
      if (r.getResponseCode() >= 300) return null;
      const text = r.getContentText();
      return text ? (JSON.parse(text) as unknown) : null;
    });
  } catch {
    return null; // No OAuth token / fetchAll failure — caller takes serial path
  }
}

/**
 * True when the write channel can accept new work right now — no cooldown
 * and the service is bound.  Unlike {@link sheetsService} this NEVER flushes
 * the pending queue (value writes merge into it rather than forcing it out).
 */
export function writesReady(): boolean {
  if (Date.now() < writeUnavailableUntil) return false;
  if (writeWindowFull()) return false;
  return sheetsServiceUnchecked() !== null;
}

/**
 * Read counterpart of {@link writesReady} — true when the read window has
 * capacity and the service is bound.  apiOnly callers use it to decide
 * between an unchecked API attempt and the raw path.
 */
export function readsReady(): boolean {
  if (Date.now() < readUnavailableUntil) return false;
  if (readWindowFull()) return false;
  return sheetsServiceUnchecked() !== null;
}

// ─── Write-call windowing ───────────────────────────────────────────────────
// The Sheets API write quota is ~60 requests/min/user.  Rather than tripping
// the quota and paying escalating cooldowns — or sleeping whole seconds —
// we track a sliding window of write calls; once saturated, write-capable
// callers report "not ready" and route their writes through SpreadsheetApp
// (which meters separately).  API-only adapters keep their retry path.

const WRITE_WINDOW_LIMIT = 55;
const READ_WINDOW_LIMIT = 55;
const writeCallTimes: number[] = [];
const readCallTimes: number[] = [];

/** True once ~55 API write calls occurred inside the last 60 seconds. */
function writeWindowFull(): boolean {
  return windowFull(writeCallTimes, WRITE_WINDOW_LIMIT);
}

/** True once ~55 API read calls occurred inside the last 60 seconds. */
function readWindowFull(): boolean {
  return windowFull(readCallTimes, READ_WINDOW_LIMIT);
}

function windowFull(times: number[], limit: number): boolean {
  const now = Date.now();
  while (times.length > 0 && now - times[0] > 60_000) {
    times.shift();
  }
  return times.length >= limit;
}

/** Record one real Sheets API write request (call sites performing writes). */
function recordWriteCall(): void {
  writeCallTimes.push(Date.now());
}

/** Record one real Sheets API read request. */
function recordReadCall(): void {
  readCallTimes.push(Date.now());
}

/** Blocking sleep — no-op outside GAS. */
export function sleepMs(ms: number): void {
  try {
    Utilities.sleep(ms);
  } catch {
    /* Not in GAS */
  }
}

/** Escape a sheet title for use inside an A1-notation range string. */
export function a1Quote(title: string): string {
  return `'${title.replace(/'/g, "''")}'`;
}

/** Convert a 1-based column number to A1 column letters (1 → A, 27 → AA). */
export function colToLetter(col: number): string {
  let s = "";
  while (col > 0) {
    const m = (col - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    col = Math.floor((col - 1) / 26);
  }
  return s;
}

/** Inverse of {@link colToLetter}: "A" → 1, "AA" → 27. */
export function letterToCol(letters: string): number {
  let n = 0;
  for (let i = 0; i < letters.length; i++) {
    n = n * 26 + (letters.charCodeAt(i) - 64);
  }
  return n;
}

/**
 * Convert a JS primitive to a Sheets API `userEnteredValue` cell object.
 * `stringValue` writes literal text (no date/number parsing — equivalent to
 * valueInputOption RAW), numberValue/boolValue keep primitives as-is.
 */
function toCellValue(v: unknown): Record<string, unknown> {
  if (typeof v === "number") return { userEnteredValue: { numberValue: v } };
  if (typeof v === "boolean") return { userEnteredValue: { boolValue: v } };
  if (v === null || v === undefined) return { userEnteredValue: { stringValue: "" } };
  return { userEnteredValue: { stringValue: String(v) } };
}

/** Convert a row of primitives to a Sheets API RowData object. */
export function toRowData(row: unknown[]): Record<string, unknown> {
  return { values: row.map(toCellValue) };
}

// ─── Per-spreadsheet sheet metadata cache ────────────────────────────────────
// One Sheets.Spreadsheets.get call enumerates every tab (title → sheetId +
// grid dims).  The map is shared across all adapter instances for the same
// spreadsheetId within a single GAS execution, and invalidated whenever any
// adapter mutates the tab structure (insert/delete/rename/wipe).

export interface SheetMeta {
  sheetId: number;
  title: string;
  rowCount: number;
  columnCount: number;
  hidden: boolean;
}

const metaBySpreadsheet = new Map<string, Map<string, SheetMeta>>();

/** Drop the cached metadata for a spreadsheet (call after structural mutations). */
export function invalidateSheetMeta(spreadsheetId: string): void {
  metaBySpreadsheet.delete(spreadsheetId);
}

/**
 * Return the title→meta map for a spreadsheet, fetching once via Sheets API
 * and caching until invalidated by a structural mutation through any adapter.
 * Returns null when the service is unavailable or the call fails.
 */
export function getSheetMeta(svc: SheetsService, spreadsheetId: string): Map<string, SheetMeta> | null {
  const cached = metaBySpreadsheet.get(spreadsheetId);
  if (cached) return cached;
  try {
    const meta = svc.Spreadsheets.get(spreadsheetId, {
      fields: "sheets.properties(sheetId,title,hidden,gridProperties)",
    });
    const map = new Map<string, SheetMeta>();
    for (const s of meta.sheets ?? []) {
      const p = s.properties;
      if (p && p.title !== undefined && p.sheetId !== undefined) {
        map.set(p.title, {
          sheetId: p.sheetId,
          title: p.title,
          rowCount: p.gridProperties?.rowCount ?? 0,
          columnCount: p.gridProperties?.columnCount ?? 0,
          hidden: p.hidden === true,
        });
      }
    }
    metaBySpreadsheet.set(spreadsheetId, map);
    markSheetsAvailable("read");
    return map;
  } catch (e) {
    markSheetsUnavailable(e, "read");
    return null;
  }
}

/** Register one sheet in the metadata cache (post-insert bookkeeping). */
export function registerSheetMeta(spreadsheetId: string, meta: SheetMeta): void {
  metaBySpreadsheet.get(spreadsheetId)?.set(meta.title, meta);
}

/** Remove one sheet from the metadata cache (post-delete bookkeeping). */
export function dropSheetMeta(spreadsheetId: string, title: string): void {
  metaBySpreadsheet.get(spreadsheetId)?.delete(title);
}

/** Cached title→meta map only — never triggers a fetch. */
export function peekSheetMeta(spreadsheetId: string): Map<string, SheetMeta> | null {
  return metaBySpreadsheet.get(spreadsheetId) ?? null;
}

/**
 * Replace the cached title→meta map in bulk — used by the parallel warm-up
 * path (UrlFetchApp.fetchAll), which bypasses the Advanced-Service plumbing.
 */
export function seedSheetMeta(spreadsheetId: string, map: Map<string, SheetMeta>): void {
  metaBySpreadsheet.set(spreadsheetId, map);
}

/**
 * Raise a sheet's cached grid rowCount (writes via setValues/Values API
 * auto-expand the grid silently — this keeps the cache in step so later
 * updateCells calls know the true capacity).
 */
export function bumpSheetMetaRows(spreadsheetId: string, title: string, rows: number): void {
  const m = metaBySpreadsheet.get(spreadsheetId)?.get(title);
  if (m && m.rowCount < rows) m.rowCount = rows;
}

/** Shift a cached grid rowCount by `delta` (deleteDimension/insertDimension). */
export function adjustSheetMetaRows(spreadsheetId: string, title: string, delta: number): void {
  const m = metaBySpreadsheet.get(spreadsheetId)?.get(title);
  if (m) m.rowCount = Math.max(1, m.rowCount + delta);
}
