/**
 * Google Apps Script adapter implementing the ISpreadsheetAdapter interface.
 *
 * Wraps a GAS `Spreadsheet` object (the whole file) and provides
 * operations for creating, listing, and deleting individual sheet tabs.
 * Defaults to the active spreadsheet when no explicit Spreadsheet object
 * is supplied.
 *
 * Performance architecture:
 *   - When the Sheets API advanced service is bound, structural operations
 *     run through `Sheets.Spreadsheets.batchUpdate` / `Spreadsheets.get`:
 *       · `getSheetNames`/`getSheets` enumerate every tab in ONE call.
 *       · `insertSheets` creates several tabs in ONE call.
 *       · `removeAllSheets` wipes N tabs in ~2 calls.
 *       · `getSheetByName` answers existence from the shared metadata cache
 *         (zero RPCs once fetched) and returns pure-API adapters that never
 *         need a raw Sheet handle.
 *   - Positive name→adapter caching survives across calls; entries evict on
 *     deletion so stale handles never leak.
 *   - API failures trigger a short exponential-backoff cooldown (transient
 *     quota/5xx errors recover automatically); the op falls back to classic
 *     SpreadsheetApp calls in the meantime.
 *
 * @module GoogleSpreadsheetAdapter
 */

import type { ISpreadsheetAdapter, NewSheetSpec } from "../core/types/ISpreadsheetAdapter.js";
import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import { GoogleSheetAdapter } from "./GoogleSheetAdapter.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";
import {
  a1Quote,
  dropSheetMeta,
  flushRawWrites,
  getSheetMeta,
  invalidateSheetMeta,
  markRawWrite,
  markSheetsAvailable,
  markSheetsUnavailable,
  parallelFetch,
  peekSheetMeta,
  readsReady,
  registerSheetMeta,
  restBatchGetUrl,
  restMetaUrl,
  seedSheetMeta,
  sheetsService,
  toRowData,
} from "./SheetsRpc.js";
import type { SheetMeta, SheetsService } from "./SheetsRpc.js";

/** Cached pairing of the raw GAS Sheet (optional) with its adapter wrapper. */
interface SheetEntry {
  sheet: GoogleAppsScript.Spreadsheet.Sheet | null;
  adapter: GoogleSheetAdapter;
}

/**
 * Production implementation of {@link ISpreadsheetAdapter} for Google Apps Script.
 * Each call that returns a sheet wraps it in a {@link GoogleSheetAdapter}.
 */
export class GoogleSpreadsheetAdapter implements ISpreadsheetAdapter {
  /** The wrapped GAS Spreadsheet object. */
  private spreadsheet: GoogleAppsScript.Spreadsheet.Spreadsheet;

  /** Positive name→entry cache. Entries are evicted on deleteSheet(). */
  private sheetByName = new Map<string, SheetEntry>();

  /** Lazily-fetched spreadsheet ID (needed by the Sheets API advanced service). */
  private spreadsheetId: string | null = null;

  /**
   * @param spreadsheet - Optional explicit Spreadsheet object.
   *                       Falls back to `SpreadsheetApp.getActiveSpreadsheet()`
   *                       when omitted (the common GAS use-case).
   */
  constructor(spreadsheet?: GoogleAppsScript.Spreadsheet.Spreadsheet) {
    this.spreadsheet = spreadsheet ?? SpreadsheetApp.getActiveSpreadsheet();
  }

  /** Spreadsheet ID, memoised (getId() itself costs an RPC). */
  private getId(): string {
    if (this.spreadsheetId === null) {
      this.spreadsheetId = this.spreadsheet.getId();
    }
    return this.spreadsheetId;
  }

  /**
   * Public spreadsheet-ID accessor — used to scope schema-fingerprint
   * keys and REST URLs.  Memoised (getId() itself costs an RPC).
   */
  getSpreadsheetId(): string | null {
    try {
      return this.getId();
    } catch {
      return null;
    }
  }

  /** Shared metadata map (null when the Sheets service is unavailable). */
  private meta(): Map<string, SheetMeta> | null {
    const svc = sheetsService("read");
    if (!svc) return null;
    return getSheetMeta(svc, this.getId());
  }

  /** Build a pure-API sheet adapter from meta (no raw Sheet handle). */
  private apiAdapter(meta: SheetMeta): GoogleSheetAdapter {
    return new GoogleSheetAdapter(null, undefined, {
      spreadsheet: this.spreadsheet,
      ssId: this.getId(),
      sheetId: meta.sheetId,
      title: meta.title,
    });
  }

  /**
   * Look up a sheet tab by name.
   * @returns A wrapped adapter, or `null` if no sheet with that name exists.
   */
  getSheetByName(name: string): ISheetAdapter | null {
    const cached = this.sheetByName.get(name);
    if (cached) return cached.adapter;

    // Meta-cache path: one shared RPC answers existence for every name.
    const meta = this.meta();
    if (meta) {
      const m = meta.get(name);
      if (!m) {
        SheetOrmLogger.log(`[Spreadsheet] getSheetByName("${name}") → absent (meta cache)`);
        return null;
      }
      const adapter = this.apiAdapter(m);
      this.sheetByName.set(name, { sheet: null, adapter });
      return adapter;
    }

    const sheet = this.spreadsheet.getSheetByName(name);
    SheetOrmLogger.log(`[Spreadsheet] getSheetByName("${name}") → ${sheet ? "found" : "null"}`);
    if (!sheet) return null;
    const adapter = new GoogleSheetAdapter(sheet, { title: name });
    this.sheetByName.set(name, { sheet, adapter });
    return adapter;
  }

  /**
   * Get or create a sheet tab.  If a sheet with the given name already
   * exists it is reused (idempotent).  Otherwise a new tab is inserted.
   */
  createSheet(name: string): ISheetAdapter {
    const existing = this.getSheetByName(name);
    if (existing) {
      SheetOrmLogger.log(`[Spreadsheet] createSheet("${name}") → reusing existing sheet`);
      return existing;
    }
    SheetOrmLogger.log(`[Spreadsheet] createSheet("${name}") → inserting new sheet`);
    return this.insertSheet(name);
  }

  /**
   * True when an insert failure means "name already taken".  The Sheets API
   * localises error messages (e.g. Polish "już istnieje"), so after the
   * regex fast-path we confirm against the shared metadata map.
   */
  private isDuplicateSheetErr(err: unknown, names: string[]): boolean {
    const msg = String((err as { message?: unknown })?.message ?? err);
    if (/already exists|już istnieje|istnieje|existiert|existe déjà|ya existe|already exists/i.test(msg)) {
      return true;
    }
    const meta = this.meta();
    return meta !== null && names.some((n) => meta.has(n));
  }

  /**
   * SpreadsheetApp-only insert — bypasses the Sheets API fast path.
   * Needed for sheets that must be visible to SpreadsheetApp immediately
   * (protected/hidden tables are verified via raw getSheetByName, and the
   * Sheets-API→SpreadsheetApp propagation can lag an entire execution).
   */
  insertSheetRaw(name: string): ISheetAdapter {
    SheetOrmLogger.log(`[Spreadsheet] insertSheetRaw("${name}")`);
    const sheet = this.spreadsheet.insertSheet(name);
    markRawWrite();
    // The meta map predates this sheet — drop it so the next lookup refetches
    // (a stale-negative would wrongly report the new tab as absent).
    invalidateSheetMeta(this.getId());
    const adapter = new GoogleSheetAdapter(sheet, { lastRow: 0, lastCol: 0, headers: [], title: name });
    this.sheetByName.set(name, { sheet, adapter });
    return adapter;
  }

  /**
   * Always insert a brand-new sheet tab (not idempotent).
   * GAS throws if a sheet with the same name already exists.
   */
  insertSheet(name: string): ISheetAdapter {
    const svc = sheetsService("write");
    if (svc) {
      try {
        const res = svc.Spreadsheets.batchUpdate(
          { requests: [{ addSheet: { properties: { title: name } } }] },
          this.getId(),
        );
        const props = res.replies?.[0]?.addSheet?.properties;
        if (props?.sheetId !== undefined) {
          const meta: SheetMeta = {
            sheetId: props.sheetId,
            title: name,
            rowCount: props.gridProperties?.rowCount ?? 1000,
            columnCount: props.gridProperties?.columnCount ?? 26,
            hidden: props.hidden === true,
          };
          registerSheetMeta(this.getId(), meta);
          // Fresh sheet → seed empty dimensions
          const adapter = new GoogleSheetAdapter(
            null,
            { lastRow: 0, lastCol: 0, headers: [] },
            {
              spreadsheet: this.spreadsheet,
              ssId: this.getId(),
              sheetId: props.sheetId,
              title: name,
            },
          );
          this.sheetByName.set(name, { sheet: null, adapter });
          markSheetsAvailable("write");
          SheetOrmLogger.log(`[Spreadsheet] insertSheet("${name}") → Sheets API addSheet`);
          return adapter;
        }
      } catch (err) {
        // Duplicate-name failures are an expected part of the
        // insert-first protocol — rethrow without disabling the service.
        if (this.isDuplicateSheetErr(err, [name])) throw err;
        markSheetsUnavailable(err, "write");
      }
    }
    return this.insertSheetRaw(name);
  }

  /**
   * Insert several sheet tabs in ONE batched request (Sheets API only).
   * Returns the adapters keyed to each name, or null when the API is
   * unavailable / the batch failed (callers then fall back to per-sheet
   * inserts).  Not part of ISpreadsheetAdapter — optional fast path.
   */
  insertSheets(names: string[]): ISheetAdapter[] | null {
    if (names.length === 0) return [];
    const svc = sheetsService("write");
    if (!svc) return null;
    try {
      const res = svc.Spreadsheets.batchUpdate(
        {
          requests: names.map((title) => ({
            addSheet: { properties: { title } },
          })),
        },
        this.getId(),
      );
      const replies = res.replies ?? [];
      const adapters: ISheetAdapter[] = [];
      for (let i = 0; i < names.length; i++) {
        const props = replies[i]?.addSheet?.properties;
        if (props?.sheetId === undefined) return null;
        const meta: SheetMeta = {
          sheetId: props.sheetId,
          title: names[i],
          rowCount: props.gridProperties?.rowCount ?? 1000,
          columnCount: props.gridProperties?.columnCount ?? 26,
          hidden: props.hidden === true,
        };
        registerSheetMeta(this.getId(), meta);
        const adapter = new GoogleSheetAdapter(
          null,
          { lastRow: 0, lastCol: 0, headers: [] },
          {
            spreadsheet: this.spreadsheet,
            ssId: this.getId(),
            sheetId: props.sheetId,
            title: names[i],
          },
        );
        this.sheetByName.set(names[i], { sheet: null, adapter });
        adapters.push(adapter);
      }
      markSheetsAvailable("write");
      SheetOrmLogger.log(`[Spreadsheet] insertSheets ×${names.length} → Sheets API batch`);
      return adapters;
    } catch (err) {
      if (!this.isDuplicateSheetErr(err, names)) markSheetsUnavailable(err, "write");
      // batchUpdate applies requests sequentially — earlier addSheets may have
      // committed before a later one failed, so drop the meta map to keep the
      // next lookup honest.
      invalidateSheetMeta(this.getId());
      return null;
    }
  }

  /**
   * Insert several sheet tabs AND seed their content in ONE batchUpdate.
   * Client-specified `sheetId`s let addSheet + updateCells fuse into a
   * single write call — cutting insert+headers from 2-3 RPCs to 1.
   * Returns null on any failure (duplicate name, API unavailable) — the
   * caller then falls back to individual insertSheet + setHeaders calls.
   */
  insertSheetsWithData(specs: NewSheetSpec[]): ISheetAdapter[] | null {
    if (specs.length === 0) return [];
    const svc = sheetsService("write");
    if (!svc) return null;
    const ssId = this.getId();

    const requests: Array<Record<string, unknown>> = [];
    const ids: number[] = [];
    const grids: Array<{ rowCount: number; columnCount: number }> = [];
    for (const spec of specs) {
      // Positive int32 chosen client-side so follow-up requests in the SAME
      // batchUpdate can address the new sheet (avoiding a read round-trip).
      const sheetId = Math.floor(Math.random() * 0x3ffffffe) + 1;
      ids.push(sheetId);
      // Header row (row 1) is always emitted so data rows land at row 2+,
      // matching the writeRowsAt(0) → sheetRow 2 convention — even for
      // header-less index sheets an empty row 1 is written.
      const content: unknown[][] = [];
      if (spec.headers) content.push([...spec.headers]);
      else if (spec.rows && spec.rows.length > 0) content.push([]);
      if (spec.rows) for (const r of spec.rows) content.push(r);
      const width = content.length > 0 ? Math.max(...content.map((r) => r.length)) : 0;
      // updateCells cannot write outside the grid — size it to the seed
      // (default 1000×26 would reject a >999-row seed).
      const gridProps = {
        rowCount: Math.max(1000, content.length),
        columnCount: Math.max(26, width),
      };
      grids.push(gridProps);
      requests.push({
        addSheet: {
          properties: { title: spec.name, sheetId, gridProperties: gridProps },
        },
      });
      if (content.length > 0 && content.some((r) => r.length > 0)) {
        requests.push({
          updateCells: {
            range: {
              sheetId,
              startRowIndex: 0,
              endRowIndex: content.length,
              startColumnIndex: 0,
              endColumnIndex: width,
            },
            rows: content.map(toRowData),
            fields: "userEnteredValue",
          },
        });
      }
    }

    try {
      svc.Spreadsheets.batchUpdate({ requests }, ssId);
    } catch (err) {
      if (
        !this.isDuplicateSheetErr(
          err,
          specs.map((s) => s.name),
        )
      ) {
        markSheetsUnavailable(err, "write");
      }
      invalidateSheetMeta(ssId);
      return null;
    }
    markSheetsAvailable("write");

    const adapters: ISheetAdapter[] = [];
    for (let i = 0; i < specs.length; i++) {
      const spec = specs[i];
      const sheetId = ids[i];
      registerSheetMeta(ssId, {
        sheetId,
        title: spec.name,
        rowCount: grids[i].rowCount,
        columnCount: grids[i].columnCount,
        hidden: false,
      });
      // Seed full state — adapter needs zero RPCs for subsequent reads.
      const content: unknown[][] = [];
      if (spec.headers) content.push([...spec.headers]);
      else if (spec.rows && spec.rows.length > 0) content.push([]);
      if (spec.rows) for (const r of spec.rows) content.push(r);
      const hasContent = content.length > 0 && content.some((r) => r.length > 0);
      const adapter = new GoogleSheetAdapter(
        null,
        hasContent
          ? {
              lastRow: content.length,
              lastCol: Math.max(...content.map((r) => r.length)),
              headers: spec.headers ? [...spec.headers] : [],
              grid: content,
            }
          : { lastRow: 0, lastCol: 0, headers: [] },
        { spreadsheet: this.spreadsheet, ssId, sheetId, title: spec.name },
      );
      this.sheetByName.set(spec.name, { sheet: null, adapter });
      adapters.push(adapter);
    }
    SheetOrmLogger.log(`[Spreadsheet] insertSheetsWithData ×${specs.length} → Sheets API fused`);
    return adapters;
  }

  /** Delete a sheet tab by name.  No-op if the sheet does not exist. */
  deleteSheet(name: string): void {
    const svc = sheetsService("write");
    const meta = svc ? getSheetMeta(svc, this.getId()) : null;
    const entry = this.sheetByName.get(name);
    const sheetId = entry?.adapter ? this.resolveSheetId(entry) : meta?.get(name)?.sheetId;
    if (svc && sheetId !== undefined && sheetId !== null) {
      try {
        svc.Spreadsheets.batchUpdate({ requests: [{ deleteSheet: { sheetId } }] }, this.getId());
        markSheetsAvailable("write");
        dropSheetMeta(this.getId(), name);
        this.sheetByName.delete(name);
        SheetOrmLogger.log(`[Spreadsheet] deleteSheet("${name}") → Sheets API`);
        return;
      } catch (err) {
        markSheetsUnavailable(err, "write");
      }
    }
    const sheet = entry?.sheet ?? this.spreadsheet.getSheetByName(name);
    if (sheet) {
      SheetOrmLogger.log(`[Spreadsheet] deleteSheet("${name}")`);
      this.spreadsheet.deleteSheet(sheet);
      markRawWrite();
    }
    dropSheetMeta(this.getId(), name);
    this.sheetByName.delete(name);
  }

  /** Resolve the numeric sheetId for a cached entry (meta → raw fallback). */
  private resolveSheetId(entry: SheetEntry): number | undefined {
    const meta = this.meta()?.get(entry.adapter.getName());
    if (meta) return meta.sheetId;
    return entry.sheet ? entry.sheet.getSheetId() : undefined;
  }

  /**
   * Return an array of all sheet tab names in the spreadsheet.
   * Uses the Sheets API (one RPC for every tab) when available;
   * falls back to per-sheet getName() calls otherwise.
   */
  getSheetNames(): string[] {
    const meta = this.meta();
    if (meta) {
      SheetOrmLogger.log(`[Spreadsheet] getSheetNames → Sheets API, ${meta.size} tabs`);
      return [...meta.keys()];
    }
    return this.spreadsheet.getSheets().map((s) => s.getName());
  }

  /** Return a Map of sheet name → adapter for every tab in the spreadsheet. */
  getSheets(): Map<string, ISheetAdapter> {
    const meta = this.meta();
    if (meta) {
      const map = new Map<string, ISheetAdapter>();
      for (const [name, m] of meta) {
        const cached = this.sheetByName.get(name);
        if (cached) {
          map.set(name, cached.adapter);
        } else {
          const adapter = this.apiAdapter(m);
          this.sheetByName.set(name, { sheet: null, adapter });
          map.set(name, adapter);
        }
      }
      return map;
    }
    const map = new Map<string, ISheetAdapter>();
    for (const sheet of this.spreadsheet.getSheets()) {
      const name = sheet.getName();
      const cached = this.sheetByName.get(name);
      if (cached) {
        map.set(name, cached.adapter);
      } else {
        const adapter = new GoogleSheetAdapter(sheet, { title: name });
        this.sheetByName.set(name, { sheet, adapter });
        map.set(name, adapter);
      }
    }
    return map;
  }

  /**
   * Warm several sheets' grid caches with ONE `Values.batchGet` call.
   * Sheets that are absent from the shared metadata (e.g. an index table not
   * created yet) or already warm are skipped.  Any API failure is swallowed —
   * a cold sheet simply pays its own lazy read later.
   */
  prefetchSheets(names: string[]): void {
    const svc = sheetsService("read");
    if (!svc) return;
    const meta = peekSheetMeta(this.getId());
    const cold: Array<{ name: string; adapter: GoogleSheetAdapter }> = [];
    for (const name of names) {
      let entry = this.sheetByName.get(name);
      if (!entry && meta?.has(name)) {
        // Meta-known but no adapter yet — materialise via the meta cache
        // (zero RPC) so the prefetched grid lands on the same instance.
        this.getSheetByName(name);
        entry = this.sheetByName.get(name);
      }
      const adapter = entry?.adapter;
      if (adapter && !adapter.isGridWarm()) cold.push({ name, adapter });
    }
    if (cold.length === 0) return;
    try {
      const res = svc.Spreadsheets.Values.batchGet(this.getId(), {
        ranges: cold.map((c) => a1Quote(c.name)),
        valueRenderOption: "UNFORMATTED_VALUE",
        dateTimeRenderOption: "SERIAL_NUMBER",
      });
      markSheetsAvailable("read");
      const vrs = (res.valueRanges ?? []) as Array<{
        range?: string;
        values?: unknown[][];
      }>;
      for (let i = 0; i < cold.length; i++) {
        const vr = vrs[i];
        cold[i].adapter.ingestValueRange(vr?.range, vr?.values ?? []);
      }
      SheetOrmLogger.log(`[Spreadsheet] prefetchSheets ×${cold.length} → Values.batchGet`);
    } catch (e) {
      markSheetsUnavailable(e, "read");
    }
  }

  /**
   * Parallel cold-start warm-up: fires the spreadsheet-metadata fetch AND the
   * `Values.batchGet` grid prefetch CONCURRENTLY via `UrlFetchApp.fetchAll`
   * (REST `values:batchGet` ranges reference sheet names, not sheetIds, so the
   * two calls are genuinely independent).  Halves the serial startup latency.
   *
   * When metadata is already warm the serial `prefetchSheets` path is already
   * optimal (one batched call) — this delegates to it.
   *
   * batchGet with a non-existent sheet name fails the whole REST request; in
   * that case the meta still seeds and missing grids fall back to the serial
   * prefetch — nothing is lost.
   */
  warmUpSheets(names: string[]): boolean {
    const ssId = this.getId();
    if (peekSheetMeta(ssId)) {
      this.prefetchSheets(names);
      return true;
    }
    if (typeof UrlFetchApp === "undefined" || !readsReady()) return false;
    const results = parallelFetch([
      {
        url: restMetaUrl(ssId, "sheets(properties(sheetId,title,hidden,gridProperties))"),
      },
      {
        url:
          `${restBatchGetUrl(ssId)}?` +
          names.map((n) => `ranges=${encodeURIComponent(n)}`).join("&") +
          "&valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=SERIAL_NUMBER",
      },
    ]);
    if (results === null) return false;

    // 1) Seed the shared meta map from the REST Spreadsheets.get response.
    const metaRes = results[0] as {
      sheets?: Array<{
        properties?: {
          sheetId?: number;
          title?: string;
          hidden?: boolean;
          gridProperties?: { rowCount?: number; columnCount?: number };
        };
      }>;
    } | null;
    if (metaRes) {
      const map = new Map<string, SheetMeta>();
      for (const s of metaRes.sheets ?? []) {
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
      seedSheetMeta(ssId, map);
    }

    // 2) Seed grids — only for names the (fresh) meta confirms exist.
    const gridsRes = results[1] as {
      valueRanges?: Array<{ range?: string; values?: unknown[][] }>;
    } | null;
    const meta = peekSheetMeta(ssId);
    if (meta && gridsRes?.valueRanges) {
      for (let i = 0; i < names.length; i++) {
        const vr = gridsRes.valueRanges[i];
        if (!vr || !meta.has(names[i])) continue;
        const adapter = this.getSheetByName(names[i]) as GoogleSheetAdapter | null;
        adapter?.ingestValueRange(vr.range, vr.values ?? []);
      }
    }
    SheetOrmLogger.log(`[Spreadsheet] warmUpSheets ×${names.length} → fetchAll(meta+batchGet)`);
    return true;
  }

  /**
   * Remove all sheets and leave a single empty "Sheet1" tab.
   *
   * GAS requires at least one sheet in a spreadsheet, so the first tab
   * is kept, cleared, and renamed to "Sheet1" while all others are deleted.
   *
   * With the Sheets API enabled the entire operation — metadata fetch plus
   * every deleteSheet/clear/rename request — takes just 2 RPCs regardless
   * of how many tabs exist.
   */
  removeAllSheets(): void {
    const svc = sheetsService("write");
    if (svc) {
      try {
        this.removeAllSheetsViaApi(svc);
        return;
      } catch (err) {
        markSheetsUnavailable(err, "write");
        SheetOrmLogger.log("[Spreadsheet] removeAllSheets via Sheets API failed → SpreadsheetApp fallback");
      }
    }
    const sheets = this.spreadsheet.getSheets();
    SheetOrmLogger.log(`[Spreadsheet] removeAllSheets() → deleting ${sheets.length} sheet(s)`);
    if (sheets.length === 0) return;
    // Keep the first sheet to satisfy the GAS one-sheet minimum requirement
    const keeper = sheets[0];
    for (let i = 1; i < sheets.length; i++) {
      this.spreadsheet.deleteSheet(sheets[i]);
    }
    // Clear all content and rename to default
    keeper.clear();
    keeper.setName("Sheet1");
    markRawWrite();
    // Every prior name is gone; the sole survivor is "Sheet1" (empty).
    this.sheetByName.clear();
    this.sheetByName.set("Sheet1", {
      sheet: keeper,
      adapter: new GoogleSheetAdapter(keeper, { lastRow: 0, lastCol: 0, headers: [] }),
    });
    invalidateSheetMeta(this.getId());
  }

  /** Sheets API implementation of {@link removeAllSheets}. */
  private removeAllSheetsViaApi(svc: SheetsService): void {
    invalidateSheetMeta(this.getId()); // force a fresh enumeration
    const meta = getSheetMeta(svc, this.getId());
    const sheets = meta ? [...meta.values()] : [];
    if (sheets.length === 0) return;

    const keeper = sheets[0];
    const requests: Array<Record<string, unknown>> = [];
    // Delete every tab except the keeper (single batch — no per-sheet RPCs)
    for (let i = 1; i < sheets.length; i++) {
      requests.push({ deleteSheet: { sheetId: sheets[i].sheetId } });
    }
    // Clear keeper contents, then rename to "Sheet1" — order matters when
    // another tab currently holds the "Sheet1" name (deleted above).
    requests.push({
      updateCells: {
        range: { sheetId: keeper.sheetId },
        fields: "userEnteredValue",
      },
    });
    requests.push({
      updateSheetProperties: {
        properties: { sheetId: keeper.sheetId, title: "Sheet1" },
        fields: "title",
      },
    });
    svc.Spreadsheets.batchUpdate({ requests }, this.getId());

    // Rebuild the meta map: only the renamed keeper survives.
    invalidateSheetMeta(this.getId());
    this.sheetByName.clear();
    registerSheetMeta(this.getId(), {
      sheetId: keeper.sheetId,
      title: "Sheet1",
      rowCount: keeper.rowCount,
      columnCount: keeper.columnCount,
      hidden: false,
    });
    SheetOrmLogger.log(
      `[Spreadsheet] removeAllSheets via Sheets API → deleted ${sheets.length - 1}, kept "${keeper.title}"→"Sheet1"`,
    );
  }

  /**
   * Resolve a raw Sheet handle resiliently: the bound Spreadsheet object's
   * tab list can be stale right after Sheets API inserts, so on miss we
   * flush pending writes and retry through a fresh openById() handle.
   */
  private rawSheetByName(name: string): GoogleAppsScript.Spreadsheet.Sheet | null {
    const entry = this.sheetByName.get(name);
    if (entry?.sheet) return entry.sheet;
    let sheet = this.spreadsheet.getSheetByName(name);
    if (!sheet) {
      flushRawWrites();
      try {
        sheet = SpreadsheetApp.openById(this.getId()).getSheetByName(name);
      } catch {
        sheet = null;
      }
    }
    return sheet;
  }

  /**
   * Protect a sheet tab and restrict editing to the given email addresses.
   *
   * Uses the GAS `Protection` API — no Sheets API equivalent exists.
   * Existing editors (except the owner) are removed, and only the
   * specified `editors` are granted edit access.
   *
   * No-op if the sheet does not exist.
   */
  protectSheet(name: string, editors: string[]): void {
    const sheet = this.rawSheetByName(name);
    if (!sheet) return;
    SheetOrmLogger.log(`[Spreadsheet] protectSheet("${name}") → editors: [${editors.join(", ")}]`);
    const protection = sheet.protect().setDescription("Protected by SheetORM");
    protection.removeEditors(protection.getEditors());
    if (editors.length > 0) {
      protection.addEditors(editors);
    }
  }

  /**
   * Hide a sheet tab from the bottom tab bar.
   * Sheets API path uses updateSheetProperties(hidden:true) — one RPC;
   * falls back to sheet.hideSheet() otherwise.
   * No-op if the sheet does not exist.
   */
  hideSheet(name: string): void {
    const svc = sheetsService("write");
    const meta = svc ? getSheetMeta(svc, this.getId()) : null;
    const sheetId = meta?.get(name)?.sheetId;
    if (svc && sheetId !== undefined) {
      try {
        svc.Spreadsheets.batchUpdate(
          {
            requests: [
              {
                updateSheetProperties: {
                  properties: { sheetId, hidden: true },
                  fields: "hidden",
                },
              },
            ],
          },
          this.getId(),
        );
        meta!.get(name)!.hidden = true;
        markSheetsAvailable("write");
        SheetOrmLogger.log(`[Spreadsheet] hideSheet("${name}") → Sheets API`);
        return;
      } catch (err) {
        markSheetsUnavailable(err, "write");
      }
    }
    const sheet = this.rawSheetByName(name);
    if (!sheet) return;
    SheetOrmLogger.log(`[Spreadsheet] hideSheet("${name}")`);
    sheet.hideSheet();
    markRawWrite();
  }
}
