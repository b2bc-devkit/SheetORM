import type { ISheetAdapter } from "./ISheetAdapter.js";

/**
 * Specification for a sheet to create with initial content.
 * `headers` land in row 1; `rows` are the data rows starting at row 2
 * (row 1 stays empty when `headers` is omitted — matching the index-sheet
 * convention where data always starts at row 2).
 */
export interface NewSheetSpec {
  name: string;
  headers?: string[];
  rows?: unknown[][];
}

/**
 * Abstraction over a Google Spreadsheet (the file-level container of sheets).
 *
 * Provides sheet-level CRUD: look up, create, delete, and list individual
 * sheet tabs. The production implementation is GoogleSpreadsheetAdapter;
 * tests use MockSpreadsheetAdapter.
 */
export interface ISpreadsheetAdapter {
  /** Look up a sheet by name, returning null if it does not exist. */
  getSheetByName(name: string): ISheetAdapter | null;

  /** Get or create a sheet by name (reuses an existing sheet if found). */
  createSheet(name: string): ISheetAdapter;

  /**
   * Insert a brand-new sheet without a prior existence check.
   * Use only when the caller has already confirmed the sheet does not exist
   * (e.g. getSheetByName returned null). Saves one redundant getSheetByName
   * API call compared to createSheet().
   */
  insertSheet(name: string): ISheetAdapter;

  /**
   * Insert multiple sheet tabs in a single batched request (optional fast path).
   *
   * @returns Adapters for the new sheets in argument order, or `null` when the
   *          implementation cannot batch (callers must fall back to individual
   *          insertSheet calls).  Implementations must roll back nothing —
   *          partial creation is possible on failure and callers handle it.
   */
  insertSheets?(names: string[]): ISheetAdapter[] | null;

  /**
   * Insert sheets AND seed headers/data rows in a single batched request
   * (optional fast path — Sheets API addSheet + updateCells fusion).
   * When any name collides or the implementation cannot batch, returns
   * `null`; callers then fall back to insertSheet/createSheet + setHeaders.
   */
  insertSheetsWithData?(specs: NewSheetSpec[]): ISheetAdapter[] | null;

  /** Delete a sheet by name (no-op if the sheet does not exist). */
  deleteSheet(name: string): void;

  /** Return a list of all sheet tab names in the spreadsheet. */
  getSheetNames(): string[];

  /**
   * Return all existing sheets as a name → adapter map in a single API call.
   * Used at startup to avoid one getSheetByName() round-trip per table / index sheet.
   */
  getSheets(): Map<string, ISheetAdapter>;

  /**
   * Fused multi-sheet warm-up (optional fast path): reads several sheets'
   * grids in ONE batched call (Sheets `Values.batchGet`) instead of one read
   * per sheet.  Missing, unresolved, or already-warm sheets are skipped;
   * failures are non-fatal — subsequent reads fall back per sheet.
   */
  prefetchSheets?(names: string[]): void;

  /** Delete all sheets except one (GAS requires at least one sheet to exist). */
  removeAllSheets(): void;

  /**
   * The spreadsheet's stable ID (optional).  Used to namespace cross-execution
   * state such as schema fingerprints — script properties are per-script, not
   * per-spreadsheet, so keys must carry the spreadsheet identity.
   */
  getSpreadsheetId?(): string | null;

  /**
   * Parallel cold-start warm-up (optional): fetches sheet metadata AND grids
   * concurrently via `UrlFetchApp.fetchAll` — two independent calls that would
   * otherwise serialize (~150 ms each on the wire).  Returns `false` when the
   * implementation cannot parallelise; callers then run the serial path.
   */
  warmUpSheets?(names: string[]): boolean;

  /**
   * Protect a sheet tab and restrict editing to the given email addresses.
   * No-op if the sheet does not exist.
   *
   * @param name    - The sheet tab name to protect.
   * @param editors - Email addresses allowed to edit the protected sheet.
   */
  protectSheet(name: string, editors: string[]): void;

  /**
   * Hide a sheet tab from the bottom tab bar.
   * The sheet remains accessible from the "All sheets" menu.
   * No-op if the sheet does not exist.
   *
   * @param name - The sheet tab name to hide.
   */
  hideSheet(name: string): void;
}
