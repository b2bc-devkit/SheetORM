/**
 * Abstraction over a single Google Sheet tab (worksheet).
 *
 * All row indexes are **0-based data indexes** — row 0 corresponds to
 * sheet row 2 (row 1 is the header). This convention keeps the ORM layer
 * independent of the header-row offset used by Google Sheets.
 *
 * The production implementation is GoogleSheetAdapter; tests use
 * MockSheetAdapter (an in-memory array-backed stub).
 */
export interface ISheetAdapter {
  /** Return the sheet / tab name. */
  getName(): string;

  /** Read the header row (row 1) and return column names. */
  getHeaders(): string[];

  /** Overwrite the header row (row 1) with the given column names. */
  setHeaders(headers: string[]): void;

  /** Read all data rows (row 2+) as a 2D array. */
  getAllData(): unknown[][];

  /** Return the number of data rows (excludes the header row). */
  getRowCount(): number;

  /** Append a single row after the last occupied row. */
  appendRow(values: unknown[]): void;

  /** Append multiple rows after the last occupied row in one API call. */
  appendRows(rows: unknown[][]): void;

  /** Write rows starting at the given 0-based data index (overwrites existing cells). */
  writeRowsAt(startRowIndex: number, rows: unknown[][]): void;

  /** Overwrite a single row at the given 0-based data index. */
  updateRow(rowIndex: number, values: unknown[]): void;

  /** Overwrite multiple rows; contiguous groups are batched into single setValues() calls. */
  updateRows(updates: Array<{ rowIndex: number; values: unknown[] }>): void;

  /** Delete a single row at the given 0-based data index (shifts rows below up). */
  deleteRow(rowIndex: number): void;

  /** Delete multiple rows by 0-based data indexes (deletes bottom-to-top to avoid index shift). */
  deleteRows(rowIndexes: number[]): void;

  /** Read a single row at the given 0-based data index. */
  getRow(rowIndex: number): unknown[];

  /**
   * Read specific data rows by 0-based index in ONE batched call where the
   * backend supports it (Sheets `Values.batchGet` with N row ranges).
   * Returns rows aligned with `rowIndexes`; `null` entries mark missing rows.
   * Optional — callers fall back to per-row getRow() or a full scan.
   */
  readRowsAt?(rowIndexes: number[]): Array<unknown[] | null>;

  /**
   * Read a single sheet COLUMN (data rows only, row 2+), returned as a
   * per-row scalar array.  Used to rebuild id→rowIndex maps with a narrow
   * payload (one column instead of the full grid).  Optional.
   */
  readIdsColumn?(colIndex: number): unknown[] | null;

  /**
   * Write only the specified cells of a data row (sparse dirty-column update).
   * `cells` is an array of `[colIndex, value]` pairs over the FULL header
   * layout (0-based, incl. system columns); implementations may merge adjacent
   * columns into span writes.  Optional — callers fall back to updateRow().
   */
  updateRowSparse?(rowIndex: number, cells: Array<readonly [number, unknown]>): void;

  /**
   * Numeric Sheets `sheetId` (grid scope), or `null` when unavailable.
   * Used for cross-execution identity checks (schema fingerprints) — a sheet
   * recreated under the same name receives a NEW sheetId, which makes stale
   * fingerprints detectable.
   */
  getSheetId?(): number | null;

  /** Replace all data rows (row 2+) with the provided 2D array; surplus old rows are cleared. */
  replaceAllData(rows: unknown[][]): void;

  /** Clear the entire sheet (headers + data). */
  clear(): void;

  /** Force-flush pending changes to the spreadsheet (calls SpreadsheetApp.flush()). */
  flush(): void;

  /**
   * Write the header row and all data rows in a single setValues() API call.
   * Used for newly-created sheets to avoid a separate setHeaders() round-trip.
   */
  writeAllRowsWithHeaders(headers: string[], rows: unknown[][]): void;
}
