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

  /** Replace all data rows (row 2+) with the provided 2D array; surplus old rows are cleared. */
  replaceAllData(rows: unknown[][]): void;

  /** Clear the entire sheet (headers + data). */
  clear(): void;

  /** Force-flush pending changes to the spreadsheet (calls SpreadsheetApp.flush()). */
  flush(): void;

  /**
   * L1 optimisation: Write the header row and all data rows in a single
   * setValues() API call. Used for newly-created sheets to avoid a separate
   * setHeaders() round-trip (~700 ms saved per new table).
   */
  writeAllRowsWithHeaders(headers: string[], rows: unknown[][]): void;
}
