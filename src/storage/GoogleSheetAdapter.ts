/**
 * Google Apps Script adapter implementing the ISheetAdapter interface.
 *
 * Wraps a single GAS `Sheet` object (one tab in a Spreadsheet) and
 * translates 0-based data indexes used by the ORM into 1-based sheet
 * rows required by the GAS Range API. Row 1 is always the header row;
 * data rows start at row 2.
 *
 * All multi-cell reads/writes are performed with `getRange().getValues()`
 * / `setValues()` to minimise GAS API calls (each call adds ~300 ms of
 * latency in the Apps Script runtime).
 *
 * @module GoogleSheetAdapter
 */

import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

/**
 * Production implementation of {@link ISheetAdapter} for Google Apps Script.
 * Delegates every operation to the underlying `GoogleAppsScript.Spreadsheet.Sheet`.
 */
export class GoogleSheetAdapter implements ISheetAdapter {
  /** The wrapped GAS Sheet object representing one spreadsheet tab. */
  private sheet: GoogleAppsScript.Spreadsheet.Sheet;

  /**
   * @param sheet - A GAS Sheet object obtained via Spreadsheet.getSheetByName()
   *                or Spreadsheet.insertSheet().
   */
  constructor(sheet: GoogleAppsScript.Spreadsheet.Sheet) {
    this.sheet = sheet;
  }

  /** Return the name of the underlying sheet tab. */
  getName(): string {
    return this.sheet.getName();
  }

  /**
   * Read the header row (row 1) and return the column names as strings.
   * Returns an empty array if the sheet has no columns.
   */
  getHeaders(): string[] {
    const lastCol = this.sheet.getLastColumn();
    if (lastCol === 0) return [];
    return this.sheet
      .getRange(1, 1, 1, lastCol)
      .getValues()[0]
      .map((v) => String(v));
  }

  /**
   * Overwrite the header row (row 1) with the given column names.
   * No-op if the headers array is empty.
   */
  setHeaders(headers: string[]): void {
    if (headers.length === 0) return;
    this.sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }

  /**
   * Read all data rows (row 2+) as a 2D array.
   * Returns an empty array if the sheet has no data rows.
   */
  getAllData(): unknown[][] {
    const lastRow = this.sheet.getLastRow();
    const lastCol = this.sheet.getLastColumn();
    // If there is only a header row (or nothing at all), return empty
    if (lastRow <= 1 || lastCol === 0) {
      SheetOrmLogger.log(`[Sheet:${this.sheet.getName()}] getAllData → 0 rows (empty)`);
      return [];
    }
    // Read from row 2 (first data row) to the last occupied row
    const result = this.sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
    SheetOrmLogger.log(
      `[Sheet:${this.sheet.getName()}] getAllData → ${result.length} rows × ${lastCol} cols`,
    );
    return result;
  }

  /**
   * Return the number of data rows (excludes the header row).
   * Computed as `lastRow - 1`, where lastRow includes the header.
   */
  getRowCount(): number {
    const lastRow = this.sheet.getLastRow();
    const count = Math.max(0, lastRow - 1); // exclude header row
    SheetOrmLogger.log(`[Sheet:${this.sheet.getName()}] getRowCount → ${count}`);
    return count;
  }

  /** Append a single row after the last occupied row using GAS appendRow(). */
  appendRow(values: unknown[]): void {
    this.sheet.appendRow(values);
  }

  /**
   * Append multiple rows in a single setValues() API call.
   * Much faster than calling appendRow() in a loop (~300 ms per call saved).
   */
  appendRows(rows: unknown[][]): void {
    if (rows.length === 0) return;
    // Start writing immediately below the last occupied row
    const startRow = this.sheet.getLastRow() + 1;
    const numCols = rows[0].length;
    SheetOrmLogger.log(
      `[Sheet:${this.sheet.getName()}] appendRows ${rows.length} rows × ${numCols} cols at sheetRow=${startRow}`,
    );
    this.sheet.getRange(startRow, 1, rows.length, numCols).setValues(rows);
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
      `[Sheet:${this.sheet.getName()}] writeRowsAt dataIdx=${startRowIndex} sheetRow=${sheetRow} rows=${rows.length} cols=${numCols}`,
    );
    this.sheet.getRange(sheetRow, 1, rows.length, numCols).setValues(rows);
  }

  /**
   * Overwrite a single data row.
   *
   * @param rowIndex - 0-based data index (sheet row = index + 2).
   * @param values   - Column values.
   */
  updateRow(rowIndex: number, values: unknown[]): void {
    const sheetRow = rowIndex + 2; // +2: 1-based + header offset
    SheetOrmLogger.log(`[Sheet:${this.sheet.getName()}] updateRow dataIdx=${rowIndex} sheetRow=${sheetRow}`);
    this.sheet.getRange(sheetRow, 1, 1, values.length).setValues([values]);
  }

  /**
   * Overwrite multiple rows, batching contiguous groups into single
   * setValues() calls to minimise GAS API round-trips.
   *
   * Example: updating rows 3, 4, 5, 10, 11 produces two setValues() calls
   * instead of five.
   */
  updateRows(updates: Array<{ rowIndex: number; values: unknown[] }>): void {
    if (updates.length === 0) return;

    // Sort by rowIndex so we can detect contiguous sequences
    const sorted = [...updates].sort((a, b) => a.rowIndex - b.rowIndex);

    // Accumulate contiguous groups and flush each as a single setValues()
    let groupStart = sorted[0].rowIndex;
    let groupRows: unknown[][] = [sorted[0].values];
    let groupCount = 0;

    for (let i = 1; i < sorted.length; i++) {
      if (sorted[i].rowIndex === sorted[i - 1].rowIndex + 1) {
        // Contiguous — add to current group
        groupRows.push(sorted[i].values);
      } else {
        // Non-contiguous — flush the current group and start a new one
        SheetOrmLogger.log(
          `[Sheet:${this.sheet.getName()}] updateRows group#${groupCount} dataIdx=${groupStart} rows=${groupRows.length}`,
        );
        this.sheet.getRange(groupStart + 2, 1, groupRows.length, groupRows[0].length).setValues(groupRows);
        groupStart = sorted[i].rowIndex;
        groupRows = [sorted[i].values];
        groupCount++;
      }
    }
    // Flush the final group
    SheetOrmLogger.log(
      `[Sheet:${this.sheet.getName()}] updateRows group#${groupCount} dataIdx=${groupStart} rows=${groupRows.length} (final); total updates=${updates.length}`,
    );
    this.sheet.getRange(groupStart + 2, 1, groupRows.length, groupRows[0].length).setValues(groupRows);
  }

  /**
   * Delete a single data row. GAS deleteRow() shifts all rows below up by one.
   * @param rowIndex - 0-based data index.
   */
  deleteRow(rowIndex: number): void {
    const sheetRow = rowIndex + 2;
    this.sheet.deleteRow(sheetRow);
  }

  /**
   * Delete multiple data rows by index.
   * Rows are deleted from bottom to top so that earlier indexes remain valid.
   */
  deleteRows(rowIndexes: number[]): void {
    // Sort descending to avoid index shift issues
    const sorted = [...rowIndexes].sort((a, b) => b - a);
    for (const idx of sorted) {
      this.deleteRow(idx);
    }
  }

  /**
   * Read a single data row.
   * @param rowIndex - 0-based data index.
   * @returns Array of cell values.
   */
  getRow(rowIndex: number): unknown[] {
    const sheetRow = rowIndex + 2;
    const lastCol = this.sheet.getLastColumn();
    if (lastCol === 0) return [];
    return this.sheet.getRange(sheetRow, 1, 1, lastCol).getValues()[0];
  }

  /**
   * Replace all data rows (row 2+) with the provided 2D array.
   *
   * If the new data has fewer rows than the old data, surplus old rows
   * are cleared. If the new data is narrower (fewer columns), surplus
   * columns in the written region are also cleared.
   */
  replaceAllData(rows: unknown[][]): void {
    const lastRow = this.sheet.getLastRow();
    const lastCol = this.sheet.getLastColumn();
    const oldDataRows = Math.max(0, lastRow - 1);
    const newCols = rows.length > 0 ? rows[0].length : 0;
    const clearCols = Math.max(lastCol, newCols);

    // Write the new data
    if (rows.length > 0) {
      this.sheet.getRange(2, 1, rows.length, newCols).setValues(rows);
    }

    // Clear surplus columns in written rows if new data is narrower
    if (rows.length > 0 && newCols < lastCol) {
      this.sheet.getRange(2, newCols + 1, rows.length, lastCol - newCols).clearContent();
    }

    // Clear surplus old rows below the new data
    if (oldDataRows > rows.length && clearCols > 0) {
      this.sheet.getRange(rows.length + 2, 1, oldDataRows - rows.length, clearCols).clearContent();
    }
  }

  /** Clear the entire sheet (headers + data). */
  clear(): void {
    this.sheet.clear();
  }

  /** Force-flush pending changes to the spreadsheet (calls SpreadsheetApp.flush()). */
  flush(): void {
    SpreadsheetApp.flush();
  }

  /**
   * L1 optimisation: Write the header row and all data rows in a single
   * setValues() API call.  Row 1 = headers, row 2+ = data.
   *
   * Used for newly-created sheets to avoid a separate setHeaders() round-trip
   * (~700 ms saved per new table at first save).
   */
  writeAllRowsWithHeaders(headers: string[], rows: unknown[][]): void {
    const numCols = headers.length;
    // Combine headers and data into one contiguous 2D array
    const allRows: unknown[][] = [headers, ...rows];
    SheetOrmLogger.log(
      `[Sheet:${this.sheet.getName()}] writeAllRowsWithHeaders ${rows.length} data rows + header (${allRows.length} total)`,
    );
    this.sheet.getRange(1, 1, allRows.length, numCols).setValues(allRows);
  }
}
