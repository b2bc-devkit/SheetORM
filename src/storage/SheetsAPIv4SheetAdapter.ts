// SheetORM — Sheets REST API v4 sheet adapter

import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

// ─── A1 notation helpers ──────────────────────────────────────────────────────

/** Convert a 1-based column index to its A1 letter(s), e.g. 1 → "A", 27 → "AA". */
function colLetter(col: number): string {
  let result = "";
  while (col > 0) {
    col--;
    result = String.fromCharCode(65 + (col % 26)) + result;
    col = Math.floor(col / 26);
  }
  return result;
}

/**
 * Build an A1-notation range string for a block of cells.
 * Sheet names containing single quotes are escaped per A1 spec.
 */
function a1Range(sheetName: string, startRow: number, numRows: number, numCols: number): string {
  const escaped = sheetName.replace(/'/g, "''");
  const endRow = startRow + numRows - 1;
  return `'${escaped}'!A${startRow}:${colLetter(numCols)}${endRow}`;
}

// ─── Parent interface (avoids circular import) ────────────────────────────────

/** Minimal interface of SheetsAPIv4SpreadsheetAdapter used by the sheet adapter. */
interface IPendingRangeCollector {
  addPendingRange(range: string, values: unknown[][]): void;
  flushAllPending(): void;
}

// ─── Sheet adapter ────────────────────────────────────────────────────────────

/**
 * ISheetAdapter that buffers writeRowsAt() and appendRows() calls as A1-notation
 * ValueRange objects on the parent SheetsAPIv4SpreadsheetAdapter.
 *
 * Read operations always go to the native GAS Sheet API — after saveAll() the
 * ORM queries from its in-memory cache, not from the sheet directly.
 *
 * Non-batch write operations (updateRow, deleteRow, etc.) fall through to the
 * native GAS API since they are infrequent and outside the saveAll() hot path.
 */
export class SheetsAPIv4SheetAdapter implements ISheetAdapter {
  private sheet: GoogleAppsScript.Spreadsheet.Sheet;
  private parent: IPendingRangeCollector;

  /**
   * Tracks how many rows have been buffered via appendRows() since the last
   * flush.  This ensures that consecutive appendRows() calls compute the
   * correct start row even though the actual Google Sheet has not yet been
   * updated (pending rows are still in the parent's buffer).
   */
  private appendedOffset: number = 0;

  constructor(sheet: GoogleAppsScript.Spreadsheet.Sheet, parent: IPendingRangeCollector) {
    this.sheet = sheet;
    this.parent = parent;
  }

  // ── Identity ─────────────────────────────────────────────────────────────

  getName(): string {
    return this.sheet.getName();
  }

  // ── Reads (always native GAS) ─────────────────────────────────────────────

  getHeaders(): string[] {
    const lastCol = this.sheet.getLastColumn();
    if (lastCol === 0) return [];
    return this.sheet
      .getRange(1, 1, 1, lastCol)
      .getValues()[0]
      .map((v) => String(v));
  }

  getAllData(): unknown[][] {
    const lastRow = this.sheet.getLastRow();
    const lastCol = this.sheet.getLastColumn();
    if (lastRow <= 1 || lastCol === 0) {
      SheetOrmLogger.log(`[V4Sheet:${this.sheet.getName()}] getAllData → 0 rows (empty)`);
      return [];
    }
    const result = this.sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
    SheetOrmLogger.log(
      `[V4Sheet:${this.sheet.getName()}] getAllData → ${result.length} rows × ${lastCol} cols`,
    );
    return result;
  }

  getRowCount(): number {
    const count = Math.max(0, this.sheet.getLastRow() - 1);
    SheetOrmLogger.log(`[V4Sheet:${this.sheet.getName()}] getRowCount → ${count}`);
    return count;
  }

  getRow(rowIndex: number): unknown[] {
    const sheetRow = rowIndex + 2;
    const lastCol = this.sheet.getLastColumn();
    if (lastCol === 0) return [];
    return this.sheet.getRange(sheetRow, 1, 1, lastCol).getValues()[0];
  }

  // ── Header write (native — must be physically in the sheet so that       ──
  // ── getLastRow() returns 1 when appendRows() computes its start offset)  ──

  setHeaders(headers: string[]): void {
    if (headers.length === 0) return;
    this.sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }

  // ── Buffered batch writes ─────────────────────────────────────────────────

  /** Single-row append — delegates to native (not on the saveAll hot path). */
  appendRow(values: unknown[]): void {
    this.sheet.appendRow(values);
  }

  /**
   * Buffers a multi-row append as a ValueRange.
   *
   * `getLastRow()` returns the number of rows that are physically in the sheet
   * (headers + previously flushed data).  `appendedOffset` adds the rows that
   * are already buffered but not yet flushed, giving the correct next free row.
   */
  appendRows(rows: unknown[][]): void {
    if (rows.length === 0) return;
    const startRow = this.sheet.getLastRow() + 1 + this.appendedOffset;
    const numCols = rows[0].length;
    SheetOrmLogger.log(
      `[V4Sheet:${this.sheet.getName()}] appendRows ${rows.length} rows × ${numCols} cols at sheetRow=${startRow}`,
    );
    this.parent.addPendingRange(a1Range(this.sheet.getName(), startRow, rows.length, numCols), rows);
    this.appendedOffset += rows.length;
  }

  /**
   * Buffers a positioned multi-row write as a ValueRange.
   *
   * `startRowIndex` is 0-based data index (row 0 = first data row beneath the
   * header), matching the convention used throughout SheetRepository.
   */
  writeRowsAt(startRowIndex: number, rows: unknown[][]): void {
    if (rows.length === 0) return;
    const sheetRow = startRowIndex + 2; // 0-based data index + 1 (header) + 1 (1-base)
    const numCols = rows[0].length;
    SheetOrmLogger.log(
      `[V4Sheet:${this.sheet.getName()}] writeRowsAt dataIdx=${startRowIndex} sheetRow=${sheetRow} rows=${rows.length} cols=${numCols}`,
    );
    this.parent.addPendingRange(a1Range(this.sheet.getName(), sheetRow, rows.length, numCols), rows);
  }

  // ── Native fallback writes (UPDATE / DELETE — not on the saveAll hot path) ─

  updateRow(rowIndex: number, values: unknown[]): void {
    const sheetRow = rowIndex + 2;
    this.sheet.getRange(sheetRow, 1, 1, values.length).setValues([values]);
  }

  updateRows(updates: Array<{ rowIndex: number; values: unknown[] }>): void {
    if (updates.length === 0) return;
    // Group contiguous rows and buffer each group via writeRowsAt() (buffered)
    const sorted = [...updates].sort((a, b) => a.rowIndex - b.rowIndex);
    let groupStart = sorted[0].rowIndex;
    let groupRows: unknown[][] = [sorted[0].values];
    for (let i = 1; i < sorted.length; i++) {
      if (sorted[i].rowIndex === sorted[i - 1].rowIndex + 1) {
        groupRows.push(sorted[i].values);
      } else {
        this.writeRowsAt(groupStart, groupRows);
        groupStart = sorted[i].rowIndex;
        groupRows = [sorted[i].values];
      }
    }
    this.writeRowsAt(groupStart, groupRows);
  }

  deleteRow(rowIndex: number): void {
    this.sheet.deleteRow(rowIndex + 2);
  }

  deleteRows(rowIndexes: number[]): void {
    const sorted = [...rowIndexes].sort((a, b) => b - a);
    for (const idx of sorted) {
      this.deleteRow(idx);
    }
  }

  replaceAllData(rows: unknown[][]): void {
    const lastRow = this.sheet.getLastRow();
    const lastCol = this.sheet.getLastColumn();
    const oldDataRows = Math.max(0, lastRow - 1);
    const newCols = rows.length > 0 ? rows[0].length : 0;
    const clearCols = Math.max(lastCol, newCols);

    if (rows.length > 0) {
      this.sheet.getRange(2, 1, rows.length, newCols).setValues(rows);
    }

    // Clear surplus columns in written rows if new data is narrower than old data
    if (rows.length > 0 && newCols < lastCol) {
      this.sheet.getRange(2, newCols + 1, rows.length, lastCol - newCols).clearContent();
    }

    if (oldDataRows > rows.length && clearCols > 0) {
      this.sheet.getRange(rows.length + 2, 1, oldDataRows - rows.length, clearCols).clearContent();
    }
  }

  clear(): void {
    this.sheet.clear();
    this.appendedOffset = 0;
  }

  /**
   * Flush all pending writes (across ALL sheets) in one HTTP batchUpdate call.
   * Resets the appendedOffset since the sheet now reflects the flushed state.
   */
  flush(): void {
    this.parent.flushAllPending();
    this.appendedOffset = 0;
  }
}
