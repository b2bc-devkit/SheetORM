// SheetORM — Google Sheet adapter implementing ISheetAdapter

import type { ISheetAdapter } from "../core/types/ISheetAdapter";

/**
 * Adapter wrapping a real Google Apps Script Sheet object.
 */
export class GoogleSheetAdapter implements ISheetAdapter {
  private sheet: GoogleAppsScript.Spreadsheet.Sheet;

  constructor(sheet: GoogleAppsScript.Spreadsheet.Sheet) {
    this.sheet = sheet;
  }

  getName(): string {
    return this.sheet.getName();
  }

  getHeaders(): string[] {
    const lastCol = this.sheet.getLastColumn();
    if (lastCol === 0) return [];
    return this.sheet
      .getRange(1, 1, 1, lastCol)
      .getValues()[0]
      .map((v) => String(v));
  }

  setHeaders(headers: string[]): void {
    if (headers.length === 0) return;
    this.sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }

  getAllData(): unknown[][] {
    const lastRow = this.sheet.getLastRow();
    const lastCol = this.sheet.getLastColumn();
    if (lastRow <= 1 || lastCol === 0) return []; // row 1 = headers
    return this.sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  }

  getRowCount(): number {
    const lastRow = this.sheet.getLastRow();
    return Math.max(0, lastRow - 1); // exclude header
  }

  appendRow(values: unknown[]): void {
    this.sheet.appendRow(values);
  }

  appendRows(rows: unknown[][]): void {
    if (rows.length === 0) return;
    const startRow = this.sheet.getLastRow() + 1;
    const numCols = rows[0].length;
    this.sheet.getRange(startRow, 1, rows.length, numCols).setValues(rows);
  }

  writeRowsAt(startRowIndex: number, rows: unknown[][]): void {
    if (rows.length === 0) return;
    // startRowIndex is 0-based data index; sheet row = startRowIndex + 2 (row 1 = header)
    const sheetRow = startRowIndex + 2;
    const numCols = rows[0].length;
    this.sheet.getRange(sheetRow, 1, rows.length, numCols).setValues(rows);
  }

  updateRow(rowIndex: number, values: unknown[]): void {
    // rowIndex is 0-based data index → sheet row = rowIndex + 2 (row 1 = header)
    const sheetRow = rowIndex + 2;
    this.sheet.getRange(sheetRow, 1, 1, values.length).setValues([values]);
  }

  updateRows(updates: Array<{ rowIndex: number; values: unknown[] }>): void {
    for (const u of updates) {
      this.updateRow(u.rowIndex, u.values);
    }
  }

  deleteRow(rowIndex: number): void {
    const sheetRow = rowIndex + 2;
    this.sheet.deleteRow(sheetRow);
  }

  deleteRows(rowIndexes: number[]): void {
    // Delete from bottom to top to avoid index shifting
    const sorted = [...rowIndexes].sort((a, b) => b - a);
    for (const idx of sorted) {
      this.deleteRow(idx);
    }
  }

  getRow(rowIndex: number): unknown[] {
    const sheetRow = rowIndex + 2;
    const lastCol = this.sheet.getLastColumn();
    if (lastCol === 0) return [];
    return this.sheet.getRange(sheetRow, 1, 1, lastCol).getValues()[0];
  }

  replaceAllData(rows: unknown[][]): void {
    const lastRow = this.sheet.getLastRow();
    const lastCol = this.sheet.getLastColumn();
    const oldDataRows = Math.max(0, lastRow - 1);
    const numCols = lastCol || (rows.length > 0 ? rows[0].length : 0);

    if (rows.length > 0) {
      this.sheet.getRange(2, 1, rows.length, numCols).setValues(rows);
    }

    if (oldDataRows > rows.length && numCols > 0) {
      this.sheet.getRange(rows.length + 2, 1, oldDataRows - rows.length, numCols).clearContent();
    }
  }

  clear(): void {
    this.sheet.clear();
  }

  flush(): void {
    SpreadsheetApp.flush();
  }
}
