// Mock implementations of ISheetAdapter and ISpreadsheetAdapter for testing

import { ISheetAdapter, ISpreadsheetAdapter } from "../src/core/types";

export class MockSheetAdapter implements ISheetAdapter {
  private name: string;
  private headers: string[] = [];
  private data: unknown[][] = [];

  constructor(name: string) {
    this.name = name;
  }

  getName(): string {
    return this.name;
  }

  getHeaders(): string[] {
    return [...this.headers];
  }

  setHeaders(headers: string[]): void {
    this.headers = [...headers];
  }

  getAllData(): unknown[][] {
    return this.data.map((row) => [...row]);
  }

  getRowCount(): number {
    return this.data.length;
  }

  appendRow(values: unknown[]): void {
    this.data.push([...values]);
  }

  appendRows(rows: unknown[][]): void {
    for (const row of rows) {
      this.data.push([...row]);
    }
  }

  writeRowsAt(startRowIndex: number, rows: unknown[][]): void {
    for (let i = 0; i < rows.length; i++) {
      const idx = startRowIndex + i;
      if (idx >= this.data.length) {
        this.data.push([...rows[i]]);
      } else {
        this.data[idx] = [...rows[i]];
      }
    }
  }

  updateRow(rowIndex: number, values: unknown[]): void {
    if (rowIndex === this.data.length) {
      this.data.push([...values]);
    } else if (rowIndex >= 0 && rowIndex < this.data.length) {
      this.data[rowIndex] = [...values];
    }
  }

  updateRows(updates: Array<{ rowIndex: number; values: unknown[] }>): void {
    for (const u of updates) {
      this.updateRow(u.rowIndex, u.values);
    }
  }

  deleteRow(rowIndex: number): void {
    if (rowIndex >= 0 && rowIndex < this.data.length) {
      this.data.splice(rowIndex, 1);
    }
  }

  deleteRows(rowIndexes: number[]): void {
    const sorted = [...rowIndexes].sort((a, b) => b - a);
    for (const idx of sorted) {
      this.deleteRow(idx);
    }
  }

  getRow(rowIndex: number): unknown[] {
    if (rowIndex >= 0 && rowIndex < this.data.length) {
      return [...this.data[rowIndex]];
    }
    return [];
  }

  replaceAllData(rows: unknown[][]): void {
    this.data = rows.map((row) => [...row]);
  }

  clear(): void {
    this.headers = [];
    this.data = [];
  }

  flush(): void {
    // no-op in mock
  }

  // Test helpers
  _getRawData(): unknown[][] {
    return this.data;
  }
}

export class MockSpreadsheetAdapter implements ISpreadsheetAdapter {
  private sheets = new Map<string, MockSheetAdapter>();

  getSheetByName(name: string): ISheetAdapter | null {
    return this.sheets.get(name) ?? null;
  }

  createSheet(name: string): ISheetAdapter {
    const existing = this.sheets.get(name);
    if (existing) return existing;
    const sheet = new MockSheetAdapter(name);
    this.sheets.set(name, sheet);
    return sheet;
  }

  deleteSheet(name: string): void {
    this.sheets.delete(name);
  }

  getSheetNames(): string[] {
    return Array.from(this.sheets.keys());
  }

  // Test helper
  _getSheet(name: string): MockSheetAdapter | undefined {
    return this.sheets.get(name);
  }
}
