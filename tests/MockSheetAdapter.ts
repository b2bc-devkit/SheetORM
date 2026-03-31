import type { ISheetAdapter } from "../src/core/types/ISheetAdapter";

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
    // Fill any gap between current data length and startRowIndex with empty rows
    const headerLen = this.headers.length;
    while (this.data.length < startRowIndex) {
      this.data.push(new Array(headerLen).fill(""));
    }
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
    if (rowIndex < 0) return;
    // Fill any gap between current data length and rowIndex with empty rows
    const headerLen = this.headers.length;
    while (this.data.length <= rowIndex) {
      this.data.push(new Array(headerLen).fill(""));
    }
    this.data[rowIndex] = [...values];
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
    // Out of range: return array of empty strings matching header width (mirrors real adapter)
    return this.headers.length > 0 ? new Array(this.headers.length).fill("") : [];
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

  writeAllRowsWithHeaders(headers: string[], rows: unknown[][]): void {
    this.headers = [...headers] as string[];
    this.data = rows.map((row) => [...row]);
  }

  _getRawData(): unknown[][] {
    return this.data;
  }
}
