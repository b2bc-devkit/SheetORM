export interface ISheetAdapter {
  getName(): string;
  getHeaders(): string[];
  setHeaders(headers: string[]): void;
  getAllData(): unknown[][];
  getRowCount(): number;
  appendRow(values: unknown[]): void;
  appendRows(rows: unknown[][]): void;
  writeRowsAt(startRowIndex: number, rows: unknown[][]): void;
  updateRow(rowIndex: number, values: unknown[]): void;
  updateRows(updates: Array<{ rowIndex: number; values: unknown[] }>): void;
  deleteRow(rowIndex: number): void;
  deleteRows(rowIndexes: number[]): void;
  getRow(rowIndex: number): unknown[];
  replaceAllData(rows: unknown[][]): void;
  clear(): void;
  flush(): void;
}
