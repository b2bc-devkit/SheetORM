import type { ISheetAdapter } from "./ISheetAdapter.js";

export interface ISpreadsheetAdapter {
  getSheetByName(name: string): ISheetAdapter | null;
  createSheet(name: string): ISheetAdapter;
  deleteSheet(name: string): void;
  getSheetNames(): string[];
}
