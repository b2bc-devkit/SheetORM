import type { ISheetAdapter } from "./ISheetAdapter.js";

export interface ISpreadsheetAdapter {
  getSheetByName(name: string): ISheetAdapter | null;
  createSheet(name: string): ISheetAdapter;
  /** Insert a brand-new sheet without a prior existence check. Use only when the caller has already
   *  confirmed the sheet does not exist (e.g. getSheetByName returned null). Saves one redundant
   *  getSheetByName API call compared to createSheet(). */
  insertSheet(name: string): ISheetAdapter;
  deleteSheet(name: string): void;
  getSheetNames(): string[];
  /** H3: Return all existing sheets as a name→adapter map in a single API call.
   *  Use at the start of ensureRepository() to avoid one getSheetByName() per table/index. */
  getSheets(): Map<string, ISheetAdapter>;
  removeAllSheets(): void;
}
