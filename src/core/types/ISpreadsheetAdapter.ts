import type { ISheetAdapter } from "./ISheetAdapter.js";

/**
 * Abstraction over a Google Spreadsheet (the file-level container of sheets).
 *
 * Provides sheet-level CRUD: look up, create, delete, and list individual
 * sheet tabs. The production implementation is GoogleSpreadsheetAdapter;
 * tests use MockSpreadsheetAdapter.
 */
export interface ISpreadsheetAdapter {
  /** Look up a sheet by name, returning null if it does not exist. */
  getSheetByName(name: string): ISheetAdapter | null;

  /** Get or create a sheet by name (reuses an existing sheet if found). */
  createSheet(name: string): ISheetAdapter;

  /**
   * Insert a brand-new sheet without a prior existence check.
   * Use only when the caller has already confirmed the sheet does not exist
   * (e.g. getSheetByName returned null). Saves one redundant getSheetByName
   * API call compared to createSheet().
   */
  insertSheet(name: string): ISheetAdapter;

  /** Delete a sheet by name (no-op if the sheet does not exist). */
  deleteSheet(name: string): void;

  /** Return a list of all sheet tab names in the spreadsheet. */
  getSheetNames(): string[];

  /**
   * Return all existing sheets as a name → adapter map in a single API call.
   * Used at startup to avoid one getSheetByName() round-trip per table / index sheet.
   */
  getSheets(): Map<string, ISheetAdapter>;

  /** Delete all sheets except one (GAS requires at least one sheet to exist). */
  removeAllSheets(): void;

  /**
   * Protect a sheet tab and restrict editing to the given email addresses.
   * No-op if the sheet does not exist.
   *
   * @param name    - The sheet tab name to protect.
   * @param editors - Email addresses allowed to edit the protected sheet.
   */
  protectSheet(name: string, editors: string[]): void;
}
