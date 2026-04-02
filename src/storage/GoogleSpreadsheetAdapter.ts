/**
 * Google Apps Script adapter implementing the ISpreadsheetAdapter interface.
 *
 * Wraps a GAS `Spreadsheet` object (the whole file) and provides
 * operations for creating, listing, and deleting individual sheet tabs.
 * Defaults to the active spreadsheet when no explicit Spreadsheet object
 * is supplied.
 *
 * @module GoogleSpreadsheetAdapter
 */

import type { ISpreadsheetAdapter } from "../core/types/ISpreadsheetAdapter.js";
import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import { GoogleSheetAdapter } from "./GoogleSheetAdapter.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

/**
 * Production implementation of {@link ISpreadsheetAdapter} for Google Apps Script.
 * Each call that returns a sheet wraps it in a {@link GoogleSheetAdapter}.
 */
export class GoogleSpreadsheetAdapter implements ISpreadsheetAdapter {
  /** The wrapped GAS Spreadsheet object. */
  private spreadsheet: GoogleAppsScript.Spreadsheet.Spreadsheet;

  /**
   * @param spreadsheet - Optional explicit Spreadsheet object.
   *                       Falls back to `SpreadsheetApp.getActiveSpreadsheet()`
   *                       when omitted (the common GAS use-case).
   */
  constructor(spreadsheet?: GoogleAppsScript.Spreadsheet.Spreadsheet) {
    this.spreadsheet = spreadsheet ?? SpreadsheetApp.getActiveSpreadsheet();
  }

  /**
   * Look up a sheet tab by name.
   * @returns A wrapped adapter, or `null` if no sheet with that name exists.
   */
  getSheetByName(name: string): ISheetAdapter | null {
    const sheet = this.spreadsheet.getSheetByName(name);
    SheetOrmLogger.log(`[Spreadsheet] getSheetByName("${name}") → ${sheet ? "found" : "null"}`);
    return sheet ? new GoogleSheetAdapter(sheet) : null;
  }

  /**
   * Get or create a sheet tab.  If a sheet with the given name already
   * exists it is reused (idempotent).  Otherwise a new tab is inserted.
   */
  createSheet(name: string): ISheetAdapter {
    const existing = this.spreadsheet.getSheetByName(name);
    if (existing) {
      SheetOrmLogger.log(`[Spreadsheet] createSheet("${name}") → reusing existing sheet`);
      return new GoogleSheetAdapter(existing);
    }
    SheetOrmLogger.log(`[Spreadsheet] createSheet("${name}") → inserting new sheet`);
    const sheet = this.spreadsheet.insertSheet(name);
    return new GoogleSheetAdapter(sheet);
  }

  /**
   * Always insert a brand-new sheet tab (not idempotent).
   * GAS throws if a sheet with the same name already exists.
   */
  insertSheet(name: string): ISheetAdapter {
    SheetOrmLogger.log(`[Spreadsheet] insertSheet("${name}")`);
    const sheet = this.spreadsheet.insertSheet(name);
    return new GoogleSheetAdapter(sheet);
  }

  /** Delete a sheet tab by name.  No-op if the sheet does not exist. */
  deleteSheet(name: string): void {
    const sheet = this.spreadsheet.getSheetByName(name);
    if (sheet) {
      SheetOrmLogger.log(`[Spreadsheet] deleteSheet("${name}")`);
      this.spreadsheet.deleteSheet(sheet);
    }
  }

  /** Return an array of all sheet tab names in the spreadsheet. */
  getSheetNames(): string[] {
    return this.spreadsheet.getSheets().map((s) => s.getName());
  }

  /** Return a Map of sheet name → adapter for every tab in the spreadsheet. */
  getSheets(): Map<string, ISheetAdapter> {
    const map = new Map<string, ISheetAdapter>();
    for (const sheet of this.spreadsheet.getSheets()) {
      map.set(sheet.getName(), new GoogleSheetAdapter(sheet));
    }
    return map;
  }

  /**
   * Remove all sheets and leave a single empty "Sheet1" tab.
   *
   * GAS requires at least one sheet in a spreadsheet, so the first tab
   * is kept, cleared, and renamed to "Sheet1" while all others are deleted.
   */
  removeAllSheets(): void {
    const sheets = this.spreadsheet.getSheets();
    SheetOrmLogger.log(`[Spreadsheet] removeAllSheets() → deleting ${sheets.length} sheet(s)`);
    if (sheets.length === 0) return;
    // Keep the first sheet to satisfy the GAS one-sheet minimum requirement
    const keeper = sheets[0];
    for (let i = 1; i < sheets.length; i++) {
      this.spreadsheet.deleteSheet(sheets[i]);
    }
    // Clear all content and rename to default
    keeper.clear();
    keeper.setName("Sheet1");
  }
}
