// SheetORM — Google Spreadsheet adapter implementing ISpreadsheetAdapter

import type { ISpreadsheetAdapter } from "../core/types/ISpreadsheetAdapter.js";
import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import { GoogleSheetAdapter } from "./GoogleSheetAdapter.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

/**
 * Adapter wrapping a Google Apps Script Spreadsheet.
 */
export class GoogleSpreadsheetAdapter implements ISpreadsheetAdapter {
  private spreadsheet: GoogleAppsScript.Spreadsheet.Spreadsheet;

  constructor(spreadsheet?: GoogleAppsScript.Spreadsheet.Spreadsheet) {
    this.spreadsheet = spreadsheet ?? SpreadsheetApp.getActiveSpreadsheet();
  }

  getSheetByName(name: string): ISheetAdapter | null {
    const sheet = this.spreadsheet.getSheetByName(name);
    SheetOrmLogger.log(`[Spreadsheet] getSheetByName("${name}") → ${sheet ? "found" : "null"}`);
    return sheet ? new GoogleSheetAdapter(sheet) : null;
  }

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

  insertSheet(name: string): ISheetAdapter {
    SheetOrmLogger.log(`[Spreadsheet] insertSheet("${name}")`);
    const sheet = this.spreadsheet.insertSheet(name);
    return new GoogleSheetAdapter(sheet);
  }

  deleteSheet(name: string): void {
    const sheet = this.spreadsheet.getSheetByName(name);
    if (sheet) {
      SheetOrmLogger.log(`[Spreadsheet] deleteSheet("${name}")`);
      this.spreadsheet.deleteSheet(sheet);
    }
  }

  getSheetNames(): string[] {
    return this.spreadsheet.getSheets().map((s) => s.getName());
  }

  getSheets(): Map<string, ISheetAdapter> {
    const map = new Map<string, ISheetAdapter>();
    for (const sheet of this.spreadsheet.getSheets()) {
      map.set(sheet.getName(), new GoogleSheetAdapter(sheet));
    }
    return map;
  }

  removeAllSheets(): void {
    const sheets = this.spreadsheet.getSheets();
    SheetOrmLogger.log(`[Spreadsheet] removeAllSheets() → deleting ${sheets.length} sheet(s)`);
    if (sheets.length === 0) return;
    // GAS requires at least one sheet — keep the first, delete the rest, then clear and rename it
    const keeper = sheets[0];
    for (let i = 1; i < sheets.length; i++) {
      this.spreadsheet.deleteSheet(sheets[i]);
    }
    keeper.clear();
    keeper.setName("Sheet1");
  }
}
