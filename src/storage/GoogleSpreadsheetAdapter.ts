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

  removeAllSheets(): void {
    const sheets = this.spreadsheet.getSheets();
    SheetOrmLogger.log(`[Spreadsheet] removeAllSheets() → deleting ${sheets.length} sheet(s)`);
    if (sheets.length === 0) return;
    // GAS requires at least one sheet — insert a placeholder, delete originals, then rename it
    const placeholder = this.spreadsheet.insertSheet("__sheetorm_placeholder__");
    for (const sheet of sheets) {
      this.spreadsheet.deleteSheet(sheet);
    }
    placeholder.setName("Sheet1");
  }
}
