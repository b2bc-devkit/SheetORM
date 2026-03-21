// SheetORM — Google Spreadsheet adapter implementing ISpreadsheetAdapter

import type { ISpreadsheetAdapter } from "../core/types/ISpreadsheetAdapter.js";
import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import { GoogleSheetAdapter } from "./GoogleSheetAdapter.js";

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
    return sheet ? new GoogleSheetAdapter(sheet) : null;
  }

  createSheet(name: string): ISheetAdapter {
    const existing = this.spreadsheet.getSheetByName(name);
    if (existing) {
      return new GoogleSheetAdapter(existing);
    }
    const sheet = this.spreadsheet.insertSheet(name);
    return new GoogleSheetAdapter(sheet);
  }

  deleteSheet(name: string): void {
    const sheet = this.spreadsheet.getSheetByName(name);
    if (sheet) {
      this.spreadsheet.deleteSheet(sheet);
    }
  }

  getSheetNames(): string[] {
    return this.spreadsheet.getSheets().map((s) => s.getName());
  }
}
