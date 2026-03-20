// SheetORM — Sheets REST API v4 spreadsheet adapter for GAS runtime
//
// Buffers all write operations (writeRowsAt / appendRows) in memory and
// sends them to Google Sheets in a SINGLE HTTP batchUpdate call when
// flushAllPending() (or sheet.flush()) is invoked.
//
// Read operations still use the native GAS SpreadsheetApp API — after
// saveAll() the ORM reads from its in-memory cache, so no extra reads
// hit the network before the flush.
//
// Intended use:
//   const v4 = new SheetsAPIv4SpreadsheetAdapter(SpreadsheetApp.getActiveSpreadsheet());
//   Registry.getInstance().configure({ adapter: v4 });
//   MyRecord.saveAll(items);   // buffers entity + index writes
//   v4.flushAllPending();      // one HTTP request instead of N setValues() calls

import type { ISpreadsheetAdapter } from "../core/types/ISpreadsheetAdapter";
import type { ISheetAdapter } from "../core/types/ISheetAdapter";
import { SheetsAPIv4SheetAdapter } from "./SheetsAPIv4SheetAdapter";

// ─── Spreadsheet adapter ──────────────────────────────────────────────────────

/**
 * ISpreadsheetAdapter implementation that routes write operations through the
 * Sheets REST API v4 `spreadsheets.values.batchUpdate` endpoint.
 *
 * All pending ValueRange objects are accumulated in-memory. Call
 * `flushAllPending()` once per logical batch (e.g. after every saveAll()) to
 * send all buffered writes in a single HTTP request.
 */
export class SheetsAPIv4SpreadsheetAdapter implements ISpreadsheetAdapter {
  private ss: GoogleAppsScript.Spreadsheet.Spreadsheet;
  private pending: Array<{ range: string; values: unknown[][] }> = [];

  constructor(spreadsheet?: GoogleAppsScript.Spreadsheet.Spreadsheet) {
    this.ss = spreadsheet ?? SpreadsheetApp.getActiveSpreadsheet();
  }

  /** Called by SheetsAPIv4SheetAdapter to register a pending write. */
  addPendingRange(range: string, values: unknown[][]): void {
    this.pending.push({ range, values });
  }

  /**
   * Flush all buffered write operations to Google Sheets in one HTTP call.
   *
   * Uses the Sheets REST API v4 `spreadsheets.values.batchUpdate` endpoint with
   * `valueInputOption: "RAW"`.  The OAuth token is obtained from
   * `ScriptApp.getOAuthToken()`, which already holds the `spreadsheets` scope
   * because the script uses SpreadsheetApp.
   *
   * Throws if the API returns a non-200 status code.
   */
  flushAllPending(): void {
    if (this.pending.length === 0) return;

    const id = this.ss.getId();
    const token = ScriptApp.getOAuthToken();
    const url = `https://sheets.googleapis.com/v4/spreadsheets/${id}/values:batchUpdate`;
    const body = { valueInputOption: "RAW", data: this.pending };

    const response = UrlFetchApp.fetch(url, {
      method: "post",
      contentType: "application/json",
      headers: { Authorization: `Bearer ${token}` },
      payload: JSON.stringify(body),
      muteHttpExceptions: true,
    });

    const status = response.getResponseCode();
    if (status !== 200) {
      const responseText = response.getContentText().slice(0, 400);
      const enableApiUrl =
        "https://console.cloud.google.com/marketplace/product/google/sheets.googleapis.com?q=search&referrer=search";

      if (status === 403) {
        throw new Error(
          `Sheets API v4 batchUpdate failed (HTTP ${status}): ${responseText} | Enable API: ${enableApiUrl}`,
        );
      }

      throw new Error(
        `Sheets API v4 batchUpdate failed (HTTP ${status}): ${responseText}`,
      );
    }

    this.pending = [];
  }

  getSheetByName(name: string): ISheetAdapter | null {
    const sheet = this.ss.getSheetByName(name);
    return sheet ? new SheetsAPIv4SheetAdapter(sheet, this) : null;
  }

  createSheet(name: string): ISheetAdapter {
    const existing = this.ss.getSheetByName(name);
    const sheet = existing ?? this.ss.insertSheet(name);
    return new SheetsAPIv4SheetAdapter(sheet, this);
  }

  deleteSheet(name: string): void {
    const sheet = this.ss.getSheetByName(name);
    if (sheet) this.ss.deleteSheet(sheet);
  }

  getSheetNames(): string[] {
    return this.ss.getSheets().map((s) => s.getName());
  }
}
