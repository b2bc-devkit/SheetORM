import type { ISheetAdapter } from "../src/core/types/ISheetAdapter";
import type { ISpreadsheetAdapter } from "../src/core/types/ISpreadsheetAdapter";
import { MockSheetAdapter } from "./MockSheetAdapter";

export class MockSpreadsheetAdapter implements ISpreadsheetAdapter {
  private sheets = new Map<string, MockSheetAdapter>();
  private protections = new Map<string, string[]>();
  private hiddenSheets = new Set<string>();

  getSheetByName(name: string): ISheetAdapter | null {
    return this.sheets.get(name) ?? null;
  }

  createSheet(name: string): ISheetAdapter {
    const existing = this.sheets.get(name);
    if (existing) return existing;
    const sheet = new MockSheetAdapter(name);
    this.sheets.set(name, sheet);
    return sheet;
  }

  insertSheet(name: string): ISheetAdapter {
    // Mirror real GAS behaviour: duplicate sheet names throw.
    if (this.sheets.has(name)) {
      throw new Error(`Sheet with name "${name}" already exists`);
    }
    const sheet = new MockSheetAdapter(name);
    this.sheets.set(name, sheet);
    return sheet;
  }

  insertSheets(names: string[]): ISheetAdapter[] | null {
    // All-or-nothing mock of the batched API path: fail (return null) when any
    // name already exists so callers exercise the per-name fallback too.
    if (names.some((n) => this.sheets.has(n))) return null;
    return names.map((n) => this.insertSheet(n));
  }

  insertSheetsWithData(
    specs: Array<{ name: string; headers?: string[]; rows?: unknown[][] }>,
  ): ISheetAdapter[] | null {
    if (specs.some((s) => this.sheets.has(s.name))) return null;
    return specs.map((spec) => {
      const sheet = this.insertSheet(spec.name) as MockSheetAdapter;
      if (spec.headers !== undefined || (spec.rows && spec.rows.length > 0)) {
        sheet.writeAllRowsWithHeaders(spec.headers ?? [], spec.rows ?? []);
      }
      return sheet;
    });
  }

  deleteSheet(name: string): void {
    this.sheets.delete(name);
  }

  getSheetNames(): string[] {
    return Array.from(this.sheets.keys());
  }

  getSheets(): Map<string, ISheetAdapter> {
    return new Map(this.sheets);
  }

  removeAllSheets(): void {
    this.sheets.clear();
    this.protections.clear();
    this.hiddenSheets.clear();
  }

  prefetchSheets(names: string[]): void {
    void names; // all data is in-memory — nothing to warm
  }

  getSpreadsheetId(): string | null {
    return "mock-spreadsheet";
  }

  warmUpSheets(names: string[]): boolean {
    void names;
    return false; // serial fallback — the in-memory mock needs no warm-up
  }

  protectSheet(name: string, editors: string[]): void {
    this.protections.set(name, [...editors]);
  }

  hideSheet(name: string): void {
    this.hiddenSheets.add(name);
  }

  _getSheet(name: string): MockSheetAdapter | undefined {
    return this.sheets.get(name);
  }

  _getProtection(name: string): string[] | undefined {
    return this.protections.get(name);
  }

  _clearProtections(): void {
    this.protections.clear();
  }

  _isHidden(name: string): boolean {
    return this.hiddenSheets.has(name);
  }
}
