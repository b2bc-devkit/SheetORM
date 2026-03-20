import type { ISheetAdapter } from "../src/core/types/ISheetAdapter";
import type { ISpreadsheetAdapter } from "../src/core/types/ISpreadsheetAdapter";
import { MockSheetAdapter } from "./MockSheetAdapter";

export class MockSpreadsheetAdapter implements ISpreadsheetAdapter {
  private sheets = new Map<string, MockSheetAdapter>();

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

  deleteSheet(name: string): void {
    this.sheets.delete(name);
  }

  getSheetNames(): string[] {
    return Array.from(this.sheets.keys());
  }

  _getSheet(name: string): MockSheetAdapter | undefined {
    return this.sheets.get(name);
  }
}
