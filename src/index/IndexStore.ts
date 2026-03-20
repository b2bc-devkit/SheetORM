// SheetORM — IndexStore: manages secondary indexes stored in separate sheets
// Inspired by the index-table pattern from document-oriented ORMs

import { ISpreadsheetAdapter, ICacheProvider } from "../core/types";

/**
 * Combined (per-class) index sheet layout (idx_{ClassName}s):
 *   Row 1 (headers): ["field", "value", "entityId"]
 *   Rows 2+: [fieldName, indexedValue, entityId]
 *
 * For unique indexes, there should be at most one row per value per field.
 */

export interface IndexMeta {
  tableName: string;
  field: string;
  unique: boolean;
}

export class IndexStore {
  private adapter: ISpreadsheetAdapter;
  private cache: ICacheProvider | null;
  private indexRegistry: Map<string, IndexMeta> = new Map();

  constructor(adapter: ISpreadsheetAdapter, cache?: ICacheProvider) {
    this.adapter = adapter;
    this.cache = cache ?? null;
  }

  private registryKey(tableName: string, field: string): string {
    return `${tableName}::${field}`;
  }

  /**
   * Get all indexed fields for a table.
   */
  getIndexedFields(tableName: string): IndexMeta[] {
    const result: IndexMeta[] = [];
    for (const meta of this.indexRegistry.values()) {
      if (meta.tableName === tableName) {
        result.push(meta);
      }
    }
    return result;
  }

  /**
   * Register index metadata (used during schema initialization).
   */
  registerIndex(tableName: string, field: string, unique: boolean): void {
    this.indexRegistry.set(this.registryKey(tableName, field), {
      tableName,
      field,
      unique,
    });
  }

  // ─── Combined (per-class) index sheet methods ───────────────────────────────
  // Used when a Record class has @Indexed fields; all index data is stored in a
  // single sheet named idx_{ClassName}s (e.g. idx_Cars) with columns:
  //   [field, value, entityId]

  /**
   * Create the combined index sheet for a Record class (if not already present).
   * Sheet name equals the class's indexTableName (e.g. idx_Cars).
   */
  createCombinedIndex(indexTableName: string): void {
    const existing = this.adapter.getSheetByName(indexTableName);
    if (!existing) {
      const sheet = this.adapter.createSheet(indexTableName);
      sheet.setHeaders(["field", "value", "entityId"]);
    }
  }

  /**
   * Check whether a combined index sheet exists for the given indexTableName.
   */
  existsCombined(indexTableName: string): boolean {
    return this.adapter.getSheetByName(indexTableName) !== null;
  }

  /**
   * Add an entry to the combined index sheet.
   */
  addToCombined(indexTableName: string, field: string, value: unknown, entityId: string): void {
    const meta = this.indexRegistry.get(this.registryKey(indexTableName, field));
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return;

    const valueStr = String(value);

    if (meta?.unique) {
      const data = sheet.getAllData();
      for (let i = 0; i < data.length; i++) {
        if (String(data[i][0]) === field && String(data[i][1]) === valueStr) {
          if (String(data[i][2]) !== entityId) {
            throw new Error(
              `Unique index violation: ${indexTableName}.${field} already has value "${valueStr}" for entity ${String(data[i][2])}`,
            );
          }
          // Same entity already indexed with this value
          return;
        }
      }
    }

    sheet.appendRow([field, valueStr, entityId]);
    this.clearCache();
  }

  /**
   * Remove all combined index entries for an entity.
   */
  removeAllFromCombined(indexTableName: string, entityId: string): void {
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return;

    const data = sheet.getAllData();
    const rowsToDelete: number[] = [];

    for (let i = 0; i < data.length; i++) {
      if (String(data[i][2]) === entityId) {
        rowsToDelete.push(i);
      }
    }

    if (rowsToDelete.length > 0) {
      for (let i = rowsToDelete.length - 1; i >= 0; i--) {
        sheet.deleteRow(rowsToDelete[i]);
      }
      this.clearCache();
    }
  }

  /**
   * Update combined index entries for an entity (remove old values, add new).
   */
  updateInCombined(
    indexTableName: string,
    entityId: string,
    oldValues: Record<string, unknown>,
    newValues: Record<string, unknown>,
  ): void {
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return;

    const indexedFields = this.getIndexedFields(indexTableName);

    for (const meta of indexedFields) {
      const field = meta.field;
      const oldVal = oldValues[field];
      const newVal = newValues[field];

      if (oldVal === newVal) continue;

      const oldStr = oldVal !== undefined && oldVal !== null && oldVal !== "" ? String(oldVal) : null;
      const newStr = newVal !== undefined && newVal !== null && newVal !== "" ? String(newVal) : null;

      const data = sheet.getAllData();

      // Uniqueness check before any writes
      if (newStr !== null && meta.unique) {
        for (let i = 0; i < data.length; i++) {
          if (
            String(data[i][0]) === field &&
            String(data[i][1]) === newStr &&
            String(data[i][2]) !== entityId
          ) {
            throw new Error(
              `Unique index violation: ${indexTableName}.${field} already has value "${newStr}" for entity ${String(data[i][2])}`,
            );
          }
        }
      }

      // Remove old entry
      if (oldStr !== null) {
        for (let i = data.length - 1; i >= 0; i--) {
          if (String(data[i][0]) === field && String(data[i][1]) === oldStr && String(data[i][2]) === entityId) {
            sheet.deleteRow(i);
            break;
          }
        }
      }

      // Add new entry
      if (newStr !== null) {
        sheet.appendRow([field, newStr, entityId]);
      }

      this.clearCache();
    }
  }

  /**
   * Look up entity IDs in the combined index by field/value pair.
   */
  lookupCombined(indexTableName: string, field: string, value: unknown): string[] {
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return [];

    const valueStr = String(value);
    const data = sheet.getAllData();
    const ids: string[] = [];
    for (let i = 0; i < data.length; i++) {
      if (String(data[i][0]) === field && String(data[i][1]) === valueStr) {
        ids.push(String(data[i][2]));
      }
    }
    return ids;
  }

  /**
   * Delete a combined index sheet and remove registered fields for it.
   */
  dropCombinedIndex(indexTableName: string): void {
    this.adapter.deleteSheet(indexTableName);
    for (const [key, meta] of this.indexRegistry.entries()) {
      if (meta.tableName === indexTableName) {
        this.indexRegistry.delete(key);
      }
    }
    this.clearCache();
  }

  private clearCache(): void {
    if (!this.cache) return;
    this.cache.clear();
  }
}
