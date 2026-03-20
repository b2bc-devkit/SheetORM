// SheetORM — IndexStore: manages secondary indexes stored in separate sheets
// Inspired by the index-table pattern from document-oriented ORMs

import { ISpreadsheetAdapter, ISheetAdapter, INDEX_PREFIX, ICacheProvider } from "../core/types";

/**
 * Per-field index sheet layout:
 *   Row 1 (headers): ["value", "entityId"]
 *   Rows 2+: [indexedValue, entityId]
 *
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

  private indexSheetName(tableName: string, field: string): string {
    return `${INDEX_PREFIX}${tableName}_${field}`;
  }

  private registryKey(tableName: string, field: string): string {
    return `${tableName}::${field}`;
  }

  /**
   * Create a new index for a table field.
   */
  createIndex(tableName: string, field: string, options?: { unique?: boolean }): void {
    const sheetName = this.indexSheetName(tableName, field);
    const existing = this.adapter.getSheetByName(sheetName);
    if (!existing) {
      const sheet = this.adapter.createSheet(sheetName);
      sheet.setHeaders(["value", "entityId"]);
    }
    this.indexRegistry.set(this.registryKey(tableName, field), {
      tableName,
      field,
      unique: options?.unique ?? false,
    });
  }

  /**
   * Drop (delete) an index.
   */
  dropIndex(tableName: string, field: string): void {
    const sheetName = this.indexSheetName(tableName, field);
    this.adapter.deleteSheet(sheetName);
    this.indexRegistry.delete(this.registryKey(tableName, field));
    this.invalidateCache(tableName, field);
  }

  /**
   * Check if an index exists.
   */
  exists(tableName: string, field: string): boolean {
    const sheetName = this.indexSheetName(tableName, field);
    return this.adapter.getSheetByName(sheetName) !== null;
  }

  /**
   * Look up entity IDs by an indexed value.
   */
  lookup(tableName: string, field: string, value: unknown): string[] {
    const cacheKey = `idx:${tableName}:${field}:${String(value)}`;
    if (this.cache) {
      const cached = this.cache.get<string[]>(cacheKey);
      if (cached !== null) return cached;
    }

    const sheet = this.getIndexSheet(tableName, field);
    if (!sheet) return [];

    const data = sheet.getAllData();
    const searchValue = String(value);
    const ids: string[] = [];
    for (let i = 0; i < data.length; i++) {
      if (String(data[i][0]) === searchValue) {
        ids.push(String(data[i][1]));
      }
    }

    if (this.cache) {
      this.cache.set(cacheKey, ids);
    }

    return ids;
  }

  /**
   * Add an entry to an index.
   */
  add(tableName: string, field: string, value: unknown, entityId: string): void {
    const meta = this.indexRegistry.get(this.registryKey(tableName, field));
    const sheet = this.getIndexSheet(tableName, field);
    if (!sheet) return;

    if (meta?.unique) {
      // Check uniqueness
      const existing = this.lookup(tableName, field, value);
      if (existing.length > 0 && !existing.includes(entityId)) {
        throw new Error(
          `Unique index violation: ${tableName}.${field} already has value "${String(value)}" for entity ${existing[0]}`,
        );
      }
      // If same entity already indexed with this value, skip
      if (existing.includes(entityId)) return;
    }

    sheet.appendRow([String(value), entityId]);
    this.invalidateCache(tableName, field);
  }

  /**
   * Remove an entry from an index.
   */
  remove(tableName: string, field: string, value: unknown, entityId: string): void {
    const sheet = this.getIndexSheet(tableName, field);
    if (!sheet) return;

    const data = sheet.getAllData();
    const searchValue = String(value);
    const rowsToDelete: number[] = [];

    for (let i = 0; i < data.length; i++) {
      if (String(data[i][0]) === searchValue && String(data[i][1]) === entityId) {
        rowsToDelete.push(i);
      }
    }

    if (rowsToDelete.length > 0) {
      for (let i = rowsToDelete.length - 1; i >= 0; i--) {
        sheet.deleteRow(rowsToDelete[i]);
      }
      this.invalidateCache(tableName, field);
    }
  }

  /**
   * Remove all index entries for an entity.
   */
  removeAllForEntity(tableName: string, entityId: string): void {
    for (const meta of this.indexRegistry.values()) {
      if (meta.tableName !== tableName) continue;
      const sheet = this.getIndexSheet(tableName, meta.field);
      if (!sheet) continue;

      const data = sheet.getAllData();
      const rowsToDelete: number[] = [];

      for (let i = 0; i < data.length; i++) {
        if (String(data[i][1]) === entityId) {
          rowsToDelete.push(i);
        }
      }

      if (rowsToDelete.length > 0) {
        for (let i = rowsToDelete.length - 1; i >= 0; i--) {
          sheet.deleteRow(rowsToDelete[i]);
        }
        this.invalidateCache(tableName, meta.field);
      }
    }
  }

  /**
   * Update index entries for an entity (remove old, add new) in a single round trip.
   */
  updateForEntity(
    tableName: string,
    entityId: string,
    oldValues: Record<string, unknown>,
    newValues: Record<string, unknown>,
  ): void {
    for (const meta of this.indexRegistry.values()) {
      if (meta.tableName !== tableName) continue;
      const field = meta.field;
      const oldVal = oldValues[field];
      const newVal = newValues[field];

      if (oldVal === newVal) continue;

      const sheet = this.getIndexSheet(tableName, field);
      if (!sheet) continue;

      const data = sheet.getAllData();
      const oldStr = oldVal !== undefined && oldVal !== null && oldVal !== "" ? String(oldVal) : null;
      const newStr = newVal !== undefined && newVal !== null && newVal !== "" ? String(newVal) : null;

      // Check uniqueness before any writes
      if (newStr !== null && meta.unique) {
        for (let i = 0; i < data.length; i++) {
          if (String(data[i][0]) === newStr && String(data[i][1]) !== entityId) {
            throw new Error(
              `Unique index violation: ${tableName}.${field} already has value "${newStr}" for entity ${String(data[i][1])}`,
            );
          }
        }
      }

      // Remove old entry (bottom-to-top so row indices stay valid)
      if (oldStr !== null) {
        for (let i = data.length - 1; i >= 0; i--) {
          if (String(data[i][0]) === oldStr && String(data[i][1]) === entityId) {
            sheet.deleteRow(i);
            break;
          }
        }
      }

      // Add new entry
      if (newStr !== null) {
        sheet.appendRow([newStr, entityId]);
      }

      this.invalidateCache(tableName, field);
    }
  }

  /**
   * Rebuild an index from scratch using entity data.
   * Uses replaceAllData for a single bulk write (headers preserved in row 1).
   */
  rebuild(tableName: string, field: string, entities: Array<{ id: string; value: unknown }>): void {
    const sheet = this.getIndexSheet(tableName, field);
    if (!sheet) return;

    const rows: unknown[][] = [];
    for (const e of entities) {
      if (e.value !== undefined && e.value !== null && e.value !== "") {
        rows.push([String(e.value), e.id]);
      }
    }

    sheet.replaceAllData(rows);
    this.invalidateCache(tableName, field);
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

  private getIndexSheet(tableName: string, field: string): ISheetAdapter | null {
    const sheetName = this.indexSheetName(tableName, field);
    return this.adapter.getSheetByName(sheetName);
  }

  private invalidateCache(_tableName: string, _field: string): void {
    if (!this.cache) return;
    void _tableName;
    void _field;
    // Simple approach: clear all cache entries for this index.
    // A production system would use prefix-based invalidation.
    this.cache.clear();
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
    this.cache?.clear();
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
      this.cache?.clear();
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

      this.cache?.clear();
    }
  }
}
