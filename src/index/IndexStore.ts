// SheetORM — IndexStore: manages secondary indexes stored in separate sheets
// Inspired by the index-table pattern from document-oriented ORMs

import { ISpreadsheetAdapter, ISheetAdapter, INDEX_PREFIX, ICacheProvider } from "../core/types";

/**
 * Index sheet layout:
 *   Row 1 (headers): ["value", "entityId"]
 *   Rows 2+: [indexedValue, entityId]
 *
 * For unique indexes, there should be at most one row per value.
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
    const ids: string[] = [];
    const searchValue = String(value);
    for (const row of data) {
      if (String(row[0]) === searchValue) {
        ids.push(String(row[1]));
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
      sheet.deleteRows(rowsToDelete);
      this.invalidateCache(tableName, field);
    }
  }

  /**
   * Remove all index entries for an entity.
   */
  removeAllForEntity(tableName: string, entityId: string): void {
    for (const [, meta] of this.indexRegistry) {
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
        sheet.deleteRows(rowsToDelete);
        this.invalidateCache(tableName, meta.field);
      }
    }
  }

  /**
   * Update index entries for an entity (remove old, add new).
   */
  updateForEntity(
    tableName: string,
    entityId: string,
    oldValues: Record<string, unknown>,
    newValues: Record<string, unknown>,
  ): void {
    for (const [, meta] of this.indexRegistry) {
      if (meta.tableName !== tableName) continue;
      const field = meta.field;
      const oldVal = oldValues[field];
      const newVal = newValues[field];

      if (oldVal !== newVal) {
        if (oldVal !== undefined && oldVal !== null && oldVal !== "") {
          this.remove(tableName, field, oldVal, entityId);
        }
        if (newVal !== undefined && newVal !== null && newVal !== "") {
          this.add(tableName, field, newVal, entityId);
        }
      }
    }
  }

  /**
   * Rebuild an index from scratch using entity data.
   */
  rebuild(tableName: string, field: string, entities: Array<{ id: string; value: unknown }>): void {
    const sheet = this.getIndexSheet(tableName, field);
    if (!sheet) return;

    // Clear and rebuild
    sheet.clear();
    sheet.setHeaders(["value", "entityId"]);

    const rows: unknown[][] = [];
    for (const e of entities) {
      if (e.value !== undefined && e.value !== null && e.value !== "") {
        rows.push([String(e.value), e.id]);
      }
    }

    if (rows.length > 0) {
      sheet.appendRows(rows);
    }

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
}
