// SheetORM — SchemaMigrator: manages table schemas stored in a _meta sheet

import {
  ISpreadsheetAdapter,
  TableSchema,
  FieldDefinition,
  META_TABLE_NAME,
} from '../core/types';
import { buildHeaders } from '../utils/serialization';
import { IndexStore } from '../index/IndexStore';

/**
 * _meta sheet layout:
 *   Row 1 (headers): ["tableName", "schemaJson", "version"]
 *   Rows 2+: [tableName, JSON-serialized schema, version number]
 */

export class SchemaMigrator {
  private adapter: ISpreadsheetAdapter;
  private indexStore: IndexStore;

  constructor(adapter: ISpreadsheetAdapter, indexStore: IndexStore) {
    this.adapter = adapter;
    this.indexStore = indexStore;
  }

  /**
   * Initialize the _meta sheet if it doesn't exist.
   */
  private ensureMetaSheet(): void {
    let meta = this.adapter.getSheetByName(META_TABLE_NAME);
    if (!meta) {
      meta = this.adapter.createSheet(META_TABLE_NAME);
      meta.setHeaders(['tableName', 'schemaJson', 'version']);
    }
  }

  /**
   * Initialize a table: create its sheet with headers and register schema in _meta.
   */
  initialize(schema: TableSchema): void {
    this.ensureMetaSheet();

    // Create data sheet
    let sheet = this.adapter.getSheetByName(schema.tableName);
    if (!sheet) {
      sheet = this.adapter.createSheet(schema.tableName);
    }
    const headers = buildHeaders(schema.fields);
    sheet.setHeaders(headers);

    // Create indexes
    for (const idx of schema.indexes) {
      this.indexStore.createIndex(schema.tableName, idx.field, {
        unique: idx.unique,
      });
      this.indexStore.registerIndex(schema.tableName, idx.field, idx.unique ?? false);
    }

    // Store schema in _meta
    this.saveSchemaToMeta(schema, 1);
  }

  /**
   * Add a field (column) to an existing table.
   */
  addField(tableName: string, field: FieldDefinition): void {
    const schema = this.getSchema(tableName);
    if (!schema) {
      throw new Error(`Table "${tableName}" not found in _meta`);
    }

    if (schema.fields.some((f) => f.name === field.name)) {
      return; // Field already exists
    }

    schema.fields.push(field);

    // Update sheet headers
    const sheet = this.adapter.getSheetByName(tableName);
    if (sheet) {
      const headers = buildHeaders(schema.fields);
      sheet.setHeaders(headers);
    }

    // Bump version
    this.saveSchemaToMeta(schema, this.getVersion(tableName) + 1);
  }

  /**
   * Remove a field (column) from a table schema.
   * Note: does not remove data from existing rows (sheets don't easily support column deletion).
   */
  removeField(tableName: string, fieldName: string): void {
    const schema = this.getSchema(tableName);
    if (!schema) {
      throw new Error(`Table "${tableName}" not found in _meta`);
    }

    schema.fields = schema.fields.filter((f) => f.name !== fieldName);
    this.saveSchemaToMeta(schema, this.getVersion(tableName) + 1);
  }

  /**
   * Get the current schema for a table from _meta.
   */
  getSchema(tableName: string): TableSchema | null {
    this.ensureMetaSheet();
    const meta = this.adapter.getSheetByName(META_TABLE_NAME);
    if (!meta) return null;

    const data = meta.getAllData();
    for (const row of data) {
      if (String(row[0]) === tableName) {
        try {
          return JSON.parse(String(row[1])) as TableSchema;
        } catch {
          return null;
        }
      }
    }
    return null;
  }

  /**
   * Check if a table exists in _meta.
   */
  tableExists(tableName: string): boolean {
    return this.getSchema(tableName) !== null;
  }

  /**
   * Sync schema: add missing columns from the schema definition to the sheet.
   */
  sync(schema: TableSchema): void {
    const existing = this.getSchema(schema.tableName);
    if (!existing) {
      this.initialize(schema);
      return;
    }

    // Add missing fields
    for (const field of schema.fields) {
      if (!existing.fields.some((f) => f.name === field.name)) {
        this.addField(schema.tableName, field);
      }
    }

    // Add missing indexes
    for (const idx of schema.indexes) {
      if (!this.indexStore.exists(schema.tableName, idx.field)) {
        this.indexStore.createIndex(schema.tableName, idx.field, {
          unique: idx.unique,
        });
        this.indexStore.registerIndex(schema.tableName, idx.field, idx.unique ?? false);
      }
    }
  }

  private getVersion(tableName: string): number {
    this.ensureMetaSheet();
    const meta = this.adapter.getSheetByName(META_TABLE_NAME);
    if (!meta) return 0;

    const data = meta.getAllData();
    for (const row of data) {
      if (String(row[0]) === tableName) {
        return Number(row[2]) || 0;
      }
    }
    return 0;
  }

  private saveSchemaToMeta(schema: TableSchema, version: number): void {
    const meta = this.adapter.getSheetByName(META_TABLE_NAME);
    if (!meta) return;

    const data = meta.getAllData();
    const schemaJson = JSON.stringify(schema);
    const existingRowIndex = data.findIndex((row) => String(row[0]) === schema.tableName);

    if (existingRowIndex >= 0) {
      meta.updateRow(existingRowIndex, [schema.tableName, schemaJson, version]);
    } else {
      meta.appendRow([schema.tableName, schemaJson, version]);
    }
  }
}
