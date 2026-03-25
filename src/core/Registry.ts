// SheetORM — Global Registry: singleton managing adapter, repositories, and class map

import type { Entity } from "./types/Entity.js";
import type { ICacheProvider } from "./types/ICacheProvider.js";
import type { ISpreadsheetAdapter } from "./types/ISpreadsheetAdapter.js";
import type { TableSchema } from "./types/TableSchema.js";
import { SheetRepository } from "./SheetRepository.js";
import { IndexStore } from "../index/IndexStore.js";
import { MemoryCache } from "./cache/MemoryCache.js";
import { Serialization } from "../utils/Serialization.js";
import { GoogleSpreadsheetAdapter } from "../storage/GoogleSpreadsheetAdapter.js";
import { Decorators } from "./Decorators.js";
import type { RecordStatic } from "./RecordStatic.js";

export class Registry {
  private static instance: Registry | null = null;

  private adapter: ISpreadsheetAdapter | null = null;
  private cache: ICacheProvider | null = null;
  private indexStore: IndexStore | null = null;
  private repos = new Map<string, SheetRepository<Entity>>();
  private classesByTable = new Map<string, RecordStatic>();
  private classesByName = new Map<string, RecordStatic>();

  static getInstance(): Registry {
    if (!Registry.instance) {
      Registry.instance = new Registry();
    }
    return Registry.instance;
  }

  static reset(): void {
    Registry.instance = null;
  }

  configure(options: { adapter?: ISpreadsheetAdapter; cache?: ICacheProvider }): void {
    this.adapter = options.adapter ?? null;
    this.cache = options.cache ?? null;
    this.indexStore = null;
    this.repos.clear();
    this.classesByTable.clear();
    this.classesByName.clear();
  }

  private getAdapter(): ISpreadsheetAdapter {
    if (!this.adapter) {
      this.adapter = new GoogleSpreadsheetAdapter();
    }
    return this.adapter;
  }

  private ensureIndexStore(): IndexStore {
    if (!this.indexStore) {
      const adapter = this.getAdapter();
      if (!this.cache) this.cache = new MemoryCache();
      // IndexStore gets its own cache instance so that invalidateCache() → cache.clear()
      // does NOT contaminate the entity data cache used by SheetRepository.
      this.indexStore = new IndexStore(adapter, new MemoryCache());
    }
    return this.indexStore;
  }

  private ensureTable(schema: TableSchema, indexStore: IndexStore): void {
    const adapter = this.getAdapter();

    let sheet = adapter.getSheetByName(schema.tableName);
    if (!sheet) {
      sheet = adapter.createSheet(schema.tableName);
    }
    sheet.setHeaders(Serialization.buildHeaders(schema.fields));

    if (schema.indexes.length === 0) return;

    if (!schema.indexTableName) {
      throw new Error(
        `Table schema "${schema.tableName}" defines indexes but has no indexTableName. ` +
          "Legacy per-field _idx_* indexes were removed; use a combined index table name (e.g. idx_ClassName).",
      );
    }

    // Combined index sheet: one sheet (idx_ClassName) holds all indexed fields
    indexStore.createCombinedIndex(schema.indexTableName);
    for (const idx of schema.indexes) {
      indexStore.registerIndex(schema.indexTableName, idx.field, idx.unique ?? false);
    }
  }

  registerClass(ctor: RecordStatic): void {
    if (!this.classesByTable.has(ctor.tableName)) {
      this.classesByTable.set(ctor.tableName, ctor);
    }
    if (ctor.name && !this.classesByName.has(ctor.name)) {
      this.classesByName.set(ctor.name, ctor);
    }
  }

  ensureRepository<T extends Entity>(ctor: RecordStatic): SheetRepository<T> {
    const tableName = ctor.tableName;

    if (this.repos.has(tableName)) {
      return this.repos.get(tableName) as unknown as SheetRepository<T>;
    }

    this.registerClass(ctor);

    const indexStore = this.ensureIndexStore();

    const schema: TableSchema = {
      tableName,
      indexTableName: ctor.indexTableName,
      fields: Decorators.getFields(ctor),
      indexes: Decorators.getIndexes(ctor),
    };

    this.ensureTable(schema, indexStore);

    const repo = new SheetRepository<T>(this.getAdapter(), schema, indexStore, this.cache!);

    this.repos.set(tableName, repo as unknown as SheetRepository<Entity>);
    return repo;
  }

  getClassByName(name: string): RecordStatic | undefined {
    return this.classesByName.get(name) ?? this.classesByTable.get(name);
  }

  getIndexStore(): IndexStore {
    return this.ensureIndexStore();
  }

  clearCache(): void {
    if (this.cache) this.cache.clear();
    if (this.indexStore) this.indexStore.clearAllCaches();
  }
}
