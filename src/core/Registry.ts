// SheetORM — Global Registry: singleton managing adapter, repositories, and class map

import type { Entity } from "./types/Entity.js";
import type { ICacheProvider } from "./types/ICacheProvider.js";
import type { ISheetAdapter } from "./types/ISheetAdapter.js";
import type { ISpreadsheetAdapter } from "./types/ISpreadsheetAdapter.js";
import type { TableSchema } from "./types/TableSchema.js";
import { SheetRepository } from "./SheetRepository.js";
import { IndexStore } from "../index/IndexStore.js";
import { MemoryCache } from "./cache/MemoryCache.js";
import { Serialization } from "../utils/Serialization.js";
import { GoogleSpreadsheetAdapter } from "../storage/GoogleSpreadsheetAdapter.js";
import { Decorators } from "./Decorators.js";
import type { RecordStatic } from "./RecordStatic.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

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
    SheetOrmLogger.log(`[Registry] configure (adapter=${options.adapter?.constructor.name ?? "default"})`);
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

  private ensureTable(
    schema: TableSchema,
    indexStore: IndexStore,
  ): { sheet: ISheetAdapter; created: boolean } {
    const adapter = this.getAdapter();
    SheetOrmLogger.log(`[Registry] ensureTable "${schema.tableName}" (indexes=${schema.indexes.length})`);

    let sheet = adapter.getSheetByName(schema.tableName);
    let created = false;
    if (!sheet) {
      sheet = adapter.insertSheet(schema.tableName);
      created = true;
    }
    SheetOrmLogger.log(
      `[Registry] ensureTable "${schema.tableName}" → ${created ? "insertSheet (G4 new)" : "existing sheet"}`,
    );
    sheet.setHeaders(Serialization.buildHeaders(schema.fields));

    if (schema.indexes.length === 0) return { sheet, created };

    if (!schema.indexTableName) {
      throw new Error(
        `Table schema "${schema.tableName}" defines indexes but has no indexTableName. ` +
          "Legacy per-field _idx_* indexes were removed; use a combined index table name (e.g. idx_ClassName).",
      );
    }

    indexStore.createCombinedIndex(schema.indexTableName);
    for (const idx of schema.indexes) {
      indexStore.registerIndex(schema.indexTableName, idx.field, idx.unique ?? false);
    }

    return { sheet, created };
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
      SheetOrmLogger.log(`[Registry] ensureRepository "${tableName}" → cache hit`);
      return this.repos.get(tableName) as unknown as SheetRepository<T>;
    }

    SheetOrmLogger.log(`[Registry] ensureRepository "${tableName}" → creating`);

    this.registerClass(ctor);

    const indexStore = this.ensureIndexStore();

    const schema: TableSchema = {
      tableName,
      indexTableName: ctor.indexTableName,
      fields: Decorators.getFields(ctor),
      indexes: Decorators.getIndexes(ctor),
    };

    const { sheet, created } = this.ensureTable(schema, indexStore);

    const repo = new SheetRepository<T>(
      this.getAdapter(),
      schema,
      indexStore,
      this.cache!,
      undefined,
      sheet,
      created ? 0 : undefined,
    );

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
