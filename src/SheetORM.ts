// SheetORM — Main facade class providing a unified entry point to the ORM

import {
  Entity,
  ISpreadsheetAdapter,
  ICacheProvider,
  TableSchema,
  LifecycleHooks,
} from './core/types';
import { SheetRepository } from './core/SheetRepository';
import { IndexStore } from './index/IndexStore';
import { SchemaMigrator } from './schema/SchemaMigrator';
import { MemoryCache } from './utils/cache';
import { GoogleSpreadsheetAdapter } from './storage/GoogleSheetsAdapter';

export interface SheetORMOptions {
  adapter?: ISpreadsheetAdapter;
  cache?: ICacheProvider;
  cacheTtlMs?: number;
}

export class SheetORM {
  private adapter: ISpreadsheetAdapter;
  private cache: ICacheProvider;
  private indexStore: IndexStore;
  private migrator: SchemaMigrator;
  private schemas = new Map<string, TableSchema>();
  private repositories = new Map<string, SheetRepository<Entity>>();

  constructor(options?: SheetORMOptions) {
    this.adapter = options?.adapter ?? new GoogleSpreadsheetAdapter();
    this.cache = options?.cache ?? new MemoryCache(options?.cacheTtlMs ?? 60_000);
    this.indexStore = new IndexStore(this.adapter, this.cache);
    this.migrator = new SchemaMigrator(this.adapter, this.indexStore);
  }

  /**
   * Factory: create a SheetORM instance.
   */
  static create(options?: SheetORMOptions): SheetORM {
    return new SheetORM(options);
  }

  /**
   * Register a table schema. Initializes the sheet and indexes.
   */
  register(schema: TableSchema): void {
    this.migrator.sync(schema);
    this.schemas.set(schema.tableName, schema);
  }

  /**
   * Get a typed repository for a registered table.
   */
  getRepository<T extends Entity>(
    tableName: string,
    hooks?: LifecycleHooks<T>,
  ): SheetRepository<T> {
    const schema = this.schemas.get(tableName);
    if (!schema) {
      throw new Error(
        `Table "${tableName}" is not registered. Call register() first.`,
      );
    }

    const cacheKey = tableName;
    if (!hooks && this.repositories.has(cacheKey)) {
      return this.repositories.get(cacheKey) as unknown as SheetRepository<T>;
    }

    const repo = new SheetRepository<T>(
      this.adapter,
      schema,
      this.indexStore,
      this.cache,
      hooks,
    );

    if (!hooks) {
      this.repositories.set(cacheKey, repo as unknown as SheetRepository<Entity>);
    }

    return repo;
  }

  /**
   * Get the schema migrator.
   */
  getMigrator(): SchemaMigrator {
    return this.migrator;
  }

  /**
   * Get the index store.
   */
  getIndexStore(): IndexStore {
    return this.indexStore;
  }

  /**
   * Clear all caches.
   */
  clearCache(): void {
    this.cache.clear();
  }
}
