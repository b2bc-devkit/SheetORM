// SheetORM — Global Registry: singleton managing adapter, repositories, and class map

import {
  Entity,
  FieldDefinition,
  ICacheProvider,
  IndexDefinition,
  ISpreadsheetAdapter,
  TableSchema,
} from "./types";
import { SheetRepository } from "./SheetRepository";
import { IndexStore } from "../index/IndexStore";
import { SchemaMigrator } from "../schema/SchemaMigrator";
import { MemoryCache } from "../utils/cache";
import { GoogleSpreadsheetAdapter } from "../storage/GoogleSheetsAdapter";

export interface RecordStatic {
  new (data?: { [key: string]: unknown }): Entity;
  tableName: string;
  fields: FieldDefinition[];
  indexes: IndexDefinition[];
  name: string;
}

export class Registry {
  private static instance: Registry | null = null;

  private adapter: ISpreadsheetAdapter | null = null;
  private cache: ICacheProvider | null = null;
  private indexStore: IndexStore | null = null;
  private migrator: SchemaMigrator | null = null;
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
    this.migrator = null;
    this.repos.clear();
  }

  private getAdapter(): ISpreadsheetAdapter {
    if (!this.adapter) {
      this.adapter = new GoogleSpreadsheetAdapter();
    }
    return this.adapter;
  }

  private ensureInfrastructure(): {
    indexStore: IndexStore;
    migrator: SchemaMigrator;
  } {
    if (!this.indexStore || !this.migrator) {
      const adapter = this.getAdapter();
      if (!this.cache) this.cache = new MemoryCache();
      this.indexStore = new IndexStore(adapter, this.cache);
      this.migrator = new SchemaMigrator(adapter, this.indexStore);
    }
    return { indexStore: this.indexStore, migrator: this.migrator };
  }

  registerClass(ctor: RecordStatic): void {
    if (!ctor.tableName) {
      throw new Error(`Record subclass "${ctor.name}" must define static tableName`);
    }
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

    const { migrator, indexStore } = this.ensureInfrastructure();

    const schema: TableSchema = {
      tableName,
      fields: ctor.fields ?? [],
      indexes: ctor.indexes ?? [],
    };

    migrator.sync(schema);

    const repo = new SheetRepository<T>(this.getAdapter(), schema, indexStore, this.cache!);

    this.repos.set(tableName, repo as unknown as SheetRepository<Entity>);
    return repo;
  }

  getClassByName(name: string): RecordStatic | undefined {
    return this.classesByName.get(name) ?? this.classesByTable.get(name);
  }

  getMigrator(): SchemaMigrator {
    return this.ensureInfrastructure().migrator;
  }

  getIndexStore(): IndexStore {
    return this.ensureInfrastructure().indexStore;
  }

  clearCache(): void {
    if (this.cache) this.cache.clear();
  }
}
