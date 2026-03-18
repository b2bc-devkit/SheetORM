// SheetORM — Record: ActiveRecord-style base class for entities
// Subclass this to define ORM-managed models with automatic table creation

import {
  Entity,
  FieldDefinition,
  FilterOperator,
  IndexDefinition,
  QueryOptions,
  PaginatedResult,
  GroupResult,
} from "./types";
import { Registry, RecordStatic } from "./Registry";
import { QueryBuilder } from "../query/QueryBuilder";

export interface RecordConstructor<T extends Record = Record> {
  new (data?: { [key: string]: unknown }): T;
  tableName: string;
  fields: FieldDefinition[];
  indexes: IndexDefinition[];
}

function asCtor(cls: unknown): RecordStatic {
  return cls as unknown as RecordStatic;
}

function toInstances<T extends Record>(ctor: RecordConstructor<T>, entities: Entity[]): T[] {
  return entities.map((e) => {
    const instance = new ctor();
    Object.assign(instance, e);
    return instance;
  });
}

export class Record implements Entity {
  __id!: string;
  __createdAt?: string;
  __updatedAt?: string;
  [key: string]: unknown;

  static tableName: string;
  static fields: FieldDefinition[] = [];
  static indexes: IndexDefinition[] = [];

  constructor(data?: { [key: string]: unknown }) {
    if (data) {
      Object.assign(this, data);
    }
  }

  // ─── Instance Methods ────────────────────────────

  save(): this {
    const Ctor = this.constructor as unknown as RecordStatic;
    const repo = Registry.getInstance().ensureRepository(Ctor);

    const partial: { [key: string]: unknown } = {};
    for (const field of Ctor.fields) {
      if (this[field.name] !== undefined) {
        partial[field.name] = this[field.name];
      }
    }
    if (this.__id) partial.__id = this.__id;

    const saved = repo.save(partial as Partial<Entity>);
    Object.assign(this, saved);
    return this;
  }

  delete(): boolean {
    if (!this.__id) return false;
    const Ctor = this.constructor as unknown as RecordStatic;
    const repo = Registry.getInstance().ensureRepository(Ctor);
    return repo.delete(this.__id);
  }

  set(field: string, value: unknown): this {
    this[field] = value;
    return this;
  }

  get(field: string): unknown {
    return this[field];
  }

  toJSON(): { [key: string]: unknown } {
    const Ctor = this.constructor as unknown as RecordStatic;
    const result: { [key: string]: unknown } = {
      __id: this.__id,
      __createdAt: this.__createdAt,
      __updatedAt: this.__updatedAt,
    };
    for (const field of Ctor.fields) {
      result[field.name] = this[field.name];
    }
    return result;
  }

  // ─── Static Query Methods ────────────────────────

  static where<T extends Record>(
    this: RecordConstructor<T>,
    field: string,
    operator: FilterOperator,
    value: unknown,
  ): QueryBuilder<T> {
    const ctor = this;
    const repo = Registry.getInstance().ensureRepository(asCtor(ctor));
    return new QueryBuilder<T>(() => toInstances(ctor, repo.find())).where(field, operator, value);
  }

  static findById<T extends Record>(this: RecordConstructor<T>, id: string): T | null {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    const entity = repo.findById(id);
    if (!entity) return null;
    return Object.assign(new this(), entity);
  }

  static find<T extends Record>(this: RecordConstructor<T>, options?: QueryOptions): T[] {
    const ctor = this;
    const repo = Registry.getInstance().ensureRepository(asCtor(ctor));
    return toInstances(ctor, repo.find(options));
  }

  static findOne<T extends Record>(this: RecordConstructor<T>, options?: QueryOptions): T | null {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    const entity = repo.findOne(options);
    if (!entity) return null;
    return Object.assign(new this(), entity);
  }

  static count(options?: QueryOptions): number {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return repo.count(options);
  }

  static deleteAll(options?: QueryOptions): number {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return repo.deleteAll(options);
  }

  static query<T extends Record>(this: RecordConstructor<T>): QueryBuilder<T> {
    const ctor = this;
    const repo = Registry.getInstance().ensureRepository(asCtor(ctor));
    return new QueryBuilder<T>(() => toInstances(ctor, repo.find()));
  }

  static select<T extends Record>(
    this: RecordConstructor<T>,
    offset: number,
    limit: number,
    options?: QueryOptions,
  ): PaginatedResult<T> {
    const ctor = this;
    const repo = Registry.getInstance().ensureRepository(asCtor(ctor));
    const result = repo.select(offset, limit, options);
    return {
      ...result,
      items: toInstances(ctor, result.items),
    };
  }

  static groupBy<T extends Record>(
    this: RecordConstructor<T>,
    field: string,
    options?: QueryOptions,
  ): GroupResult<T>[] {
    const ctor = this;
    const repo = Registry.getInstance().ensureRepository(asCtor(ctor));
    const groups = repo.groupBy(field, options);
    return groups.map((g) => ({
      ...g,
      items: toInstances(ctor, g.items),
    }));
  }
}

// ─── Wire up QueryBuilder.from() resolver ──────────

QueryBuilder._setFromResolver(
  (
    classOrName:
      | string
      | {
          new (data?: { [key: string]: unknown }): Entity;
          tableName: string;
          fields: FieldDefinition[];
          indexes: IndexDefinition[];
        },
  ) => {
    const registry = Registry.getInstance();
    let ctor: RecordStatic;

    if (typeof classOrName === "string") {
      const found = registry.getClassByName(classOrName);
      if (!found) {
        throw new Error(`Record class "${classOrName}" not found. Ensure the class has been imported.`);
      }
      ctor = found;
    } else {
      ctor = classOrName as unknown as RecordStatic;
      registry.registerClass(ctor);
    }

    const repo = registry.ensureRepository(ctor);
    return () => repo.find().map((e: Entity) => Object.assign(new (ctor as any)(), e));
  },
);
