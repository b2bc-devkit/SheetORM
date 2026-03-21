// SheetORM — Record: ActiveRecord-style base class for entities
// Subclass this to define ORM-managed models with automatic table creation
// Fields are auto-discovered from ESNext class field declarations.
// Use @Indexed() to mark indexed fields, @Field() for type/required overrides.

import type { Entity } from "./types/Entity.js";
import type { FilterOperator } from "./types/FilterOperator.js";
import type { QueryOptions } from "./types/QueryOptions.js";
import type { PaginatedResult } from "./types/PaginatedResult.js";
import type { GroupResult } from "./types/GroupResult.js";
import { Registry } from "./Registry.js";
import type { RecordStatic } from "./RecordStatic.js";
import { Query } from "../query/Query.js";
import { Decorators } from "./Decorators.js";
import type { RecordConstructor } from "./RecordConstructor.js";

type QueryableRecordClass =
  | string
  | {
      new (): Entity;
      tableName: string;
    };

function asCtor(cls: unknown): RecordStatic {
  return cls as unknown as RecordStatic;
}

function toInstances<T extends Record>(ctor: RecordConstructor<T>, entities: Entity[]): T[] {
  const len = entities.length;
  const result: T[] = new Array(len);
  for (let i = 0; i < len; i++) {
    const instance = new ctor();
    Object.assign(instance, entities[i]);
    result[i] = instance;
  }
  return result;
}

export class Record implements Entity {
  declare __id: string;
  declare __createdAt: string | undefined;
  declare __updatedAt: string | undefined;
  [key: string]: unknown;

  static get tableName(): string {
    return "tbl_" + this.name + "s";
  }

  static get indexTableName(): string {
    return "idx_" + this.name + "s";
  }

  // ─── Static Factory ──────────────────────────────

  static create<T extends Record>(this: RecordConstructor<T>, data: { [key: string]: unknown }): T {
    const instance = new this();
    return Object.assign(instance, data);
  }

  // ─── Instance Methods ────────────────────────────

  save(): this {
    const Ctor = this.constructor as unknown as RecordStatic;
    const fields = Decorators.getFields(Ctor);
    const repo = Registry.getInstance().ensureRepository(Ctor);

    const partial: { [key: string]: unknown } = {};
    for (let i = 0; i < fields.length; i++) {
      const name = fields[i].name;
      const val = this[name];
      if (val !== undefined) {
        partial[name] = val;
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
    const fields = Decorators.getFields(Ctor);
    const result: { [key: string]: unknown } = {
      __id: this.__id,
      __createdAt: this.__createdAt,
      __updatedAt: this.__updatedAt,
    };
    for (let i = 0; i < fields.length; i++) {
      const name = fields[i].name;
      result[name] = this[name];
    }
    return result;
  }

  // ─── Static Query Methods ────────────────────────

  static where<T extends Record>(
    this: RecordConstructor<T>,
    field: string,
    operator: FilterOperator,
    value: unknown,
  ): Query<T> {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return new Query<T>(() => toInstances(this, repo.find())).where(field, operator, value);
  }

  static findById<T extends Record>(this: RecordConstructor<T>, id: string): T | null {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    const entity = repo.findById(id);
    if (!entity) return null;
    return Object.assign(new this(), entity);
  }

  static find<T extends Record>(this: RecordConstructor<T>, options?: QueryOptions): T[] {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return toInstances(this, repo.find(options));
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

  static query<T extends Record>(this: RecordConstructor<T>): Query<T> {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return new Query<T>(() => toInstances(this, repo.find()));
  }

  static select<T extends Record>(
    this: RecordConstructor<T>,
    offset: number,
    limit: number,
    options?: QueryOptions,
  ): PaginatedResult<T> {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    const result = repo.select(offset, limit, options);
    return {
      ...result,
      items: toInstances(this, result.items),
    };
  }

  static groupBy<T extends Record>(
    this: RecordConstructor<T>,
    field: string,
    options?: QueryOptions,
  ): GroupResult<T>[] {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    const groups = repo.groupBy(field, options);
    return groups.map((g) => ({
      ...g,
      items: toInstances(this, g.items),
    }));
  }

  static saveAll<T extends Record>(
    this: RecordConstructor<T>,
    items: Array<{ [key: string]: unknown }>,
  ): T[] {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    const entities = repo.saveAll(items as Array<Partial<Entity>>);
    return toInstances(this, entities);
  }
}

// ─── Wire up Query.from() resolver ──────────

Query._setFromResolver((classOrName: QueryableRecordClass) => {
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
  return () => repo.find().map((e: Entity) => Object.assign(new ctor(), e));
});
