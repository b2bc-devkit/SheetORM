/**
 * ActiveRecord base class for ORM-managed models.
 *
 * Subclass `Record` to define entities stored in Google Sheets.  Fields
 * are auto-discovered from ESNext class field declarations at registration
 * time via the decorator metadata system ({@link Decorators}).
 *
 * Follows the **ActiveRecord** pattern — each instance knows how to
 * `save()` and `delete()` itself, while static methods like `find()`,
 * `where()`, and `query()` provide collection-level operations.
 *
 * @example
 * ```ts
 * class Car extends Record {
 *   brand = "";
 *   year  = 0;
 * }
 * const car = Car.create({ brand: "Toyota", year: 2024 }).save();
 * ```
 *
 * @module Record
 */

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

/**
 * Union type accepted by Query.from():  either a class name string or a
 * class constructor with a `tableName` property.
 */
type QueryableRecordClass =
  | string
  | {
      new (): Entity;
      tableName: string;
    };

/**
 * Cast an unknown class reference to the internal {@link RecordStatic}
 * shape so that `tableName`, `indexTableName`, etc. are available.
 */
function asCtor(cls: unknown): RecordStatic {
  return cls as unknown as RecordStatic;
}

/**
 * Hydrate raw Entity objects into fully-typed Record instances.
 *
 * Allocates the result array up-front and uses `Object.assign` to copy
 * entity properties onto each freshly-constructed instance.
 *
 * @param ctor     - The concrete Record subclass constructor.
 * @param entities - Raw entity rows returned by SheetRepository.
 * @returns An array of hydrated Record subclass instances.
 */
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

/**
 * Base class for all SheetORM models.
 *
 * Provides both instance-level persistence (`save`, `delete`) and
 * static query helpers (`find`, `where`, `query`, `select`, `groupBy`,
 * `saveAll`).  System columns (`__id`, `__createdAt`, `__updatedAt`)
 * are declared here so every subclass inherits them.
 */
export class Record implements Entity {
  /** UUID primary key, assigned automatically on first save. */
  declare __id: string;
  /** ISO-8601 creation timestamp, set by SheetRepository on insert. */
  declare __createdAt: string | undefined;
  /** ISO-8601 last-update timestamp, set by SheetRepository on every save. */
  declare __updatedAt: string | undefined;
  /** Index signature allowing arbitrary field access by name. */
  [key: string]: unknown;

  /**
   * Convention-based table name derived from the class name.
   * e.g. class `Car` → `"tbl_Cars"`.
   */
  static get tableName(): string {
    return "tbl_" + this.name + "s";
  }

  /**
   * Convention-based combined index table name.
   * e.g. class `Car` → `"idx_Cars"`.
   */
  static get indexTableName(): string {
    return "idx_" + this.name + "s";
  }

  /**
   * Whether the auto-created sheet should be protected.
   * Override in a subclass to return `true` and enable sheet protection.
   */
  static isProtected(): boolean {
    return false;
  }

  /**
   * Email addresses of editors allowed to edit the protected sheet.
   * Only used when `isProtected()` returns `true`.
   * Override in a subclass to specify allowed editors.
   */
  static protectedFor(): string[] {
    return [];
  }

  // ─── Static Factory ──────────────────────────────

  /**
   * Create an in-memory instance pre-populated with the given data.
   * The instance is **not** persisted until `save()` is called.
   *
   * @param data - Plain object whose keys map to field names.
   * @returns A new, unsaved Record subclass instance.
   */
  static create<T extends Record>(this: RecordConstructor<T>, data: { [key: string]: unknown }): T {
    const instance = new this();
    return Object.assign(instance, data);
  }

  // ─── Instance Methods ────────────────────────────

  /**
   * Persist this instance to the underlying Google Sheet.
   *
   * On first save (no `__id`) a new row is inserted; on subsequent
   * saves the existing row is updated in-place.  Only fields declared
   * via decorators (or auto-discovered) are written; dynamic index-
   * signature properties are ignored.
   *
   * @returns `this` for fluent chaining.
   */
  save(): this {
    const Ctor = this.constructor as unknown as RecordStatic;
    const fields = Decorators.getFields(Ctor);
    const repo = Registry.getInstance().ensureRepository(Ctor);

    // Build a partial entity containing only declared fields with defined values
    const partial: { [key: string]: unknown } = {};
    for (let i = 0; i < fields.length; i++) {
      const name = fields[i].name;
      const val = this[name];
      if (val !== undefined) {
        partial[name] = val;
      }
    }
    if (this.__id) partial.__id = this.__id;

    // Delegate to the repository and hydrate the returned entity (with system columns)
    const saved = repo.save(partial as Partial<Entity>);
    Object.assign(this, saved);
    return this;
  }

  /**
   * Delete this instance's row from the sheet.
   * @returns `true` if the row was found and deleted, `false` if `__id` is missing.
   */
  delete(): boolean {
    if (!this.__id) return false;
    const Ctor = this.constructor as unknown as RecordStatic;
    const repo = Registry.getInstance().ensureRepository(Ctor);
    return repo.delete(this.__id);
  }

  /**
   * Fluent setter — updates a single field and returns `this`.
   * @param field - The field name to set.
   * @param value - The new value.
   */
  set(field: string, value: unknown): this {
    this[field] = value;
    return this;
  }

  /**
   * Read a single field value by name.
   * @param field - The field name to read.
   */
  get(field: string): unknown {
    return this[field];
  }

  /**
   * Serialise this Record to a plain object.
   *
   * Only includes declared class fields (from decorator metadata) plus
   * system columns.  Dynamic properties set via the index signature are
   * intentionally excluded to keep JSON output predictable.
   */
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

  /**
   * Start a fluent query chain with an initial filter.
   * Equivalent to `Record.query().where(field, operator, value)`.
   */
  static where<T extends Record>(
    this: RecordConstructor<T>,
    field: string,
    operator: FilterOperator,
    value: unknown,
  ): Query<T> {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return new Query<T>(() => toInstances(this, repo.find())).where(field, operator, value);
  }

  /**
   * Look up a single record by its UUID primary key.
   * Uses the repository's fast-path cache when available.
   *
   * @returns The hydrated instance, or `null` if not found.
   */
  static findById<T extends Record>(this: RecordConstructor<T>, id: string): T | null {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    const entity = repo.findById(id);
    if (!entity) return null;
    return Object.assign(new this(), entity);
  }

  /**
   * Return all records matching the optional query options.
   * Without options, returns every record in the table.
   */
  static find<T extends Record>(this: RecordConstructor<T>, options?: QueryOptions): T[] {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return toInstances(this, repo.find(options));
  }

  /**
   * Return the first record matching the query options, or `null`.
   */
  static findOne<T extends Record>(this: RecordConstructor<T>, options?: QueryOptions): T | null {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    const entity = repo.findOne(options);
    if (!entity) return null;
    return Object.assign(new this(), entity);
  }

  /** Count records matching the optional query options. */
  static count(options?: QueryOptions): number {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return repo.count(options);
  }

  /**
   * Delete all records matching the query options.
   * @returns The number of records deleted.
   */
  static deleteAll(options?: QueryOptions): number {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return repo.deleteAll(options);
  }

  /**
   * Start an empty fluent query chain.
   * Call `.where()`, `.orderBy()`, `.limit()`, etc. to build the query.
   */
  static query<T extends Record>(this: RecordConstructor<T>): Query<T> {
    const repo = Registry.getInstance().ensureRepository(asCtor(this));
    return new Query<T>(() => toInstances(this, repo.find()));
  }

  /**
   * Return a paginated subset of records.
   *
   * @param offset  - Number of records to skip (0-based).
   * @param limit   - Maximum number of records to return.
   * @param options - Optional filters/sorting.
   * @returns A {@link PaginatedResult} with `items`, `total`, `offset`, `limit`.
   */
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

  /**
   * Group records by a field value.
   *
   * @param field   - The field to group by.
   * @param options - Optional pre-grouping filters.
   * @returns An array of {@link GroupResult}, each with `key` and `items`.
   */
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

  /**
   * Batch-save multiple records in a single operation.
   *
   * Delegates to `SheetRepository.saveAll()` which uses entity-batch and
   * index-batch optimisations to minimise GAS API calls.
   *
   * @param items - Array of plain data objects.
   * @returns Array of hydrated, persisted Record instances.
   */
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
// This glue code teaches Query.from(ClassName) how to locate the correct
// Registry entry and build a data-loader function without creating a
// circular dependency between Query and Record at import time.

Query._setFromResolver((classOrName: QueryableRecordClass) => {
  const registry = Registry.getInstance();
  let ctor: RecordStatic;

  if (typeof classOrName === "string") {
    // Look up by class name — the class must have been imported/registered first
    const found = registry.getClassByName(classOrName);
    if (!found) {
      throw new Error(`Record class "${classOrName}" not found. Ensure the class has been imported.`);
    }
    ctor = found;
  } else {
    // Class reference passed directly — ensure it's registered in the Registry
    ctor = classOrName as unknown as RecordStatic;
    registry.registerClass(ctor);
  }

  // Return a factory function that loads all entities and hydrates them
  const repo = registry.ensureRepository(ctor);
  return () => repo.find().map((e: Entity) => Object.assign(new ctor(), e));
});
