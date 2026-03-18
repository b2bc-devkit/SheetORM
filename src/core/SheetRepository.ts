// SheetORM — SheetRepository: main CRUD + query interface for a single entity type
// Inspired by the Repository pattern from common ORM architectures

import {
  Entity,
  ISpreadsheetAdapter,
  ISheetAdapter,
  TableSchema,
  QueryOptions,
  PaginatedResult,
  GroupResult,
  LifecycleHooks,
  ICacheProvider,
  SYSTEM_COLUMNS,
} from '../core/types';
import { generateUUID } from '../utils/uuid';
import { buildHeaders, entityToRow, rowToEntity } from '../utils/serialization';
import { IndexStore } from '../index/IndexStore';
import { QueryBuilder } from '../query/QueryBuilder';
import {
  executeQuery,
  filterEntities,
  sortEntities,
  paginateEntities,
  groupEntities,
} from '../query/QueryEngine';

export class SheetRepository<T extends Entity> {
  private adapter: ISpreadsheetAdapter;
  private schema: TableSchema;
  private indexStore: IndexStore;
  private cache: ICacheProvider | null;
  private hooks: LifecycleHooks<T>;
  private headers: string[];
  private batchBuffer: Array<{ type: 'save' | 'delete'; data: unknown }> | null = null;

  constructor(
    adapter: ISpreadsheetAdapter,
    schema: TableSchema,
    indexStore: IndexStore,
    cache?: ICacheProvider,
    hooks?: LifecycleHooks<T>,
  ) {
    this.adapter = adapter;
    this.schema = schema;
    this.indexStore = indexStore;
    this.cache = cache ?? null;
    this.hooks = hooks ?? {};
    this.headers = buildHeaders(schema.fields);
  }

  // ─── CRUD ──────────────────────────────────────────

  /**
   * Save (create or update) an entity. If __id is present and exists, updates. Otherwise creates.
   */
  save(partial: Partial<T> & { __id?: string }): T {
    if (this.batchBuffer) {
      this.batchBuffer.push({ type: 'save', data: partial });
      // Return a placeholder (will be committed in batch)
      const now = new Date().toISOString();
      return {
        ...partial,
        __id: partial.__id ?? generateUUID(),
        __createdAt: now,
        __updatedAt: now,
      } as T;
    }

    return this.doSave(partial);
  }

  private doSave(partial: Partial<T> & { __id?: string }): T {
    const sheet = this.getSheet();
    const now = new Date().toISOString();
    const isNew = !partial.__id || !this.rowIndexById(sheet, partial.__id);

    // Lifecycle: validate
    if (this.hooks.onValidate) {
      const errors = this.hooks.onValidate(partial as Partial<T>);
      if (errors && errors.length > 0) {
        throw new Error(`Validation failed: ${errors.join(', ')}`);
      }
    }

    // Lifecycle: beforeSave
    let entityData = partial;
    if (this.hooks.beforeSave) {
      const result = this.hooks.beforeSave(partial as Partial<T>, isNew);
      if (result) entityData = result as Partial<T> & { __id?: string };
    }

    // Apply defaults for required fields
    for (const field of this.schema.fields) {
      if (entityData[field.name] === undefined && field.defaultValue !== undefined) {
        (entityData as Record<string, unknown>)[field.name] = field.defaultValue;
      }
    }

    // Validate required fields
    for (const field of this.schema.fields) {
      if (
        field.required &&
        (entityData[field.name] === undefined || entityData[field.name] === null || entityData[field.name] === '')
      ) {
        throw new Error(`Required field "${field.name}" is missing for table "${this.schema.tableName}"`);
      }
    }

    let entity: T;

    if (isNew) {
      // CREATE
      entity = {
        ...entityData,
        __id: entityData.__id ?? generateUUID(),
        __createdAt: now,
        __updatedAt: now,
      } as T;

      const row = entityToRow(entity, this.schema.fields, this.headers);
      sheet.appendRow(row);

      // Add to indexes
      this.addToIndexes(entity);
    } else {
      // UPDATE
      const existingIdx = this.rowIndexById(sheet, partial.__id!)!;
      const existingRow = sheet.getRow(existingIdx);
      const existingEntity = rowToEntity<T>(existingRow, this.headers, this.schema.fields);

      entity = {
        ...existingEntity,
        ...entityData,
        __id: existingEntity.__id,
        __createdAt: existingEntity.__createdAt,
        __updatedAt: now,
      } as T;

      const row = entityToRow(entity, this.schema.fields, this.headers);
      sheet.updateRow(existingIdx, row);

      // Update indexes
      const oldValues: Record<string, unknown> = {};
      const newValues: Record<string, unknown> = {};
      for (const field of this.schema.fields) {
        oldValues[field.name] = existingEntity[field.name];
        newValues[field.name] = entity[field.name];
      }
      this.indexStore.updateForEntity(this.schema.tableName, entity.__id, oldValues, newValues);
    }

    // Invalidate cache
    this.invalidateDataCache();

    // Lifecycle: afterSave
    if (this.hooks.afterSave) {
      this.hooks.afterSave(entity, isNew);
    }

    return entity;
  }

  /**
   * Save multiple entities in batch.
   */
  saveAll(entities: Array<Partial<T>>): T[] {
    return entities.map((e) => this.save(e));
  }

  /**
   * Find an entity by ID.
   */
  findById(id: string): T | null {
    const all = this.loadAllEntities();
    return all.find((e) => e.__id === id) ?? null;
  }

  /**
   * Find entities matching query options.
   */
  find(options?: QueryOptions): T[] {
    const all = this.loadAllEntities();
    if (!options) return all;
    return executeQuery(all, options);
  }

  /**
   * Find the first entity matching query options.
   */
  findOne(options?: QueryOptions): T | null {
    const opts: QueryOptions = { ...options, limit: 1 };
    const results = this.find(opts);
    return results.length > 0 ? results[0] : null;
  }

  /**
   * Delete an entity by ID. Returns true if found and deleted.
   */
  delete(id: string): boolean {
    if (this.batchBuffer) {
      this.batchBuffer.push({ type: 'delete', data: id });
      return true;
    }

    return this.doDelete(id);
  }

  private doDelete(id: string): boolean {
    // Lifecycle: beforeDelete
    if (this.hooks.beforeDelete) {
      const result = this.hooks.beforeDelete(id);
      if (result === false) return false;
    }

    const sheet = this.getSheet();
    const rowIdx = this.rowIndexById(sheet, id);
    if (rowIdx === null) return false;

    // Remove from indexes
    this.indexStore.removeAllForEntity(this.schema.tableName, id);

    sheet.deleteRow(rowIdx);
    this.invalidateDataCache();

    // Lifecycle: afterDelete
    if (this.hooks.afterDelete) {
      this.hooks.afterDelete(id);
    }

    return true;
  }

  /**
   * Delete all entities matching a query.
   */
  deleteAll(options?: QueryOptions): number {
    const entities = this.find(options);
    let count = 0;
    for (const entity of entities) {
      if (this.delete(entity.__id)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Count entities matching a query.
   */
  count(options?: QueryOptions): number {
    const all = this.loadAllEntities();
    if (!options || !options.where) return all.length;
    return filterEntities(all, options.where).length;
  }

  /**
   * Paginated select.
   */
  select(offset: number, limit: number, options?: QueryOptions): PaginatedResult<T> {
    let entities = this.loadAllEntities();

    if (options?.where) {
      entities = filterEntities(entities, options.where);
    }
    if (options?.orderBy) {
      entities = sortEntities(entities, options.orderBy);
    }

    return paginateEntities(entities, offset, limit);
  }

  /**
   * Group entities by a field.
   */
  groupBy(field: string, options?: QueryOptions): GroupResult<T>[] {
    let entities = this.loadAllEntities();

    if (options?.where) {
      entities = filterEntities(entities, options.where);
    }
    if (options?.orderBy) {
      entities = sortEntities(entities, options.orderBy);
    }

    return groupEntities(entities, field);
  }

  /**
   * Create a fluent query builder for this repository.
   */
  query(): QueryBuilder<T> {
    return new QueryBuilder<T>(() => this.loadAllEntities());
  }

  // ─── Batch Operations ──────────────────────────────

  /**
   * Start buffering operations for batch commit.
   */
  beginBatch(): void {
    this.batchBuffer = [];
  }

  /**
   * Commit all buffered operations.
   */
  commitBatch(): void {
    if (!this.batchBuffer) return;
    const buffer = this.batchBuffer;
    this.batchBuffer = null;

    for (const op of buffer) {
      if (op.type === 'save') {
        this.doSave(op.data as Partial<T> & { __id?: string });
      } else if (op.type === 'delete') {
        this.doDelete(op.data as string);
      }
    }
  }

  /**
   * Discard buffered operations.
   */
  rollbackBatch(): void {
    this.batchBuffer = null;
  }

  /**
   * Check if batch mode is active.
   */
  isBatchActive(): boolean {
    return this.batchBuffer !== null;
  }

  // ─── Internal Helpers ──────────────────────────────

  private getSheet(): ISheetAdapter {
    const sheet = this.adapter.getSheetByName(this.schema.tableName);
    if (!sheet) {
      throw new Error(`Sheet "${this.schema.tableName}" not found. Did you call initialize()?`);
    }
    return sheet;
  }

  /**
   * Load all entities from the sheet, with caching.
   */
  private loadAllEntities(): T[] {
    const cacheKey = `data:${this.schema.tableName}`;
    if (this.cache) {
      const cached = this.cache.get<T[]>(cacheKey);
      if (cached !== null) return cached;
    }

    const sheet = this.getSheet();
    const data = sheet.getAllData();
    const entities: T[] = data.map((row) =>
      rowToEntity<T>(row, this.headers, this.schema.fields),
    );

    if (this.cache) {
      this.cache.set(cacheKey, entities);
    }

    return entities;
  }

  /**
   * Find the 0-based row index for an entity by its __id.
   */
  private rowIndexById(sheet: ISheetAdapter, id: string): number | null {
    const data = sheet.getAllData();
    const idColIdx = this.headers.indexOf(SYSTEM_COLUMNS.ID);
    if (idColIdx < 0) return null;

    for (let i = 0; i < data.length; i++) {
      if (String(data[i][idColIdx]) === id) {
        return i;
      }
    }
    return null;
  }

  /**
   * Add a new entity to all relevant indexes.
   */
  private addToIndexes(entity: T): void {
    const indexedFields = this.indexStore.getIndexedFields(this.schema.tableName);
    for (const meta of indexedFields) {
      const value = entity[meta.field];
      if (value !== undefined && value !== null && value !== '') {
        this.indexStore.add(this.schema.tableName, meta.field, value, entity.__id);
      }
    }
  }

  private invalidateDataCache(): void {
    if (!this.cache) return;
    this.cache.delete(`data:${this.schema.tableName}`);
  }
}
