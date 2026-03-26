// SheetORM — SheetRepository: main CRUD + query interface for a single entity type
// Inspired by the Repository pattern from common ORM architectures

import type { Entity } from "../core/types/Entity.js";
import type { FieldDefinition } from "../core/types/FieldDefinition.js";
import type { ISpreadsheetAdapter } from "../core/types/ISpreadsheetAdapter.js";
import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import type { TableSchema } from "../core/types/TableSchema.js";
import type { QueryOptions } from "../core/types/QueryOptions.js";
import type { PaginatedResult } from "../core/types/PaginatedResult.js";
import type { GroupResult } from "../core/types/GroupResult.js";
import type { LifecycleHooks } from "../core/types/LifecycleHooks.js";
import type { ICacheProvider } from "../core/types/ICacheProvider.js";
import { SystemColumns } from "../core/types/SystemColumns.js";
import { Uuid } from "../utils/Uuid.js";
import { Serialization } from "../utils/Serialization.js";
import { IndexStore } from "../index/IndexStore.js";
import { Query } from "../query/Query.js";
import { QueryEngine } from "../query/QueryEngine.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

export class SheetRepository<T extends Entity> {
  private adapter: ISpreadsheetAdapter;
  private schema: TableSchema;
  private indexStore: IndexStore;
  private cache: ICacheProvider | null;
  private hooks: LifecycleHooks<T>;
  private headers: string[];
  private idColIdx: number;
  private requiredFields: TableSchema["fields"];
  private defaultableFields: TableSchema["fields"];
  private dataCacheKey: string;
  private fieldMap: Map<string, FieldDefinition>;
  private idToRowIndex: Map<string, number> | null = null;
  private batchBuffer: Array<{ type: "save" | "delete"; data: unknown }> | null = null;
  private entityBatch: Array<{
    entity: T;
    row: unknown[];
    dataIndex: number;
    mode: "create" | "update";
  }> | null = null;
  /** Cached sheet reference for the duration of saveAll() — avoids 1000× getSheetByName(). */
  private batchSheet: ISheetAdapter | null = null;
  /** Row count captured once at saveAll() start — avoids 1000× getLastRow(). */
  private batchBaseRowCount: number | null = null;

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
    this.headers = Serialization.buildHeaders(schema.fields);
    this.idColIdx = this.headers.indexOf(SystemColumns.ID);
    this.requiredFields = schema.fields.filter((f) => f.required);
    this.defaultableFields = schema.fields.filter((f) => f.defaultValue !== undefined);
    this.dataCacheKey = `data:${schema.tableName}`;

    // Pre-build field lookup map once (reused by entityToRow/rowToEntity)
    this.fieldMap = new Map();
    for (const f of schema.fields) {
      this.fieldMap.set(f.name, f);
    }
  }

  // ─── CRUD ──────────────────────────────────────────

  /**
   * Save (create or update) an entity. If __id is present and exists, updates. Otherwise creates.
   */
  save(partial: Partial<T> & { __id?: string }): T {
    if (this.batchBuffer) {
      const now = new Date().toISOString();
      const id = partial.__id ?? Uuid.generate();
      // Use presence of __createdAt as heuristic: explicit __id with no __createdAt
      // likely means a new entity with a caller-supplied ID, not an update.
      const isLikelyUpdate = Boolean(partial.__id && partial.__createdAt);
      const buffered = { ...partial, __id: id };
      this.batchBuffer.push({ type: "save", data: buffered });
      return {
        ...buffered,
        ...(!isLikelyUpdate ? { __createdAt: now } : {}),
        __updatedAt: now,
      } as T;
    }

    return this.doSave(partial);
  }

  private doSave(partial: Partial<T> & { __id?: string }): T {
    const sheet = this.batchSheet ?? this.getSheet();
    const now = new Date().toISOString();

    // ── Existence check: prefer in-memory index, fall back to single API call ──
    let existingIdx: number | null = null;
    let existingEntity: T | null = null;

    if (partial.__id) {
      // Try in-memory lookup (idToRowIndex + entity cache)
      const cachedIdx = this.idToRowIndex?.get(partial.__id);
      if (cachedIdx !== undefined && this.cache) {
        const cached = this.cache.get<T[]>(this.dataCacheKey);
        if (cached) {
          const cachedEntity = cached[cachedIdx];
          // Validate ID — gap rows may cause cache index drift
          if (cachedEntity?.__id === partial.__id) {
            existingIdx = cachedIdx;
            existingEntity = cachedEntity;
          }
        }
      }

      // Fallback: fast scan — check only the ID column, deserialize just the target entity
      if (existingIdx === null) {
        const data = sheet.getAllData();
        const rowIndex = new Map<string, number>();
        const col = this.idColIdx;
        for (let i = 0; i < data.length; i++) {
          const rowId = String(data[i][col]);
          rowIndex.set(rowId, i);
          if (rowId === partial.__id) {
            existingIdx = i;
            existingEntity = Serialization.rowToEntity<T>(
              data[i],
              this.headers,
              this.schema.fields,
              this.fieldMap,
            );
          }
        }
        this.idToRowIndex = rowIndex;
      }
    }

    const isNew = existingIdx === null;

    // Lifecycle: validate
    if (this.hooks.onValidate) {
      const errors = this.hooks.onValidate(partial as Partial<T>);
      if (errors && errors.length > 0) {
        throw new Error(`Validation failed: ${errors.join(", ")}`);
      }
    }

    // Lifecycle: beforeSave
    let entityData = partial;
    if (this.hooks.beforeSave) {
      const result = this.hooks.beforeSave(partial as Partial<T>, isNew);
      if (result) entityData = result as Partial<T> & { __id?: string };
    }

    // Apply defaults for fields with defaultValue
    for (let i = 0; i < this.defaultableFields.length; i++) {
      const field = this.defaultableFields[i];
      if (entityData[field.name] === undefined) {
        (entityData as Record<string, unknown>)[field.name] = field.defaultValue;
      }
    }

    // Validate required fields
    for (let i = 0; i < this.requiredFields.length; i++) {
      const field = this.requiredFields[i];
      const val = entityData[field.name];
      if (val === undefined || val === null || val === "") {
        throw new Error(`Required field "${field.name}" is missing for table "${this.schema.tableName}"`);
      }
    }

    let entity: T;

    if (isNew) {
      // CREATE
      entity = {
        ...entityData,
        __id: entityData.__id ?? Uuid.generate(),
        __createdAt: now,
        __updatedAt: now,
      } as T;

      const row = Serialization.entityToRow(entity, this.schema.fields, this.headers, this.fieldMap);

      // Write at computed position — single setValues call, no flush needed
      // In batch mode, account for already-buffered entities that haven't been flushed yet.
      // batchBaseRowCount is captured once at saveAll() start to avoid 1000× getLastRow().
      const dataIndex =
        (this.batchBaseRowCount ?? sheet.getRowCount()) +
        (this.entityBatch ? this.entityBatch.filter((item) => item.mode === "create").length : 0);
      if (!this.idToRowIndex) {
        if (dataIndex === 0) {
          this.idToRowIndex = new Map();
          // Bootstrap empty cache so subsequent finds are cache hits
          if (this.cache && !this.cache.has(this.dataCacheKey)) {
            this.cache.set(this.dataCacheKey, []);
          }
        }
      }
      // In entity batch mode: buffer the row; otherwise write immediately
      if (this.entityBatch !== null) {
        this.entityBatch.push({ entity, row, dataIndex, mode: "create" });
      } else {
        sheet.updateRow(dataIndex, row);
      }

      // Add to indexes
      this.addToIndexes(entity);

      // Update in-memory row index (always, even in batch — so next entity gets correct dataIndex)
      if (this.idToRowIndex) {
        this.idToRowIndex.set(entity.__id, dataIndex);
      }
    } else {
      // UPDATE — use cached entity or freshly deserialized one
      entity = {
        ...existingEntity!,
        ...entityData,
        __id: existingEntity!.__id,
        __createdAt: existingEntity!.__createdAt,
        __updatedAt: now,
      } as T;

      const row = Serialization.entityToRow(entity, this.schema.fields, this.headers, this.fieldMap);
      if (this.entityBatch !== null) {
        this.entityBatch.push({ entity, row, dataIndex: existingIdx!, mode: "update" });
      } else {
        sheet.updateRow(existingIdx!, row);
      }

      // Update indexes
      const oldValues: Record<string, unknown> = {};
      const newValues: Record<string, unknown> = {};
      for (const field of this.schema.fields) {
        oldValues[field.name] = existingEntity![field.name];
        newValues[field.name] = entity[field.name];
      }
      if (this.schema.indexTableName) {
        this.indexStore.updateInCombined(this.schema.indexTableName, entity.__id, oldValues, newValues);
      }
    }

    // Update entity cache in place (avoid full invalidation)
    this.updateCacheAfterSave(entity, isNew);

    // Lifecycle: afterSave
    if (this.hooks.afterSave) {
      this.hooks.afterSave(entity, isNew);
    }

    return entity;
  }

  /**
   * Save multiple entities in batch, flushing all index writes in a single
   * sheet call at the end instead of one per entity.
   */
  saveAll(entities: Array<Partial<T>>): T[] {
    if (entities.length === 0) return [];
    const sheet = this.getSheet();
    SheetOrmLogger.log(`[Repo:${this.schema.tableName}] saveAll START — ${entities.length} entities`);
    this.entityBatch = [];
    this.batchSheet = sheet;
    this.batchBaseRowCount = sheet.getRowCount();
    if (this.schema.indexTableName) {
      this.indexStore.beginIndexBatch();
    }
    try {
      const results = entities.map((e) => this.doSave(e));
      this.flushEntityBatch(sheet);
      if (this.schema.indexTableName) {
        this.indexStore.flushIndexBatch();
      }
      this.batchSheet = null;
      this.batchBaseRowCount = null;
      SheetOrmLogger.log(`[Repo:${this.schema.tableName}] saveAll DONE — ${entities.length} entities`);
      return results;
    } catch (err) {
      this.entityBatch = null;
      this.batchSheet = null;
      this.batchBaseRowCount = null;
      if (this.schema.indexTableName) {
        this.indexStore.cancelIndexBatch();
      }
      if (this.cache) this.cache.delete(this.dataCacheKey);
      this.idToRowIndex = null;
      throw err;
    }
  }

  /**
   * Find an entity by ID.
   */
  findById(id: string): T | null {
    // Fast path: hit cached row-index map to avoid full scan
    if (this.idToRowIndex && this.cache) {
      const rowIdx = this.idToRowIndex.get(id);
      if (rowIdx === undefined) return null;
      const cached = this.cache.get<T[]>(this.dataCacheKey);
      if (cached) {
        const hit = cached[rowIdx];
        // Validate ID — gap rows may cause cache index drift
        if (hit?.__id === id) return this.cloneEntity(hit);
        // Index/cache divergence (e.g. gap rows): scan cached array directly
        // to avoid unnecessary sheet re-read via loadAllEntities()
        const found = cached.find((e) => e?.__id === id);
        return found ? this.cloneEntity(found) : null;
      }
    }
    const all = this.loadAllEntities();
    const found = all.find((e) => e.__id === id);
    return found ? this.cloneEntity(found) : null;
  }

  /**
   * Find entities matching query options.
   * When a `search` operator targets an @Indexed field and a combined index
   * sheet exists, the n-gram search index is used to narrow candidates before
   * the full filter pipeline runs (Solr-like optimisation).
   */
  find(options?: QueryOptions): T[] {
    const all = this.loadAllEntities();
    if (!options) return this.cloneEntities(all);

    if (options.where && !options.whereGroups && this.schema.indexTableName) {
      const searchFilters: { field: string; value: string }[] = [];
      const otherFilters: typeof options.where = [];

      for (const f of options.where) {
        if (f.operator === "search" && this.isIndexedField(f.field)) {
          searchFilters.push({ field: f.field, value: String(f.value) });
        } else {
          otherFilters.push(f);
        }
      }

      if (searchFilters.length > 0) {
        let candidateIds: Set<string> | null = null;
        for (const sf of searchFilters) {
          const ids = this.indexStore.searchCombined(this.schema.indexTableName, sf.field, sf.value);
          const idSet = new Set(ids);
          if (candidateIds === null) {
            candidateIds = idSet;
          } else {
            for (const id of candidateIds) {
              if (!idSet.has(id)) candidateIds.delete(id);
            }
          }
        }

        if (!candidateIds || candidateIds.size === 0) return [];

        const narrowed = all.filter((e) => candidateIds!.has(e.__id));
        return this.cloneEntities(
          QueryEngine.executeQuery(narrowed, {
            ...options,
            where: otherFilters.length > 0 ? otherFilters : undefined,
          }),
        );
      }
    }

    return this.cloneEntities(QueryEngine.executeQuery(all, options));
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
   * In batch mode, returns true to indicate the delete was queued
   * (actual removal happens on commitBatch).
   */
  delete(id: string): boolean {
    if (this.batchBuffer) {
      this.batchBuffer.push({ type: "delete", data: id });
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

    // Try in-memory index first, fall back to sheet scan
    let rowIdx: number | null = null;
    if (this.idToRowIndex) {
      const idx = this.idToRowIndex.get(id);
      if (idx !== undefined) {
        // Verify the row actually contains the expected ID (guard against stale index)
        const row = sheet.getRow(idx);
        if (row && String(row[this.idColIdx]) === id) {
          rowIdx = idx;
        } else {
          // Stale index — fall back to full scan
          rowIdx = this.rowIndexById(sheet, id);
        }
      }
    }
    if (rowIdx === null) {
      rowIdx = this.rowIndexById(sheet, id);
    }
    if (rowIdx === null) return false;

    // Remove from indexes
    if (this.schema.indexTableName) {
      this.indexStore.removeAllFromCombined(this.schema.indexTableName, id);
    }

    sheet.deleteRow(rowIdx);

    // Update idToRowIndex: remove deleted ID and shift rows above it down
    if (this.idToRowIndex) {
      this.idToRowIndex.delete(id);
      for (const [key, idx] of this.idToRowIndex) {
        if (idx > rowIdx) {
          this.idToRowIndex.set(key, idx - 1);
        }
      }
    }

    // Update entity cache in place
    this.updateCacheAfterDelete(id);

    // Lifecycle: afterDelete
    if (this.hooks.afterDelete) {
      this.hooks.afterDelete(id);
    }

    return true;
  }

  /**
   * Delete all entities matching a query.
   * Uses bulk write (replaceAllData) for 3+ deletions, individual deletes for 1-2.
   */
  deleteAll(options?: QueryOptions): number {
    if (this.batchBuffer) {
      const entities = this.find(options);
      for (const entity of entities) {
        this.batchBuffer.push({ type: "delete", data: entity.__id });
      }
      return entities.length;
    }

    const all = this.loadAllEntities();
    const toDelete = options ? QueryEngine.executeQuery(all, options) : [...all];
    if (toDelete.length === 0) return 0;

    // For small batches, individual deletes are cheaper than replaceAllData
    if (toDelete.length <= 2) {
      let count = 0;
      for (const entity of toDelete) {
        if (this.doDelete(entity.__id)) count++;
      }
      return count;
    }

    // Bulk delete: snapshot → filter → write back (2 API calls vs N)
    const deleteIds = new Set<string>();
    for (const entity of toDelete) {
      if (this.hooks.beforeDelete && this.hooks.beforeDelete(entity.__id) === false) continue;
      deleteIds.add(entity.__id);
    }
    if (deleteIds.size === 0) return 0;

    const remaining = all.filter((e) => !deleteIds.has(e.__id));
    const sheet = this.getSheet();
    const rows = remaining.map((e) =>
      Serialization.entityToRow(e, this.schema.fields, this.headers, this.fieldMap),
    );
    sheet.replaceAllData(rows);

    if (this.schema.indexTableName) {
      this.indexStore.removeMultipleFromCombined(this.schema.indexTableName, [...deleteIds]);
    }
    for (const id of deleteIds) {
      if (this.hooks.afterDelete) this.hooks.afterDelete(id);
    }

    if (this.cache) {
      this.cache.set(this.dataCacheKey, remaining);
    }

    const rowIndex = new Map<string, number>();
    for (let i = 0; i < remaining.length; i++) {
      rowIndex.set(remaining[i].__id, i);
    }
    this.idToRowIndex = rowIndex;

    return deleteIds.size;
  }

  /**
   * Count entities matching a query.
   */
  count(options?: QueryOptions): number {
    const all = this.loadAllEntities();
    if (!options || (!options.where && !options.whereGroups)) return all.length;
    return QueryEngine.executeQuery(all, options).length;
  }

  /**
   * Paginated select.
   */
  select(offset: number, limit: number, options?: QueryOptions): PaginatedResult<T> {
    let entities = this.cloneEntities(this.loadAllEntities());

    if (options) {
      entities = QueryEngine.executeQuery(entities, { ...options, offset: undefined, limit: undefined });
    }

    return QueryEngine.paginateEntities(entities, offset, limit);
  }

  /**
   * Group entities by a field.
   */
  groupBy(field: string, options?: QueryOptions): GroupResult<T>[] {
    let entities = this.cloneEntities(this.loadAllEntities());

    if (options) {
      entities = QueryEngine.executeQuery(entities, options);
    }

    return QueryEngine.groupEntities(entities, field);
  }

  /**
   * Create a fluent query for this repository.
   */
  query(): Query<T> {
    return new Query<T>(() => this.cloneEntities(this.loadAllEntities()));
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

    try {
      for (const op of buffer) {
        if (op.type === "save") {
          this.doSave(op.data as Partial<T> & { __id?: string });
        } else if (op.type === "delete") {
          this.doDelete(op.data as string);
        }
      }
    } catch (err) {
      if (this.cache) this.cache.delete(this.dataCacheKey);
      this.idToRowIndex = null;
      throw err;
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
      throw new Error(
        `Sheet "${this.schema.tableName}" not found. Ensure Registry is configured and the table has been initialized.`,
      );
    }
    return sheet;
  }

  /**
   * Load all entities from the sheet, with caching.
   */
  private loadAllEntities(): T[] {
    if (this.entityBatch) {
      throw new Error(
        "Cannot reload entities during active entity batch — lifecycle hooks must not trigger find/count during saveAll()",
      );
    }

    if (this.cache) {
      const cached = this.cache.get<T[]>(this.dataCacheKey);
      if (cached !== null) return cached;
    }

    const sheet = this.getSheet();
    const data = sheet.getAllData();
    const len = data.length;
    const headers = this.headers;
    const fields = this.schema.fields;
    const fMap = this.fieldMap;
    const entities: T[] = [];
    const rowIndex = new Map<string, number>();
    for (let i = 0; i < len; i++) {
      const entity = Serialization.rowToEntity<T>(data[i], headers, fields, fMap);
      if (!entity.__id || entity.__id === "undefined" || entity.__id === "null") continue;
      rowIndex.set(entity.__id, i);
      entities.push(entity);
    }
    this.idToRowIndex = rowIndex;

    if (this.cache) {
      this.cache.set(this.dataCacheKey, entities);
    }

    return entities;
  }

  /**
   * Find the 0-based row index for an entity by its __id.
   */
  private rowIndexById(sheet: ISheetAdapter, id: string): number | null {
    const data = sheet.getAllData();
    const col = this.idColIdx;
    if (col < 0) return null;

    // Populate idToRowIndex while scanning (we already have all the data)
    const rowIndex = new Map<string, number>();
    let result: number | null = null;
    for (let i = 0; i < data.length; i++) {
      const rowId = String(data[i][col]);
      rowIndex.set(rowId, i);
      if (rowId === id) result = i;
    }
    this.idToRowIndex = rowIndex;
    return result;
  }

  /**
   * Add a new entity to all relevant indexes.
   */
  private addToIndexes(entity: T): void {
    if (this.schema.indexTableName) {
      const indexedFields = this.indexStore.getIndexedFields(this.schema.indexTableName);
      const entries: Array<{ field: string; value: unknown }> = [];
      for (const meta of indexedFields) {
        const value = entity[meta.field];
        if (value !== undefined && value !== null && value !== "") {
          entries.push({ field: meta.field, value });
        }
      }
      if (entries.length > 0) {
        this.indexStore.addAllFieldsToCombined(this.schema.indexTableName, entries, entity.__id);
      }
    }
  }

  private flushEntityBatch(sheet: ISheetAdapter): void {
    if (!this.entityBatch || this.entityBatch.length === 0) {
      this.entityBatch = null;
      return;
    }
    const batch = this.entityBatch;
    this.entityBatch = null;
    const creates = batch.filter((i) => i.mode === "create").length;
    const updates = batch.filter((i) => i.mode === "update").length;
    SheetOrmLogger.log(
      `[Repo:${this.schema.tableName}] flushEntityBatch ${batch.length} rows (creates=${creates} updates=${updates}); calling sheet.updateRows()`,
    );
    sheet.updateRows(
      [...batch]
        .sort((a, b) => a.dataIndex - b.dataIndex)
        .map((item) => ({ rowIndex: item.dataIndex, values: item.row })),
    );
  }

  private cloneEntity(entity: T): T {
    return { ...entity };
  }

  private cloneEntities(entities: T[]): T[] {
    return entities.map((entity) => this.cloneEntity(entity));
  }

  private updateCacheAfterSave(entity: T, isNew: boolean): void {
    if (!this.cache) return;
    const cached = this.cache.get<T[]>(this.dataCacheKey);
    if (!cached) return;

    if (isNew) {
      cached.push(entity);
    } else {
      for (let i = 0; i < cached.length; i++) {
        if (cached[i]?.__id === entity.__id) {
          cached[i] = entity;
          return;
        }
      }
    }
  }

  // Note: idToRowIndex is updated separately in doDelete(). This method only handles the entity array cache.
  private updateCacheAfterDelete(id: string): void {
    if (!this.cache) return;
    const cached = this.cache.get<T[]>(this.dataCacheKey);
    if (!cached) return;

    for (let i = 0; i < cached.length; i++) {
      if (cached[i]?.__id === id) {
        cached.splice(i, 1);
        return;
      }
    }
  }

  private isIndexedField(fieldName: string): boolean {
    if (!this.schema.indexTableName) return false;
    const indexed = this.indexStore.getIndexedFields(this.schema.indexTableName);
    return indexed.some((m) => m.field === fieldName);
  }
}
