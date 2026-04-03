/**
 * Main CRUD and query interface for a single entity type.
 *
 * Each `SheetRepository<T>` manages one Google Sheets tab, converting
 * between in-memory {@link Entity} objects and sheet rows via
 * {@link Serialization}.  Inspired by the Repository pattern from
 * common ORM architectures (Hibernate, TypeORM, etc.).
 *
 * Performance optimisations (referenced by codename in comments):
 *   - **B5**  — Known row count passed from Registry avoids getLastRow().
 *   - **B7**  — `sheetCache` memoises the ISheetAdapter for the tab.
 *   - **K1**  — Reuses physicalRowCount across saves inside saveAll().
 *   - **K2**  — Skips index sheet lookup when the class has no @Indexed fields.
 *   - **L1**  — Defers header write to the first data flush (saves ~700 ms).
 *   - **L2**  — Bulk delete rewrite for delete-only batch commits.
 *
 * @module SheetRepository
 */

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
import type { IndexMeta } from "../index/IndexMeta.js";
import { Query } from "../query/Query.js";
import { QueryEngine } from "../query/QueryEngine.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

/**
 * Repository providing CRUD, query, pagination, and batch operations
 * for entities of type `T` stored in a single Google Sheet tab.
 *
 * @typeParam T - Entity type managed by this repository.
 */
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
  /** Fast-lookup map: entity ID → 1-based sheet row index for O(1) row access. */
  private idToRowIndex: Map<string, number> | null = null;

  /** Buffered operations when a batch is active (beginBatch/commitBatch). */
  private batchBuffer: Array<{ type: "save" | "delete"; data: unknown }> | null = null;

  /**
   * Entity batch accumulator used by saveAll().
   * Each entry stores the entity, its serialised row, 0-based data index, and
   * whether it is a "create" or "update".  Flushed via flushEntityBatch().
   */
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
  /** Cached entity array used by updateCacheAfterSave in batch mode — avoids 1000× cache.get() log calls. */
  private batchCachedData: T[] | null = null;
  /** Indexed fields metadata, cached for the duration of saveAll() — avoids 1000× getIndexedFields() array alloc. */
  private batchIndexedFields: IndexMeta[] | null = null;
  /** Physical sheet row count, tracked to avoid repeated getLastRow() API calls in non-batch save sequences. */
  private physicalRowCount: number | null = null;
  /** True when idToRowIndex was built from a complete sheet scan (bootstrap or loadAllEntities/rowIndexById). */
  private idToRowIndexComplete = false;
  /** Memoized sheet reference — avoids repeated getSheetByName() API calls within a session (B7). */
  private sheetCache: ISheetAdapter | null = null;
  /** Memoized set of indexed field names — avoids repeated getIndexedFields() array allocations in find(). */
  private indexedFieldNames: Set<string>;
  /** True when headers haven't been written to the sheet yet (new sheet from ensureTable). */
  private headersDeferred: boolean;

  /**
   * Constructs a new repository for the given table schema.
   *
   * @param adapter         - Spreadsheet adapter providing sheet management.
   * @param schema          - Table schema (name, fields, indexes).
   * @param indexStore      - Shared IndexStore for secondary indexes.
   * @param cache           - Optional cache provider for in-memory row caching.
   * @param hooks           - Optional lifecycle hooks (onValidate, beforeSave, afterSave, beforeDelete, afterDelete).
   * @param initialSheet    - Pre-resolved sheet adapter (avoids redundant getSheetByName call).
   * @param initialRowCount - Pre-fetched row count (avoids redundant getLastRow call).
   * @param headersDeferred - True if the sheet was just created and headers still need writing.
   */
  constructor(
    adapter: ISpreadsheetAdapter,
    schema: TableSchema,
    indexStore: IndexStore,
    cache?: ICacheProvider,
    hooks?: LifecycleHooks<T>,
    initialSheet?: ISheetAdapter,
    initialRowCount?: number,
    headersDeferred?: boolean,
  ) {
    this.adapter = adapter;
    this.schema = schema;
    this.indexStore = indexStore;
    this.cache = cache ?? null;
    this.hooks = hooks ?? {};

    // Seed memoised sheet & row count when provided by Registry
    this.sheetCache = initialSheet ?? null;
    this.physicalRowCount = initialRowCount ?? null;

    // Build column header array from field definitions (used for serialisation)
    this.headers = Serialization.buildHeaders(schema.fields);
    // Locate the __id column position for fast primary-key lookups
    this.idColIdx = this.headers.indexOf(SystemColumns.ID);

    // Pre-filter required and defaultable fields for validation/defaults in save()
    this.requiredFields = schema.fields.filter((f) => f.required);
    this.defaultableFields = schema.fields.filter((f) => f.defaultValue !== undefined);

    // Cache key for the data cache (all-entity array)
    this.dataCacheKey = `data:${schema.tableName}`;

    // Pre-build field lookup map once (reused by entityToRow / rowToEntity)
    this.fieldMap = new Map();
    for (const f of schema.fields) {
      this.fieldMap.set(f.name, f);
    }

    // Pre-build indexed-field name set once — avoids getIndexedFields() array alloc on every find()
    this.indexedFieldNames = new Set(schema.indexes.map((idx) => idx.field));

    // Defer header write to first data flush — saves one GAS API call per new sheet
    this.headersDeferred = headersDeferred ?? false;

    SheetOrmLogger.log(
      `[Repo:${schema.tableName}] constructor — ` +
        `sheetCache=${initialSheet ? "seeded" : "null"} ` +
        `physicalRowCount=${initialRowCount !== undefined ? String(initialRowCount) : "unknown"} ` +
        `indexedFields=[${schema.indexes.map((i) => i.field).join(",")}]`,
    );
  }

  // ─── CRUD ──────────────────────────────────────────

  /**
   * Save (create or update) an entity.
   *
   * When a batch is active (see {@link beginBatch}), the operation is buffered
   * and deferred until {@link commitBatch} is called.  Otherwise delegates
   * immediately to {@link doSave}.
   *
   * @param partial - Partial entity with optional `__id`.
   * @returns The saved entity with system columns populated.
   */
  save(partial: Partial<T> & { __id?: string }): T {
    if (this.batchBuffer) {
      const now = new Date().toISOString();
      const id = partial.__id ?? Uuid.generate();
      // Heuristic: __id present *with* __createdAt → likely an existing entity (update).
      // __id present *without* __createdAt → caller-supplied ID for a new entity.
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

  /**
   * Internal save implementation — resolves existence, validates, applies
   * defaults, and writes to the sheet or buffers into an entity batch.
   *
   * @param partial - Partial entity data with optional `__id`.
   * @returns Fully populated entity with system columns.
   */
  private doSave(partial: Partial<T> & { __id?: string }): T {
    const sheet = this.batchSheet ?? this.getSheet();
    // Per-entity log is suppressed in batch mode to avoid 1000× Logger.log() overhead in GAS
    if (!this.entityBatch) {
      SheetOrmLogger.log(
        `[Repo:${this.schema.tableName}] doSave — batchMode=false id=${partial.__id ?? "(new)"}`,
      );
    }
    const now = new Date().toISOString();

    // ── Existence check: prefer in-memory index, fall back to single API call ──
    let existingIdx: number | null = null;
    let existingEntity: T | null = null;

    if (partial.__id) {
      // Try in-memory lookup: idToRowIndex gives us the 0-based data row index,
      // and the entity cache gives us the current field values (needed for UPDATE merge).
      const cachedIdx = this.idToRowIndex?.get(partial.__id);
      if (cachedIdx !== undefined && this.cache) {
        const cached = this.cache.get<T[]>(this.dataCacheKey);
        if (cached) {
          const cachedEntity = cached[cachedIdx];
          // Validate that the cached ID matches — gap rows may cause cache index drift
          if (cachedEntity?.__id === partial.__id) {
            existingIdx = cachedIdx;
            existingEntity = cachedEntity;
          }
        }
      }

      // Fallback: full-scan the sheet's ID column, deserialize only the matching row
      if (existingIdx === null) {
        if (this.idToRowIndex && this.idToRowIndexComplete && !this.idToRowIndex.has(partial.__id)) {
          // idToRowIndex covers all rows; entity's ID is absent → definitely new, skip sheet read.
        } else {
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
          // Rebuild the full index as a side effect of the scan
          this.idToRowIndex = rowIndex;
          this.physicalRowCount = data.length;
          this.idToRowIndexComplete = true;
        }
      }
    }

    const isNew = existingIdx === null;
    if (!this.entityBatch) {
      SheetOrmLogger.log(
        `[Repo:${this.schema.tableName}] doSave — isNew=${isNew}${existingIdx !== null ? ` rowIdx=${existingIdx}` : ""}`,
      );
    }

    // ── Lifecycle: validate ──
    if (this.hooks.onValidate) {
      const errors = this.hooks.onValidate(partial as Partial<T>);
      if (errors && errors.length > 0) {
        throw new Error(`Validation failed: ${errors.join(", ")}`);
      }
    }

    // ── Lifecycle: beforeSave ──
    let entityData = partial;
    if (this.hooks.beforeSave) {
      const result = this.hooks.beforeSave(partial as Partial<T>, isNew);
      if (result) entityData = result as Partial<T> & { __id?: string };
    }

    // Apply defaults for fields with defaultValue (only when undefined)
    for (let i = 0; i < this.defaultableFields.length; i++) {
      const field = this.defaultableFields[i];
      if (entityData[field.name] === undefined) {
        (entityData as Record<string, unknown>)[field.name] = field.defaultValue;
      }
    }

    // Validate required fields (must not be undefined, null, or empty string)
    for (let i = 0; i < this.requiredFields.length; i++) {
      const field = this.requiredFields[i];
      const val = entityData[field.name];
      if (val === undefined || val === null || val === "") {
        throw new Error(`Required field "${field.name}" is missing for table "${this.schema.tableName}"`);
      }
    }

    let entity: T;

    if (isNew) {
      // ── CREATE path ──
      entity = {
        ...entityData,
        __id: entityData.__id ?? Uuid.generate(),
        __createdAt: now,
        __updatedAt: now,
      } as T;

      const row = Serialization.entityToRow(entity, this.schema.fields, this.headers, this.fieldMap);

      // Compute the 0-based data row index for writing.
      // In batch mode (saveAll), batchBaseRowCount was captured once at batch start.
      // Otherwise reuse physicalRowCount to avoid getLastRow() API calls.
      const baseCount =
        this.batchBaseRowCount !== null
          ? this.batchBaseRowCount
          : this.physicalRowCount !== null
            ? this.physicalRowCount
            : sheet.getRowCount();
      // Account for already-buffered CREATE entities that haven't been flushed yet
      const dataIndex =
        baseCount + (this.entityBatch ? this.entityBatch.filter((item) => item.mode === "create").length : 0);

      // Bootstrap idToRowIndex and cache when first entity is written to an empty sheet
      if (!this.idToRowIndex) {
        if (dataIndex === 0) {
          this.idToRowIndex = new Map();
          this.physicalRowCount = 0;
          this.idToRowIndexComplete = true;
          // Seed empty cache so subsequent finds are cache hits
          if (this.cache && !this.cache.has(this.dataCacheKey)) {
            this.cache.set(this.dataCacheKey, []);
          }
        }
      }

      // In entity batch mode (saveAll): buffer the row; otherwise write immediately
      if (this.entityBatch !== null) {
        this.entityBatch.push({ entity, row, dataIndex, mode: "create" });
      } else {
        // Write headers + first data row in a single API call for newly-created sheets
        if (this.headersDeferred) {
          sheet.writeAllRowsWithHeaders(this.headers, [row]);
          this.headersDeferred = false;
        } else {
          sheet.updateRow(dataIndex, row);
        }
        // Keep physicalRowCount in sync
        if (this.physicalRowCount !== null) this.physicalRowCount++;
      }

      // Add to secondary indexes (@Indexed fields)
      this.addToIndexes(entity);

      // Update in-memory row index (always, even in batch — so the next entity gets the correct dataIndex)
      if (this.idToRowIndex) {
        this.idToRowIndex.set(entity.__id, dataIndex);
      }
    } else {
      // ── UPDATE path — merge existing fields with new values ──
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

      // Update secondary indexes with old → new value changes
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

    // Update entity cache in place (avoid full invalidation after every save)
    this.updateCacheAfterSave(entity, isNew);

    // ── Lifecycle: afterSave ──
    if (this.hooks.afterSave) {
      this.hooks.afterSave(entity, isNew);
    }

    return entity;
  }

  /**
   * Save multiple entities in a single optimised batch.
   *
   * 1. Captures the sheet reference and row count once (K1, B7).
   * 2. Calls {@link doSave} for each entity — rows are buffered instead of
   *    written individually.
   * 3. Flushes all buffered rows via {@link flushEntityBatch} (single API call).
   * 4. Flushes index writes via {@link IndexStore.flushIndexBatch}.
   *
   * On error the batch state is fully reset and the cache is invalidated.
   *
   * @param entities - Array of partial entities to save.
   * @returns Array of fully populated saved entities.
   */
  saveAll(entities: Array<Partial<T>>): T[] {
    if (entities.length === 0) return [];
    const sheet = this.getSheet();
    SheetOrmLogger.log(`[Repo:${this.schema.tableName}] saveAll START — ${entities.length} entities`);

    // Initialise batch state
    this.entityBatch = [];
    this.batchSheet = sheet;

    // Reuse physicalRowCount when available — avoids a getLastRow() API call (~700 ms)
    // when the row count is already known in-session (e.g. new table seeded to 0).
    this.batchBaseRowCount = this.physicalRowCount !== null ? this.physicalRowCount : sheet.getRowCount();

    if (this.schema.indexTableName) {
      this.indexStore.beginIndexBatch();
      // Pre-fetch indexed fields once — avoids N × getIndexedFields() array allocation
      this.batchIndexedFields = this.indexStore.getIndexedFields(this.schema.indexTableName);
    }

    try {
      // Execute all saves (rows buffered into this.entityBatch)
      const results = entities.map((e) => this.doSave(e));

      // Count how many new rows were created to update physicalRowCount
      const savedCreates = (this.entityBatch ?? []).filter((i) => i.mode === "create").length;

      // Flush buffered rows to the sheet in one updateRows() call
      this.flushEntityBatch(sheet);

      // Flush index batch (single write per index sheet)
      if (this.schema.indexTableName) {
        this.indexStore.flushIndexBatch();
      }

      // Update physicalRowCount with the number of new rows
      if (this.batchBaseRowCount !== null) {
        this.physicalRowCount = this.batchBaseRowCount + savedCreates;
      }

      // Clear batch state
      this.batchSheet = null;
      this.batchBaseRowCount = null;
      this.batchCachedData = null;
      this.batchIndexedFields = null;
      SheetOrmLogger.log(`[Repo:${this.schema.tableName}] saveAll DONE — ${entities.length} entities`);
      return results;
    } catch (err) {
      // Error recovery: clear all batch state and invalidate caches
      this.entityBatch = null;
      this.batchSheet = null;
      this.batchBaseRowCount = null;
      this.batchCachedData = null;
      this.batchIndexedFields = null;
      if (this.schema.indexTableName) {
        this.indexStore.cancelIndexBatch();
      }
      if (this.cache) this.cache.delete(this.dataCacheKey);
      this.idToRowIndex = null;
      this.physicalRowCount = null;
      this.idToRowIndexComplete = false;
      throw err;
    }
  }

  /**
   * Find an entity by its primary key (`__id`).
   *
   * Uses a fast path when idToRowIndex + cache are populated, falling back
   * to a full {@link loadAllEntities} scan otherwise.
   *
   * @param id - Entity `__id` value.
   * @returns The matching entity or `null` if not found.
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
    // Slow path: load all entities from sheet (populates cache as a side-effect)
    const all = this.loadAllEntities();
    const found = all.find((e) => e.__id === id);
    return found ? this.cloneEntity(found) : null;
  }

  /**
   * Find entities matching query options (filter, sort, paginate, group).
   *
   * When a `search` operator targets an `@Indexed` field and a combined index
   * sheet exists, the n-gram search index is used to narrow candidates before
   * the full filter pipeline runs (Solr-like optimisation).
   *
   * @param options - Optional query options (where, whereGroups, orderBy, offset, limit).
   * @returns Array of matching entities.
   */
  find(options?: QueryOptions): T[] {
    const all = this.loadAllEntities();
    if (!options) return this.cloneEntities(all);

    if (options.where && !options.whereGroups && this.schema.indexTableName) {
      // Separate search-operator filters (n-gram indexed) from other filters
      const searchFilters: { field: string; value: string }[] = [];
      const otherFilters: typeof options.where = [];

      for (const f of options.where) {
        if (f.operator === "search" && this.isIndexedField(f.field)) {
          searchFilters.push({ field: f.field, value: String(f.value) });
        } else {
          otherFilters.push(f);
        }
      }

      // N-gram index optimisation: narrow candidates via IndexStore.searchCombined
      // before running the full filter pipeline (analogous to Solr pre-filtering)
      if (searchFilters.length > 0) {
        let candidateIds: Set<string> | null = null;
        for (const sf of searchFilters) {
          const ids = this.indexStore.searchCombined(this.schema.indexTableName, sf.field, sf.value);
          const idSet = new Set(ids);
          // Intersect candidate sets across multiple search filters (AND semantics)
          if (candidateIds === null) {
            candidateIds = idSet;
          } else {
            for (const id of candidateIds) {
              if (!idSet.has(id)) candidateIds.delete(id);
            }
          }
        }

        if (!candidateIds || candidateIds.size === 0) return [];

        // Filter the in-memory entities to only those matching the index hits
        const narrowed = all.filter((e) => candidateIds!.has(e.__id));
        return this.cloneEntities(
          QueryEngine.executeQuery(narrowed, {
            ...options,
            where: otherFilters.length > 0 ? otherFilters : undefined,
          }),
        );
      }
    }

    // Standard path: run full QueryEngine pipeline on all entities
    return this.cloneEntities(QueryEngine.executeQuery(all, options));
  }

  /**
   * Find the first entity matching query options.
   *
   * Delegates to {@link find} with `limit: 1`.
   *
   * @param options - Optional query options.
   * @returns The first matching entity or `null`.
   */
  findOne(options?: QueryOptions): T | null {
    const opts: QueryOptions = { ...options, limit: 1 };
    const results = this.find(opts);
    return results.length > 0 ? results[0] : null;
  }

  /**
   * Delete an entity by ID.
   *
   * When a batch is active, the delete is queued and `true` is returned
   * immediately; actual removal happens on {@link commitBatch}.
   *
   * @param id - Entity `__id` to delete.
   * @returns `true` if the entity was found and deleted (or queued).
   */
  delete(id: string): boolean {
    if (this.batchBuffer) {
      this.batchBuffer.push({ type: "delete", data: id });
      return true;
    }

    return this.doDelete(id);
  }

  /**
   * Internal delete implementation.
   *
   * 1. Runs `beforeDelete` lifecycle hook (can veto deletion by returning `false`).
   * 2. Resolves the row index via in-memory cache or sheet scan.
   * 3. Removes from secondary indexes.
   * 4. Deletes the sheet row and adjusts `idToRowIndex`.
   * 5. Updates entity cache and runs `afterDelete` hook.
   *
   * @param id - Entity `__id` to delete.
   * @returns `true` if the entity was found and deleted.
   */
  private doDelete(id: string): boolean {
    // Lifecycle: beforeDelete — can veto deletion by returning false
    if (this.hooks.beforeDelete) {
      const result = this.hooks.beforeDelete(id);
      if (result === false) return false;
    }

    const sheet = this.getSheet();

    // Fast path: verify entity from in-memory cache — avoids sheet.getRow() API call
    // (a getRange().getValues() round-trip to GAS, ~200 ms per call).
    let rowIdx: number | null = null;
    if (this.idToRowIndex) {
      const idx = this.idToRowIndex.get(id);
      if (idx !== undefined) {
        const cached = this.cache?.get<T[]>(this.dataCacheKey);
        if (cached?.[idx]?.__id === id) {
          rowIdx = idx;
        } else {
          // Cache cold or stale index — verify via sheet read
          const row = sheet.getRow(idx);
          if (row && String(row[this.idColIdx]) === id) {
            rowIdx = idx;
          } else {
            // Stale index — fall back to full scan
            rowIdx = this.rowIndexById(sheet, id);
          }
        }
      }
    }
    if (rowIdx === null) {
      rowIdx = this.rowIndexById(sheet, id);
    }
    if (rowIdx === null) return false;

    // Remove from secondary indexes
    if (this.schema.indexTableName) {
      this.indexStore.removeAllFromCombined(this.schema.indexTableName, id);
    }

    // Delete the sheet row and adjust the tracked row count
    sheet.deleteRow(rowIdx);
    if (this.physicalRowCount !== null) this.physicalRowCount--;

    // Update idToRowIndex: remove deleted ID and shift rows above it down by one
    if (this.idToRowIndex) {
      this.idToRowIndex.delete(id);
      for (const [key, idx] of this.idToRowIndex) {
        if (idx > rowIdx) {
          this.idToRowIndex.set(key, idx - 1);
        }
      }
    }

    // Update entity cache in place (remove the deleted entity)
    this.updateCacheAfterDelete(id);

    // Lifecycle: afterDelete
    if (this.hooks.afterDelete) {
      this.hooks.afterDelete(id);
    }

    return true;
  }

  /**
   * Delete all entities matching a query (or all entities if no options given).
   *
   * Uses bulk write (replaceAllData) for 3+ deletions — two API calls instead
   * of N individual deleteRow() calls.  For 1–2 deletions, individual deletes
   * are cheaper due to lower overhead.
   *
   * @param options - Optional query options to select entities to delete.
   * @returns Number of entities deleted.
   */
  deleteAll(options?: QueryOptions): number {
    // In batch mode: queue deletes for later commitBatch()
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

    // For small batches (≤2), individual deletes are cheaper than replaceAllData
    if (toDelete.length <= 2) {
      let count = 0;
      for (const entity of toDelete) {
        if (this.doDelete(entity.__id)) count++;
      }
      return count;
    }

    // ── Bulk delete: snapshot → filter out deleted → write back (2 API calls vs N) ──
    const deleteIds = new Set<string>();
    for (const entity of toDelete) {
      // beforeDelete can veto individual deletions
      if (this.hooks.beforeDelete && this.hooks.beforeDelete(entity.__id) === false) continue;
      deleteIds.add(entity.__id);
    }
    if (deleteIds.size === 0) return 0;

    // Retain only entities not in the delete set
    const remaining = all.filter((e) => !deleteIds.has(e.__id));
    const sheet = this.getSheet();
    const rows = remaining.map((e) =>
      Serialization.entityToRow(e, this.schema.fields, this.headers, this.fieldMap),
    );
    // Replace the entire sheet data with the filtered rows
    sheet.replaceAllData(rows);

    // Clean up secondary indexes for all deleted entities
    if (this.schema.indexTableName) {
      this.indexStore.removeMultipleFromCombined(this.schema.indexTableName, [...deleteIds]);
    }
    // afterDelete hooks for each deleted entity
    for (const id of deleteIds) {
      if (this.hooks.afterDelete) this.hooks.afterDelete(id);
    }

    // Rebuild cache and idToRowIndex from the remaining entities
    if (this.cache) {
      this.cache.set(this.dataCacheKey, remaining);
    }

    const rowIndex = new Map<string, number>();
    for (let i = 0; i < remaining.length; i++) {
      rowIndex.set(remaining[i].__id, i);
    }
    this.idToRowIndex = rowIndex;
    this.physicalRowCount = rows.length;
    this.idToRowIndexComplete = true;

    return deleteIds.size;
  }

  /**
   * Count entities matching a query.
   *
   * @param options - Optional query options (only `where`/`whereGroups` are meaningful).
   * @returns Total count of matching entities.
   */
  count(options?: QueryOptions): number {
    const all = this.loadAllEntities();
    if (!options || (!options.where && !options.whereGroups)) return all.length;
    return QueryEngine.executeQuery(all, options).length;
  }

  /**
   * Paginated select — applies optional filters, then paginates.
   *
   * @param offset - 0-based offset.
   * @param limit  - Maximum number of results.
   * @param options - Optional query options applied before pagination.
   * @returns {@link PaginatedResult} with `data`, `total`, `offset`, and `limit`.
   */
  select(offset: number, limit: number, options?: QueryOptions): PaginatedResult<T> {
    let entities = this.cloneEntities(this.loadAllEntities());

    if (options) {
      // Filter/sort first, then paginate (offset/limit from options are stripped to avoid double application)
      entities = QueryEngine.executeQuery(entities, { ...options, offset: undefined, limit: undefined });
    }

    return QueryEngine.paginateEntities(entities, offset, limit);
  }

  /**
   * Group entities by a field value.
   *
   * @param field   - Field name to group by.
   * @param options - Optional query options applied before grouping.
   * @returns Array of {@link GroupResult} objects.
   */
  groupBy(field: string, options?: QueryOptions): GroupResult<T>[] {
    let entities = this.cloneEntities(this.loadAllEntities());

    if (options) {
      entities = QueryEngine.executeQuery(entities, options);
    }

    return QueryEngine.groupEntities(entities, field);
  }

  /**
   * Create a fluent {@link Query} builder for this repository.
   *
   * @returns A new Query instance backed by a snapshot of all current entities.
   */
  query(): Query<T> {
    return new Query<T>(() => this.cloneEntities(this.loadAllEntities()));
  }

  // ─── Batch Operations ──────────────────────────────

  /**
   * Start buffering save/delete operations for later atomic commit.
   *
   * Call {@link commitBatch} to apply all buffered operations, or
   * {@link rollbackBatch} to discard them.
   */
  beginBatch(): void {
    this.batchBuffer = [];
  }

  /**
   * Commit all buffered operations from {@link beginBatch}.
   *
   * If the buffer contains only deletes, a bulk rewrite (replaceAllData) is
   * used instead of N individual deleteRow() calls.
   */
  commitBatch(): void {
    if (!this.batchBuffer) return;
    const buffer = this.batchBuffer;
    this.batchBuffer = null;

    try {
      // Optimize delete-only batches (common for deleteAll() in batch mode)
      // by applying one bulk rewrite instead of N × deleteRow() API calls.
      if (buffer.length > 0 && buffer.every((op) => op.type === "delete")) {
        this.commitDeleteBatch(buffer.map((op) => op.data as string));
        return;
      }

      // Mixed batch: replay operations sequentially
      for (const op of buffer) {
        if (op.type === "save") {
          this.doSave(op.data as Partial<T> & { __id?: string });
        } else if (op.type === "delete") {
          this.doDelete(op.data as string);
        }
      }
    } catch (err) {
      // On error: invalidate caches to avoid stale state
      if (this.cache) this.cache.delete(this.dataCacheKey);
      this.idToRowIndex = null;
      this.physicalRowCount = null;
      this.idToRowIndexComplete = false;
      throw err;
    }
  }

  /**
   * Commit a delete-only batch via bulk replaceAllData.
   *
   * Falls back to per-ID deletes for tiny (≤2) or duplicate-heavy batches
   * to preserve semantics and avoid unnecessary full-sheet rewrites.
   *
   * @param ids - Array of entity IDs to delete.
   */
  private commitDeleteBatch(ids: string[]): void {
    if (ids.length === 0) return;

    // For very small batches or duplicates, individual deletes are cheaper
    const uniqueCount = new Set(ids).size;
    if (ids.length <= 2 || uniqueCount !== ids.length) {
      for (const id of ids) {
        this.doDelete(id);
      }
      return;
    }

    const all = this.loadAllEntities();
    if (all.length === 0) return;

    // Build set of existing IDs to skip deletes of already-removed entities
    const existingIds = new Set(all.map((e) => e.__id));
    const deleteIds: string[] = [];

    // Preserve caller order; run beforeDelete hooks (can veto)
    for (const id of ids) {
      if (!existingIds.has(id)) continue;
      if (this.hooks.beforeDelete && this.hooks.beforeDelete(id) === false) continue;
      deleteIds.push(id);
    }
    if (deleteIds.length === 0) return;

    // Filter out deleted entities and rewrite the entire sheet
    const deleteSet = new Set(deleteIds);
    const remaining = all.filter((e) => !deleteSet.has(e.__id));
    const rows = remaining.map((e) =>
      Serialization.entityToRow(e, this.schema.fields, this.headers, this.fieldMap),
    );

    const sheet = this.getSheet();
    sheet.replaceAllData(rows);

    // Clean up secondary indexes for deleted entities
    if (this.schema.indexTableName) {
      this.indexStore.removeMultipleFromCombined(this.schema.indexTableName, deleteIds);
    }

    // Run afterDelete hooks
    if (this.hooks.afterDelete) {
      for (const id of deleteIds) {
        this.hooks.afterDelete(id);
      }
    }

    // Rebuild cache and idToRowIndex from the remaining entities
    if (this.cache) {
      this.cache.set(this.dataCacheKey, remaining);
    }

    const rowIndex = new Map<string, number>();
    for (let i = 0; i < remaining.length; i++) {
      rowIndex.set(remaining[i].__id, i);
    }
    this.idToRowIndex = rowIndex;
    this.physicalRowCount = rows.length;
    this.idToRowIndexComplete = true;
  }

  /**
   * Discard all buffered operations without applying them.
   */
  rollbackBatch(): void {
    this.batchBuffer = null;
  }

  /**
   * Check whether batch mode is currently active.
   *
   * @returns `true` when {@link beginBatch} was called but not yet committed or rolled back.
   */
  isBatchActive(): boolean {
    return this.batchBuffer !== null;
  }

  // ─── Internal Helpers ──────────────────────────────

  /**
   * Return the memoised ISheetAdapter for this table, resolving it once
   * from the spreadsheet adapter and caching it for subsequent calls (B7).
   *
   * @throws Error if the sheet doesn't exist.
   */
  private getSheet(): ISheetAdapter {
    if (this.sheetCache) return this.sheetCache;
    const sheet = this.adapter.getSheetByName(this.schema.tableName);
    if (!sheet) {
      throw new Error(
        `Sheet "${this.schema.tableName}" not found. Ensure Registry is configured and the table has been initialized.`,
      );
    }
    this.sheetCache = sheet;
    return sheet;
  }

  /**
   * Load all entities from the sheet, deserialise them, and populate the cache.
   *
   * Returns the cached array on a cache hit.  On a cache miss, reads all rows
   * from the sheet, converts them via {@link Serialization.rowToEntity},
   * builds {@link idToRowIndex}, and stores the result.
   *
   * @throws Error if called during an active entity batch (saveAll).
   * @returns Array of all entities in the table.
   */
  private loadAllEntities(): T[] {
    if (this.entityBatch) {
      throw new Error(
        "Cannot reload entities during active entity batch — lifecycle hooks must not trigger find/count during saveAll()",
      );
    }

    // Cache hit path
    if (this.cache) {
      const cached = this.cache.get<T[]>(this.dataCacheKey);
      if (cached !== null) return cached;
    }

    // Cache miss: read all rows from the sheet
    const sheet = this.getSheet();
    const data = sheet.getAllData();
    const len = data.length;
    const headers = this.headers;
    const fields = this.schema.fields;
    const fMap = this.fieldMap;
    const entities: T[] = [];
    const rowIndex = new Map<string, number>();

    // Deserialise each row, skipping rows with invalid/missing IDs
    for (let i = 0; i < len; i++) {
      const entity = Serialization.rowToEntity<T>(data[i], headers, fields, fMap);
      if (!entity.__id || entity.__id === "undefined" || entity.__id === "null") continue;
      rowIndex.set(entity.__id, i);
      entities.push(entity);
    }

    // Update in-memory state
    this.idToRowIndex = rowIndex;
    this.physicalRowCount = data.length;
    this.idToRowIndexComplete = true;

    if (this.cache) {
      this.cache.set(this.dataCacheKey, entities);
    }

    return entities;
  }

  /**
   * Find the 0-based data row index for an entity by its `__id` via a full
   * sheet scan.  Rebuilds {@link idToRowIndex} as a side-effect.
   *
   * @param sheet - The sheet adapter to read from.
   * @param id    - Entity `__id` to search for.
   * @returns 0-based row index, or `null` if not found.
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
    this.physicalRowCount = data.length;
    this.idToRowIndexComplete = true;
    return result;
  }

  /**
   * Add a newly-created entity to all relevant secondary indexes.
   *
   * In batch mode, uses pre-cached {@link batchIndexedFields} to avoid
   * repeated `getIndexedFields()` allocations.
   *
   * @param entity - The entity to index.
   */
  private addToIndexes(entity: T): void {
    if (this.schema.indexTableName) {
      // Use pre-cached fields in batch mode to avoid N × getIndexedFields() array allocation
      const indexedFields =
        this.batchIndexedFields ?? this.indexStore.getIndexedFields(this.schema.indexTableName);
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

  /**
   * Flush all buffered entity rows to the sheet in a single API call.
   *
   * Called at the end of {@link saveAll}.  Sorts buffered entries by
   * `dataIndex` to build a contiguous row array, then writes them via
   * `sheet.updateRows()` (or `writeAllRowsWithHeaders` for the L1 path).
   *
   * @param sheet - The sheet adapter to write to.
   */
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
    // Sort by dataIndex for contiguous writes
    const sorted = [...batch].sort((a, b) => a.dataIndex - b.dataIndex);
    // Write header row + data rows in a single API call for newly-created sheets
    if (this.headersDeferred) {
      sheet.writeAllRowsWithHeaders(
        this.headers,
        sorted.map((item) => item.row as unknown[]),
      );
      this.headersDeferred = false;
    } else {
      sheet.updateRows(sorted.map((item) => ({ rowIndex: item.dataIndex, values: item.row })));
    }
  }

  /** Shallow-clone an entity to prevent external mutation of cached data. */
  private cloneEntity(entity: T): T {
    return { ...entity };
  }

  /** Shallow-clone an array of entities. */
  private cloneEntities(entities: T[]): T[] {
    return entities.map((entity) => this.cloneEntity(entity));
  }

  /**
   * Update the entity cache in place after a save (create or update).
   *
   * In batch mode, reuses {@link batchCachedData} to avoid repeated
   * `cache.get()` calls that would trigger verbose logging overhead.
   *
   * @param entity - The saved entity.
   * @param isNew  - Whether this was a create (append) or update (replace).
   */
  private updateCacheAfterSave(entity: T, isNew: boolean): void {
    if (!this.cache) return;
    // In batch mode, reuse pre-fetched array ref to avoid N × cache.get() Logger.log() calls in GAS.
    // batchCachedData is lazily initialised here on the first entity (after the cache entry is created
    // by the CREATE path in doSave for entity #1 when dataIndex === 0).
    let cached: T[] | null;
    if (this.entityBatch !== null) {
      if (!this.batchCachedData) {
        this.batchCachedData = this.cache.get<T[]>(this.dataCacheKey);
      }
      cached = this.batchCachedData;
    } else {
      cached = this.cache.get<T[]>(this.dataCacheKey);
    }
    if (!cached) return;

    if (isNew) {
      // Append new entity to the cached array
      cached.push(entity);
    } else {
      // Replace existing entity in the cached array
      for (let i = 0; i < cached.length; i++) {
        if (cached[i]?.__id === entity.__id) {
          cached[i] = entity;
          return;
        }
      }
    }
  }

  /**
   * Remove a deleted entity from the entity cache.
   *
   * Note: {@link idToRowIndex} is updated separately in {@link doDelete}.
   * This method only handles the entity array cache.
   *
   * @param id - The `__id` of the deleted entity.
   */
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

  /**
   * Check whether a field name has an `@Indexed` decorator.
   *
   * Uses the pre-built {@link indexedFieldNames} set for O(1) lookup.
   *
   * @param fieldName - Name of the field to check.
   * @returns `true` if the field is indexed.
   */
  private isIndexedField(fieldName: string): boolean {
    return this.indexedFieldNames.has(fieldName);
  }
}
