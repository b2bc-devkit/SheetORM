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
import type { Filter } from "../core/types/Filter.js";
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
  /** Cache key for the persisted id→rowIndex map (derived-structure warm start). */
  private dixCacheKey: string;
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
  /** Running count of buffered "create" entries — avoids O(n²) entityBatch.filter() per save. */
  private batchCreateCount = 0;
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
  /** Tombstoned (logically deleted) physical rows — only when schema.tombstones. */
  private tombstoneRows = 0;

  /** Marker written into `__id` by tombstone deletes (`schema.tombstones`). */
  private static readonly TOMBSTONE_ID = "#TOMB#";

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

    // Build column header array from field definitions (used for serialisation).
    // Packed storage: `__id | __data` — fields live inside the JSON payload.
    this.headers = schema.packed
      ? Serialization.buildPackedHeaders()
      : Serialization.buildHeaders(schema.fields);
    // Locate the __id column position for fast primary-key lookups
    this.idColIdx = this.headers.indexOf(SystemColumns.ID);

    // Pre-filter required and defaultable fields for validation/defaults in save()
    this.requiredFields = schema.fields.filter((f) => f.required);
    this.defaultableFields = schema.fields.filter((f) => f.defaultValue !== undefined);

    // Cache key for the data cache (all-entity array)
    this.dataCacheKey = `data:${schema.tableName}`;
    this.dixCacheKey = `dix:${schema.tableName}`;

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
              existingEntity = this.rowToEntity<T>(data[i], this.headers, this.schema.fields, this.fieldMap);
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

      const row = this.entityToRow(entity, this.schema.fields, this.headers, this.fieldMap);

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
      const dataIndex = baseCount + (this.entityBatch ? this.batchCreateCount : 0);

      // Bootstrap idToRowIndex and cache when first entity is written to an empty sheet
      if (!this.idToRowIndex) {
        if (dataIndex === 0) {
          this.idToRowIndex = new Map();
          this.physicalRowCount = 0;
          this.idToRowIndexComplete = true;
          // Seed empty cache so subsequent finds are cache hits
          if (this.cache && !this.cache.has(this.dataCacheKey)) {
            this.cache.set(this.dataCacheKey, [], this.schema.cacheTtlMs);
          }
        }
      }

      // In entity batch mode (saveAll): buffer the row; otherwise write immediately
      if (this.entityBatch !== null) {
        this.entityBatch.push({ entity, row, dataIndex, mode: "create" });
        this.batchCreateCount++;
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

      const row = this.entityToRow(entity, this.schema.fields, this.headers, this.fieldMap);
      if (this.entityBatch !== null) {
        this.entityBatch.push({ entity, row, dataIndex: existingIdx!, mode: "update" });
      } else if (sheet.updateRowSparse) {
        // Sparse write — emit only cells whose SERIALISED value changed
        // (always includes __updatedAt).  A no-op field save becomes a
        // single-cell write instead of a full-row rewrite.
        const oldRow = this.entityToRow(existingEntity!, this.schema.fields, this.headers, this.fieldMap);
        const cells: Array<readonly [number, unknown]> = [];
        for (let i = 0; i < row.length; i++) {
          if (row[i] !== oldRow[i]) cells.push([i, row[i]]);
        }
        sheet.updateRowSparse(existingIdx!, cells);
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
    this.batchCreateCount = 0;
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
      const savedCreates = this.batchCreateCount;

      // Flush buffered rows to the sheet in one updateRows() call
      this.flushEntityBatch(sheet);

      // Flush index batch (single write per index sheet)
      if (this.schema.indexTableName) {
        this.indexStore.flushIndexBatch();
      }

      // Single write-through commit for all in-place cache mutations made
      // during the batch (updateCacheAfterSave defers them in batch mode).
      if (this.cache) {
        const cached = this.batchCachedData ?? this.cache.get<T[]>(this.dataCacheKey);
        if (cached) this.commitDataCache(cached);
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
      this.batchCreateCount = 0;
      this.batchSheet = null;
      this.batchBaseRowCount = null;
      this.batchCachedData = null;
      this.batchIndexedFields = null;
      if (this.schema.indexTableName) {
        this.indexStore.cancelIndexBatch();
      }
      if (this.cache) {
        this.cache.delete(this.dataCacheKey);
        this.cache.delete(this.dixCacheKey);
      }
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
    if (!options) return this.cloneEntities(this.loadAllEntities());

    if (options.where && !options.whereGroups && this.schema.indexTableName) {
      const idxTable = this.schema.indexTableName;
      // Separate index-answerable predicates from residual filters
      const searchFilters: { field: string; value: string }[] = [];
      const eqFilters: Filter[] = [];
      const otherFilters: typeof options.where = [];

      for (const f of options.where) {
        if (f.operator === "search" && this.isIndexedField(f.field)) {
          searchFilters.push({ field: f.field, value: String(f.value) });
        } else if (this.isIndexNarrowable(f)) {
          eqFilters.push(f);
        } else {
          otherFilters.push(f);
        }
      }

      // Index narrowing — n-gram postings (search) AND equality postings
      // (`=`/`in`) both resolve to entity-id sets via the combined index;
      // intersecting them preserves AND semantics.
      if (searchFilters.length > 0 || eqFilters.length > 0) {
        let candidateIds: Set<string> | null = null;
        const intersect = (cur: Set<string> | null, idSet: Set<string>): Set<string> => {
          if (cur === null) return idSet;
          for (const id of cur) {
            if (!idSet.has(id)) cur.delete(id);
          }
          return cur;
        };

        for (const sf of searchFilters) {
          candidateIds = intersect(
            candidateIds,
            new Set(this.indexStore.searchCombined(idxTable, sf.field, sf.value)),
          );
        }
        for (const ef of eqFilters) {
          const vals = ef.operator === "in" ? (ef.value as unknown[]) : [ef.value];
          const set = new Set<string>();
          for (const v of vals) {
            for (const id of this.indexStore.lookupCombined(idxTable, ef.field, String(v))) {
              set.add(id);
            }
          }
          candidateIds = intersect(candidateIds, set);
        }

        if (!candidateIds || candidateIds.size === 0) return [];

        // Sparse-read path: fetch ONLY the candidate rows (one batchGet)
        // instead of scanning the whole table — executed BEFORE any full
        // loadAllEntities() so a selective query never pays for it.
        // Drift-safe — every fetched row's __id must itself be a candidate;
        // any mismatch falls back to the scan path.
        if (candidateIds.size <= 500) {
          const sparse = this.readEntitiesByIds(candidateIds);
          if (sparse !== null) {
            // Re-apply the FULL predicate set — the index only narrows the
            // search space; correctness is re-proven row by row.
            return this.cloneEntities(QueryEngine.executeQuery(sparse, options));
          }
        }

        // Fallback: narrow the fully-loaded entity list in memory.
        const all = this.loadAllEntities();
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
    const all = this.loadAllEntities();
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

    if (this.schema.tombstones) {
      // Tombstone delete: overwrite __id with the marker — one cell write,
      // zero row-shifting.  The physical row is reclaimed by compaction once
      // dead space crosses the threshold.
      this.markTombstoneRow(sheet, rowIdx);
      this.idToRowIndex?.delete(id); // no shift: physical rows don't move
      this.tombstoneRows++;
      this.updateCacheAfterDelete(id);
      if (this.hooks.afterDelete) this.hooks.afterDelete(id);
      this.maybeCompactTombstones();
      return true;
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

    // Single deletions: the dedicated path handles bookkeeping inline.
    if (toDelete.length === 1) {
      return this.doDelete(toDelete[0].__id) ? 1 : 0;
    }

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

    // ── Tombstone mode ────────────────────────────────────────────────────
    // Mark deleted rows in place (one batched sparse write for all of them —
    // zero row-shift) unless the delete covers EVERYTHING, where a physical
    // clear is strictly better than leaving a full sheet of tombstones.
    if (this.schema.tombstones && remaining.length > 0) {
      for (const entity of toDelete) {
        if (!deleteIds.has(entity.__id)) continue;
        const idx = this.idToRowIndex?.get(entity.__id);
        if (idx !== undefined) this.markTombstoneRow(sheet, idx);
      }
      if (this.schema.indexTableName) {
        this.indexStore.removeMultipleFromCombined(this.schema.indexTableName, [...deleteIds]);
      }
      for (const id of deleteIds) {
        if (this.hooks.afterDelete) this.hooks.afterDelete(id);
      }
      if (this.idToRowIndex) {
        for (const id of deleteIds) this.idToRowIndex.delete(id); // no shift
      }
      if (this.cache) {
        const cached = this.cache.get<T[]>(this.dataCacheKey);
        if (cached) {
          for (let i = 0; i < cached.length; i++) {
            if (cached[i] && deleteIds.has(cached[i].__id)) {
              cached[i] = { __id: SheetRepository.TOMBSTONE_ID } as T;
            }
          }
          this.commitDataCache(cached);
        }
      }
      this.tombstoneRows += deleteIds.size;
      this.persistRowIndex();
      this.maybeCompactTombstones();
      return deleteIds.size;
    }

    // ── Adaptive strategy ─────────────────────────────────────────────────
    // Sparse deletions (deleted < remaining): ONE batched deleteDimension
    // request — request payload ∝ deleted rows, survivors never rewritten.
    // Dense deletions: replaceAllData — payload ∝ survivors instead.
    // Both are ~1 write RPC; the choice minimises bytes on the wire.
    const sparse = deleteIds.size < remaining.length;
    let deletedIndexes: number[] | null = null;
    if (sparse) {
      deletedIndexes = [];
      for (const id of deleteIds) {
        const idx = this.idToRowIndex?.get(id);
        if (idx === undefined) {
          deletedIndexes = null; // unresolvable position → dense fallback
          break;
        }
        deletedIndexes.push(idx);
      }
      if (deletedIndexes !== null) {
        sheet.deleteRows(deletedIndexes);
      }
    }
    if (deletedIndexes === null) {
      const rows = remaining.map((e) => this.entityToRow(e, this.schema.fields, this.headers, this.fieldMap));
      sheet.replaceAllData(rows);
    }

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
      this.cache.set(this.dataCacheKey, remaining, this.schema.cacheTtlMs);
    }

    const rowIndex = new Map<string, number>();
    if (deletedIndexes !== null) {
      // Sparse path: survivors keep relative order; their physical index
      // shifts left by the number of deleted rows below them.
      const sortedDel = [...deletedIndexes].sort((a, b) => a - b);
      for (const e of remaining) {
        const oldIdx = this.idToRowIndex?.get(e.__id);
        if (oldIdx === undefined) continue;
        // Count deleted indexes strictly below oldIdx (binary search on sorted list)
        let lo = 0;
        let hi = sortedDel.length;
        while (lo < hi) {
          const mid = (lo + hi) >> 1;
          if (sortedDel[mid] < oldIdx) lo = mid + 1;
          else hi = mid;
        }
        rowIndex.set(e.__id, oldIdx - lo);
      }
      if (this.physicalRowCount !== null) this.physicalRowCount -= deleteIds.size;
    } else {
      // Dense path: survivors were rewritten contiguously at the top.
      for (let i = 0; i < remaining.length; i++) {
        rowIndex.set(remaining[i].__id, i);
      }
      this.physicalRowCount = remaining.length;
      this.tombstoneRows = 0; // physical rewrite dropped every marker
    }
    this.idToRowIndex = rowIndex;
    this.idToRowIndexComplete = true;
    this.persistRowIndex();

    return deleteIds.size;
  }

  /**
   * Count entities matching a query.
   *
   * @param options - Optional query options (only `where`/`whereGroups` are meaningful).
   * @returns Total count of matching entities.
   */
  count(options?: QueryOptions): number {
    // Preserve the re-entrancy guard: reads are forbidden while an entity
    // batch (saveAll) is active — delegates to loadAllEntities' throw.
    if (this.entityBatch) this.loadAllEntities();
    if (!options || (!options.where && !options.whereGroups)) {
      // No-filter fast paths — zero deserialization:
      //   · cache hit          → cached entity count (exact)
      //   · complete row index → its size IS the valid-entity count
      //   · cold start         → physical row count via the adapter's cached
      //                          grid/meta (one cheap call; for ORM-managed
      //                          sheets physical rows == valid entities —
      //                          phantom rows only arise from corruption).
      const cached = this.cache?.get<T[]>(this.dataCacheKey);
      if (cached != null) {
        if (!this.schema.tombstones) return cached.length;
        // Tombstone mode: slots include dead rows — count live ids only.
        let live = 0;
        for (const e of cached) if (this.isLiveId(e?.__id)) live++;
        return live;
      }
      if (this.idToRowIndexComplete && this.idToRowIndex) return this.idToRowIndex.size;
      if (this.schema.tombstones) {
        // Physical row count includes tombstoned rows — need the live-id
        // count.  One narrow column read beats a full-grid load.
        const ids = this.getSheet().readIdsColumn?.(this.idColIdx);
        if (ids) {
          let live = 0;
          for (const v of ids) if (this.isLiveId(v === null || v === undefined ? v : String(v))) live++;
          return live;
        }
        return this.loadAllEntities().length;
      }
      return this.getSheet().getRowCount();
    }
    const all = this.loadAllEntities();
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
      if (this.cache) {
        this.cache.delete(this.dataCacheKey);
        this.cache.delete(this.dixCacheKey);
      }
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

    const sheet = this.getSheet();

    // Tombstone mode: mark deleted rows in place instead of rewriting the
    // survivors (unless nothing survives — then a physical clear wins).
    if (this.schema.tombstones && remaining.length > 0) {
      for (const id of deleteIds) {
        const idx = this.idToRowIndex?.get(id);
        if (idx !== undefined) this.markTombstoneRow(sheet, idx);
      }
      if (this.schema.indexTableName) {
        this.indexStore.removeMultipleFromCombined(this.schema.indexTableName, deleteIds);
      }
      if (this.hooks.afterDelete) {
        for (const id of deleteIds) this.hooks.afterDelete(id);
      }
      if (this.idToRowIndex) {
        for (const id of deleteIds) this.idToRowIndex.delete(id);
      }
      if (this.cache) {
        const cached = this.cache.get<T[]>(this.dataCacheKey);
        if (cached) {
          for (let i = 0; i < cached.length; i++) {
            if (cached[i] && deleteSet.has(cached[i].__id)) {
              cached[i] = { __id: SheetRepository.TOMBSTONE_ID } as T;
            }
          }
          this.commitDataCache(cached);
        }
      }
      this.tombstoneRows += deleteIds.length;
      this.persistRowIndex();
      this.maybeCompactTombstones();
      return;
    }

    const rows = remaining.map((e) => this.entityToRow(e, this.schema.fields, this.headers, this.fieldMap));
    sheet.replaceAllData(rows);
    this.tombstoneRows = 0; // physical rewrite dropped every marker

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
      this.cache.set(this.dataCacheKey, remaining, this.schema.cacheTtlMs);
    }

    const rowIndex = new Map<string, number>();
    for (let i = 0; i < remaining.length; i++) {
      rowIndex.set(remaining[i].__id, i);
    }
    this.idToRowIndex = rowIndex;
    this.physicalRowCount = rows.length;
    this.idToRowIndexComplete = true;
    this.persistRowIndex();
  }
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

    // Cache hit path — also rehydrate the derived structures the miss path
    // builds (idToRowIndex / physicalRowCount / complete flag).  A persisted
    // data cache (GasCacheProvider) makes this the ONLY thing needed for a
    // warm cross-execution start: no extra RPC, no separate index payload —
    // entity array order IS row order.  Guard: the mapping is only valid
    // when every physical row produced an entity (length match), otherwise
    // phantom/gap rows would shift all indexes.
    const sheet = this.getSheet();
    if (this.cache) {
      const cached = this.cache.get<T[]>(this.dataCacheKey);
      if (cached !== null) {
        const physical = sheet.getRowCount();
        if (physical === cached.length) {
          const rowIndex = new Map<string, number>();
          let tombs = 0;
          for (let i = 0; i < cached.length; i++) {
            const id = cached[i].__id;
            if (id === SheetRepository.TOMBSTONE_ID) tombs++;
            else if (this.isLiveId(id)) rowIndex.set(id, i);
          }
          this.idToRowIndex = rowIndex;
          this.physicalRowCount = physical;
          this.idToRowIndexComplete = true;
          if (this.schema.tombstones) this.tombstoneRows = tombs;
          this.persistRowIndex(); // keep dix: warm for cold-start executions
        }
        // Tombstone mode: cached slots include dead rows — return live only.
        return this.schema.tombstones ? cached.filter((e) => this.isLiveId(e?.__id)) : cached;
      }
    }

    // Cache miss: read all rows from the sheet
    const data = sheet.getAllData();
    const len = data.length;
    const headers = this.headers;
    const fields = this.schema.fields;
    const fMap = this.fieldMap;
    const entities: T[] = [];
    const rowIndex = new Map<string, number>();
    // Tombstone mode: the cache payload must preserve physical row slots —
    // dead rows go in as marker entities so cached.length stays === physical.
    const slots: T[] | null = this.schema.tombstones ? [] : null;
    let tombs = 0;

    // Deserialise each row, skipping rows with invalid/missing IDs
    for (let i = 0; i < len; i++) {
      const entity = this.rowToEntity<T>(data[i], headers, fields, fMap);
      const id = entity.__id;
      if (this.isLiveId(id)) {
        rowIndex.set(id, i);
        entities.push(entity);
        slots?.push(entity);
      } else if (slots) {
        if (id === SheetRepository.TOMBSTONE_ID) tombs++;
        slots.push({ __id: id === SheetRepository.TOMBSTONE_ID ? SheetRepository.TOMBSTONE_ID : "" } as T);
      }
    }
    if (slots) this.tombstoneRows = tombs;

    // Update in-memory state
    this.idToRowIndex = rowIndex;
    this.physicalRowCount = data.length;
    this.idToRowIndexComplete = true;

    if (this.cache) {
      this.cache.set(this.dataCacheKey, slots ?? entities, this.schema.cacheTtlMs);
    }
    this.persistRowIndex();
    if (slots) this.maybeCompactTombstones();

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
      if (this.isLiveId(rowId)) rowIndex.set(rowId, i);
      if (rowId === id) result = i;
    }
    this.idToRowIndex = rowIndex;
    this.physicalRowCount = data.length;
    this.idToRowIndexComplete = true;
    this.persistRowIndex();
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
      this.batchCreateCount = 0;
      return;
    }
    const batch = this.entityBatch;
    this.entityBatch = null;
    this.batchCreateCount = 0;
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
      // Dirty-slice merging: build write spans that are contiguous on the
      // sheet.  When an UPDATE row is separated from the previous span row by
      // a gap, the gap positions are re-filled with reserialised rows of the
      // entities currently occupying them (rewritten with identical values),
      // turning N scattered writes into one bounding-box setValues() call.
      // If a gap row can't be resolved from cache, the span is flushed and a
      // new one starts — falling back to the old contiguous-group behaviour.
      const idxToEntity = this.buildIndexToEntityMap();
      let spanStart = sorted[0].dataIndex;
      let spanRows: unknown[][] = [sorted[0].row];
      let prev = sorted[0].dataIndex;
      const emitSpan = () => sheet.writeRowsAt(spanStart, spanRows);
      for (let i = 1; i < sorted.length; i++) {
        const idx = sorted[i].dataIndex;
        if (idx === prev + 1) {
          spanRows.push(sorted[i].row);
        } else if (idx > prev + 1 && idxToEntity !== null) {
          // Fill gap rows prev+1 .. idx-1 from the entity cache
          const fill: unknown[][] = [];
          let resolvable = true;
          for (let g = prev + 1; g < idx; g++) {
            const gapEntity = idxToEntity.get(g);
            if (!gapEntity) {
              resolvable = false;
              break;
            }
            fill.push(this.entityToRow(gapEntity, this.schema.fields, this.headers, this.fieldMap));
          }
          if (resolvable) {
            spanRows.push(...fill, sorted[i].row);
          } else {
            emitSpan();
            spanStart = idx;
            spanRows = [sorted[i].row];
          }
        } else {
          emitSpan();
          spanStart = idx;
          spanRows = [sorted[i].row];
        }
        prev = idx;
      }
      emitSpan();
    }
  }

  /**
   * Build a dataIndex → entity map for gap-filling merged write spans.
   * Returns null when the entity cache or row index is unavailable, in which
   * case flushEntityBatch falls back to contiguous grouping only.
   */
  private buildIndexToEntityMap(): Map<number, T> | null {
    if (!this.cache || !this.idToRowIndex) return null;
    const cached = this.batchCachedData ?? this.cache.get<T[]>(this.dataCacheKey);
    if (!cached) return null;
    const map = new Map<number, T>();
    for (const entity of cached) {
      const idx = this.idToRowIndex.get(entity.__id);
      if (idx !== undefined) map.set(idx, entity);
    }
    return map;
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
   * Re-persist a mutated entity array under `data:<table>`.
   *
   * In-place mutations (push/splice/replace) update a reference-stored
   * MemoryCache transparently, but providers with a remote tier
   * (e.g. {@link GasCacheProvider}) only observe explicit `set()` calls —
   * without this re-set the remote copy would stay stale for the whole TTL
   * and leak into subsequent executions.
   *
   * @param cached - The entity array that was just mutated in place.
   */
  private commitDataCache(cached: T[]): void {
    this.cache?.set(this.dataCacheKey, cached, this.schema.cacheTtlMs);
  }

  /**
   * Update the entity cache in place after a save (create or update).
   *
   * In batch mode, reuses {@link batchCachedData} to avoid repeated
   * `cache.get()` calls that would trigger verbose logging overhead, and
   * defers the remote write-through to a single commit at the end of
   * {@link saveAll} (one `set()` for the whole batch, not one per entity).
   *
   * @param entity - The saved entity.
   * @param isNew  - Whether this was a create (append) or update (replace).
   */
  private updateCacheAfterSave(entity: T, isNew: boolean): void {
    if (!this.cache) return;
    // In batch mode, reuse pre-fetched array ref to avoid N × cache.get() Logger.log() calls in GAS.
    // batchCachedData is lazily initialised here on the first entity (after the cache entry is created
    // by the CREATE path in doSave for entity #1 when dataIndex === 0).
    const inBatch = this.entityBatch !== null;
    let cached: T[] | null;
    if (inBatch) {
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
          break;
        }
      }
    }
    // Batch mode defers the remote write-through to saveAll()'s single commit.
    if (!inBatch) this.commitDataCache(cached);
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
        if (this.schema.tombstones) {
          // Tombstone mode: keep the slot (the physical row still exists) —
          // mark it in place so cache↔row alignment is preserved.
          cached[i] = { __id: SheetRepository.TOMBSTONE_ID } as T;
        } else {
          cached.splice(i, 1);
        }
        this.commitDataCache(cached); // write-through for remote-tier providers
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

  /**
   * True when a filter predicate can be answered by the combined index:
   * `=`/`in` on an @Indexed field with no null/empty arm (those values are
   * deliberately not indexed — `addToIndexes` skips them, so they would
   * break recall).
   */
  private isIndexNarrowable(f: Filter): boolean {
    if (!this.isIndexedField(f.field)) return false;
    const nonEmpty = (v: unknown) => v !== null && v !== undefined && v !== "";
    if (f.operator === "=") return nonEmpty(f.value);
    if (f.operator === "in") {
      return Array.isArray(f.value) && f.value.length > 0 && f.value.every(nonEmpty);
    }
    return false;
  }

  /**
   * Serialise an entity into its sheet row — packed storage writes
   * `[__id, json]`; the default layout writes one column per field.
   * Same signature as `Serialization.entityToRow` so call sites stay
   * layout-agnostic.
   */
  private entityToRow(
    entity: T,
    fields: FieldDefinition[],
    headers: string[],
    fieldMap: Map<string, FieldDefinition>,
  ): unknown[] {
    return this.schema.packed
      ? Serialization.entityToPackedRow(entity, fields, fieldMap)
      : Serialization.entityToRow(entity, fields, headers, fieldMap);
  }

  /** Deserialise a sheet row into an entity — packed-aware counterpart of {@link entityToRow}. */
  private rowToEntity<T2 extends Entity>(
    row: unknown[],
    headers: string[],
    fields: FieldDefinition[],
    fieldMap: Map<string, FieldDefinition>,
  ): T2 {
    return this.schema.packed
      ? Serialization.packedRowToEntity<T2>(row, fields, fieldMap)
      : Serialization.rowToEntity<T2>(row, headers, fields, fieldMap);
  }

  /** Whether an `__id` cell value represents a live (queryable) entity. */
  private isLiveId(id: unknown): boolean {
    return (
      id !== undefined &&
      id !== null &&
      id !== "" &&
      id !== "undefined" &&
      id !== "null" &&
      id !== SheetRepository.TOMBSTONE_ID
    );
  }

  /**
   * Tombstone-delete the physical row at `rowIdx`: overwrite `__id` with the
   * marker — no row-shift, no rewrite of surviving rows.  Uses a sparse cell
   * write when the adapter supports it, otherwise a full-row overwrite with
   * the marker spliced in.
   */
  private markTombstoneRow(sheet: ISheetAdapter, rowIdx: number): void {
    if (sheet.updateRowSparse) {
      sheet.updateRowSparse(rowIdx, [[this.idColIdx, SheetRepository.TOMBSTONE_ID]]);
      return;
    }
    const row = sheet.getRow(rowIdx);
    row[this.idColIdx] = SheetRepository.TOMBSTONE_ID;
    sheet.updateRow(rowIdx, row);
  }

  /**
   * Reclaim tombstoned rows once dead space crosses the threshold
   * (≥4 tombstones AND ≥25% of physical rows).  One batched deleteDimension
   * per pass; `idToRowIndex`, the entity cache and the row counter are all
   * rebuilt afterwards.
   */
  private maybeCompactTombstones(): void {
    if (!this.schema.tombstones || this.tombstoneRows === 0) return;
    const phys = this.physicalRowCount;
    if (phys === null) return;
    // Compact when dead space crosses 25% (with a 4-row minimum to avoid
    // churn on tiny tables), or when EVERY row is dead — a table of pure
    // tombstones should always collapse to empty.
    const dead = this.tombstoneRows;
    if (!(dead === phys || (dead >= 4 && dead * 4 >= phys))) return;

    const sheet = this.getSheet();
    const data = sheet.getAllData();
    const col = this.idColIdx;
    const tombIdx: number[] = [];
    for (let i = 0; i < data.length; i++) {
      if (data[i][col] === SheetRepository.TOMBSTONE_ID) tombIdx.push(i);
    }
    if (tombIdx.length === 0) {
      this.tombstoneRows = 0;
      return;
    }
    sheet.deleteRows(tombIdx);

    // Survivors shift left by the count of tombstones below them.
    const sortedDel = tombIdx; // already ascending (scan order)
    const rowIndex = new Map<string, number>();
    if (this.idToRowIndex) {
      for (const [id, oldIdx] of this.idToRowIndex) {
        let lo = 0;
        let hi = sortedDel.length;
        while (lo < hi) {
          const mid = (lo + hi) >> 1;
          if (sortedDel[mid] < oldIdx) lo = mid + 1;
          else hi = mid;
        }
        rowIndex.set(id, oldIdx - lo);
      }
      this.idToRowIndex = rowIndex;
    }
    this.physicalRowCount = data.length - tombIdx.length;
    this.tombstoneRows = 0;

    // Drop tombstone slots from the entity cache (kept aligned with rows).
    if (this.cache) {
      const cached = this.cache.get<T[]>(this.dataCacheKey);
      if (cached) {
        const delSet = new Set(tombIdx);
        this.commitDataCache(cached.filter((_, i) => !delSet.has(i)));
      }
    }
    this.persistRowIndex();
    SheetOrmLogger.log(
      `[Repo:${this.schema.tableName}] tombstone compaction — reclaimed ${tombIdx.length} rows`,
    );
  }

  /**
   * Persist the current id→rowIndex map under `dix:<table>` — the derived
   * structure survives across executions through a persistent provider
   * (GasCacheProvider).  Always paired with `physicalRowCount` so staleness
   * is detectable via a cheap rowCount comparison.
   */
  private persistRowIndex(): void {
    if (!this.cache || !this.idToRowIndex || this.physicalRowCount === null) return;
    this.cache.set(
      this.dixCacheKey,
      { r: this.physicalRowCount, m: [...this.idToRowIndex] },
      this.schema.cacheTtlMs,
    );
  }

  /**
   * Resolve a COMPLETE id→dataRowIndex map without a full-grid read, or
   * return `null` to signal "fall back to loadAllEntities".
   *
   * Order of attempts:
   *  1. In-session complete map (already verified this execution).
   *  2. Persisted `dix:` entry — trusted only when its stored physical row
   *     count still matches the sheet (catches structural drift; content
   *     drift is caught per-row by readEntitiesByIds' id verification).
   *  3. A single narrow ids-column read (`A2:A`) — tiny payload, rebuilds
   *     the full map and re-persists it.
   */
  private resolveRowIndex(sheet: ISheetAdapter): Map<string, number> | null {
    if (this.idToRowIndexComplete && this.idToRowIndex) return this.idToRowIndex;

    if (this.cache) {
      const dix = this.cache.get<{ r: number; m: Array<[string, number]> }>(this.dixCacheKey);
      if (dix !== null) {
        if (dix.r === sheet.getRowCount()) {
          const map = new Map<string, number>(dix.m);
          this.idToRowIndex = map;
          this.physicalRowCount = dix.r;
          this.idToRowIndexComplete = true;
          return map;
        }
        // Stale row count — drop the derived entry.
        this.cache.delete(this.dixCacheKey);
      }
    }

    if (sheet.readIdsColumn) {
      const ids = sheet.readIdsColumn(this.idColIdx);
      if (ids) {
        const map = new Map<string, number>();
        let tombs = 0;
        for (let i = 0; i < ids.length; i++) {
          const v = ids[i];
          if (v === SheetRepository.TOMBSTONE_ID) tombs++;
          else if (this.isLiveId(v)) map.set(String(v), i);
        }
        this.idToRowIndex = map;
        this.physicalRowCount = ids.length;
        this.idToRowIndexComplete = true;
        if (this.schema.tombstones) {
          this.tombstoneRows = tombs;
          this.maybeCompactTombstones();
        }
        this.persistRowIndex();
        return map;
      }
    }
    return null;
  }

  /**
   * Fetch ONLY the rows for `candidateIds` — one `Values.batchGet` with N
   * row ranges instead of a full-table read.  Rows are deserialised in
   * sheet order (same ordering a full scan produces).
   *
   * Drift safety: every fetched row must carry an `__id` that is itself a
   * candidate; any mismatch/empty row means the row map drifted → return
   * `null` and let the caller fall back to the full-scan path.
   */
  private readEntitiesByIds(candidateIds: Set<string>): T[] | null {
    const sheet = this.getSheet();
    if (!sheet.readRowsAt) return null;
    const map = this.resolveRowIndex(sheet);
    if (!map) return null;

    const hits: Array<{ idx: number }> = [];
    for (const id of candidateIds) {
      const idx = map.get(id);
      if (idx !== undefined) hits.push({ idx });
      // id absent from a verified map → stale index residue; skip.
    }
    hits.sort((a, b) => a.idx - b.idx); // preserve sheet-order semantics

    const rows = sheet.readRowsAt(hits.map((h) => h.idx));
    if (rows.length !== hits.length) return null;
    const entities: T[] = [];
    for (const row of rows) {
      if (!row) return null;
      const ent = this.rowToEntity<T>(row, this.headers, this.schema.fields, this.fieldMap);
      if (!ent.__id || !candidateIds.has(ent.__id)) return null; // drift → rescan
      entities.push(ent);
    }
    return entities;
  }
}
