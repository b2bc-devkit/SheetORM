/**
 * IndexStore — secondary index manager for SheetORM.
 *
 * Manages per-class index sheets that accelerate lookup and full-text
 * search operations.  Each Record class with `@Indexed` fields gets a
 * single index sheet named `idx_{ClassName}s` (e.g. `idx_Cars`).
 *
 * **Sheet layout** (3-column, no header row since J1 optimisation):
 * ```
 *   Col A (field)    Col B (value)    Col C (entityId)
 *   "brand"          "Toyota"         "abc-123"
 *   "model"          "Corolla"        "abc-123"
 * ```
 *
 * **Search strategy**: Solr-like n-gram search (trigram by default).
 * Text is normalised → tokenised → trigram-indexed.  Queries intersect
 * posting lists and verify candidates with a substring match.
 *
 * **Performance optimisations** (codenames reference the B-series and
 * C-series GAS API reduction work):
 *
 *   - **B3/B5**: Known row count avoids `getAllData()` on the write path.
 *   - **B6**: Empty-table detection skips `getAllData()` entirely.
 *   - **C1**: `createCombinedIndex` seeds `indexRowCount` with one
 *     `getLastRow()` call so subsequent appends never need a full read.
 *   - **J1**: Newly created index sheets skip `setHeaders()` — column
 *     positions are hard-coded (0=field, 1=value, 2=entityId).
 *   - Sheet references are memoised in `indexSheetCache` to avoid
 *     repeated `getSheetByName()` API calls (~300 ms each in GAS).
 *   - Batch mode (`beginIndexBatch` / `flushIndexBatch`) accumulates
 *     all index writes in memory and flushes them in a single
 *     `writeRowsAt()` call per index table.
 *
 * @module IndexStore
 */

import type { ISpreadsheetAdapter } from "../core/types/ISpreadsheetAdapter.js";
import type { ISheetAdapter } from "../core/types/ISheetAdapter.js";
import type { ICacheProvider } from "../core/types/ICacheProvider.js";
import type { IndexMeta } from "./IndexMeta.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

/**
 * In-memory n-gram search index for a single (tableName, field) pair.
 *
 * Built lazily by {@link IndexStore.buildSearchIndex} and cached in
 * `searchIndexCache`.  Invalidated on any write to the parent index table.
 *
 * - `entries` — raw (value, entityId) pairs extracted from the sheet.
 * - `normalized` — lowercase/collapsed version of each value (parallel array).
 * - `tokenIndex` — exact-token → posting list (entry indices).
 * - `ngramIndex` — trigram → posting list for fuzzy matching.
 */
interface SearchIndex {
  entries: Array<{ value: string; entityId: string }>;
  normalized: string[];
  tokenIndex: Map<string, number[]>;
  ngramIndex: Map<string, number[]>;
}

/**
 * Manages secondary index sheets and provides n-gram text search.
 *
 * One IndexStore instance is owned by a {@link Registry} and shared
 * across all SheetRepository instances created from that registry.
 */
export class IndexStore {
  /** Spreadsheet-level adapter for creating/deleting/finding sheets. */
  private adapter: ISpreadsheetAdapter;
  /** Optional cache provider for memoising raw index data between reads. */
  private cache: ICacheProvider | null;
  /** Registry of all declared indexes: key = "tableName::field" → IndexMeta. */
  private indexRegistry: Map<string, IndexMeta> = new Map();
  /** Lazy-built n-gram search indexes, keyed by "tableName::field". */
  private searchIndexCache: Map<string, SearchIndex> = new Map();
  /** Pending batch writes (tableName → rows), or null when batch mode is off. */
  private indexBatch: Map<string, unknown[][]> | null = null;
  /** Memoised sheet references — avoids duplicate getSheetByName() API calls within a session. */
  private indexSheetCache: Map<string, ISheetAdapter> = new Map();
  /** Tracks known row count per index table — avoids full getAllData() reads to determine append position. */
  private indexRowCount: Map<string, number> = new Map();
  /** Character-level n-gram length used for search indexing (trigram = 3). */
  private static readonly NGRAM_SIZE = 3;

  /**
   * @param adapter - Spreadsheet adapter for sheet-level operations.
   * @param cache   - Optional cache provider (e.g. MemoryCache) for index data.
   */
  constructor(adapter: ISpreadsheetAdapter, cache?: ICacheProvider) {
    this.adapter = adapter;
    this.cache = cache ?? null;
  }

  /**
   * Build a composite registry key for a (tableName, field) pair.
   * Used as the key in `indexRegistry` and for search cache invalidation.
   */
  private registryKey(tableName: string, field: string): string {
    return `${tableName}::${field}`;
  }

  /**
   * Retrieve all registered {@link IndexMeta} entries for a given table.
   *
   * @param tableName - Index table name (e.g. `"idx_Cars"`).
   * @returns Array of IndexMeta for every indexed field in that table.
   */
  getIndexedFields(tableName: string): IndexMeta[] {
    const result: IndexMeta[] = [];
    for (const meta of this.indexRegistry.values()) {
      if (meta.tableName === tableName) {
        result.push(meta);
      }
    }
    return result;
  }

  /**
   * Register an indexed field during schema initialisation.
   *
   * Called by {@link Registry.registerClass} for each `@Indexed` decorator
   * found on a Record subclass.
   *
   * @param tableName - Index table name (e.g. `"idx_Cars"`).
   * @param field     - Field name to index (e.g. `"brand"`).
   * @param unique    - Whether the index enforces uniqueness.
   */
  registerIndex(tableName: string, field: string, unique: boolean): void {
    this.indexRegistry.set(this.registryKey(tableName, field), {
      tableName,
      field,
      unique,
    });
  }

  // ─── Batch index write buffering ────────────────────────────────────────────
  // When batch mode is active (`indexBatch !== null`), all index writes are
  // accumulated in an in-memory Map instead of hitting the sheet immediately.
  // `flushIndexBatch()` writes all buffered rows in a single `writeRowsAt()`
  // call per index table, reducing GAS API round-trips from N to 1.

  /**
   * Activate batch mode — subsequent `addAllFieldsToCombined` calls will
   * buffer rows instead of writing to the sheet.
   *
   * Must be paired with either {@link flushIndexBatch} (commit) or
   * {@link cancelIndexBatch} (rollback).
   */
  beginIndexBatch(): void {
    this.indexBatch = new Map();
    SheetOrmLogger.log(`[Index] beginIndexBatch`);
  }

  /**
   * Flush all buffered index rows to their respective index sheets.
   *
   * For each index table with pending rows:
   * - **With cache**: reads combined data, appends rows via `writeRowsAt`,
   *   and updates the in-memory cache array.
   * - **Without cache (B5)**: uses the known row count to write at the
   *   correct offset, or falls back to `appendRows`.
   *
   * After flushing, batch mode is deactivated and the search index cache
   * is invalidated for affected tables.
   */
  flushIndexBatch(): void {
    if (!this.indexBatch) return;
    const batch = this.indexBatch;
    this.indexBatch = null; // Deactivate batch mode before writing
    for (const [indexTableName, rows] of batch) {
      if (rows.length === 0) continue;
      const sheet = this.getIndexSheet(indexTableName);
      if (!sheet) continue;
      SheetOrmLogger.log(`[Index:${indexTableName}] flushIndexBatch ${rows.length} rows`);
      if (this.cache) {
        // Cache path: read existing data, append new rows, update cache
        const data = this.getCombinedData(indexTableName);
        sheet.writeRowsAt(data.length, rows);
        for (const row of rows) data.push(row);
        this.indexRowCount.set(indexTableName, data.length);
        this.invalidateSearchCacheForTable(indexTableName);
      } else {
        // No-cache path (B5): use known row count to avoid getAllData()
        const knownCount = this.indexRowCount.get(indexTableName);
        if (knownCount !== undefined) {
          sheet.writeRowsAt(knownCount, rows);
          this.indexRowCount.set(indexTableName, knownCount + rows.length);
        } else {
          // Fallback: let the adapter append (position unknown)
          sheet.appendRows(rows);
        }
        this.invalidateSearchCacheForTable(indexTableName);
      }
    }
  }

  /**
   * Discard all buffered index rows without writing (error/rollback path).
   */
  cancelIndexBatch(): void {
    SheetOrmLogger.log(`[Index] cancelIndexBatch`);
    this.indexBatch = null;
  }

  /**
   * Return a memoised sheet reference for the given index table name.
   *
   * On the first call for a table name, delegates to `adapter.getSheetByName()`
   * (a GAS API call costing ~300 ms).  Subsequent calls return the cached
   * reference from `indexSheetCache`.
   *
   * @param indexTableName - Sheet name (e.g. `"idx_Cars"`).
   * @returns Sheet adapter, or `null` if the sheet does not exist.
   */
  private getIndexSheet(indexTableName: string): ISheetAdapter | null {
    const cached = this.indexSheetCache.get(indexTableName);
    if (cached !== undefined) return cached;
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (sheet) this.indexSheetCache.set(indexTableName, sheet);
    return sheet;
  }

  // ─── Combined (per-class) index sheet methods ───────────────────────────────
  // Each Record class with @Indexed fields gets ONE index sheet named
  // idx_{ClassName}s.  All indexed fields for that class share the sheet,
  // distinguished by column A ("field").

  /**
   * Create or recognise the combined index sheet for a Record class.
   *
   * - If the sheet does **not** exist: creates it via `insertSheet()` and
   *   skips the header row (J1 optimisation — saves ~700 ms).
   * - If the sheet **already** exists: seeds `indexRowCount` with a single
   *   `getLastRow()` call (C1 optimisation) so subsequent writes can append
   *   without reading all data.
   *
   * The caller can pass a pre-loaded sheet reference or `null` (confirmed
   * non-existent) to avoid a redundant `getSheetByName()` lookup.
   *
   * @param indexTableName  - Sheet name (e.g. `"idx_Cars"`).
   * @param preloadedSheet  - Optional: already-fetched sheet, or `null` to
   *                          signal the sheet definitely does not exist.
   */
  createCombinedIndex(indexTableName: string, preloadedSheet?: ISheetAdapter | null): void {
    // undefined = not provided (fall back to getSheetByName)
    // null      = caller confirmed the sheet does not exist (skip getSheetByName, go straight to insertSheet)
    // ISheetAdapter = use this sheet directly
    const existing =
      preloadedSheet !== undefined ? preloadedSheet : this.adapter.getSheetByName(indexTableName);
    if (!existing) {
      // Create new index sheet — J1: skip setHeaders() (column positions are hard-coded)
      const sheet = this.adapter.insertSheet(indexTableName);
      this.indexSheetCache.set(indexTableName, sheet);
      this.indexRowCount.set(indexTableName, 0); // Brand new sheet has 0 data rows
      SheetOrmLogger.log(
        `[Index] createCombinedIndex "${indexTableName}" → insertSheet (J1 no-header) rowCount=0`,
      );
    } else {
      // Sheet already exists — C1: seed row count with one getLastRow() call
      this.indexSheetCache.set(indexTableName, existing);
      const rowCount = existing.getRowCount();
      this.indexRowCount.set(indexTableName, rowCount);
      SheetOrmLogger.log(
        `[Index] createCombinedIndex "${indexTableName}" → existing (C1) rowCount=${rowCount}`,
      );
    }
  }

  /**
   * Check whether a combined index sheet exists for the given table name.
   * Uses the memoised sheet cache when available.
   */
  existsCombined(indexTableName: string): boolean {
    return this.getIndexSheet(indexTableName) !== null;
  }

  /**
   * Add a single (field, value, entityId) entry to the combined index sheet.
   *
   * Enforces unique index constraints: if a unique index already contains
   * the value for a different entity, throws an error.
   *
   * @param indexTableName - Sheet name.
   * @param field          - Indexed field name.
   * @param value          - Field value to index.
   * @param entityId       - Owning entity's UUID.
   * @throws Error on unique index violation.
   */
  addToCombined(indexTableName: string, field: string, value: unknown, entityId: string): void {
    const meta = this.indexRegistry.get(this.registryKey(indexTableName, field));
    const sheet = this.getIndexSheet(indexTableName);
    if (!sheet) return;

    const valueStr = String(value);

    // Unique constraint check: scan existing data for duplicate values
    if (meta?.unique) {
      const data = this.getCombinedData(indexTableName);
      for (let i = 0; i < data.length; i++) {
        if (String(data[i][0]) === field && String(data[i][1]) === valueStr) {
          if (String(data[i][2]) !== entityId) {
            throw new Error(
              `Unique index violation: ${indexTableName}.${field} already has value "${valueStr}" for entity ${String(data[i][2])}`,
            );
          }
          // Same entity already indexed with this value — no-op
          return;
        }
      }
    }

    const newRow: unknown[] = [field, valueStr, entityId];
    if (this.cache) {
      // Cache path: append to both the sheet and the in-memory cache array
      const data = this.getCombinedData(indexTableName);
      sheet.writeRowsAt(data.length, [newRow]);
      data.push(newRow);
      this.indexRowCount.set(indexTableName, data.length);
      this.searchIndexCache.delete(`${this.registryKey(indexTableName, field)}`);
    } else {
      // No-cache path: use known row count (B5) or fall back to appendRow
      const knownCount = this.indexRowCount.get(indexTableName);
      if (knownCount !== undefined) {
        sheet.writeRowsAt(knownCount, [newRow]);
        this.indexRowCount.set(indexTableName, knownCount + 1);
      } else {
        sheet.appendRow(newRow);
      }
      this.invalidateSearchCacheForTable(indexTableName);
    }
  }

  /**
   * Add index entries for **all** indexed fields of a single entity in one
   * batch operation.  Reduces N separate `appendRow()` API calls to a single
   * `writeRowsAt()` call (or buffers them if batch mode is active).
   *
   * Unique constraint checks are performed against both the existing sheet
   * data AND any pending (unflushed) batch entries.
   *
   * @param indexTableName - Sheet name.
   * @param entries        - Array of { field, value } pairs to index.
   * @param entityId       - Owning entity's UUID.
   * @throws Error on unique index violation.
   */
  addAllFieldsToCombined(
    indexTableName: string,
    entries: Array<{ field: string; value: unknown }>,
    entityId: string,
  ): void {
    // Do NOT call getSheetByName here — in batch mode the sheet is never needed
    // for the write path; fetching it per-entity causes 1000× GAS API calls.
    const rows: unknown[][] = [];
    let data: unknown[][] | null = null;

    for (const { field, value } of entries) {
      const valueStr = String(value);
      const meta = this.indexRegistry.get(this.registryKey(indexTableName, field));

      // Unique constraint check: scan existing data + pending batch entries
      if (meta?.unique) {
        if (!data) data = this.getCombinedData(indexTableName);
        let alreadyIndexed = false;
        // Check existing (committed) sheet data
        for (let i = 0; i < data.length; i++) {
          if (String(data[i][0]) === field && String(data[i][1]) === valueStr) {
            if (String(data[i][2]) !== entityId) {
              throw new Error(
                `Unique index violation: ${indexTableName}.${field} already has value "${valueStr}" for entity ${String(data[i][2])}`,
              );
            }
            alreadyIndexed = true;
            break;
          }
        }
        // Also check pending (unflushed) batch entries for the same table
        if (!alreadyIndexed && this.indexBatch !== null) {
          const pending = this.indexBatch.get(indexTableName);
          if (pending) {
            for (let i = 0; i < pending.length; i++) {
              if (String(pending[i][0]) === field && String(pending[i][1]) === valueStr) {
                if (String(pending[i][2]) !== entityId) {
                  throw new Error(
                    `Unique index violation: ${indexTableName}.${field} already has value "${valueStr}" for entity ${String(pending[i][2])}`,
                  );
                }
                alreadyIndexed = true;
                break;
              }
            }
          }
        }
        if (alreadyIndexed) continue; // Value already indexed for this entity — skip
      }

      rows.push([field, valueStr, entityId]);
    }

    if (rows.length > 0) {
      if (this.indexBatch !== null) {
        // ── Batch mode: accumulate rows in memory, no sheet API call needed ──
        let pending = this.indexBatch.get(indexTableName);
        if (!pending) {
          pending = [];
          this.indexBatch.set(indexTableName, pending);
        }
        for (const row of rows) pending.push(row);
        return;
      }

      // ── Non-batch mode: write to sheet immediately ──
      SheetOrmLogger.log(
        `[Index:${indexTableName}] addAllFieldsToCombined — non-batch, writing ${rows.length} rows entity=${entityId.slice(0, 8)}`,
      );
      if (this.cache) {
        // Cache-enabled path: tries three strategies in order of cheapness:
        //   1. Cache HIT   → use cached array length as append offset
        //   2. C1 row count → previously seeded by createCombinedIndex
        //   3. Fallback     → full getAllData() read
        const sheet = this.getIndexSheet(indexTableName);
        if (!sheet) return;
        const cacheKey = `cidx:${indexTableName}`;
        const cachedData = this.cache.get<unknown[][]>(cacheKey);
        if (cachedData !== null) {
          // Strategy 1: cache HIT — append at cached length
          sheet.writeRowsAt(cachedData.length, rows);
          for (const row of rows) cachedData.push(row);
          this.indexRowCount.set(indexTableName, cachedData.length);
        } else {
          const knownCount = this.indexRowCount.get(indexTableName);
          if (knownCount !== undefined) {
            // Strategy 2 (C1): row count known — skip getAllData()
            sheet.writeRowsAt(knownCount, rows);
            this.indexRowCount.set(indexTableName, knownCount + rows.length);
          } else {
            // Strategy 3: fallback — full read, then append
            const cacheData = this.getCombinedData(indexTableName);
            sheet.writeRowsAt(cacheData.length, rows);
            for (const row of rows) cacheData.push(row);
            this.indexRowCount.set(indexTableName, cacheData.length);
          }
        }
        this.searchIndexCache.clear(); // Invalidate n-gram search caches
      } else {
        // No-cache path (B3): use known row count to avoid appendRows
        const sheet = this.getIndexSheet(indexTableName);
        if (!sheet) return;
        const knownCount = this.indexRowCount.get(indexTableName);
        if (knownCount !== undefined) {
          sheet.writeRowsAt(knownCount, rows);
          this.indexRowCount.set(indexTableName, knownCount + rows.length);
        } else {
          // Fallback: position unknown, let adapter append
          sheet.appendRows(rows);
        }
        this.searchIndexCache.clear(); // Invalidate n-gram search caches
      }
    }
  }

  /**
   * Remove **all** index entries for a single entity from the combined sheet.
   *
   * Reads the sheet data once, filters out rows belonging to `entityId`,
   * and rewrites the entire sheet with `replaceAllData()`.  This is far
   * cheaper than N individual `deleteRow()` calls (each ~300 ms in GAS
   * plus O(n) row-shifting).
   *
   * @param indexTableName - Sheet name.
   * @param entityId       - Entity UUID whose entries should be removed.
   */
  removeAllFromCombined(indexTableName: string, entityId: string): void {
    const sheet = this.getIndexSheet(indexTableName);
    if (!sheet) return;

    const data = this.getCombinedData(indexTableName);
    const remaining = data.filter((row) => String(row[2]) !== entityId);
    if (remaining.length === data.length) return; // Nothing to remove

    // Bulk rewrite: single replaceAllData() replaces N deleteRow() calls
    sheet.replaceAllData(remaining);
    this.indexRowCount.set(indexTableName, remaining.length);
    if (this.cache) {
      this.cache.set(`cidx:${indexTableName}`, remaining);
      this.searchIndexCache.clear();
    } else {
      this.clearCache();
    }
  }

  /**
   * Remove index entries for **multiple** entities in a single bulk operation.
   *
   * Reads sheet data once, filters out all rows whose entityId is in the
   * provided set, and rewrites with `replaceAllData()`.  Use this instead
   * of calling `removeAllFromCombined()` in a loop — it avoids N redundant
   * sheet reads and writes.
   *
   * @param indexTableName - Sheet name.
   * @param entityIds      - Array of entity UUIDs to remove.
   */
  removeMultipleFromCombined(indexTableName: string, entityIds: string[]): void {
    if (entityIds.length === 0) return;
    const sheet = this.getIndexSheet(indexTableName);
    if (!sheet) return;

    // Convert to Set for O(1) membership checks during filtering
    const idSet = new Set(entityIds);
    const data = this.getCombinedData(indexTableName);
    const remaining = data.filter((row) => !idSet.has(String(row[2])));

    if (remaining.length === data.length) return; // No matching rows found

    sheet.replaceAllData(remaining);
    this.indexRowCount.set(indexTableName, remaining.length);
    if (this.cache) {
      this.cache.set(`cidx:${indexTableName}`, remaining);
      this.searchIndexCache.clear();
    } else {
      this.clearCache();
    }
  }

  /**
   * Update index entries for an entity after its field values have changed.
   *
   * Reads the sheet data **once**, diffs old vs new values for each indexed
   * field, then applies the minimal set of writes.  Two paths exist:
   *
   * 1. **In-place update** (fast): when no field is being cleared (old→null),
   *    each changed row is overwritten at its current position with a single
   *    `writeRowsAt()`.  New fields that previously had no value are appended.
   * 2. **Fallback delete+insert**: when a field is cleared, the affected rows
   *    are removed with `replaceAllData()` and new rows appended.
   *
   * Unique constraint checks run on the snapshot **before** any writes to
   * prevent inconsistent state.
   *
   * @param indexTableName - Sheet name.
   * @param entityId       - Entity UUID.
   * @param oldValues      - Previous field values (before the update).
   * @param newValues      - New field values (after the update).
   * @throws Error on unique index violation.
   */
  updateInCombined(
    indexTableName: string,
    entityId: string,
    oldValues: Record<string, unknown>,
    newValues: Record<string, unknown>,
  ): void {
    const sheet = this.getIndexSheet(indexTableName);
    if (!sheet) return;

    const indexedFields = this.getIndexedFields(indexTableName);

    // Collect field-level diffs: which indexed fields changed?
    type Change = { field: string; oldStr: string | null; newStr: string | null; unique: boolean };
    const changes: Change[] = [];

    for (const meta of indexedFields) {
      const field = meta.field;
      const oldVal = oldValues[field];
      const newVal = newValues[field];
      if (oldVal === newVal) continue; // No change for this field
      // Convert to string form (null/undefined/empty → null = "field has no value")
      const oldStr = oldVal !== undefined && oldVal !== null && oldVal !== "" ? String(oldVal) : null;
      const newStr = newVal !== undefined && newVal !== null && newVal !== "" ? String(newVal) : null;
      changes.push({ field, oldStr, newStr, unique: meta.unique });
    }

    if (changes.length === 0) return; // No indexed fields changed

    // Read sheet data ONCE (may hit getCombinedData cache)
    const data = this.getCombinedData(indexTableName);

    // ── Uniqueness pre-check: validate all new values before any writes ──
    for (const { field, newStr, unique } of changes) {
      if (newStr !== null && unique) {
        for (let i = 0; i < data.length; i++) {
          if (
            String(data[i][0]) === field &&
            String(data[i][1]) === newStr &&
            String(data[i][2]) !== entityId
          ) {
            throw new Error(
              `Unique index violation: ${indexTableName}.${field} already has value "${newStr}" for entity ${String(data[i][2])}`,
            );
          }
        }
      }
    }

    // ── Path 1: In-place update (no field is being cleared) ─────────────────
    // When every change has a non-null newStr, we can overwrite each index row
    // at its existing position.  This avoids replaceAllData()/deleteRow() which
    // are expensive in GAS (~300 ms per deleteRow + O(n) row-shifting).
    const hasPureDeletion = changes.some((c) => c.oldStr !== null && c.newStr === null);
    if (!hasPureDeletion) {
      // Build field → old/new string maps for single-pass lookup
      const fieldOldMapOpt = new Map<string, string>();
      const fieldNewMapOpt = new Map<string, string>();
      for (const { field, oldStr, newStr } of changes) {
        if (oldStr !== null) fieldOldMapOpt.set(field, oldStr);
        if (newStr !== null) fieldNewMapOpt.set(field, newStr);
      }
      // Single pass over data: find rows matching (field, oldValue, entityId) and overwrite
      for (let i = 0; i < data.length; i++) {
        const fc = String(data[i][0]);
        const oldStr = fieldOldMapOpt.get(fc);
        if (oldStr === undefined || String(data[i][1]) !== oldStr || String(data[i][2]) !== entityId)
          continue;
        const newStr = fieldNewMapOpt.get(fc);
        if (newStr !== undefined) {
          const newRow: unknown[] = [fc, newStr, entityId];
          sheet.writeRowsAt(i, [newRow]); // Overwrite in-place
          data[i] = newRow; // Keep cache consistent
        }
      }
      // Handle pure insertions: field was empty/null → now has a value (no existing row to overwrite)
      const insertRows: unknown[][] = changes
        .filter((c) => c.oldStr === null && c.newStr !== null)
        .map((c) => [c.field, c.newStr!, entityId]);
      if (insertRows.length > 0) {
        if (this.cache) {
          sheet.writeRowsAt(data.length, insertRows);
          for (const r of insertRows) data.push(r);
          this.indexRowCount.set(indexTableName, data.length);
        } else {
          for (const r of insertRows) sheet.appendRow(r as unknown[]);
          const prev = this.indexRowCount.get(indexTableName);
          if (prev !== undefined) this.indexRowCount.set(indexTableName, prev + insertRows.length);
        }
      }
      if (!this.cache) this.clearCache();
      else this.searchIndexCache.clear();
      return;
    }

    // ── Fallback path: handles clearing a field to empty/null (pure deletion) ─
    const fieldOldMap = new Map<string, string>();
    for (const { field, oldStr } of changes) {
      if (oldStr !== null) fieldOldMap.set(field, oldStr);
    }
    const rowsToDelete: number[] = [];
    for (let i = 0; i < data.length; i++) {
      const fc = String(data[i][0]);
      const oldStr = fieldOldMap.get(fc);
      if (oldStr !== undefined && String(data[i][1]) === oldStr && String(data[i][2]) === entityId) {
        rowsToDelete.push(i);
      }
    }

    // ── Path 2: Fallback delete+insert (a field is being cleared) ─────────
    // When a field value is set to empty/null, the corresponding index row
    // must be removed.  We filter out stale rows, rewrite the sheet with
    // replaceAllData(), then append new entries.

    // Single replaceAllData instead of N individual deleteRow() calls —
    // avoids the O(n) row-shift cost per call in GAS.
    const deleteSet = new Set(rowsToDelete);
    const filteredData = data.filter((_, i) => !deleteSet.has(i));
    sheet.replaceAllData(filteredData);
    // Update cached array reference in-place so subsequent writes land at correct offset
    data.length = 0;
    for (const row of filteredData) data.push(row);
    this.indexRowCount.set(indexTableName, data.length);

    // Append new index entries for changed fields in a single batch
    const newRows: unknown[][] = [];
    for (const { field, newStr } of changes) {
      if (newStr !== null) {
        newRows.push([field, newStr, entityId]);
      }
    }
    if (newRows.length > 0) {
      if (this.cache) {
        sheet.writeRowsAt(data.length, newRows);
        for (const row of newRows) data.push(row);
        this.indexRowCount.set(indexTableName, data.length);
        this.searchIndexCache.clear();
      } else {
        for (const row of newRows) sheet.appendRow(row as unknown[]);
        const prev = this.indexRowCount.get(indexTableName);
        if (prev !== undefined) this.indexRowCount.set(indexTableName, prev + newRows.length);
        this.clearCache();
      }
    } else if (!this.cache) {
      this.clearCache();
    } else {
      this.searchIndexCache.clear();
    }
  }

  /**
   * Look up entity IDs that match a specific (field, value) pair in the index.
   *
   * Performs a linear scan of the combined data.  Deduplicates results
   * using a Set to handle potential index corruption.
   *
   * @param indexTableName - Sheet name.
   * @param field          - Indexed field name.
   * @param value          - Value to look up.
   * @returns Array of matching entity UUIDs (deduplicated).
   */
  lookupCombined(indexTableName: string, field: string, value: unknown): string[] {
    const data = this.getCombinedData(indexTableName);
    const valueStr = String(value);
    const seen = new Set<string>();
    const ids: string[] = [];
    for (let i = 0; i < data.length; i++) {
      if (String(data[i][0]) === field && String(data[i][1]) === valueStr) {
        const id = String(data[i][2]);
        if (!seen.has(id)) {
          seen.add(id);
          ids.push(id);
        }
      }
    }
    return ids;
  }

  /**
   * Delete an entire combined index sheet and unregister all its fields.
   *
   * The sheet is physically deleted via the adapter, all caches are cleared,
   * and the field registry entries are removed.  Cache must be cleared
   * **before** removing registry entries because `clearCache()` iterates
   * the registry to find cache keys to invalidate.
   *
   * @param indexTableName - Sheet name to drop.
   */
  dropCombinedIndex(indexTableName: string): void {
    this.adapter.deleteSheet(indexTableName);
    this.indexSheetCache.delete(indexTableName);
    this.indexRowCount.delete(indexTableName);
    // Clear cache BEFORE removing registry entries — clearCache() reads indexRegistry
    this.clearCache();
    for (const [key, meta] of this.indexRegistry.entries()) {
      if (meta.tableName === indexTableName) {
        this.indexRegistry.delete(key);
      }
    }
  }

  // ─── N-gram search (Solr-like approach) ──────────────────────────────────
  // Text search uses a Solr-inspired algorithm originally developed for the
  // TyreSizeCatalog project.  The approach:
  //   1. Normalise text → lowercase, collapse separators/whitespace.
  //   2. Tokenise → split on spaces.
  //   3. Build trigram posting lists for each indexed value.
  //   4. At query time, intersect posting lists for query trigrams.
  //   5. Verify surviving candidates with a substring match.

  /**
   * Normalise a string for search indexing and querying.
   *
   * Steps: lowercase → trim → convert dashes/underscores/em-dashes to spaces
   * → remove commas → collapse whitespace.
   *
   * @param s - Raw string to normalise.
   * @returns Normalised lowercase string.
   */
  static normalizeForSearch(s: string): string {
    if (!s) return "";
    let t = s.toLowerCase().trim();
    // Convert dashes, em-dashes, underscores → space for uniform tokenisation
    t = t.replace(/[\u2010-\u2015\-\u2013\u2014_]/g, " ");
    // Remove commas
    t = t.replace(/,/g, " ");
    // Collapse whitespace
    t = t.replace(/\s+/g, " ").trim();
    return t;
  }

  /**
   * Tokenise a normalised string into individual search tokens.
   *
   * @param normalized - Already-normalised string (via {@link normalizeForSearch}).
   * @returns Array of non-empty tokens split on spaces.
   */
  static tokenize(normalized: string): string[] {
    if (!normalized) return [];
    return normalized.split(" ").filter((t) => t.length > 0);
  }

  /**
   * Generate character-level n-grams from a string.
   *
   * Whitespace is stripped before generating grams so that token boundaries
   * do not create artificial gaps.  For the default `NGRAM_SIZE = 3`, the
   * string "toyota" produces {"toy", "oyo", "yot", "ota"}.
   *
   * @param s - Input string.
   * @param n - N-gram length (typically 3 = trigram).
   * @returns Set of unique n-gram substrings.
   */
  static ngrams(s: string, n: number): Set<string> {
    const out = new Set<string>();
    if (!s) return out;
    const t = s.replace(/\s+/g, ""); // Strip whitespace for contiguous grams
    if (t.length < n) return out;
    for (let i = 0; i <= t.length - n; i++) {
      out.add(t.substring(i, i + n));
    }
    return out;
  }

  /**
   * Build an in-memory {@link SearchIndex} for a (tableName, field) pair.
   *
   * Scans all index rows for `field`, normalises each value, and builds:
   *   - **Token postings**: exact-token → list of entry indices.
   *   - **Trigram postings**: character-trigram → list of entry indices.
   *     Trigrams are generated from both individual tokens and the
   *     compacted (whitespace-free) normalised form.
   *
   * The result is cached in `searchIndexCache` and reused until the
   * index table is modified.
   *
   * @param indexTableName - Sheet name.
   * @param field          - Indexed field to build the search index for.
   * @returns Fully populated SearchIndex.
   */
  private buildSearchIndex(indexTableName: string, field: string): SearchIndex {
    const data = this.getCombinedData(indexTableName);
    const entries: Array<{ value: string; entityId: string }> = [];
    const normalized: string[] = [];
    const tokenIndex = new Map<string, number[]>(); // token → posting list
    const ngramIndex = new Map<string, number[]>(); // trigram → posting list

    {
      // Iterate over every row in the raw index data looking for rows belonging
      // to `field`.  Each matching row contributes one entry to the search index.
      for (let i = 0; i < data.length; i++) {
        if (String(data[i][0]) !== field) continue; // Skip rows for other fields
        const value = String(data[i][1]);
        const entityId = String(data[i][2]);
        const idx = entries.length; // Ordinal position within entries array
        entries.push({ value, entityId });

        const norm = IndexStore.normalizeForSearch(value);
        normalized.push(norm);

        // --- Token postings ---
        // Split the normalised text into tokens and record which entry index
        // each token appears in.  This allows O(1) exact-token lookup later.
        const tokens = IndexStore.tokenize(norm);
        for (const tk of tokens) {
          let postings = tokenIndex.get(tk);
          if (!postings) {
            postings = [];
            tokenIndex.set(tk, postings);
          }
          postings.push(idx);
        }

        // --- Trigram postings ---
        // Collect trigrams from two sources:
        //   1. Individual tokens  — captures within-word substrings.
        //   2. Compacted (no-space) normalised form — captures cross-word
        //      substrings for multi-word fuzzy matching.
        const ngs = new Set<string>();
        for (const tk of tokens) {
          for (const ng of IndexStore.ngrams(tk, IndexStore.NGRAM_SIZE)) ngs.add(ng);
        }
        for (const ng of IndexStore.ngrams(norm.replace(/ /g, ""), IndexStore.NGRAM_SIZE)) ngs.add(ng);
        for (const ng of ngs) {
          let postings = ngramIndex.get(ng);
          if (!postings) {
            postings = [];
            ngramIndex.set(ng, postings);
          }
          postings.push(idx);
        }
      }
    }

    return { entries, normalized, tokenIndex, ngramIndex };
  }

  /**
   * Approximate a token's posting list using trigram intersection.
   *
   * When a query token does not appear verbatim in the `tokenIndex`, we
   * fall back to n-gram matching: decompose the token into trigrams, look
   * up each trigram's posting list, and intersect them all.  This yields
   * candidate entries that share every trigram with the query token —
   * a superset of true matches that is refined later by substring
   * verification in {@link searchCombined}.
   *
   * @param token      - The query token to approximate.
   * @param ngramIndex - Trigram → posting-list map from the search index.
   * @returns Intersection of all trigram posting lists / empty if a trigram is missing.
   */
  private static postingsForTokenViaNgrams(token: string, ngramIndex: Map<string, number[]>): number[] {
    const ngs = IndexStore.ngrams(token, IndexStore.NGRAM_SIZE);
    if (ngs.size === 0) return []; // Token too short for any trigram

    const lists: number[][] = [];
    for (const ng of ngs) {
      const p = ngramIndex.get(ng);
      if (!p) return []; // A missing trigram means no entry can match
      lists.push(p);
    }
    // Sort shortest-first so that each intersection step works on the
    // smallest possible set, improving performance.
    lists.sort((a, b) => a.length - b.length);
    return IndexStore.intersectPostingLists(lists);
  }

  /**
   * Intersect N sorted posting lists into a single sorted result.
   *
   * Reduces pairwise from the first list onward.  Callers typically
   * pre-sort the input lists by ascending length so the intermediate
   * result stays small.
   *
   * @param lists - Array of sorted posting lists.
   * @returns Sorted array of indices present in every input list.
   */
  private static intersectPostingLists(lists: number[][]): number[] {
    if (lists.length === 0) return [];
    let result = lists[0];
    for (let i = 1; i < lists.length; i++) {
      result = IndexStore.intersectTwo(result, lists[i]);
      if (result.length === 0) break; // Short-circuit: no common elements remain
    }
    return result;
  }

  /**
   * Two-pointer intersection of two sorted integer arrays.
   *
   * Both `a` and `b` must be sorted in ascending order.  The output
   * contains only values present in both arrays, preserving order.
   *
   * @param a - First sorted posting list.
   * @param b - Second sorted posting list.
   * @returns Sorted intersection.
   */
  private static intersectTwo(a: number[], b: number[]): number[] {
    const out: number[] = [];
    let i = 0;
    let j = 0;
    while (i < a.length && j < b.length) {
      if (a[i] === b[j]) {
        out.push(a[i]); // Common element found
        i++;
        j++;
      } else if (a[i] < b[j]) {
        i++; // Advance the smaller pointer
      } else {
        j++; // Advance the smaller pointer
      }
    }
    return out;
  }

  /**
   * Search for entity IDs in a combined index by field using n-gram text search.
   *
   * Implements a Solr-like search algorithm (ported from TyreSizeCatalog):
   *
   * 1. **Normalise & tokenise** the query string.
   * 2. **Token lookup**: for each query token, look up the exact posting
   *    list.  If not found, fall back to trigram-based approximation
   *    via {@link postingsForTokenViaNgrams}.  Tokens shorter than
   *    `NGRAM_SIZE` skip trigram lookup and include all candidates.
   * 3. **Intersect** posting lists across all query tokens — only entries
   *    that match *every* token survive.
   * 4. **Verify** each candidate by checking whether the normalised
   *    query string is a substring of the normalised index value.
   *    This eliminates trigram false-positives.
   * 5. **Deduplicate** results by entity ID (multiple index rows can
   *    reference the same entity via different field values).
   *
   * @param indexTableName - Combined index sheet name.
   * @param field          - Indexed field to search within.
   * @param query          - Free-text search query.
   * @param limit          - Optional maximum number of results.
   * @returns Array of matching entity IDs, up to `limit`.
   */
  searchCombined(indexTableName: string, field: string, query: string, limit?: number): string[] {
    if (!query) return [];

    // Retrieve or build the in-memory search index for this (table, field) pair
    const cacheKey = `${indexTableName}::${field}`;
    let idx = this.searchIndexCache.get(cacheKey);
    if (!idx) {
      idx = this.buildSearchIndex(indexTableName, field);
      this.searchIndexCache.set(cacheKey, idx);
    }

    if (idx.entries.length === 0) return []; // No indexed data for this field

    // Normalise the query for case/diacritic-insensitive matching
    const pat = IndexStore.normalizeForSearch(query);
    if (!pat) return [];

    const qTokens = IndexStore.tokenize(pat);
    let candidates: number[];

    if (qTokens.length === 0) {
      // Empty token list (e.g. query was only whitespace) → all entries are candidates
      candidates = Array.from({ length: idx.entries.length }, (_, i) => i);
    } else {
      // Gather posting lists for each query token
      const postings: number[][] = [];
      for (const t of qTokens) {
        const p = idx.tokenIndex.get(t);
        if (p) {
          // Exact token match found in index
          postings.push(p);
        } else if (t.length < IndexStore.NGRAM_SIZE) {
          // Token is shorter than trigram size — cannot use n-gram approximation.
          // Include all candidates; substring verification will filter later.
          postings.push(Array.from({ length: idx.entries.length }, (_, i) => i));
        } else {
          // Approximate via trigram intersection
          const p2 = IndexStore.postingsForTokenViaNgrams(t, idx.ngramIndex);
          if (p2.length === 0) return []; // No trigram match → query cannot match any entry
          postings.push(p2);
        }
      }
      // Sort shortest-first for efficient intersection
      postings.sort((a, b) => a.length - b.length);
      candidates = IndexStore.intersectPostingLists(postings);
      if (candidates.length === 0) return [];
    }

    // --- Substring verification pass ---
    // Trigram intersection is approximate; verify each candidate by checking
    // that the full normalised query appears as a substring of the normalised
    // index value.  Also deduplicate by entity ID.
    const maxResults =
      limit !== undefined && Number.isFinite(limit) && limit >= 0 ? Math.floor(limit) : candidates.length;
    if (maxResults === 0) return [];
    const seen = new Set<string>(); // Track seen entity IDs for deduplication
    const out: string[] = [];
    for (const pos of candidates) {
      if (idx.normalized[pos].includes(pat)) {
        const entityId = idx.entries[pos].entityId;
        if (!seen.has(entityId)) {
          seen.add(entityId);
          out.push(entityId);
        }
        if (out.length >= maxResults) break; // Early exit once limit is reached
      }
    }
    return out;
  }

  /**
   * Retrieve the raw rows of a combined index sheet, with transparent caching.
   *
   * Avoids redundant `getAllData()` GAS API calls when multiple operations
   * (lookup, search, update) access the same index sheet within a single
   * save cycle.  The cache is invalidated via {@link clearCache} after any
   * write operation.
   *
   * **B6 optimisation**: when `indexRowCount` records 0 rows for the table
   * (e.g. immediately after {@link createCombinedIndex}), the method skips
   * the `getAllData()` call entirely and returns an empty array, saving
   * ~300 ms per call in Google Apps Script.
   *
   * @param indexTableName - Combined index sheet name.
   * @returns 2-D array of raw row data (`[field, value, entityId]` per row).
   */
  private getCombinedData(indexTableName: string): unknown[][] {
    if (this.cache) {
      // --- Cached path ---
      const cacheKey = `cidx:${indexTableName}`;
      const cached = this.cache.get<unknown[][]>(cacheKey);
      if (cached !== null) {
        // Cache hit — return without touching GAS API
        SheetOrmLogger.log(`[Index:${indexTableName}] getCombinedData cache HIT — ${cached.length} rows`);
        return cached;
      }
      // B6: sheet is known-empty (just created via createCombinedIndex) — seed cache directly,
      // skip the getAllData() API call that would wastefully read 0 rows from GAS.
      if (this.indexRowCount.get(indexTableName) === 0) {
        const empty: unknown[][] = [];
        this.cache.set(cacheKey, empty);
        SheetOrmLogger.log(`[Index:${indexTableName}] getCombinedData cache SEED (empty, skip getAllData)`);
        return empty;
      }
      // Cache miss — fetch from GAS and populate cache
      const sheet = this.getIndexSheet(indexTableName);
      const data = sheet ? sheet.getAllData() : [];
      SheetOrmLogger.log(
        `[Index:${indexTableName}] getCombinedData cache MISS — read ${data.length} rows from sheet`,
      );
      this.cache.set(cacheKey, data);
      this.indexRowCount.set(indexTableName, data.length); // Keep row count in sync
      return data;
    }
    // --- No-cache path ---
    // B6: skip getAllData() when table is known-empty
    if (this.indexRowCount.get(indexTableName) === 0) {
      SheetOrmLogger.log(`[Index:${indexTableName}] getCombinedData (no cache) — empty (skip getAllData)`);
      return [];
    }
    const sheet = this.getIndexSheet(indexTableName);
    const data = sheet ? sheet.getAllData() : [];
    SheetOrmLogger.log(
      `[Index:${indexTableName}] getCombinedData (no cache) — read ${data.length} rows from sheet`,
    );
    this.indexRowCount.set(indexTableName, data.length); // Update tracked row count
    return data;
  }

  /**
   * Invalidate search-index cache entries for a specific index table.
   *
   * Iterates over all keys in `searchIndexCache` and removes any whose
   * prefix matches `indexTableName::`.  Called after write operations so
   * that subsequent searches rebuild their in-memory posting lists.
   *
   * @param indexTableName - Sheet name whose search cache should be cleared.
   */
  private invalidateSearchCacheForTable(indexTableName: string): void {
    const prefix = `${indexTableName}::`;
    for (const key of this.searchIndexCache.keys()) {
      if (key.startsWith(prefix)) this.searchIndexCache.delete(key);
    }
  }

  /**
   * Clear all internal index caches — both in-memory search indexes and
   * the data-level cache entries (prefixed `cidx:`).
   *
   * Does not clear the entire shared {@link ICacheProvider}; instead it
   * selectively deletes only index-related keys to preserve other cached
   * data (e.g. entity caches).
   */
  private clearCache(): void {
    this.searchIndexCache.clear(); // Drop all in-memory search indexes
    if (!this.cache) return;
    // Only invalidate index-specific keys rather than clearing entire cache
    const cleared = new Set<string>();
    for (const key of this.indexRegistry.keys()) {
      const tableName = key.split("::")[0];
      if (!cleared.has(tableName)) {
        cleared.add(tableName);
        this.cache.delete(`cidx:${tableName}`); // Remove cached raw row data
      }
    }
  }

  /**
   * Public entry point for clearing all index caches (search + data).
   * Called by {@link Registry.clearCache} to ensure full cache coherence
   * when the Registry's data is invalidated.
   */
  clearAllCaches(): void {
    this.clearCache();
  }
}
