// SheetORM — IndexStore: manages secondary indexes stored in separate sheets
// Inspired by the index-table pattern from document-oriented ORMs

import type { ISpreadsheetAdapter } from "../core/types/ISpreadsheetAdapter.js";
import type { ICacheProvider } from "../core/types/ICacheProvider.js";
import type { IndexMeta } from "./IndexMeta.js";

/**
 * Combined (per-class) index sheet layout (idx_{ClassName}s):
 *   Row 1 (headers): ["field", "value", "entityId"]
 *   Rows 2+: [fieldName, indexedValue, entityId]
 *
 * For unique indexes, there should be at most one row per value per field.
 */

/** In-memory n-gram search index for a single field inside a combined index sheet. */
interface SearchIndex {
  entries: Array<{ value: string; entityId: string }>;
  normalized: string[];
  tokenIndex: Map<string, number[]>;
  ngramIndex: Map<string, number[]>;
}

export class IndexStore {
  private adapter: ISpreadsheetAdapter;
  private cache: ICacheProvider | null;
  private indexRegistry: Map<string, IndexMeta> = new Map();
  private searchIndexCache: Map<string, SearchIndex> = new Map();
  private indexBatch: Map<string, unknown[][]> | null = null;
  private static readonly NGRAM_SIZE = 3;

  constructor(adapter: ISpreadsheetAdapter, cache?: ICacheProvider) {
    this.adapter = adapter;
    this.cache = cache ?? null;
  }

  private registryKey(tableName: string, field: string): string {
    return `${tableName}::${field}`;
  }

  /**
   * Get all indexed fields for a table.
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
   * Register index metadata (used during schema initialization).
   */
  registerIndex(tableName: string, field: string, unique: boolean): void {
    this.indexRegistry.set(this.registryKey(tableName, field), {
      tableName,
      field,
      unique,
    });
  }

  // ─── Batch index write buffering ────────────────────────────────────────────

  /**
   * Begin buffering all addAllFieldsToCombined calls. While active, no index
   * writes hit the sheet — entries accumulate in memory instead.
   * Call flushIndexBatch() to write everything in a single setValues call per index table.
   */
  beginIndexBatch(): void {
    this.indexBatch = new Map();
  }

  /**
   * Write all buffered index entries (one writeRowsAt per index table) and clear the buffer.
   */
  flushIndexBatch(): void {
    if (!this.indexBatch) return;
    const batch = this.indexBatch;
    this.indexBatch = null;
    for (const [indexTableName, rows] of batch) {
      if (rows.length === 0) continue;
      const sheet = this.adapter.getSheetByName(indexTableName);
      if (!sheet) continue;
      if (this.cache) {
        const data = this.getCombinedData(indexTableName);
        sheet.writeRowsAt(data.length, rows);
        for (const row of rows) data.push(row);
        this.invalidateSearchCacheForTable(indexTableName);
      } else {
        sheet.appendRows(rows);
        this.invalidateSearchCacheForTable(indexTableName);
      }
    }
  }

  /**
   * Discard buffered entries without writing (used in error paths).
   */
  cancelIndexBatch(): void {
    this.indexBatch = null;
  }

  // ─── Combined (per-class) index sheet methods ───────────────────────────────
  // Used when a Record class has @Indexed fields; all index data is stored in a
  // single sheet named idx_{ClassName}s (e.g. idx_Cars) with columns:
  //   [field, value, entityId]

  /**
   * Create the combined index sheet for a Record class (if not already present).
   * Sheet name equals the class's indexTableName (e.g. idx_Cars).
   */
  createCombinedIndex(indexTableName: string): void {
    const existing = this.adapter.getSheetByName(indexTableName);
    if (!existing) {
      const sheet = this.adapter.createSheet(indexTableName);
      sheet.setHeaders(["field", "value", "entityId"]);
    }
  }

  /**
   * Check whether a combined index sheet exists for the given indexTableName.
   */
  existsCombined(indexTableName: string): boolean {
    return this.adapter.getSheetByName(indexTableName) !== null;
  }

  /**
   * Add an entry to the combined index sheet.
   */
  addToCombined(indexTableName: string, field: string, value: unknown, entityId: string): void {
    const meta = this.indexRegistry.get(this.registryKey(indexTableName, field));
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return;

    const valueStr = String(value);

    if (meta?.unique) {
      const data = this.getCombinedData(indexTableName);
      for (let i = 0; i < data.length; i++) {
        if (String(data[i][0]) === field && String(data[i][1]) === valueStr) {
          if (String(data[i][2]) !== entityId) {
            throw new Error(
              `Unique index violation: ${indexTableName}.${field} already has value "${valueStr}" for entity ${String(data[i][2])}`,
            );
          }
          // Same entity already indexed with this value
          return;
        }
      }
    }

    const newRow: unknown[] = [field, valueStr, entityId];
    if (this.cache) {
      const data = this.getCombinedData(indexTableName);
      sheet.writeRowsAt(data.length, [newRow]);
      data.push(newRow);
      this.searchIndexCache.delete(`${this.registryKey(indexTableName, field)}`);
    } else {
      sheet.appendRow(newRow);
      this.invalidateSearchCacheForTable(indexTableName);
    }
  }

  /**
   * Add entries for multiple fields of a single entity in one batch appendRows() call.
   * Reduces N separate appendRow() API calls to a single setValues() call.
   */
  addAllFieldsToCombined(
    indexTableName: string,
    entries: Array<{ field: string; value: unknown }>,
    entityId: string,
  ): void {
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return;

    const rows: unknown[][] = [];
    let data: unknown[][] | null = null;

    for (const { field, value } of entries) {
      const valueStr = String(value);
      const meta = this.indexRegistry.get(this.registryKey(indexTableName, field));

      if (meta?.unique) {
        if (!data) data = this.getCombinedData(indexTableName);
        let alreadyIndexed = false;
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
        // Also check pending batch entries (not yet flushed to sheet)
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
        if (alreadyIndexed) continue;
      }

      rows.push([field, valueStr, entityId]);
    }

    if (rows.length > 0) {
      if (this.indexBatch !== null) {
        // Batch mode: accumulate rows, no sheet write yet
        let pending = this.indexBatch.get(indexTableName);
        if (!pending) {
          pending = [];
          this.indexBatch.set(indexTableName, pending);
        }
        for (const row of rows) pending.push(row);
        return;
      }

      if (this.cache) {
        const data = this.getCombinedData(indexTableName);
        sheet.writeRowsAt(data.length, rows);
        for (const row of rows) data.push(row);
        this.searchIndexCache.clear();
      } else {
        sheet.appendRows(rows);
        this.searchIndexCache.clear();
      }
    }
  }

  /**
   * Remove all combined index entries for an entity.
   */
  removeAllFromCombined(indexTableName: string, entityId: string): void {
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return;

    const data = this.getCombinedData(indexTableName);
    const rowsToDelete: number[] = [];

    for (let i = 0; i < data.length; i++) {
      if (String(data[i][2]) === entityId) {
        rowsToDelete.push(i);
      }
    }

    if (rowsToDelete.length > 0) {
      for (let i = rowsToDelete.length - 1; i >= 0; i--) {
        sheet.deleteRow(rowsToDelete[i]);
        data.splice(rowsToDelete[i], 1);
      }
      if (!this.cache) this.clearCache();
      else this.searchIndexCache.clear();
    }
  }

  /**
   * Remove combined index entries for multiple entities in one bulk operation.
   * Reads data once, filters out all matching rows, writes back with replaceAllData().
   * Use this instead of N separate removeAllFromCombined() calls.
   */
  removeMultipleFromCombined(indexTableName: string, entityIds: string[]): void {
    if (entityIds.length === 0) return;
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return;

    const idSet = new Set(entityIds);
    const data = this.getCombinedData(indexTableName);
    const remaining = data.filter((row) => !idSet.has(String(row[2])));

    if (remaining.length === data.length) return;

    sheet.replaceAllData(remaining);
    if (this.cache) {
      this.cache.set(`cidx:${indexTableName}`, remaining);
      this.searchIndexCache.clear();
    } else {
      this.clearCache();
    }
  }

  /**
   * Update combined index entries for an entity (remove old values, add new).
   * Reads the sheet ONCE, collects all changes, then applies deletions + appends.
   */
  updateInCombined(
    indexTableName: string,
    entityId: string,
    oldValues: Record<string, unknown>,
    newValues: Record<string, unknown>,
  ): void {
    const sheet = this.adapter.getSheetByName(indexTableName);
    if (!sheet) return;

    const indexedFields = this.getIndexedFields(indexTableName);

    type Change = { field: string; oldStr: string | null; newStr: string | null; unique: boolean };
    const changes: Change[] = [];

    for (const meta of indexedFields) {
      const field = meta.field;
      const oldVal = oldValues[field];
      const newVal = newValues[field];
      if (oldVal === newVal) continue;
      const oldStr = oldVal !== undefined && oldVal !== null && oldVal !== "" ? String(oldVal) : null;
      const newStr = newVal !== undefined && newVal !== null && newVal !== "" ? String(newVal) : null;
      changes.push({ field, oldStr, newStr, unique: meta.unique });
    }

    if (changes.length === 0) return;

    // Read sheet data ONCE (may hit getCombinedData cache)
    const data = this.getCombinedData(indexTableName);

    // Uniqueness checks — all on the same snapshot, before any writes
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

    // Collect all rows to delete in one pass
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

    // Delete from bottom to top, keeping cache in sync
    for (let i = rowsToDelete.length - 1; i >= 0; i--) {
      sheet.deleteRow(rowsToDelete[i]);
      data.splice(rowsToDelete[i], 1);
    }

    // Collect and write new entries in one batch
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
        this.searchIndexCache.clear();
      } else {
        for (const row of newRows) sheet.appendRow(row as unknown[]);
        this.clearCache();
      }
    } else if (!this.cache) {
      this.clearCache();
    } else {
      this.searchIndexCache.clear();
    }
  }

  /**
   * Look up entity IDs in the combined index by field/value pair.
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
   * Delete a combined index sheet and remove registered fields for it.
   */
  dropCombinedIndex(indexTableName: string): void {
    this.adapter.deleteSheet(indexTableName);
    // Clear cache BEFORE removing registry entries, since clearCache()
    // iterates indexRegistry to find cache keys to invalidate.
    this.clearCache();
    for (const [key, meta] of this.indexRegistry.entries()) {
      if (meta.tableName === indexTableName) {
        this.indexRegistry.delete(key);
      }
    }
  }

  // ─── N-gram search (Solr-like approach) ──────────────────────────────────

  /**
   * Normalize a string for search indexing / querying:
   * lowercase, trim, collapse whitespace, normalize separators.
   */
  static normalizeForSearch(s: string): string {
    if (!s) return "";
    let t = s.toLowerCase().trim();
    // Normalize dashes, em-dashes, underscores → space
    t = t.replace(/[\u2010-\u2015\-\u2013\u2014_]/g, " ");
    // Remove commas
    t = t.replace(/,/g, " ");
    // Collapse whitespace
    t = t.replace(/\s+/g, " ").trim();
    return t;
  }

  /**
   * Tokenize a normalized string into search tokens (split on spaces).
   */
  static tokenize(normalized: string): string[] {
    if (!normalized) return [];
    return normalized.split(" ").filter((t) => t.length > 0);
  }

  /**
   * Generate character-level n-grams of length `n` from a string.
   * Whitespace is stripped before generating n-grams.
   */
  static ngrams(s: string, n: number): Set<string> {
    const out = new Set<string>();
    if (!s) return out;
    const t = s.replace(/\s+/g, "");
    if (t.length < n) return out;
    for (let i = 0; i <= t.length - n; i++) {
      out.add(t.substring(i, i + n));
    }
    return out;
  }

  /**
   * Build an in-memory search index for a given field in a combined index sheet.
   * Stores token postings and trigram postings for fast Solr-like lookup.
   */
  private buildSearchIndex(indexTableName: string, field: string): SearchIndex {
    const data = this.getCombinedData(indexTableName);
    const entries: Array<{ value: string; entityId: string }> = [];
    const normalized: string[] = [];
    const tokenIndex = new Map<string, number[]>();
    const ngramIndex = new Map<string, number[]>();

    {
      for (let i = 0; i < data.length; i++) {
        if (String(data[i][0]) !== field) continue;
        const value = String(data[i][1]);
        const entityId = String(data[i][2]);
        const idx = entries.length;
        entries.push({ value, entityId });

        const norm = IndexStore.normalizeForSearch(value);
        normalized.push(norm);

        // Token → postings
        const tokens = IndexStore.tokenize(norm);
        for (const tk of tokens) {
          let postings = tokenIndex.get(tk);
          if (!postings) {
            postings = [];
            tokenIndex.set(tk, postings);
          }
          postings.push(idx);
        }

        // Trigrams from tokens + compacted form → postings
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
   * For a token not present in the token index, approximate its postings
   * by intersecting the posting lists of its trigrams.
   */
  private static postingsForTokenViaNgrams(token: string, ngramIndex: Map<string, number[]>): number[] {
    const ngs = IndexStore.ngrams(token, IndexStore.NGRAM_SIZE);
    if (ngs.size === 0) return [];

    const lists: number[][] = [];
    for (const ng of ngs) {
      const p = ngramIndex.get(ng);
      if (!p) return []; // missing trigram → token cannot be present
      lists.push(p);
    }
    lists.sort((a, b) => a.length - b.length);
    return IndexStore.intersectPostingLists(lists);
  }

  /**
   * Intersect multiple sorted posting lists.
   */
  private static intersectPostingLists(lists: number[][]): number[] {
    if (lists.length === 0) return [];
    let result = lists[0];
    for (let i = 1; i < lists.length; i++) {
      result = IndexStore.intersectTwo(result, lists[i]);
      if (result.length === 0) break;
    }
    return result;
  }

  /**
   * Intersect two sorted posting lists using two-pointer technique.
   */
  private static intersectTwo(a: number[], b: number[]): number[] {
    const out: number[] = [];
    let i = 0;
    let j = 0;
    while (i < a.length && j < b.length) {
      if (a[i] === b[j]) {
        out.push(a[i]);
        i++;
        j++;
      } else if (a[i] < b[j]) {
        i++;
      } else {
        j++;
      }
    }
    return out;
  }

  /**
   * Search for entity IDs in a combined index by field using n-gram text search.
   *
   * Algorithm (Solr-like, ported from TyreSizeCatalog):
   * 1. Normalize + tokenize query
   * 2. For each token: exact match in tokenIndex, or fallback to trigram intersection
   * 3. Intersect postings across all query tokens
   * 4. Verify candidates with substring match on normalized value
   */
  searchCombined(indexTableName: string, field: string, query: string, limit?: number): string[] {
    if (!query) return [];

    const cacheKey = `${indexTableName}::${field}`;
    let idx = this.searchIndexCache.get(cacheKey);
    if (!idx) {
      idx = this.buildSearchIndex(indexTableName, field);
      this.searchIndexCache.set(cacheKey, idx);
    }

    if (idx.entries.length === 0) return [];

    const pat = IndexStore.normalizeForSearch(query);
    if (!pat) return [];

    const qTokens = IndexStore.tokenize(pat);
    let candidates: number[];

    if (qTokens.length === 0) {
      candidates = Array.from({ length: idx.entries.length }, (_, i) => i);
    } else {
      const postings: number[][] = [];
      for (const t of qTokens) {
        const p = idx.tokenIndex.get(t);
        if (p) {
          postings.push(p);
        } else if (t.length < IndexStore.NGRAM_SIZE) {
          // Token shorter than n-gram size — include all candidates; substring verification will filter
          postings.push(Array.from({ length: idx.entries.length }, (_, i) => i));
        } else {
          const p2 = IndexStore.postingsForTokenViaNgrams(t, idx.ngramIndex);
          if (p2.length === 0) return [];
          postings.push(p2);
        }
      }
      postings.sort((a, b) => a.length - b.length);
      candidates = IndexStore.intersectPostingLists(postings);
      if (candidates.length === 0) return [];
    }

    // Verify candidates with substring match on normalized text
    const maxResults =
      limit !== undefined && Number.isFinite(limit) && limit >= 0 ? Math.floor(limit) : candidates.length;
    if (maxResults === 0) return [];
    const seen = new Set<string>();
    const out: string[] = [];
    for (const pos of candidates) {
      if (idx.normalized[pos].includes(pat)) {
        const entityId = idx.entries[pos].entityId;
        if (!seen.has(entityId)) {
          seen.add(entityId);
          out.push(entityId);
        }
        if (out.length >= maxResults) break;
      }
    }
    return out;
  }

  /**
   * Return cached raw rows for a combined index sheet (avoids redundant getAllData calls
   * across multiple lookupCombined / searchCombined / updateInCombined invocations).
   * Cache is invalidated by clearCache() after any write.
   */
  private getCombinedData(indexTableName: string): unknown[][] {
    if (this.cache) {
      const cacheKey = `cidx:${indexTableName}`;
      const cached = this.cache.get<unknown[][]>(cacheKey);
      if (cached !== null) return cached;
      const sheet = this.adapter.getSheetByName(indexTableName);
      const data = sheet ? sheet.getAllData() : [];
      this.cache.set(cacheKey, data);
      return data;
    }
    const sheet = this.adapter.getSheetByName(indexTableName);
    return sheet ? sheet.getAllData() : [];
  }

  private invalidateSearchCacheForTable(indexTableName: string): void {
    const prefix = `${indexTableName}::`;
    for (const key of this.searchIndexCache.keys()) {
      if (key.startsWith(prefix)) this.searchIndexCache.delete(key);
    }
  }

  private clearCache(): void {
    this.searchIndexCache.clear();
    if (!this.cache) return;
    // Only invalidate index-specific keys rather than clearing entire cache
    const cleared = new Set<string>();
    for (const key of this.indexRegistry.keys()) {
      const tableName = key.split("::")[0];
      if (!cleared.has(tableName)) {
        cleared.add(tableName);
        this.cache.delete(`cidx:${tableName}`);
      }
    }
  }

  /**
   * Public entry point for clearing all index caches (search + data).
   * Called by Registry.clearCache() to ensure full cache coherence.
   */
  clearAllCaches(): void {
    this.clearCache();
  }
}
