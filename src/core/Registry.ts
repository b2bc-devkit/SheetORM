/**
 * Global singleton registry that manages the ORM runtime state.
 *
 * Responsibilities:
 * - Holds the single {@link ISpreadsheetAdapter} (defaults to GAS active spreadsheet).
 * - Maintains a per-table {@link SheetRepository} cache so the same repo
 *   is reused for the lifetime of the script execution.
 * - Creates and wires up the shared {@link IndexStore} used by all repos.
 * - Keeps a class map so `Query.from("ClassName")` can resolve by name.
 *
 * @module Registry
 */

import type { Entity } from "./types/Entity.js";
import type { ICacheProvider } from "./types/ICacheProvider.js";
import type { ISheetAdapter } from "./types/ISheetAdapter.js";
import type { ISpreadsheetAdapter } from "./types/ISpreadsheetAdapter.js";
import type { TableSchema } from "./types/TableSchema.js";
import { SheetRepository } from "./SheetRepository.js";
import { IndexStore } from "../index/IndexStore.js";
import { MemoryCache } from "./cache/MemoryCache.js";
import { GoogleSpreadsheetAdapter } from "../storage/GoogleSpreadsheetAdapter.js";
import { Decorators } from "./Decorators.js";
import type { RecordStatic } from "./RecordStatic.js";
import { SystemColumns } from "./types/SystemColumns.js";
import { Serialization } from "../utils/Serialization.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";
import { fingerprintMatches, writeFingerprint } from "../storage/SchemaFingerprint.js";

/**
 * Central registry for SheetORM.
 *
 * Accessed via `Registry.getInstance()`.  Call `Registry.reset()` in
 * tests to start with a clean slate.
 */
export class Registry {
  /** Singleton instance (lazy-initialised). */
  private static instance: Registry | null = null;

  /** The spreadsheet-level adapter (wraps GAS Spreadsheet or mock). */
  private adapter: ISpreadsheetAdapter | null = null;
  /** Optional entity data cache (defaults to MemoryCache). */
  private cache: ICacheProvider | null = null;
  /** Shared secondary-index manager. */
  private indexStore: IndexStore | null = null;
  /** tableName → SheetRepository cache. */
  private repos = new Map<string, SheetRepository<Entity>>();
  /** tableName → RecordStatic class map for lookup by table name. */
  private classesByTable = new Map<string, RecordStatic>();
  /** className → RecordStatic class map for lookup by constructor name. */
  private classesByName = new Map<string, RecordStatic>();

  /** Return (or create) the singleton Registry instance. */
  static getInstance(): Registry {
    if (!Registry.instance) {
      Registry.instance = new Registry();
    }
    return Registry.instance;
  }

  /** Destroy the singleton — used by test suites to reset state between runs. */
  static reset(): void {
    Registry.instance = null;
  }

  /**
   * Configure the registry with a custom adapter and/or cache.
   * Clears all previously created repos, class maps, and the index store.
   */
  configure(options: { adapter?: ISpreadsheetAdapter; cache?: ICacheProvider }): void {
    SheetOrmLogger.log(`[Registry] configure (adapter=${options.adapter?.constructor.name ?? "default"})`);
    this.adapter = options.adapter ?? null;
    this.cache = options.cache ?? null;
    this.indexStore = null;
    this.repos.clear();
    this.classesByTable.clear();
    this.classesByName.clear();
  }

  /**
   * Return the current adapter, falling back to a default
   * GoogleSpreadsheetAdapter (active spreadsheet) if none was configured.
   */
  private getAdapter(): ISpreadsheetAdapter {
    if (!this.adapter) {
      this.adapter = new GoogleSpreadsheetAdapter();
    }
    return this.adapter;
  }

  /**
   * Lazily create the shared IndexStore.
   * The IndexStore shares the configured cache provider so a persistent
   * implementation (e.g. {@link GasCacheProvider}) warms `cidx:` entries
   * across executions too — invalidation is per-key (`cidx:<table>`), never
   * a blanket `clear()`, so it cannot contaminate entity caches.  With no
   * configured provider it falls back to a private MemoryCache as before.
   */
  private ensureIndexStore(): IndexStore {
    if (!this.indexStore) {
      const adapter = this.getAdapter();
      if (!this.cache) this.cache = new MemoryCache();
      this.indexStore = new IndexStore(adapter, this.cache);
    }
    return this.indexStore;
  }

  /**
   * Ensure the sheet tab for a given schema exists, and register
   * any declared secondary indexes with the IndexStore.
   *
   * @param schema     - The table schema (name, fields, indexes).
   * @param indexStore - The shared IndexStore.
   * @param ctor       - The Record subclass constructor (for protection config).
   * @returns The sheet adapter and whether it was freshly created.
   */
  private ensureTable(
    schema: TableSchema,
    indexStore: IndexStore,
    ctor: RecordStatic,
  ): { sheet: ISheetAdapter; created: boolean } {
    const adapter = this.getAdapter();
    SheetOrmLogger.log(`[Registry] ensureTable "${schema.tableName}" (indexes=${schema.indexes.length})`);

    // Always ensure headers are present on the sheet.
    // Previously headers for new sheets were deferred to the first data write
    // (L1 optimisation), but this caused a bug: if a read-only operation
    // (e.g. findOne) triggered ensureTable, the GAS execution could end
    // before any write flushed the deferred headers, leaving the sheet
    // without headers for all subsequent executions.
    const expectedHeaders = schema.packed
      ? Serialization.buildPackedHeaders()
      : [
          SystemColumns.ID,
          SystemColumns.CREATED_AT,
          SystemColumns.UPDATED_AT,
          ...schema.fields.map((f) => f.name),
        ];

    // Check-first strategy: when the Sheets API metadata cache is warm,
    // getSheetByName() costs ZERO RPCs — so probing is free and the common
    // "table already exists" path (shared class names across tests) pays
    // nothing.  Only a real miss proceeds to creation.
    let sheet: ISheetAdapter | null = adapter.getSheetByName(schema.tableName);
    let created = false;

    if (!sheet) {
      // Data-table tabs are ALWAYS created through SpreadsheetApp: tests and
      // protection/hide APIs address the sheet through the raw GAS handle,
      // and a Sheets-API-created tab can stay invisible to SpreadsheetApp for
      // an unpredictable amount of time.  Index tabs are never raw-checked,
      // so they keep the fast API path inside createCombinedIndex.
      const rawInserter = (adapter as { insertSheetRaw?: (n: string) => ISheetAdapter }).insertSheetRaw;
      try {
        sheet = rawInserter
          ? rawInserter.call(adapter, schema.tableName)
          : adapter.insertSheet(schema.tableName);
        created = true;
      } catch {
        // Duplicate-name or transient failure — re-resolve to be sure.
        sheet = adapter.getSheetByName(schema.tableName);
        created = false;
      }
      if (!sheet) {
        // insertSheet threw for a non-duplicate reason (e.g. quota/permission)
        // and no sheet exists — retry the insert so the real error surfaces.
        sheet = adapter.insertSheet(schema.tableName);
        created = true;
      }
    }
    SheetOrmLogger.log(
      `[Registry] ensureTable "${schema.tableName}" → ${created ? "insertSheet (G4 new)" : "existing sheet"}`,
    );

    // Schema fingerprint (PropertiesService): a verified (schema hash +
    // sheetId + TTL) match proves this tab already carries the expected
    // headers → skip BOTH the header-verification read AND the entity-grid
    // prefetch (the grid loads lazily on first data access — write-only
    // executions then pay zero read RPCs for this table).
    const ssId = adapter.getSpreadsheetId?.() ?? null;
    const sheetId = sheet.getSheetId?.() ?? null;
    const fpHit = !created && fingerprintMatches(ssId, schema, sheetId);

    const hasIndex = schema.indexTableName !== undefined && schema.indexes.length > 0;
    if (fpHit) {
      // Still warm the index grid — it is ORM-internal and always consistent.
      if (hasIndex) {
        if (!adapter.warmUpSheets?.([schema.indexTableName!])) {
          adapter.prefetchSheets?.([schema.indexTableName!]);
        }
      }
      SheetOrmLogger.log(
        `[Registry] ensureTable "${schema.tableName}" → fingerprint hit, header check skipped`,
      );
    } else {
      // Fused grid warm-up: prefers the parallel cold-start path (fetchAll —
      // meta + grids concurrently); falls back to ONE serial Values.batchGet
      // seeding the entity grid AND the combined-index grid.  Sheets absent
      // from the shared metadata are skipped by the adapter.
      const warmNames = hasIndex ? [schema.tableName, schema.indexTableName!] : [schema.tableName];
      if (!adapter.warmUpSheets?.(warmNames)) {
        adapter.prefetchSheets?.(warmNames);
      }
      const existingHeaders = sheet.getHeaders();
      const hasValidHeaders = existingHeaders.length > 0 && existingHeaders.some((h) => h !== "");
      if (!hasValidHeaders) {
        sheet.setHeaders(expectedHeaders);
        SheetOrmLogger.log(
          `[Registry] ensureTable "${schema.tableName}" → wrote ${expectedHeaders.length} headers`,
        );
      }
      // Persist the verified fingerprint for future cold starts.
      writeFingerprint(ssId, schema, created ? (sheet.getSheetId?.() ?? null) : sheetId);
    }

    // Apply sheet protection when the sheet is newly created and the class opts in
    if (created && ctor.isProtected()) {
      const editors = ctor.protectedFor();
      adapter.protectSheet(schema.tableName, editors);
      SheetOrmLogger.log(
        `[Registry] ensureTable "${schema.tableName}" → protected (editors=${editors.length})`,
      );
    }

    // Hide the sheet tab when the sheet is newly created and the class opts in
    if (created && ctor.isHidden()) {
      adapter.hideSheet(schema.tableName);
      SheetOrmLogger.log(`[Registry] ensureTable "${schema.tableName}" → hidden`);
    }

    // If no indexes are defined, skip index table creation
    if (schema.indexes.length === 0) return { sheet, created };

    if (!schema.indexTableName) {
      throw new Error(
        `Table schema "${schema.tableName}" defines indexes but has no indexTableName. ` +
          "Legacy per-field _idx_* indexes were removed; use a combined index table name (e.g. idx_ClassName).",
      );
    }

    // Create the combined index sheet (meta-cached check-first; created via
    // the Sheets API path since index tabs are never inspected through the
    // raw SpreadsheetApp handle)
    indexStore.createCombinedIndex(schema.indexTableName);
    if (schema.cacheTtlMs !== undefined) {
      indexStore.setIndexCacheTtl(schema.indexTableName, schema.cacheTtlMs);
    }
    for (const idx of schema.indexes) {
      indexStore.registerIndex(schema.indexTableName, idx.field, idx.unique ?? false);
    }

    return { sheet, created };
  }

  /**
   * Register a Record subclass so it can be looked up by table name or
   * class name via `getClassByName()`.
   */
  registerClass(ctor: RecordStatic): void {
    if (!this.classesByTable.has(ctor.tableName)) {
      this.classesByTable.set(ctor.tableName, ctor);
    }
    if (ctor.name && !this.classesByName.has(ctor.name)) {
      this.classesByName.set(ctor.name, ctor);
    }
  }

  /**
   * Return (or create) the SheetRepository for a given Record subclass.
   *
   * On first call for a table name the method:
   * 1. Registers the class in the class map.
   * 2. Builds a {@link TableSchema} from decorator metadata.
   * 3. Ensures the sheet tab exists (createSheet / insertSheet).
   * 4. Creates a new SheetRepository with all optimisation flags
   *    (L1 deferred headers, K2 index skipping, B5 known row count).
   *
   * Subsequent calls return the cached repository instance.
   */
  ensureRepository<T extends Entity>(ctor: RecordStatic): SheetRepository<T> {
    const tableName = ctor.tableName;

    // Return cached repo if it already exists
    if (this.repos.has(tableName)) {
      SheetOrmLogger.log(`[Registry] ensureRepository "${tableName}" → cache hit`);
      return this.repos.get(tableName) as unknown as SheetRepository<T>;
    }

    SheetOrmLogger.log(`[Registry] ensureRepository "${tableName}" → creating`);

    this.registerClass(ctor);

    const indexStore = this.ensureIndexStore();

    // Build schema from decorator metadata
    const indexes = Decorators.getIndexes(ctor);
    const schema: TableSchema = {
      tableName,
      // Only populate indexTableName when the class has @Indexed fields.
      // Record.indexTableName always returns a string (e.g. "idx_Cars"), but for classes
      // without indexes this causes every update/delete to call getSheetByName() on a
      // non-existent index sheet, wasting ~700 ms per API call.
      indexTableName: indexes.length > 0 ? ctor.indexTableName : undefined,
      fields: Decorators.getFields(ctor),
      indexes,
      // Per-class cache TTL (overridable via static cacheTtlMs()).
      cacheTtlMs: typeof ctor.cacheTtlMs === "function" ? ctor.cacheTtlMs() : undefined,
      // Opt-in storage modes (overridable via statics).
      tombstones: typeof ctor.tombstoneDeletes === "function" ? ctor.tombstoneDeletes() : false,
      packed: typeof ctor.packedStorage === "function" ? ctor.packedStorage() : false,
    };

    // Create the sheet tab (or get existing) and register indexes
    const { sheet, created } = this.ensureTable(schema, indexStore, ctor);

    // Construct the repository with pre-resolved sheet and row-count hints:
    // - sheet:                 pre-resolved ISheetAdapter (avoids getSheetByName call)
    // - created ? 0 : undefined:  known row count (0 for new sheets, avoids getLastRow call)
    // - created:               defer header write to first data flush
    const repo = new SheetRepository<T>(
      this.getAdapter(),
      schema,
      indexStore,
      this.cache!,
      undefined,
      sheet,
      created ? 0 : undefined,
      false, // headers are always written eagerly in ensureTable
    );

    this.repos.set(tableName, repo as unknown as SheetRepository<Entity>);
    return repo;
  }

  /**
   * Look up a previously registered Record subclass by constructor name
   * or table name.
   */
  getClassByName(name: string): RecordStatic | undefined {
    return this.classesByName.get(name) ?? this.classesByTable.get(name);
  }

  /** Return the shared IndexStore (creates it if needed). */
  getIndexStore(): IndexStore {
    return this.ensureIndexStore();
  }

  /** Flush both the entity data cache and the index store caches. */
  clearCache(): void {
    if (this.cache) this.cache.clear();
    if (this.indexStore) this.indexStore.clearAllCaches();
  }
}
