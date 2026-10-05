import type { FieldDefinition } from "./FieldDefinition.js";
import type { IndexDefinition } from "./IndexDefinition.js";

/**
 * Complete schema descriptor for a SheetORM-managed table.
 *
 * Built automatically by Registry.ensureRepository() from the
 * Record subclass's static tableName / indexTableName and the
 * decorator-collected field and index metadata.
 */
export interface TableSchema {
  /** Name of the data sheet tab (e.g. "tbl_Cars"). */
  tableName: string;

  /**
   * Name of the combined index sheet tab (e.g. "idx_Cars").
   * Omitted when the class has no @Indexed fields — avoids
   * unnecessary getSheetByName() API calls on every save.
   */
  indexTableName?: string;

  /** All user-defined fields discovered from decorators and class properties. */
  fields: FieldDefinition[];

  /** Secondary index definitions from @Indexed() decorators. */
  indexes: IndexDefinition[];

  /**
   * Cache TTL (ms) for this table's cached data and index rows —
   * from `Record.cacheTtlMs()`.  Undefined → provider default.
   */
  cacheTtlMs?: number;

  /**
   * Opt-in tombstone deletes (`Record.tombstoneDeletes()`): deletes write a
   * `#TOMB#` marker into `__id` instead of removing the row; reads skip
   * marked rows; a compaction pass reclaims them past ~25% dead space.
   */
  tombstones?: boolean;

  /**
   * Opt-in packed storage (`Record.packedStorage()`): sheet layout is
   * `__id | __data` where `__data` is the JSON-serialised entity.
   */
  packed?: boolean;
}
