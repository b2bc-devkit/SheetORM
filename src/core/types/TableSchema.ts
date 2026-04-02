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
}
