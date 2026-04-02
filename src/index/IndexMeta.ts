/**
 * Lightweight metadata describing a single secondary index entry
 * as stored in the combined index sheet.
 *
 * Used by IndexStore to track which fields are indexed and whether
 * they enforce uniqueness.
 */
export interface IndexMeta {
  /** Name of the entity table that owns this index (e.g. "tbl_Cars"). */
  tableName: string;

  /** Name of the indexed field (e.g. "make", "email"). */
  field: string;

  /** When true, the index enforces that no two entities share the same field value. */
  unique: boolean;
}
