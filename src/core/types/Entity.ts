/**
 * Base entity interface for all SheetORM records.
 *
 * Every row stored in a Google Sheet is represented as an Entity object.
 * System-managed columns (__id, __createdAt, __updatedAt) are prefixed with
 * double underscores to avoid collisions with user-defined field names.
 *
 * The index signature allows arbitrary user-defined fields to coexist
 * alongside the system columns without requiring a concrete type for each model.
 */
export interface Entity {
  /** Unique identifier for the record (UUID v4, auto-generated on creation). */
  __id: string;

  /** ISO 8601 timestamp set automatically when the record is first saved. */
  __createdAt?: string;

  /** ISO 8601 timestamp updated automatically on every save. */
  __updatedAt?: string;

  /** User-defined fields (e.g. name, price) — keys match sheet column headers. */
  [key: string]: unknown;
}
