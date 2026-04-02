/**
 * Constants for the three system-managed column names.
 *
 * These columns are always present in every SheetORM table and occupy
 * the first three columns of the header row:
 *   [__id, __createdAt, __updatedAt, ...userFields]
 */
export class SystemColumns {
  /** Primary key column — holds the UUID v4 identifier. */
  static readonly ID = "__id";

  /** Auto-set to the ISO 8601 timestamp when the record is first created. */
  static readonly CREATED_AT = "__createdAt";

  /** Auto-updated to the ISO 8601 timestamp on every save. */
  static readonly UPDATED_AT = "__updatedAt";
}
