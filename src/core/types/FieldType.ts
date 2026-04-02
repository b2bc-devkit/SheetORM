/**
 * Supported field storage types for SheetORM columns.
 *
 * - "string"    — stored and retrieved as plain text.
 * - "number"    — stored as a numeric cell value; non-finite values become empty.
 * - "boolean"   — stored as TRUE / FALSE; deserialised from various truthy strings.
 * - "date"      — stored as ISO 8601 string; Date objects are converted automatically.
 * - "json"      — stored as a JSON-stringified cell; parsed back on read.
 * - "reference" — alias for "string", intended for foreign-key-like table references.
 */
export type FieldType = "string" | "number" | "boolean" | "date" | "json" | "reference";
