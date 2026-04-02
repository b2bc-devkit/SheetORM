/**
 * Comparison operators supported by the SheetORM query engine.
 *
 * Numeric / equality:
 *   "=", "!=", "<", ">", "<=", ">="
 *
 * String matching (case-insensitive):
 *   "contains"   — substring match
 *   "startsWith" — prefix match
 *   "search"     — full-text n-gram search (uses IndexStore when available)
 *
 * Collection:
 *   "in" — value is in a provided array of allowed values
 */
export type FilterOperator =
  | "="
  | "!="
  | "<"
  | ">"
  | "<="
  | ">="
  | "contains"
  | "startsWith"
  | "in"
  | "search";
