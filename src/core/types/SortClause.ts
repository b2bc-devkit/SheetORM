/**
 * A single sort instruction used in QueryOptions.orderBy.
 */
export interface SortClause {
  /** Entity field name to sort by (supports dot-separated nested paths). */
  field: string;

  /** Sort direction: ascending or descending. */
  direction: "asc" | "desc";
}
