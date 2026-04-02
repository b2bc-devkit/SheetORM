import type { FilterOperator } from "./FilterOperator.js";

/**
 * A single filter condition used in queries.
 *
 * Filters are combined with AND logic within a filter group,
 * and groups are combined with OR logic (see QueryOptions.whereGroups).
 */
export interface Filter {
  /** Entity field name to compare (supports dot-separated nested paths). */
  field: string;

  /** Comparison operator (e.g. "=", ">", "contains", "search"). */
  operator: FilterOperator;

  /** Value to compare against (type depends on the operator). */
  value: unknown;
}
