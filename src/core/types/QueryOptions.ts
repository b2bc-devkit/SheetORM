import type { Filter } from "./Filter.js";
import type { SortClause } from "./SortClause.js";

/**
 * Declarative query descriptor used by SheetRepository.find() / count() / deleteAll().
 *
 * Supports AND-connected filters (where), OR-connected filter groups (whereGroups),
 * multi-column sorting (orderBy), and pagination (offset / limit).
 */
export interface QueryOptions {
  /** Filters combined with AND logic (mutually exclusive with whereGroups). */
  where?: Filter[];

  /** Filter groups combined with OR logic; each inner array is AND-connected. */
  whereGroups?: Filter[][];

  /** Sort clauses applied in order after filtering. */
  orderBy?: SortClause[];

  /** Maximum number of results to return. */
  limit?: number;

  /** Number of results to skip before returning (0-based). */
  offset?: number;
}
