import type { Filter } from "./Filter.js";
import type { SortClause } from "./SortClause.js";

export interface QueryOptions {
  where?: Filter[];
  whereGroups?: Filter[][];
  orderBy?: SortClause[];
  limit?: number;
  offset?: number;
}
