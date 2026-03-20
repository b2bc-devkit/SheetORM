import type { Filter } from "./Filter";
import type { SortClause } from "./SortClause";

export interface QueryOptions {
  where?: Filter[];
  orderBy?: SortClause[];
  limit?: number;
  offset?: number;
}
