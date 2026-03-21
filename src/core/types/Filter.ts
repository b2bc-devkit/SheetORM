import type { FilterOperator } from "./FilterOperator.js";

export interface Filter {
  field: string;
  operator: FilterOperator;
  value: unknown;
}
