import type { FilterOperator } from "./FilterOperator";

export interface Filter {
  field: string;
  operator: FilterOperator;
  value: unknown;
}
