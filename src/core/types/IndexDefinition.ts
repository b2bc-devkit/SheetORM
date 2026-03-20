export interface IndexDefinition {
  field: string;
  unique?: boolean;
  type?: "string" | "number" | "date";
}
