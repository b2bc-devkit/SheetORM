import type { FieldDefinition } from "./FieldDefinition";
import type { IndexDefinition } from "./IndexDefinition";

export interface TableSchema {
  tableName: string;
  indexTableName?: string;
  fields: FieldDefinition[];
  indexes: IndexDefinition[];
}
