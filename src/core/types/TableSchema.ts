import type { FieldDefinition } from "./FieldDefinition.js";
import type { IndexDefinition } from "./IndexDefinition.js";

export interface TableSchema {
  tableName: string;
  indexTableName?: string;
  fields: FieldDefinition[];
  indexes: IndexDefinition[];
}
