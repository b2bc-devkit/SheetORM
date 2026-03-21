import type { FieldType } from "./FieldType.js";

export interface FieldDefinition {
  name: string;
  type?: FieldType;
  required?: boolean;
  defaultValue?: unknown;
  referenceTable?: string;
}
