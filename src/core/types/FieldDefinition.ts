import type { FieldType } from "./FieldType";

export interface FieldDefinition {
  name: string;
  type?: FieldType;
  required?: boolean;
  defaultValue?: unknown;
  referenceTable?: string;
}
