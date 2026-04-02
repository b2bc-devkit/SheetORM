import type { FieldType } from "./FieldType.js";

/**
 * Metadata describing a single field on a Record subclass.
 *
 * Collected at class-definition time by the @Field / @Indexed / @Required
 * decorators and used by Serialization to convert between entity objects
 * and Google Sheet row arrays.
 */
export interface FieldDefinition {
  /** Column / property name (must match the class field name). */
  name: string;

  /** Storage type hint used for serialize / deserialize (auto-inferred when omitted). */
  type?: FieldType;

  /** When true, SheetRepository.save() throws if the value is empty / null / undefined. */
  required?: boolean;

  /** Fallback value applied during save() when the field is undefined on the entity. */
  defaultValue?: unknown;

  /** Name of the related table (for "reference" typed fields — future foreign-key support). */
  referenceTable?: string;
}
