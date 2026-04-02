/**
 * Schema-level definition of a secondary index on a Record field.
 *
 * Created by the @Indexed() decorator and stored in the TableSchema.
 * IndexStore uses this metadata to maintain a combined per-class index sheet
 * (e.g. idx_Cars) with columns [field, value, entityId].
 */
export interface IndexDefinition {
  /** Name of the entity field to index (must match a class property). */
  field: string;

  /** When true, the index enforces uniqueness — duplicate values for different entities throw. */
  unique?: boolean;

  /** Optional storage type hint (currently informational; serialization uses FieldDefinition.type). */
  type?: "string" | "number" | "date";
}
