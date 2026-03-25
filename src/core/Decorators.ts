// SheetORM — Decorators: @Indexed, @Field for declarative Record model definitions

import type { FieldDefinition } from "./types/FieldDefinition.js";
import type { FieldType } from "./types/FieldType.js";
import type { IndexDefinition } from "./types/IndexDefinition.js";

interface DecoratedModelCtor {
  new (): object;
}

type DecoratorTarget = {
  constructor: DecoratedModelCtor;
};

// ─── Metadata storage ────────────────────────────────

const indexedMeta = new Map<DecoratedModelCtor, IndexDefinition[]>();
const fieldMeta = new Map<DecoratedModelCtor, Map<string, Partial<FieldDefinition>>>();
const fieldsCache = new Map<DecoratedModelCtor, FieldDefinition[]>();
const indexesCache = new Map<DecoratedModelCtor, IndexDefinition[]>();

function upsertFieldMetadata(
  ctor: DecoratedModelCtor,
  propertyKey: string | symbol,
  patch: Partial<FieldDefinition>,
): void {
  if (!fieldMeta.has(ctor)) fieldMeta.set(ctor, new Map());
  const name = String(propertyKey);
  const existing = fieldMeta.get(ctor)!.get(name) || {};
  fieldMeta.get(ctor)!.set(name, {
    name,
    ...existing,
    ...patch,
  });
  fieldsCache.delete(ctor);
}

// ─── Property Decorators ─────────────────────────────

/**
 * Mark a field as indexed for faster lookups.
 * Optionally specify `unique` or index storage `type`.
 */
function Indexed(options?: { unique?: boolean; type?: "string" | "number" | "date" }): PropertyDecorator {
  return (target: object, propertyKey: string | symbol) => {
    const ctor = (target as DecoratorTarget).constructor;
    if (!indexedMeta.has(ctor)) indexedMeta.set(ctor, []);
    indexedMeta.get(ctor)!.push({ field: String(propertyKey), ...options });
    // Also register in fieldMeta so it appears in getFields even with [[Set]] semantics
    upsertFieldMetadata(ctor, propertyKey, {});
    indexesCache.delete(ctor);
  };
}

/**
 * Mark a field as required using shorthand decorator syntax.
 * Equivalent to `@Field({ required: true })`.
 */
function Required(): PropertyDecorator {
  return (target: object, propertyKey: string | symbol) => {
    const ctor = (target as DecoratorTarget).constructor;
    upsertFieldMetadata(ctor, propertyKey, { required: true });
  };
}

/**
 * Override field metadata: type, required, defaultValue, referenceTable.
 * Fields without @Field() are auto-discovered and default to auto type inference.
 */
function Field(options?: {
  type?: FieldType;
  required?: boolean;
  defaultValue?: unknown;
  referenceTable?: string;
}): PropertyDecorator {
  return (target: object, propertyKey: string | symbol) => {
    const ctor = (target as DecoratorTarget).constructor;
    upsertFieldMetadata(ctor, propertyKey, options ?? {});
  };
}

// ─── Field & Index Discovery ─────────────────────────

/**
 * Discover all user-defined fields for a Record subclass.
 * Combines:
 *  1. Own properties from a temp instance (ESNext [[Define]] class fields)
 *  2. Decorated properties from @Indexed/@Field metadata ([[Set]] fields)
 */
function getFields(ctor: DecoratedModelCtor): FieldDefinition[] {
  if (fieldsCache.has(ctor)) return fieldsCache.get(ctor)!;

  // 1. Discover own properties from temp instance (undecorated fields with [[Define]])
  const temp = new ctor();
  const discoveredNames = new Set(Object.getOwnPropertyNames(temp));

  // 2. Add decorated field names from @Indexed and @Field metadata
  const idxMeta = indexedMeta.get(ctor) || [];
  for (const idx of idxMeta) discoveredNames.add(idx.field);

  const fMeta = fieldMeta.get(ctor) || new Map<string, Partial<FieldDefinition>>();
  for (const name of fMeta.keys()) discoveredNames.add(name);

  // 3. Build FieldDefinition[]
  const fields: FieldDefinition[] = [];
  for (const name of discoveredNames) {
    const override = fMeta.get(name);
    fields.push({
      name,
      type: override?.type,
      required: override?.required,
      defaultValue: override?.defaultValue,
      referenceTable: override?.referenceTable,
    });
  }

  fieldsCache.set(ctor, fields);
  return fields;
}

/**
 * Get all index definitions for a Record subclass (from @Indexed decorators).
 */
function getIndexes(ctor: DecoratedModelCtor): IndexDefinition[] {
  if (indexesCache.has(ctor)) return indexesCache.get(ctor)!;
  const indexes = indexedMeta.get(ctor) || [];
  indexesCache.set(ctor, indexes);
  return indexes;
}

/**
 * Reset derived decorator caches. Called by SheetORM.reset() for testing.
 * Note: fieldMeta and indexedMeta are primary metadata set at class-definition
 * time by decorators and must NOT be cleared here.
 */
function resetDecoratorCaches(): void {
  fieldsCache.clear();
  indexesCache.clear();
}

export class Decorators {
  static Indexed = Indexed;
  static Required = Required;
  static Field = Field;
  static getFields = getFields;
  static getIndexes = getIndexes;
  static resetDecoratorCaches = resetDecoratorCaches;
}
