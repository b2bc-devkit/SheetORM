/**
 * Decorator-based metadata system for declaring Record model fields and indexes.
 *
 * Provides three property decorators:
 *   - `@Indexed()` — mark a field for secondary indexing (lookups via IndexStore).
 *   - `@Required()` — shorthand for `@Field({ required: true })`.
 *   - `@Field()`   — override field metadata (type, required, defaultValue, etc.).
 *
 * And two discovery helpers:
 *   - `getFields(ctor)`  — return all FieldDefinition[] for a class.
 *   - `getIndexes(ctor)` — return all IndexDefinition[] for a class.
 *
 * Metadata is stored in module-scoped Maps keyed by constructor:
 *   - `indexedMeta`  — primary @Indexed entries.
 *   - `fieldMeta`    — primary @Field / @Required / @Indexed field patches.
 *   - `fieldsCache`  — derived FieldDefinition[] (invalidated on decoration).
 *   - `indexesCache` — derived IndexDefinition[] (invalidated on decoration).
 *
 * @module Decorators
 */

import type { FieldDefinition } from "./types/FieldDefinition.js";
import type { FieldType } from "./types/FieldType.js";
import type { IndexDefinition } from "./types/IndexDefinition.js";

/** Constructor shape accepted by the decorators (any class that produces an object). */
interface DecoratedModelCtor {
  new (): object;
}

/** The `target` parameter of a property decorator has a `constructor` property. */
type DecoratorTarget = {
  constructor: DecoratedModelCtor;
};

// ─── Metadata storage ────────────────────────────────

/** Primary storage for @Indexed() entries — one array of IndexDefinition per class. */
const indexedMeta = new Map<DecoratedModelCtor, IndexDefinition[]>();

/** Primary storage for per-field decorator patches — nested map: ctor → fieldName → partial def. */
const fieldMeta = new Map<DecoratedModelCtor, Map<string, Partial<FieldDefinition>>>();

/** Derived cache: merged FieldDefinition[] per class (cleared when a decorator fires). */
const fieldsCache = new Map<DecoratedModelCtor, FieldDefinition[]>();

/** Derived cache: IndexDefinition[] per class (cleared when @Indexed fires). */
const indexesCache = new Map<DecoratedModelCtor, IndexDefinition[]>();

/**
 * Merge a partial FieldDefinition patch into the metadata store for a given class/field.
 *
 * If the class or field has no metadata yet, creates fresh entries.
 * Invalidates the fieldsCache so the next `getFields()` call rebuilds.
 *
 * @param ctor        - The model constructor (class).
 * @param propertyKey - Decorated property name.
 * @param patch       - Partial field definition to merge.
 */
function upsertFieldMetadata(
  ctor: DecoratedModelCtor,
  propertyKey: string | symbol,
  patch: Partial<FieldDefinition>,
): void {
  if (!fieldMeta.has(ctor)) fieldMeta.set(ctor, new Map());
  const name = String(propertyKey);
  const existing = fieldMeta.get(ctor)!.get(name) || {};
  // Merge existing metadata with new patch; `name` is always set
  fieldMeta.get(ctor)!.set(name, {
    name,
    ...existing,
    ...patch,
  });
  // Invalidate derived cache
  fieldsCache.delete(ctor);
}

// ─── Property Decorators ─────────────────────────────

/**
 * Mark a field as indexed for faster lookups via IndexStore.
 *
 * When a Record subclass has fields decorated with `@Indexed()`, SheetORM
 * creates a combined index sheet (e.g. `idx_Cars`) and maintains index entries
 * during save/delete operations. The `search` filter operator leverages the
 * index for n-gram full-text search.
 *
 * @param options.unique - Enforce unique values across all entities (default: false).
 * @param options.type   - Storage type hint for the index ("string" | "number" | "date").
 * @returns A standard TypeScript property decorator.
 *
 * @example
 * class Car extends Record {
 *   @Indexed({ unique: true })
 *   vin!: string;
 *
 *   @Indexed()
 *   make!: string;
 * }
 */
function Indexed(options?: { unique?: boolean; type?: "string" | "number" | "date" }): PropertyDecorator {
  return (target: object, propertyKey: string | symbol) => {
    const ctor = (target as DecoratorTarget).constructor;
    // Register the index definition
    if (!indexedMeta.has(ctor)) indexedMeta.set(ctor, []);
    indexedMeta.get(ctor)!.push({ field: String(propertyKey), ...options });
    // Also register in fieldMeta so the field appears in getFields() even
    // when using ES2022 [[Set]] class field semantics (no own property on prototype)
    upsertFieldMetadata(ctor, propertyKey, {});
    // Invalidate index cache
    indexesCache.delete(ctor);
  };
}

/**
 * Mark a field as required — shorthand for `@Field({ required: true })`.
 *
 * Required fields are validated during save(); an error is thrown if the
 * value is null, undefined, or an empty string.
 *
 * @example
 * class User extends Record {
 *   @Required()
 *   email!: string;
 * }
 */
function Required(): PropertyDecorator {
  return (target: object, propertyKey: string | symbol) => {
    const ctor = (target as DecoratorTarget).constructor;
    upsertFieldMetadata(ctor, propertyKey, { required: true });
  };
}

/**
 * Override field metadata: storage type, required flag, default value, or
 * reference table name.
 *
 * Fields that do not carry any decorator are still auto-discovered from a
 * temporary instance's own properties. Use `@Field()` when you need to
 * explicitly set the storage type or a default value.
 *
 * @param options.type           - Storage type ("string" | "number" | "boolean" | "date" | "json" | "reference").
 * @param options.required       - Validate non-empty on save (default: false).
 * @param options.defaultValue   - Fallback value when the field is undefined.
 * @param options.referenceTable - Related table name (for "reference" type fields).
 *
 * @example
 * class Product extends Record {
 *   @Field({ type: "number", defaultValue: 0 })
 *   price!: number;
 *
 *   @Field({ type: "json" })
 *   metadata!: Record<string, unknown>;
 * }
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
 *
 * Merges two sources:
 *   1. **Own properties** from a temporary instance — captures ESNext [[Define]]
 *      class fields that become own properties without a decorator.
 *   2. **Decorator metadata** from `@Indexed` / `@Field` / `@Required` — captures
 *      fields declared with [[Set]] semantics (TypeScript `useDefineForClassFields: false`).
 *
 * Results are cached per constructor and invalidated whenever a decorator fires.
 *
 * @param ctor - Model constructor to inspect.
 * @returns Array of FieldDefinition objects (one per user-defined property).
 */
function getFields(ctor: DecoratedModelCtor): FieldDefinition[] {
  // Return cached result if available
  if (fieldsCache.has(ctor)) return fieldsCache.get(ctor)!;

  // 1. Discover own properties from a temp instance (undecorated fields defined via [[Define]])
  const temp = new ctor();
  const discoveredNames = new Set(Object.getOwnPropertyNames(temp));

  // 2. Add names registered through @Indexed decorator
  const idxMeta = indexedMeta.get(ctor) || [];
  for (const idx of idxMeta) discoveredNames.add(idx.field);

  // 3. Add names registered through @Field / @Required decorators
  const fMeta = fieldMeta.get(ctor) || new Map<string, Partial<FieldDefinition>>();
  for (const name of fMeta.keys()) discoveredNames.add(name);

  // 4. Build the final FieldDefinition array, merging decorator overrides
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

  // Cache for subsequent calls
  fieldsCache.set(ctor, fields);
  return fields;
}

/**
 * Get all index definitions for a Record subclass (from `@Indexed` decorators).
 *
 * @param ctor - Model constructor to inspect.
 * @returns Array of IndexDefinition objects.
 */
function getIndexes(ctor: DecoratedModelCtor): IndexDefinition[] {
  if (indexesCache.has(ctor)) return indexesCache.get(ctor)!;
  const indexes = indexedMeta.get(ctor) || [];
  indexesCache.set(ctor, indexes);
  return indexes;
}

/**
 * Reset derived decorator caches (fieldsCache and indexesCache).
 *
 * Called by `Registry.reset()` during testing to ensure a clean state.
 * Primary metadata (`fieldMeta` and `indexedMeta`) is set at class-definition
 * time by decorators and must NOT be cleared here — only derived caches.
 */
function resetDecoratorCaches(): void {
  fieldsCache.clear();
  indexesCache.clear();
}

/**
 * Public decorator utility class.
 *
 * Wraps the three property decorators and two discovery functions
 * as static methods, following the SheetORM one-export-per-file pattern.
 */
export class Decorators {
  /** Property decorator: mark a field for secondary indexing. */
  static Indexed = Indexed;

  /** Property decorator: mark a field as required (shorthand). */
  static Required = Required;

  /** Property decorator: set explicit field metadata. */
  static Field = Field;

  /** Discover all field definitions for a model class. */
  static getFields = getFields;

  /** Discover all index definitions for a model class. */
  static getIndexes = getIndexes;

  /** Reset derived caches (for testing). */
  static resetDecoratorCaches = resetDecoratorCaches;
}
