/**
 * QueryEngine — pure in-memory filter, sort, paginate, and group engine.
 *
 * Every function operates on plain entity arrays and returns new arrays
 * without mutating the input.  The implementation is heavily optimised for
 * the GAS V8 runtime (limited JIT, no SIMD, high GC cost):
 *
 *   - Filters are **compiled** into predicate closures once; the hot loop
 *     invokes closures instead of evaluating operator strings.
 *   - Sort keys are **pre-extracted** into column-major arrays to avoid
 *     repeated field navigation during comparison callbacks.
 *   - The `in` operator converts to a `Set` when the value list exceeds
 *     8 elements for O(1) membership checks.
 *   - String comparison operators (`contains`, `startsWith`, `search`)
 *     are case-insensitive and pre-lower the filter value once.
 *
 * @module QueryEngine
 */

import type { Entity } from "../core/types/Entity.js";
import type { Filter } from "../core/types/Filter.js";
import type { SortClause } from "../core/types/SortClause.js";
import type { QueryOptions } from "../core/types/QueryOptions.js";
import type { PaginatedResult } from "../core/types/PaginatedResult.js";
import type { GroupResult } from "../core/types/GroupResult.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

/**
 * Split a field path into segments for nested property access.
 *
 * Single-segment paths (no dots or slashes) return `null` to signal the
 * fast-path in {@link compileFieldAccessor} — a direct property read.
 *
 * @param field - Dot- or slash-separated field path (e.g. `"address.city"`).
 * @returns Array of segments, or `null` for a simple field name.
 */
function splitFieldPath(field: string): string[] | null {
  if (field.indexOf(".") === -1 && field.indexOf("/") === -1) return null;
  return field.replace(/\//g, ".").split(".");
}

/**
 * Pre-compile a field accessor closure for the given field path.
 *
 * Simple fields return a direct property reader; nested paths return a
 * closure that walks each segment, returning `undefined` for missing
 * intermediate objects.
 *
 * @param field - Dot-separated field path.
 * @returns Accessor function: `(entity) => fieldValue`.
 */
function compileFieldAccessor(field: string): (entity: Entity) => unknown {
  const parts = splitFieldPath(field);
  if (parts === null) {
    // Simple field — direct property access (fast path)
    return (entity: Entity) => entity[field];
  }
  // Nested field — walk the path with null-safe navigation
  return (entity: Entity) => {
    let current: unknown = entity;
    for (let i = 0; i < parts.length; i++) {
      if (current == null) return undefined;
      current = (current as Record<string, unknown>)[parts[i]];
    }
    return current;
  };
}

/**
 * Compile a single {@link Filter} into a fast predicate function.
 *
 * Pre-computes the field accessor and, for the `in` operator, converts
 * large value arrays (>8 elements) to a `Set` for O(1) membership checks.
 *
 * Supported operators: `=`, `!=`, `<`, `>`, `<=`, `>=`, `contains`,
 * `startsWith`, `in`, `search`.
 *
 * @param f - The filter to compile.
 * @returns Predicate function: `(entity) => boolean`.
 */
function compileFilter(f: Filter): (entity: Entity) => boolean {
  const accessor = compileFieldAccessor(f.field);
  const op = f.operator;
  const fv = f.value;

  switch (op) {
    case "=":
      return (e) => accessor(e) === fv;
    case "!=":
      return (e) => accessor(e) !== fv;
    case "<":
      return (e) => {
        const v = accessor(e);
        if (typeof v !== typeof fv) return false;
        return (v as number) < (fv as number);
      };
    case ">":
      return (e) => {
        const v = accessor(e);
        if (typeof v !== typeof fv) return false;
        return (v as number) > (fv as number);
      };
    case "<=":
      return (e) => {
        const v = accessor(e);
        if (typeof v !== typeof fv) return false;
        return (v as number) <= (fv as number);
      };
    case ">=":
      return (e) => {
        const v = accessor(e);
        if (typeof v !== typeof fv) return false;
        return (v as number) >= (fv as number);
      };
    case "contains": {
      // Case-insensitive substring match
      if (typeof fv !== "string") return () => false;
      const lowerC = fv.toLowerCase();
      return (e) => {
        const v = accessor(e);
        return typeof v === "string" && v.toLowerCase().includes(lowerC);
      };
    }
    case "startsWith": {
      // Case-insensitive prefix match
      if (typeof fv !== "string") return () => false;
      const lowerS = fv.toLowerCase();
      return (e) => {
        const v = accessor(e);
        return typeof v === "string" && v.toLowerCase().startsWith(lowerS);
      };
    }
    case "in": {
      // Membership check — uses Set for large lists (>8) for O(1) lookups
      if (!Array.isArray(fv)) return () => false;
      if (fv.length > 8) {
        const set = new Set(fv);
        return (e) => set.has(accessor(e));
      }
      return (e) => fv.includes(accessor(e));
    }
    case "search": {
      // Case-insensitive substring search (same as "contains" but semantically distinct —
      // in SheetRepository.find() the "search" operator can leverage n-gram indexes)
      if (typeof fv !== "string") return () => false;
      const lowerSr = fv.toLowerCase();
      return (e) => {
        const v = accessor(e);
        return typeof v === "string" && v.toLowerCase().includes(lowerSr);
      };
    }
    default:
      return () => false;
  }
}

/**
 * Filter entities by an array of filters using AND logic.
 *
 * All predicates are compiled once; the hot loop uses a labeled `continue outer`
 * to short-circuit on the first failing predicate per entity (minimal GC pressure).
 *
 * @typeParam T - Entity type.
 * @param entities - Source array (not mutated).
 * @param filters  - Filters to apply (all must match — AND).
 * @returns New array of matching entities.
 */
function filterEntities<T extends Entity>(entities: T[], filters: Filter[]): T[] {
  if (!filters || filters.length === 0) return entities;

  // Compile all filter predicates once
  const predicates = new Array<(entity: Entity) => boolean>(filters.length);
  for (let i = 0; i < filters.length; i++) {
    predicates[i] = compileFilter(filters[i]);
  }

  const len = entities.length;
  const predLen = predicates.length;
  const result: T[] = [];

  // Labeled loop: `continue outer` skips to next entity on first failing predicate
  outer: for (let i = 0; i < len; i++) {
    const entity = entities[i];
    for (let j = 0; j < predLen; j++) {
      if (!predicates[j](entity)) continue outer;
    }
    result.push(entity);
  }

  return result;
}

/**
 * Filter entities with OR-connected groups of AND-connected filters.
 *
 * An entity matches if it satisfies **any** group (OR), where within each
 * group all filters must match (AND).  Empty groups never match.
 *
 * @typeParam T - Entity type.
 * @param entities - Source array (not mutated).
 * @param groups   - Array of filter groups (outer=OR, inner=AND).
 * @returns New array of matching entities.
 */
function filterEntitiesOr<T extends Entity>(entities: T[], groups: Filter[][]): T[] {
  if (!groups || groups.length === 0) return entities;

  // Compile each group into an array of predicate closures
  const compiledGroups: Array<Array<(entity: Entity) => boolean>> = new Array(groups.length);
  for (let g = 0; g < groups.length; g++) {
    const group = groups[g];
    const predicates = new Array<(entity: Entity) => boolean>(group.length);
    for (let i = 0; i < group.length; i++) {
      predicates[i] = compileFilter(group[i]);
    }
    compiledGroups[g] = predicates;
  }

  const len = entities.length;
  const numGroups = compiledGroups.length;
  const result: T[] = [];

  for (let i = 0; i < len; i++) {
    const entity = entities[i];
    let matched = false;

    // Try each group — entity matches if ANY group's predicates all pass
    for (let g = 0; g < numGroups; g++) {
      const predicates = compiledGroups[g];
      const predLen = predicates.length;
      // An empty group never matches — it has no conditions to satisfy
      let groupMatch = predLen > 0;

      for (let j = 0; j < predLen; j++) {
        if (!predicates[j](entity)) {
          groupMatch = false;
          break;
        }
      }

      if (groupMatch) {
        matched = true;
        break;
      }
    }

    if (matched) result.push(entity);
  }

  return result;
}

/**
 * Sort entities by multiple {@link SortClause} fields.
 *
 * **Performance strategy** (optimised for GAS V8):
 *
 * 1. Sort keys are **pre-extracted** into column-major arrays
 *    (`keys[sortIndex][entityIndex]`) so the comparator never calls
 *    field accessors — it reads from pre-populated primitive arrays.
 * 2. An **index array** is sorted instead of the entities themselves,
 *    avoiding expensive object moves in the V8 sort implementation.
 * 3. The final result is assembled from the sorted indices in one pass.
 *
 * Null handling: `null`/`undefined` values sort **first** (before any
 * non-null value) regardless of direction.
 *
 * @typeParam T - Entity type.
 * @param entities - Source array (not mutated).
 * @param sorts    - Sort clauses in priority order (first = primary sort).
 * @returns New sorted array.
 */
function sortEntities<T extends Entity>(entities: T[], sorts: SortClause[]): T[] {
  if (!sorts || sorts.length === 0) return entities;

  const len = entities.length;
  const numSorts = sorts.length;

  // Pre-compile field accessors and direction multipliers (+1 for asc, -1 for desc)
  const accessors: Array<(entity: Entity) => unknown> = new Array(numSorts);
  const directions: number[] = new Array(numSorts);
  for (let s = 0; s < numSorts; s++) {
    accessors[s] = compileFieldAccessor(sorts[s].field);
    directions[s] = sorts[s].direction === "desc" ? -1 : 1;
  }

  // Pre-extract sort keys into column-major layout: keys[sortIndex][entityIndex]
  // This avoids repeated field accessor calls during the sort comparator
  const keys: unknown[][] = new Array(numSorts);
  for (let s = 0; s < numSorts; s++) {
    const accessor = accessors[s];
    const col = new Array(len);
    for (let i = 0; i < len; i++) {
      col[i] = accessor(entities[i]);
    }
    keys[s] = col;
  }

  // Build index array — sort indices instead of objects to avoid expensive moves
  const indices = new Array<number>(len);
  for (let i = 0; i < len; i++) indices[i] = i;

  // Multi-key comparator using pre-extracted key columns
  indices.sort((ai, bi) => {
    for (let s = 0; s < numSorts; s++) {
      const aVal = keys[s][ai];
      const bVal = keys[s][bi];
      let cmp: number;
      if (aVal === bVal) {
        cmp = 0;
      } else if (aVal == null) {
        cmp = -1; // Nulls sort first
      } else if (bVal == null) {
        cmp = 1; // Nulls sort first
      } else if (typeof aVal === "number" && typeof bVal === "number") {
        cmp = aVal - bVal; // Numeric comparison (avoids string coercion)
      } else {
        cmp = String(aVal).localeCompare(String(bVal)); // String fallback
      }
      if (cmp !== 0) return cmp * directions[s]; // Apply asc/desc multiplier
    }
    return 0; // All keys equal — stable order
  });

  // Build sorted result array from the sorted index permutation
  const result = new Array<T>(len);
  for (let i = 0; i < len; i++) {
    result[i] = entities[indices[i]];
  }
  return result;
}

/**
 * Apply pagination (offset + limit) to an entity array.
 *
 * Both `offset` and `limit` are sanitised: non-finite, negative, or
 * fractional values are clamped to safe defaults (0 for offset, full
 * length for limit).
 *
 * @typeParam T - Entity type.
 * @param entities - Source array.
 * @param offset   - Number of items to skip from the start.
 * @param limit    - Maximum number of items to return.
 * @returns Paginated result with metadata (total, hasNext, etc.).
 */
function paginateEntities<T>(entities: T[], offset: number, limit: number): PaginatedResult<T> {
  const safeOffset = Number.isFinite(offset) && offset >= 0 ? Math.floor(offset) : 0;
  const safeLimit = Number.isFinite(limit) && limit >= 0 ? Math.floor(limit) : entities.length;
  const total = entities.length;
  const sliced = entities.slice(safeOffset, safeOffset + safeLimit);
  return {
    items: sliced,
    total,
    offset: safeOffset,
    limit: safeLimit,
    hasNext: safeOffset + safeLimit < total,
  };
}

/**
 * Group entities by a single field, producing one {@link GroupResult} per distinct value.
 *
 * Uses a `Map` keyed by the field value to collect entities in insertion
 * order.  The pre-compiled field accessor avoids repeated path resolution.
 *
 * @typeParam T - Entity type.
 * @param entities - Source array (not mutated).
 * @param field    - Dot-separated field path to group by.
 * @returns Array of group results, each containing key, count, and items.
 */
function groupEntities<T extends Entity>(entities: T[], field: string): GroupResult<T>[] {
  const accessor = compileFieldAccessor(field);
  const groups = new Map<unknown, T[]>();

  // Distribute entities into groups by their field value
  for (let i = 0, len = entities.length; i < len; i++) {
    const entity = entities[i];
    const key = accessor(entity);
    const existing = groups.get(key);
    if (existing) {
      existing.push(entity);
    } else {
      groups.set(key, [entity]);
    }
  }

  // Convert Map entries to GroupResult array
  const results: GroupResult<T>[] = [];
  groups.forEach((items, key) => {
    results.push({ key, count: items.length, items });
  });

  return results;
}

/**
 * Execute a complete query pipeline on an in-memory entity array.
 *
 * Pipeline stages (executed in order):
 *
 * 1. **Filter**: `whereGroups` (OR of ANDs) takes precedence over `where`
 *    (simple AND).  If both are present, only `whereGroups` is applied.
 * 2. **Sort**: Multi-field sort via {@link sortEntities}.
 * 3. **Paginate**: `offset` and `limit` applied via `Array.slice`.
 *
 * This function does NOT call {@link paginateEntities} — it applies a
 * lighter slice-only pagination without producing a `PaginatedResult`
 * wrapper.  The caller (typically {@link SheetRepository}) is responsible
 * for wrapping the result if needed.
 *
 * @typeParam T - Entity type.
 * @param entities - Full entity array to query against.
 * @param options  - Query options controlling filter, sort, and pagination.
 * @returns Filtered, sorted, and paginated entity array.
 */
function executeQuery<T extends Entity>(entities: T[], options: QueryOptions): T[] {
  SheetOrmLogger.log(
    `[QueryEngine] executeQuery in=${entities.length}` +
      ` where=${options.where?.length ?? 0}` +
      ` whereGroups=${options.whereGroups?.length ?? 0}` +
      ` orderBy=${options.orderBy?.length ?? 0}` +
      ` offset=${options.offset ?? "-"} limit=${options.limit ?? "-"}`,
  );
  let result = entities;

  // Stage 1: Filter — whereGroups (OR of ANDs) takes precedence over where (AND)
  if (options.whereGroups && options.whereGroups.length > 0) {
    result = filterEntitiesOr(result, options.whereGroups);
  } else if (options.where && options.where.length > 0) {
    result = filterEntities(result, options.where);
  }

  // Stage 2: Sort — multi-field ordering
  if (options.orderBy && options.orderBy.length > 0) {
    result = sortEntities(result, options.orderBy);
  }

  // Stage 3: Paginate — slice-only (no PaginatedResult wrapper)
  if (options.offset !== undefined || options.limit !== undefined) {
    const rawOffset = options.offset ?? 0;
    const offset = Number.isFinite(rawOffset) && rawOffset >= 0 ? Math.floor(rawOffset) : 0;
    const limit =
      options.limit !== undefined
        ? Number.isFinite(options.limit) && options.limit >= 0
          ? Math.floor(options.limit)
          : result.length
        : result.length;
    result = result.slice(offset, offset + limit);
  }

  SheetOrmLogger.log(`[QueryEngine] executeQuery out=${result.length}`);
  return result;
}

/**
 * Static facade exposing the query engine's pure functions as class methods.
 *
 * All methods are stateless — they operate on the provided entity array
 * and return new arrays without side effects.  The class form exists so
 * that callers can reference `QueryEngine.filterEntities(...)` instead of
 * importing individual module-level functions.
 */
export class QueryEngine {
  /** Filter entities with AND-connected conditions. */
  static filterEntities = filterEntities;
  /** Filter entities with OR-connected groups of AND-connected conditions. */
  static filterEntitiesOr = filterEntitiesOr;
  /** Sort entities by multiple fields with asc/desc directions. */
  static sortEntities = sortEntities;
  /** Paginate entities and return metadata (total, hasNext, etc.). */
  static paginateEntities = paginateEntities;
  /** Group entities by a field into key/count/items buckets. */
  static groupEntities = groupEntities;
  /** Execute a full filter → sort → paginate pipeline. */
  static executeQuery = executeQuery;
}
