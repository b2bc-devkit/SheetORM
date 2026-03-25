// SheetORM — QueryEngine: filters, sorts, paginates in-memory entity arrays
// Optimized for GAS V8 runtime performance

import type { Entity } from "../core/types/Entity.js";
import type { Filter } from "../core/types/Filter.js";
import type { SortClause } from "../core/types/SortClause.js";
import type { QueryOptions } from "../core/types/QueryOptions.js";
import type { PaginatedResult } from "../core/types/PaginatedResult.js";
import type { GroupResult } from "../core/types/GroupResult.js";

/**
 * Resolve a field path to its parts. Single-segment paths (no dots/slashes)
 * are returned as null to signal the fast-path in getFieldValue.
 */
function splitFieldPath(field: string): string[] | null {
  if (field.indexOf(".") === -1 && field.indexOf("/") === -1) return null;
  return field.replace(/\//g, ".").split(".");
}

/** Pre-compiled accessor: returns a function that extracts a field from an entity. */
function compileFieldAccessor(field: string): (entity: Entity) => unknown {
  const parts = splitFieldPath(field);
  if (parts === null) {
    // Simple field — direct property access
    return (entity: Entity) => entity[field];
  }
  // Nested field — walk the path
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
 * Compile a single filter into a fast predicate function.
 * Pre-computes field accessor and converts 'in' arrays to Sets for large lists.
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
      if (typeof fv !== "string") return () => false;
      const lowerC = fv.toLowerCase();
      return (e) => {
        const v = accessor(e);
        return typeof v === "string" && v.toLowerCase().includes(lowerC);
      };
    }
    case "startsWith": {
      if (typeof fv !== "string") return () => false;
      const lowerS = fv.toLowerCase();
      return (e) => {
        const v = accessor(e);
        return typeof v === "string" && v.toLowerCase().startsWith(lowerS);
      };
    }
    case "in": {
      if (!Array.isArray(fv)) return () => false;
      if (fv.length > 8) {
        const set = new Set(fv);
        return (e) => set.has(accessor(e));
      }
      return (e) => fv.includes(accessor(e));
    }
    case "search": {
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
 * Filter an array of entities by an array of Filter conditions (AND logic).
 * Uses compiled predicates and manual loop for minimal GC pressure.
 */
function filterEntities<T extends Entity>(entities: T[], filters: Filter[]): T[] {
  if (!filters || filters.length === 0) return entities;

  // Compile all filters once
  const predicates = new Array<(entity: Entity) => boolean>(filters.length);
  for (let i = 0; i < filters.length; i++) {
    predicates[i] = compileFilter(filters[i]);
  }

  const len = entities.length;
  const predLen = predicates.length;
  const result: T[] = [];

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
 * Filter entities with OR-connected groups.
 * Each inner group is AND-connected; an entity matches if it passes ANY group.
 */
function filterEntitiesOr<T extends Entity>(entities: T[], groups: Filter[][]): T[] {
  if (!groups || groups.length === 0) return entities;

  // Compile each group into an array of predicates
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

    for (let g = 0; g < numGroups; g++) {
      const predicates = compiledGroups[g];
      const predLen = predicates.length;
      let groupMatch = true;

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
 * Sort entities by multiple sort clauses.
 * Pre-extracts sort keys to avoid repeated field navigation during comparisons.
 */
function sortEntities<T extends Entity>(entities: T[], sorts: SortClause[]): T[] {
  if (!sorts || sorts.length === 0) return entities;

  const len = entities.length;
  const numSorts = sorts.length;

  // Pre-compile field accessors
  const accessors: Array<(entity: Entity) => unknown> = new Array(numSorts);
  const directions: number[] = new Array(numSorts);
  for (let s = 0; s < numSorts; s++) {
    accessors[s] = compileFieldAccessor(sorts[s].field);
    directions[s] = sorts[s].direction === "desc" ? -1 : 1;
  }

  // Pre-extract sort keys: keys[sortIndex][entityIndex]
  const keys: unknown[][] = new Array(numSorts);
  for (let s = 0; s < numSorts; s++) {
    const accessor = accessors[s];
    const col = new Array(len);
    for (let i = 0; i < len; i++) {
      col[i] = accessor(entities[i]);
    }
    keys[s] = col;
  }

  // Build index array to sort
  const indices = new Array<number>(len);
  for (let i = 0; i < len; i++) indices[i] = i;

  indices.sort((ai, bi) => {
    for (let s = 0; s < numSorts; s++) {
      const aVal = keys[s][ai];
      const bVal = keys[s][bi];
      let cmp: number;
      if (aVal === bVal) {
        cmp = 0;
      } else if (aVal == null) {
        cmp = -1;
      } else if (bVal == null) {
        cmp = 1;
      } else if (typeof aVal === "number" && typeof bVal === "number") {
        cmp = aVal - bVal;
      } else {
        cmp = String(aVal).localeCompare(String(bVal));
      }
      if (cmp !== 0) return cmp * directions[s];
    }
    return 0;
  });

  // Build sorted result array
  const result = new Array<T>(len);
  for (let i = 0; i < len; i++) {
    result[i] = entities[indices[i]];
  }
  return result;
}

/**
 * Apply pagination (offset + limit) to an array.
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
 * Group entities by a field.
 * Uses pre-compiled accessor and manual iteration.
 */
function groupEntities<T extends Entity>(entities: T[], field: string): GroupResult<T>[] {
  const accessor = compileFieldAccessor(field);
  const groups = new Map<unknown, T[]>();

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

  const results: GroupResult<T>[] = [];
  groups.forEach((items, key) => {
    results.push({ key, count: items.length, items });
  });

  return results;
}

/**
 * Execute a full query pipeline: filter → sort → paginate or return all.
 */
function executeQuery<T extends Entity>(entities: T[], options: QueryOptions): T[] {
  let result = entities;

  if (options.whereGroups && options.whereGroups.length > 0) {
    result = filterEntitiesOr(result, options.whereGroups);
  } else if (options.where && options.where.length > 0) {
    result = filterEntities(result, options.where);
  }

  if (options.orderBy && options.orderBy.length > 0) {
    result = sortEntities(result, options.orderBy);
  }

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

  return result;
}

export class QueryEngine {
  static filterEntities = filterEntities;
  static filterEntitiesOr = filterEntitiesOr;
  static sortEntities = sortEntities;
  static paginateEntities = paginateEntities;
  static groupEntities = groupEntities;
  static executeQuery = executeQuery;
}
