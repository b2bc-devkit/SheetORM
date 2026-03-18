// SheetORM — QueryEngine: filters, sorts, paginates in-memory entity arrays
// Inspired by common ORM query builder patterns (fluent API, predicate evaluation)

import {
  Entity,
  Filter,
  FilterOperator,
  SortClause,
  QueryOptions,
  PaginatedResult,
  GroupResult,
} from '../core/types';

/**
 * Apply a single filter predicate to a value.
 */
function applyOperator(fieldValue: unknown, operator: FilterOperator, filterValue: unknown): boolean {
  switch (operator) {
    case '=':
      return fieldValue === filterValue;
    case '!=':
      return fieldValue !== filterValue;
    case '<':
      return (fieldValue as number) < (filterValue as number);
    case '>':
      return (fieldValue as number) > (filterValue as number);
    case '<=':
      return (fieldValue as number) <= (filterValue as number);
    case '>=':
      return (fieldValue as number) >= (filterValue as number);
    case 'contains':
      return typeof fieldValue === 'string' && typeof filterValue === 'string'
        ? fieldValue.toLowerCase().includes(filterValue.toLowerCase())
        : false;
    case 'startsWith':
      return typeof fieldValue === 'string' && typeof filterValue === 'string'
        ? fieldValue.toLowerCase().startsWith(filterValue.toLowerCase())
        : false;
    case 'in':
      return Array.isArray(filterValue) ? filterValue.includes(fieldValue) : false;
    default:
      return false;
  }
}

/**
 * Get a nested field value using dot/slash notation (e.g. "address/city").
 */
function getFieldValue(entity: Entity, field: string): unknown {
  const parts = field.replace(/\//g, '.').split('.');
  let current: unknown = entity;
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

/**
 * Filter an array of entities by an array of Filter conditions (AND logic).
 */
export function filterEntities<T extends Entity>(entities: T[], filters: Filter[]): T[] {
  if (!filters || filters.length === 0) return entities;

  return entities.filter((entity) => {
    return filters.every((f) => {
      const val = getFieldValue(entity, f.field);
      return applyOperator(val, f.operator, f.value);
    });
  });
}

/**
 * Sort entities by multiple sort clauses.
 */
export function sortEntities<T extends Entity>(entities: T[], sorts: SortClause[]): T[] {
  if (!sorts || sorts.length === 0) return entities;

  return [...entities].sort((a, b) => {
    for (const sort of sorts) {
      const aVal = getFieldValue(a, sort.field);
      const bVal = getFieldValue(b, sort.field);

      let cmp = 0;
      if (aVal === bVal) {
        cmp = 0;
      } else if (aVal === null || aVal === undefined) {
        cmp = -1;
      } else if (bVal === null || bVal === undefined) {
        cmp = 1;
      } else if (typeof aVal === 'number' && typeof bVal === 'number') {
        cmp = aVal - bVal;
      } else {
        cmp = String(aVal).localeCompare(String(bVal));
      }

      if (cmp !== 0) {
        return sort.direction === 'desc' ? -cmp : cmp;
      }
    }
    return 0;
  });
}

/**
 * Apply pagination (offset + limit) to an array.
 */
export function paginateEntities<T>(entities: T[], offset: number, limit: number): PaginatedResult<T> {
  const total = entities.length;
  const sliced = entities.slice(offset, offset + limit);
  return {
    items: sliced,
    total,
    offset,
    limit,
    hasNext: offset + limit < total,
  };
}

/**
 * Group entities by a field.
 */
export function groupEntities<T extends Entity>(entities: T[], field: string): GroupResult<T>[] {
  const groups = new Map<unknown, T[]>();

  for (const entity of entities) {
    const key = getFieldValue(entity, field);
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
export function executeQuery<T extends Entity>(entities: T[], options: QueryOptions): T[] {
  let result = entities;

  if (options.where && options.where.length > 0) {
    result = filterEntities(result, options.where);
  }

  if (options.orderBy && options.orderBy.length > 0) {
    result = sortEntities(result, options.orderBy);
  }

  if (options.offset !== undefined || options.limit !== undefined) {
    const offset = options.offset ?? 0;
    const limit = options.limit ?? result.length;
    result = result.slice(offset, offset + limit);
  }

  return result;
}
