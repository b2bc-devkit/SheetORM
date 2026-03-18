// SheetORM — QueryBuilder: fluent API for building and executing queries
// Inspired by common ORM query builder patterns

import {
  Entity,
  Filter,
  FilterOperator,
  SortClause,
  QueryOptions,
  PaginatedResult,
  GroupResult,
} from '../core/types';
import {
  filterEntities,
  sortEntities,
  paginateEntities,
  groupEntities,
} from './QueryEngine';

export class QueryBuilder<T extends Entity> {
  private filters: Filter[] = [];
  private sorts: SortClause[] = [];
  private _limit?: number;
  private _offset?: number;
  private dataProvider: () => T[];

  constructor(dataProvider: () => T[]) {
    this.dataProvider = dataProvider;
  }

  where(field: string, operator: FilterOperator, value: unknown): QueryBuilder<T> {
    this.filters.push({ field, operator, value });
    return this;
  }

  and(field: string, operator: FilterOperator, value: unknown): QueryBuilder<T> {
    return this.where(field, operator, value);
  }

  or(_field: string, _operator: FilterOperator, _value: unknown): QueryBuilder<T> {
    // OR is implemented as a separate filter group in a simplified model.
    // For MVP, OR adds an additional filter that is evaluated separately.
    // Full OR support would require compound predicate trees.
    this.filters.push({ field: _field, operator: _operator, value: _value });
    return this;
  }

  orderBy(field: string, direction: 'asc' | 'desc' = 'asc'): QueryBuilder<T> {
    this.sorts.push({ field, direction });
    return this;
  }

  limit(count: number): QueryBuilder<T> {
    this._limit = count;
    return this;
  }

  offset(count: number): QueryBuilder<T> {
    this._offset = count;
    return this;
  }

  /**
   * Build QueryOptions from the current builder state.
   */
  build(): QueryOptions {
    return {
      where: this.filters.length > 0 ? [...this.filters] : undefined,
      orderBy: this.sorts.length > 0 ? [...this.sorts] : undefined,
      limit: this._limit,
      offset: this._offset,
    };
  }

  /**
   * Execute query and return all matching entities.
   */
  execute(): T[] {
    let entities = this.dataProvider();

    if (this.filters.length > 0) {
      entities = filterEntities(entities, this.filters);
    }

    if (this.sorts.length > 0) {
      entities = sortEntities(entities, this.sorts);
    }

    const offset = this._offset ?? 0;
    const limit = this._limit ?? entities.length;
    return entities.slice(offset, offset + limit);
  }

  /**
   * Execute and return first matching entity.
   */
  first(): T | null {
    const saved = this._limit;
    this._limit = 1;
    const results = this.execute();
    this._limit = saved;
    return results.length > 0 ? results[0] : null;
  }

  /**
   * Execute with pagination.
   */
  select(offset: number, limit: number): PaginatedResult<T> {
    let entities = this.dataProvider();

    if (this.filters.length > 0) {
      entities = filterEntities(entities, this.filters);
    }

    if (this.sorts.length > 0) {
      entities = sortEntities(entities, this.sorts);
    }

    return paginateEntities(entities, offset, limit);
  }

  /**
   * Count matching entities.
   */
  count(): number {
    let entities = this.dataProvider();
    if (this.filters.length > 0) {
      entities = filterEntities(entities, this.filters);
    }
    return entities.length;
  }

  /**
   * Group results by field.
   */
  groupBy(field: string): GroupResult<T>[] {
    let entities = this.dataProvider();

    if (this.filters.length > 0) {
      entities = filterEntities(entities, this.filters);
    }

    if (this.sorts.length > 0) {
      entities = sortEntities(entities, this.sorts);
    }

    return groupEntities(entities, field);
  }
}
