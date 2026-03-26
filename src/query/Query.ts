// SheetORM — Query: fluent API for building and executing queries
// Inspired by common ORM query builder patterns

import type { Entity } from "../core/types/Entity.js";
import type { Filter } from "../core/types/Filter.js";
import type { FilterOperator } from "../core/types/FilterOperator.js";
import type { SortClause } from "../core/types/SortClause.js";
import type { QueryOptions } from "../core/types/QueryOptions.js";
import type { PaginatedResult } from "../core/types/PaginatedResult.js";
import type { GroupResult } from "../core/types/GroupResult.js";
import { QueryEngine } from "./QueryEngine.js";

type FromResolver = (
  classOrName:
    | string
    | {
        new (): Entity;
        tableName: string;
      },
) => () => Entity[];

export class Query<T extends Entity> {
  private filterGroups: Filter[][] = [[]];
  private sorts: SortClause[] = [];
  private _limit?: number;
  private _offset?: number;
  private dataProvider: () => T[];

  private static _fromResolverFn: FromResolver | null = null;

  static _setFromResolver(resolver: FromResolver): void {
    Query._fromResolverFn = resolver;
  }

  static from<U extends Entity>(ctor: { new (): U; tableName: string }): Query<U>;
  static from(name: string): Query<Entity>;
  static from(classOrName: unknown): Query<Entity> {
    if (!Query._fromResolverFn) {
      throw new Error("Query.from() is not available. Import Record from SheetORM to enable it.");
    }
    const provider = Query._fromResolverFn(classOrName as Parameters<FromResolver>[0]);
    return new Query<Entity>(provider);
  }

  constructor(dataProvider: () => T[]) {
    this.dataProvider = dataProvider;
  }

  where(field: string, operator: FilterOperator, value: unknown): Query<T> {
    this.filterGroups[this.filterGroups.length - 1].push({ field, operator, value });
    return this;
  }

  and(field: string, operator: FilterOperator, value: unknown): Query<T> {
    return this.where(field, operator, value);
  }

  or(field: string, operator: FilterOperator, value: unknown): Query<T> {
    this.filterGroups.push([{ field, operator, value }]);
    return this;
  }

  orderBy(field: string, direction: "asc" | "desc" = "asc"): Query<T> {
    this.sorts.push({ field, direction });
    return this;
  }

  limit(count: number): Query<T> {
    if (!Number.isFinite(count) || count < 0) {
      throw new Error(`limit() requires a non-negative finite number, got ${count}`);
    }
    this._limit = Math.floor(count);
    return this;
  }

  offset(count: number): Query<T> {
    if (!Number.isFinite(count) || count < 0) {
      throw new Error(`offset() requires a non-negative finite number, got ${count}`);
    }
    this._offset = Math.floor(count);
    return this;
  }

  private get hasFilters(): boolean {
    return this.filterGroups.some((g) => g.length > 0);
  }

  private get isOrQuery(): boolean {
    return this.filterGroups.filter((g) => g.length > 0).length > 1;
  }

  private get flatFilters(): Filter[] {
    return this.filterGroups.find((g) => g.length > 0) ?? [];
  }

  private applyFilters(entities: T[]): T[] {
    if (!this.hasFilters) return entities;
    if (this.isOrQuery) {
      return QueryEngine.filterEntitiesOr(
        entities,
        this.filterGroups.filter((g) => g.length > 0),
      );
    }
    return QueryEngine.filterEntities(entities, this.flatFilters);
  }

  /**
   * Build QueryOptions from the current builder state.
   */
  build(): QueryOptions {
    const nonEmpty = this.filterGroups.filter((g) => g.length > 0);
    const isOr = nonEmpty.length > 1;
    return {
      where: !isOr && nonEmpty.length === 1 ? [...nonEmpty[0]] : undefined,
      whereGroups: isOr ? nonEmpty.map((g) => [...g]) : undefined,
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
    entities = this.applyFilters(entities);

    if (this.sorts.length > 0) {
      entities = QueryEngine.sortEntities(entities, this.sorts);
    }

    const offset = this._offset ?? 0;
    const limit = this._limit;
    if (offset === 0 && limit === undefined) return entities;
    return entities.slice(offset, limit !== undefined ? offset + limit : undefined);
  }

  /**
   * Execute and return first matching entity.
   */
  first(): T | null {
    let entities = this.dataProvider();
    entities = this.applyFilters(entities);

    if (this.sorts.length > 0) {
      entities = QueryEngine.sortEntities(entities, this.sorts);
    }

    const offset = this._offset ?? 0;
    const limit = this._limit;
    if (limit === 0) return null;
    const visible = entities.slice(offset, limit !== undefined ? offset + limit : undefined);
    return visible.length > 0 ? visible[0] : null;
  }

  /**
   * Execute with pagination.
   */
  select(offset: number, limit: number): PaginatedResult<T> {
    let entities = this.dataProvider();
    entities = this.applyFilters(entities);

    if (this.sorts.length > 0) {
      entities = QueryEngine.sortEntities(entities, this.sorts);
    }

    return QueryEngine.paginateEntities(entities, offset, limit);
  }

  /**
   * Count matching entities.
   */
  count(): number {
    let entities = this.dataProvider();
    entities = this.applyFilters(entities);
    return entities.length;
  }

  /**
   * Group results by field.
   */
  groupBy(field: string): GroupResult<T>[] {
    let entities = this.dataProvider();
    entities = this.applyFilters(entities);

    if (this.sorts.length > 0) {
      entities = QueryEngine.sortEntities(entities, this.sorts);
    }

    return QueryEngine.groupEntities(entities, field);
  }
}
