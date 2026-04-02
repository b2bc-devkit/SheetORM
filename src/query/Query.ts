/**
 * Fluent query builder for composing and executing queries against SheetORM data.
 *
 * Provides a chainable API inspired by common ORM patterns:
 * ```ts
 * const cars = Query.from(Car)
 *   .where("make", "=", "Toyota")
 *   .or("make", "=", "Honda")
 *   .orderBy("year", "desc")
 *   .limit(10)
 *   .execute();
 * ```
 *
 * Filter logic:
 *   - `.where()` / `.and()` add filters to the current AND-group.
 *   - `.or()` creates a new AND-group (groups are combined with OR logic).
 *
 * Execution methods:
 *   - `execute()` — return all matching entities (with sort + pagination).
 *   - `first()`   — return the first match (or null).
 *   - `select()`  — paginated result with total count.
 *   - `count()`   — return the number of matches.
 *   - `groupBy()` — bucket results by a field value.
 *   - `build()`   — export the builder state as a QueryOptions object.
 *
 * @module Query
 */

import type { Entity } from "../core/types/Entity.js";
import type { Filter } from "../core/types/Filter.js";
import type { FilterOperator } from "../core/types/FilterOperator.js";
import type { SortClause } from "../core/types/SortClause.js";
import type { QueryOptions } from "../core/types/QueryOptions.js";
import type { PaginatedResult } from "../core/types/PaginatedResult.js";
import type { GroupResult } from "../core/types/GroupResult.js";
import { QueryEngine } from "./QueryEngine.js";
import { SheetOrmLogger } from "../utils/SheetOrmLogger.js";

/**
 * Callback registered by Record to resolve a class/name → data-provider function.
 * This indirection allows Query to remain decoupled from Record and SheetRepository.
 */
type FromResolver = (
  classOrName:
    | string
    | {
        new (): Entity;
        tableName: string;
      },
) => () => Entity[];

/**
 * Fluent query builder that composes filter groups, sorting, and pagination,
 * then delegates execution to QueryEngine.
 *
 * @typeParam T - Entity type returned by the query.
 */
export class Query<T extends Entity> {
  /**
   * Array of filter groups.  Each inner array is AND-connected;
   * groups are OR-connected.  Starts with one empty group.
   */
  private filterGroups: Filter[][] = [[]];

  /** Ordered list of sort clauses to apply after filtering. */
  private sorts: SortClause[] = [];

  /** Maximum number of results (undefined = no limit). */
  private _limit?: number;

  /** Number of results to skip (undefined = 0). */
  private _offset?: number;

  /** Callback that returns the full entity dataset (lazy-loaded). */
  private dataProvider: () => T[];

  /**
   * Module-level resolver injected by Record at import time.
   * Converts a class constructor or table name to a data-provider function.
   */
  private static _fromResolverFn: FromResolver | null = null;

  /**
   * Register the from-resolver.  Called once by Record when it wires up
   * the Query integration at module load time.
   */
  static _setFromResolver(resolver: FromResolver): void {
    Query._fromResolverFn = resolver;
  }

  /**
   * Create a new Query from a Record subclass.
   * @typeParam U - Concrete Record type.
   */
  static from<U extends Entity>(ctor: { new (): U; tableName: string }): Query<U>;
  /**
   * Create a new Query from a table name string.
   */
  static from(name: string): Query<Entity>;
  /**
   * Implementation: resolve the data provider and return a fresh Query.
   * @throws If `_fromResolverFn` has not been set (Record not imported).
   */
  static from(classOrName: unknown): Query<Entity> {
    if (!Query._fromResolverFn) {
      throw new Error("Query.from() is not available. Import Record from SheetORM to enable it.");
    }
    const provider = Query._fromResolverFn(classOrName as Parameters<FromResolver>[0]);
    return new Query<Entity>(provider);
  }

  /**
   * Construct a Query with the given data provider function.
   * @param dataProvider - Callback returning the full set of entities to query.
   */
  constructor(dataProvider: () => T[]) {
    this.dataProvider = dataProvider;
  }

  /**
   * Add a filter to the current AND-group.
   *
   * @param field    - Entity field name (supports dot-paths for nested objects).
   * @param operator - Comparison operator (=, !=, <, >, <=, >=, contains, startsWith, in, search).
   * @param value    - Value to compare against.
   * @returns this (for chaining).
   */
  where(field: string, operator: FilterOperator, value: unknown): Query<T> {
    // Append to the last (current) filter group
    this.filterGroups[this.filterGroups.length - 1].push({ field, operator, value });
    return this;
  }

  /**
   * Alias for `where()` — adds another AND condition to the current group.
   */
  and(field: string, operator: FilterOperator, value: unknown): Query<T> {
    return this.where(field, operator, value);
  }

  /**
   * Start a new OR-group with the given filter condition.
   * Previous filters remain in their own AND-group.
   */
  or(field: string, operator: FilterOperator, value: unknown): Query<T> {
    // Create a fresh AND-group for the OR branch
    this.filterGroups.push([{ field, operator, value }]);
    return this;
  }

  /**
   * Add a sort clause (multiple orderBy calls are applied in order).
   *
   * @param field     - Entity field name to sort by.
   * @param direction - "asc" (default) or "desc".
   */
  orderBy(field: string, direction: "asc" | "desc" = "asc"): Query<T> {
    this.sorts.push({ field, direction });
    return this;
  }

  /**
   * Set the maximum number of results to return.
   * @throws If count is negative or non-finite.
   */
  limit(count: number): Query<T> {
    if (!Number.isFinite(count) || count < 0) {
      throw new Error(`limit() requires a non-negative finite number, got ${count}`);
    }
    this._limit = Math.floor(count);
    return this;
  }

  /**
   * Set the number of results to skip before returning.
   * @throws If count is negative or non-finite.
   */
  offset(count: number): Query<T> {
    if (!Number.isFinite(count) || count < 0) {
      throw new Error(`offset() requires a non-negative finite number, got ${count}`);
    }
    this._offset = Math.floor(count);
    return this;
  }

  /** True if any filter group contains at least one filter. */
  private get hasFilters(): boolean {
    return this.filterGroups.some((g) => g.length > 0);
  }

  /** True if there are multiple non-empty filter groups (OR query). */
  private get isOrQuery(): boolean {
    return this.filterGroups.filter((g) => g.length > 0).length > 1;
  }

  /** Return the first non-empty filter group (for simple AND queries). */
  private get flatFilters(): Filter[] {
    return this.filterGroups.find((g) => g.length > 0) ?? [];
  }

  /**
   * Apply all filter groups to the entity dataset.
   * Delegates to `QueryEngine.filterEntities` (AND) or
   * `QueryEngine.filterEntitiesOr` (multiple OR groups).
   */
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
   * Build a declarative QueryOptions object from the current builder state.
   * Useful for passing to SheetRepository.find() or for serialisation.
   */
  build(): QueryOptions {
    const nonEmpty = this.filterGroups.filter((g) => g.length > 0);
    const isOr = nonEmpty.length > 1;
    return {
      // Simple AND query → single where array
      where: !isOr && nonEmpty.length === 1 ? [...nonEmpty[0]] : undefined,
      // Multi-group OR query → whereGroups array of arrays
      whereGroups: isOr ? nonEmpty.map((g) => [...g]) : undefined,
      orderBy: this.sorts.length > 0 ? [...this.sorts] : undefined,
      limit: this._limit,
      offset: this._offset,
    };
  }

  /**
   * Execute the query and return all matching entities.
   *
   * Pipeline: load data → filter → sort → offset/limit.
   */
  execute(): T[] {
    let entities = this.dataProvider();
    const inputCount = entities.length;

    // Apply filter groups
    entities = this.applyFilters(entities);

    // Apply sort clauses
    if (this.sorts.length > 0) {
      entities = QueryEngine.sortEntities(entities, this.sorts);
    }

    // Apply pagination (offset + limit)
    const offset = this._offset ?? 0;
    const limit = this._limit;
    if (offset !== 0 || limit !== undefined) {
      entities = entities.slice(offset, limit !== undefined ? offset + limit : undefined);
    }

    SheetOrmLogger.log(
      `[Query] execute input=${inputCount} filters=${this.filterGroups.flat().length} sort=${this.sorts.length} limit=${this._limit ?? "-"} offset=${this._offset ?? 0} → ${entities.length}`,
    );
    return entities;
  }

  /**
   * Execute the query and return the first matching entity, or null.
   *
   * More efficient than `execute()[0]` when limit is already set.
   */
  first(): T | null {
    let entities = this.dataProvider();
    const inputCount = entities.length;

    entities = this.applyFilters(entities);

    if (this.sorts.length > 0) {
      entities = QueryEngine.sortEntities(entities, this.sorts);
    }

    const offset = this._offset ?? 0;
    const limit = this._limit;
    // Short-circuit if limit is explicitly 0
    if (limit === 0) {
      SheetOrmLogger.log(
        `[Query] first  input=${inputCount} filters=${this.filterGroups.flat().length} → null (limit=0)`,
      );
      return null;
    }
    const visible = entities.slice(offset, limit !== undefined ? offset + limit : undefined);
    const result = visible.length > 0 ? visible[0] : null;
    SheetOrmLogger.log(
      `[Query] first  input=${inputCount} filters=${this.filterGroups.flat().length} → ${result ? "found" : "null"}`,
    );
    return result;
  }

  /**
   * Execute with explicit pagination, returning a PaginatedResult.
   *
   * @param offset - 0-based number of items to skip.
   * @param limit  - Maximum items per page.
   * @returns Object with items, total, offset, limit, hasNext.
   */
  select(offset: number, limit: number): PaginatedResult<T> {
    let entities = this.dataProvider();
    const inputCount = entities.length;

    entities = this.applyFilters(entities);

    if (this.sorts.length > 0) {
      entities = QueryEngine.sortEntities(entities, this.sorts);
    }

    // Delegate slicing + total count to QueryEngine
    const paged = QueryEngine.paginateEntities(entities, offset, limit);
    SheetOrmLogger.log(
      `[Query] select input=${inputCount} filters=${this.filterGroups.flat().length} offset=${offset} limit=${limit} → ${paged.items.length}/${paged.total}`,
    );
    return paged;
  }

  /**
   * Count the number of entities matching the current filters.
   * No sorting or pagination is applied.
   */
  count(): number {
    let entities = this.dataProvider();
    const inputCount = entities.length;
    entities = this.applyFilters(entities);
    SheetOrmLogger.log(
      `[Query] count  input=${inputCount} filters=${this.filterGroups.flat().length} → ${entities.length}`,
    );
    return entities.length;
  }

  /**
   * Group matching entities by a field value.
   *
   * Filters and sorts are applied before grouping.
   *
   * @param field - Entity field name to group by.
   * @returns Array of GroupResult objects (one per distinct field value).
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
