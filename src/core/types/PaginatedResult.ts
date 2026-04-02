/**
 * Paginated query result returned by select() / Query.select().
 *
 * @typeParam T - Entity type of the paginated items.
 */
export interface PaginatedResult<T> {
  /** Entities in the current page. */
  items: T[];

  /** Total number of entities matching the query (before pagination). */
  total: number;

  /** 0-based offset of the first item in the page. */
  offset: number;

  /** Maximum number of items per page. */
  limit: number;

  /** True if there are more items beyond this page. */
  hasNext: boolean;
}
