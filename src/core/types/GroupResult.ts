/**
 * Result of a groupBy() operation — one bucket per distinct field value.
 *
 * @typeParam T - Entity type contained in each group.
 */
export interface GroupResult<T> {
  /** The distinct value of the grouped field. */
  key: unknown;

  /** Number of entities in this group. */
  count: number;

  /** All entities whose grouped field equals `key`. */
  items: T[];
}
