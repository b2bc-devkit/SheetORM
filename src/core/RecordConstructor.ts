import type { Record } from "./Record.js";

/**
 * Constructor signature for Record subclasses.
 *
 * Used as a generic constraint whenever SheetORM needs to refer to a
 * class that can be `new`-ed and that carries the static `tableName`
 * and `indexTableName` properties (e.g. in SheetRepository's generic
 * parameter).
 *
 * @typeParam T - Concrete Record subclass type.
 */
export interface RecordConstructor<T extends Record = Record> {
  /** Parameterless constructor — the ORM instantiates records via `new Ctor()`. */
  new (): T;

  /** Sheet tab name where the entity data is stored (e.g. "tbl_Cars"). */
  tableName: string;

  /** Sheet tab name for the combined secondary index (e.g. "idx_Cars"). */
  indexTableName: string;
}
