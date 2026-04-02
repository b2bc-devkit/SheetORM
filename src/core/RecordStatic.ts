import type { Entity } from "./types/Entity.js";

/**
 * Type-erased static interface for Record classes.
 *
 * Similar to RecordConstructor but produces a generic Entity
 * instead of a specific Record subclass. Used internally by
 * Registry where the concrete type parameter is unknown.
 */
export interface RecordStatic {
  /** Parameterless constructor producing an Entity instance. */
  new (): Entity;

  /** Sheet tab name for entity data (e.g. "tbl_Cars"). */
  tableName: string;

  /** Sheet tab name for the combined secondary index (e.g. "idx_Cars"). */
  indexTableName: string;

  /** JavaScript class name (used for Registry class-name lookup). */
  name: string;
}
