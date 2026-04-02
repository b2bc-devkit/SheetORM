/**
 * @module SheetOrm
 *
 * NPM package entry point for the SheetORM library.
 *
 * Re-exports the core public API surface as static members of a single
 * namespace class.  Consumer code imports from this module:
 *
 * ```ts
 * import { SheetOrm } from "sheetorm";
 * const { Record, Query, Decorators, IndexStore, Registry } = SheetOrm;
 * ```
 *
 * This file is referenced by `tsconfig.npm.json` and the npm build
 * script; it does **not** include GAS-specific helpers (e.g. demos,
 * test runners) that live in `index.ts`.
 */
import { Decorators } from "./core/Decorators.js";
import { Record } from "./core/Record.js";
import { Registry } from "./core/Registry.js";
import { IndexStore } from "./index/IndexStore.js";
import { Query } from "./query/Query.js";

/**
 * Top-level namespace class that bundles every public SheetORM artifact.
 *
 * Designed as a single default import so consumers can destructure
 * only the parts they need without deep import paths.
 */
export class SheetOrm {
  /** ActiveRecord base class — extend this to define entity models. */
  static readonly Record = Record;
  /** Fluent query builder for filtering, sorting, and paginating records. */
  static readonly Query = Query;
  /** Property decorators: `@Field`, `@Indexed`, `@Required`. */
  static readonly Decorators = Decorators;
  /** Secondary-index manager with n-gram text search. */
  static readonly IndexStore = IndexStore;
  /** Singleton registry that tracks all registered entity classes. */
  static readonly Registry = Registry;
}
