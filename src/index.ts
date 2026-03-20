// SheetORM — Public API exports
// All types, classes, and utilities exported for consumers

// Core types
export type {
  Entity,
  FieldType,
  FieldDefinition,
  IndexDefinition,
  TableSchema,
  FilterOperator,
  Filter,
  SortClause,
  QueryOptions,
  PaginatedResult,
  GroupResult,
  LifecycleHooks,
  ISheetAdapter,
  ISpreadsheetAdapter,
  ICacheProvider,
} from "./core/types";

export { SYSTEM_COLUMNS, META_TABLE_NAME, INDEX_PREFIX } from "./core/types";

// ActiveRecord base class (primary API)
export { Record } from "./core/Record";
export type { RecordConstructor } from "./core/Record";

// Decorators
export { Indexed, Field, Required, getFields, getIndexes, resetDecoratorCaches } from "./core/decorators";

// Global registry
export { Registry } from "./core/Registry";
export type { RecordStatic } from "./core/Registry";


// Query
export { Query } from "./query/Query";
export {
  filterEntities,
  sortEntities,
  paginateEntities,
  groupEntities,
  executeQuery,
} from "./query/QueryEngine";

// Index
export { IndexStore } from "./index/IndexStore";
export type { IndexMeta } from "./index/IndexStore";

// Storage adapters
export { GoogleSheetAdapter, GoogleSpreadsheetAdapter } from "./storage/GoogleSheetsAdapter";

// Utilities
export { generateUUID } from "./utils/uuid";
export { MemoryCache } from "./utils/cache";
export {
  serializeValue,
  deserializeValue,
  buildHeaders,
  entityToRow,
  rowToEntity,
} from "./utils/serialization";
export { runTests, validateTests } from "./testing/runtimeParity";
export { runSheetOrmBenchmark } from "./testing/runtimeBenchmark";
