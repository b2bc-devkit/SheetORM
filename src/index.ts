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
} from './core/types';

export { SYSTEM_COLUMNS, META_TABLE_NAME, INDEX_PREFIX } from './core/types';

// Main ORM facade
export { SheetORM } from './SheetORM';
export type { SheetORMOptions } from './SheetORM';

// Repository
export { SheetRepository } from './core/SheetRepository';

// Query
export { QueryBuilder } from './query/QueryBuilder';
export {
  filterEntities,
  sortEntities,
  paginateEntities,
  groupEntities,
  executeQuery,
} from './query/QueryEngine';

// Index
export { IndexStore } from './index/IndexStore';
export type { IndexMeta } from './index/IndexStore';

// Schema
export { SchemaMigrator } from './schema/SchemaMigrator';

// Storage adapters
export {
  GoogleSheetAdapter,
  GoogleSpreadsheetAdapter,
} from './storage/GoogleSheetsAdapter';

// Utilities
export { generateUUID } from './utils/uuid';
export { MemoryCache } from './utils/cache';
export {
  serializeValue,
  deserializeValue,
  buildHeaders,
  entityToRow,
  rowToEntity,
} from './utils/serialization';

// Re-export example for GAS triggers (keep backward compat)
export { helloWorld } from './example';

// GAS Triggers
function onOpen(
  e:
    | GoogleAppsScript.Events.DocsOnOpen
    | GoogleAppsScript.Events.SlidesOnOpen
    | GoogleAppsScript.Events.SheetsOnOpen
    | GoogleAppsScript.Events.FormsOnOpen,
): void {
  console.log(e);
}

function onEdit(e: GoogleAppsScript.Events.SheetsOnEdit): void {
  console.log(e);
}

function onInstall(e: GoogleAppsScript.Events.AddonOnInstall): void {
  console.log(e);
}

function doGet(e: GoogleAppsScript.Events.DoGet): void {
  console.log(e);
}

function doPost(e: GoogleAppsScript.Events.DoPost): void {
  console.log(e);
}

export { onOpen, onEdit, onInstall, doGet, doPost };
