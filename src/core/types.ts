// SheetORM — Core type definitions
// Original implementation inspired by common ORM architectural patterns

// ─── Entity ──────────────────────────────────────────

export interface Entity {
  __id: string;
  __createdAt?: string;
  __updatedAt?: string;
  [key: string]: unknown;
}

// ─── Field & Schema ──────────────────────────────────

export type FieldType = "string" | "number" | "boolean" | "date" | "json" | "reference";

export interface FieldDefinition {
  name: string;
  type?: FieldType;
  required?: boolean;
  defaultValue?: unknown;
  referenceTable?: string;
}

export interface IndexDefinition {
  field: string;
  unique?: boolean;
  type?: "string" | "number" | "date";
}

export interface TableSchema {
  tableName: string;
  indexTableName?: string;
  fields: FieldDefinition[];
  indexes: IndexDefinition[];
}

// ─── Filter & Query ──────────────────────────────────

export type FilterOperator = "=" | "!=" | "<" | ">" | "<=" | ">=" | "contains" | "startsWith" | "in";

export interface Filter {
  field: string;
  operator: FilterOperator;
  value: unknown;
}

export interface SortClause {
  field: string;
  direction: "asc" | "desc";
}

export interface QueryOptions {
  where?: Filter[];
  orderBy?: SortClause[];
  limit?: number;
  offset?: number;
}

// ─── Results ─────────────────────────────────────────

export interface PaginatedResult<T> {
  items: T[];
  total: number;
  offset: number;
  limit: number;
  hasNext: boolean;
}

export interface GroupResult<T> {
  key: unknown;
  count: number;
  items: T[];
}

// ─── Lifecycle Hooks ─────────────────────────────────

export interface LifecycleHooks<T extends Entity> {
  beforeSave?(entity: Partial<T>, isNew: boolean): Partial<T> | void;
  afterSave?(entity: T, isNew: boolean): void;
  beforeDelete?(id: string): boolean | void;
  afterDelete?(id: string): void;
  onValidate?(entity: Partial<T>): string[] | void;
}

// ─── Storage Adapter Interfaces ──────────────────────

export interface ISheetAdapter {
  getName(): string;
  getHeaders(): string[];
  setHeaders(headers: string[]): void;
  getAllData(): unknown[][];
  getRowCount(): number;
  appendRow(values: unknown[]): void;
  appendRows(rows: unknown[][]): void;
  updateRow(rowIndex: number, values: unknown[]): void;
  updateRows(updates: Array<{ rowIndex: number; values: unknown[] }>): void;
  deleteRow(rowIndex: number): void;
  deleteRows(rowIndexes: number[]): void;
  getRow(rowIndex: number): unknown[];
  replaceAllData(rows: unknown[][]): void;
  clear(): void;
  flush(): void;
}

export interface ISpreadsheetAdapter {
  getSheetByName(name: string): ISheetAdapter | null;
  createSheet(name: string): ISheetAdapter;
  deleteSheet(name: string): void;
  getSheetNames(): string[];
}

// ─── Cache Interface ─────────────────────────────────

export interface ICacheProvider {
  get<T>(key: string): T | null;
  set<T>(key: string, value: T, ttlMs?: number): void;
  delete(key: string): void;
  clear(): void;
  has(key: string): boolean;
}

// ─── System column names ─────────────────────────────

export const SYSTEM_COLUMNS = {
  ID: "__id",
  CREATED_AT: "__createdAt",
  UPDATED_AT: "__updatedAt",
} as const;

export const META_TABLE_NAME = "_meta";
