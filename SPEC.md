# SheetORM — Specyfikacja API TypeScript

## Przegląd

SheetORM to biblioteka ORM dla Google Apps Script (GAS), która mapuje koncepcje Record/Table/Index
na Google Sheets traktowane jako tabele relacyjnej bazy danych. Inspirowana wzorcami architektonicznymi
znanych frameworków ORM (Active Record, Repository, Query Builder).

> **Uwaga**: Implementacja jest oryginalna. Nazwy i wzorce są neutralne — nie używamy nazw
> objętych znakami towarowymi.

---

## 1. Interfejsy podstawowe

### Entity
```typescript
interface Entity {
  __id: string;
  __createdAt?: string;
  __updatedAt?: string;
  [key: string]: unknown;
}
```

### FieldDefinition
```typescript
interface FieldDefinition {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date' | 'json' | 'reference';
  required?: boolean;
  defaultValue?: unknown;
  referenceTable?: string;
}
```

### IndexDefinition
```typescript
interface IndexDefinition {
  field: string;
  unique?: boolean;
  type?: 'string' | 'number' | 'date';
}
```

### TableSchema
```typescript
interface TableSchema {
  tableName: string;
  fields: FieldDefinition[];
  indexes: IndexDefinition[];
}
```

### Filter (predykat)
```typescript
type FilterOperator = '=' | '!=' | '<' | '>' | '<=' | '>=' | 'contains' | 'startsWith' | 'in';

interface Filter {
  field: string;
  operator: FilterOperator;
  value: unknown;
}
```

### SortClause
```typescript
interface SortClause {
  field: string;
  direction: 'asc' | 'desc';
}
```

### Query
```typescript
interface QueryOptions {
  where?: Filter[];
  orderBy?: SortClause[];
  limit?: number;
  offset?: number;
}
```

### PaginatedResult
```typescript
interface PaginatedResult<T> {
  items: T[];
  total: number;
  offset: number;
  limit: number;
  hasNext: boolean;
}
```

### GroupResult
```typescript
interface GroupResult<T> {
  key: unknown;
  count: number;
  items: T[];
}
```

---

## 2. SheetRepository<T> — główny interfejs repozytorium

```typescript
interface SheetRepository<T extends Entity> {
  /** Znajdź wiele encji spełniających warunki */
  find(query?: QueryOptions): T[];

  /** Znajdź jedną encję */
  findOne(query?: QueryOptions): T | null;

  /** Znajdź encję po ID */
  findById(id: string): T | null;

  /** Zapisz (utwórz lub zaktualizuj) encję */
  save(entity: Partial<T> & { __id?: string }): T;

  /** Zapisz wiele encji w batch */
  saveAll(entities: Array<Partial<T>>): T[];

  /** Usuń encję po ID */
  delete(id: string): boolean;

  /** Usuń wiele encji po warunkach */
  deleteAll(query?: QueryOptions): number;

  /** Policz encje */
  count(query?: QueryOptions): number;

  /** Paginacja */
  select(offset: number, limit: number, query?: QueryOptions): PaginatedResult<T>;

  /** Grupowanie */
  groupBy(field: string, query?: QueryOptions): GroupResult<T>[];

  /** Utwórz fluent query builder */
  query(): QueryBuilder<T>;
}
```

---

## 3. QueryBuilder<T> — fluent API

```typescript
interface QueryBuilder<T extends Entity> {
  /** Dodaj warunek WHERE */
  where(field: string, operator: FilterOperator, value: unknown): QueryBuilder<T>;

  /** Dodaj warunek AND */
  and(field: string, operator: FilterOperator, value: unknown): QueryBuilder<T>;

  /** Dodaj warunek OR — nowa grupa */
  or(field: string, operator: FilterOperator, value: unknown): QueryBuilder<T>;

  /** Sortowanie rosnące */
  orderBy(field: string, direction?: 'asc' | 'desc'): QueryBuilder<T>;

  /** Limit wyników */
  limit(count: number): QueryBuilder<T>;

  /** Offset */
  offset(count: number): QueryBuilder<T>;

  /** Wykonaj query — zwróć wszystkie wyniki */
  execute(): T[];

  /** Wykonaj — zwróć pierwszy wynik */
  first(): T | null;

  /** Wykonaj — zwróć paginowany wynik */
  select(offset: number, limit: number): PaginatedResult<T>;

  /** Policz wyniki */
  count(): number;

  /** Grupuj wyniki */
  groupBy(field: string): GroupResult<T>[];
}
```

---

## 4. IndexStore — zarządzanie indeksami

```typescript
interface IndexStore {
  /** Utwórz indeks dla pola tabeli */
  createIndex(tableName: string, field: string, options?: { unique?: boolean }): void;

  /** Usuń indeks */
  dropIndex(tableName: string, field: string): void;

  /** Wyszukaj ID-ki po wartości indeksu */
  lookup(tableName: string, field: string, value: unknown): string[];

  /** Dodaj wpis do indeksu */
  add(tableName: string, field: string, value: unknown, entityId: string): void;

  /** Usuń wpis z indeksu */
  remove(tableName: string, field: string, value: unknown, entityId: string): void;

  /** Przebuduj indeks z istniejących danych */
  rebuild(tableName: string, field: string): void;

  /** Sprawdź czy indeks istnieje */
  exists(tableName: string, field: string): boolean;
}
```

---

## 5. SchemaMigrator — migracje schematu

```typescript
interface SchemaMigrator {
  /** Zainicjuj arkusz dla schematu tabeli */
  initialize(schema: TableSchema): void;

  /** Dodaj kolumnę do istniejącej tabeli */
  addField(tableName: string, field: FieldDefinition): void;

  /** Usuń kolumnę */
  removeField(tableName: string, fieldName: string): void;

  /** Pobierz aktualny schemat tabeli */
  getSchema(tableName: string): TableSchema | null;

  /** Sprawdź czy tabela istnieje */
  tableExists(tableName: string): boolean;

  /** Synchronizuj schemat (dodaj brakujące kolumny) */
  sync(schema: TableSchema): void;
}
```

---

## 6. BatchOperation — transakcje batch

```typescript
interface BatchOperation {
  /** Rozpocznij buforowanie operacji */
  begin(): void;

  /** Zatwierdź i wykonaj buforowane operacje */
  commit(): void;

  /** Anuluj buforowane operacje */
  rollback(): void;

  /** Czy batch jest aktywny */
  isActive(): boolean;
}
```

---

## 7. ISpreadsheetAdapter — interfejs storage (DI/mock)

```typescript
interface ISpreadsheetAdapter {
  getSheetByName(name: string): ISheetAdapter | null;
  createSheet(name: string): ISheetAdapter;
  deleteSheet(name: string): void;
  getSheetNames(): string[];
}

interface ISheetAdapter {
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
  clear(): void;
  flush(): void;
}
```

---

## 8. Cache

```typescript
interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

interface ICacheProvider {
  get<T>(key: string): T | null;
  set<T>(key: string, value: T, ttlMs?: number): void;
  delete(key: string): void;
  clear(): void;
  has(key: string): boolean;
}
```

---

## 9. Lifecycle Hooks

```typescript
interface LifecycleHooks<T extends Entity> {
  beforeSave?(entity: Partial<T>, isNew: boolean): Partial<T> | void;
  afterSave?(entity: T, isNew: boolean): void;
  beforeDelete?(id: string): boolean | void;
  afterDelete?(id: string): void;
  onValidate?(entity: Partial<T>): string[] | void;
}
```

---

## 10. Przykłady użycia

### Definicja schematu i repozytorium
```typescript
const userSchema: TableSchema = {
  tableName: 'Users',
  fields: [
    { name: 'name', type: 'string', required: true },
    { name: 'email', type: 'string', required: true },
    { name: 'age', type: 'number' },
    { name: 'active', type: 'boolean', defaultValue: true },
  ],
  indexes: [
    { field: 'email', unique: true },
    { field: 'name' },
  ],
};

const orm = SheetORM.create();
orm.register(userSchema);

const users = orm.getRepository<User>('Users');
```

### CRUD
```typescript
// Create
const user = users.save({ name: 'Jan Kowalski', email: 'jan@example.com', age: 30 });
console.log(user.__id); // UUID

// Read
const found = users.findById(user.__id);
const allActive = users.find({ where: [{ field: 'active', operator: '=', value: true }] });

// Update
users.save({ ...user, age: 31 });

// Delete
users.delete(user.__id);
```

### Query Builder
```typescript
const results = users.query()
  .where('age', '>', 25)
  .and('active', '=', true)
  .orderBy('name', 'asc')
  .limit(10)
  .execute();
```

### Paginacja
```typescript
const page = users.select(0, 20, {
  where: [{ field: 'active', operator: '=', value: true }],
  orderBy: [{ field: 'name', direction: 'asc' }],
});

console.log(page.items);    // User[]
console.log(page.total);    // total count
console.log(page.hasNext);  // true/false
```

### Grupowanie
```typescript
const groups = users.groupBy('active');
// [{ key: true, count: 42, items: [...] }, { key: false, count: 8, items: [...] }]
```

### Batch operations
```typescript
const batch = orm.batch();
batch.begin();
users.save({ name: 'A', email: 'a@x.com' });
users.save({ name: 'B', email: 'b@x.com' });
batch.commit();
```

---

## 11. Schematy arkuszy

### Arkusz encji (np. `Users`)
| __id | __createdAt | __updatedAt | name | email | age | active |
|------|-------------|-------------|------|-------|-----|--------|
| uuid-1 | 2024-01-01T... | 2024-01-02T... | Jan | jan@x.com | 30 | true |

### Arkusz indeksu (np. `_idx_Users_email`)
| value | entityId |
|-------|----------|
| jan@x.com | uuid-1 |

### Arkusz metadanych (`_meta`)
| tableName | schemaJson | version |
|-----------|------------|---------|
| Users | {...} | 1 |

---

## 12. Mapowanie typów

| Typ FieldDefinition | Typ kolumny Sheets | Serializacja |
|---|---|---|
| string | tekst | bezpośrednio |
| number | liczba | bezpośrednio |
| boolean | true/false | bezpośrednio |
| date | ISO 8601 string | `new Date().toISOString()` |
| json | tekst (JSON) | `JSON.stringify` / `JSON.parse` |
| reference | tekst (UUID) | UUID encji z innego arkusza |
