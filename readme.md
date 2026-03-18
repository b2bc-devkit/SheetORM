# SheetORM

A TypeScript ORM for Google Sheets running in Google Apps Script (GAS). Inspired by Dari/Brightspot ORM patterns, SheetORM brings a structured, type-safe persistence layer to spreadsheet-based applications.

## Features

- **Type-safe repository pattern** — Generic `SheetRepository<T>` with full CRUD operations
- **Fluent query builder** — `where()`, `and()`, `or()`, `orderBy()`, `limit()`, `offset()`
- **Secondary indexes** — Stored in dedicated sheets for fast lookup by indexed fields
- **Schema migrations** — Tracked in `_meta` sheet with addField / removeField support
- **In-memory caching** — Configurable TTL cache to reduce sheet reads
- **Lifecycle hooks** — `beforeSave`, `afterSave`, `beforeDelete`, `afterDelete`
- **Batch operations** — `beginBatch` / `commitBatch` / `rollbackBatch` for safe bulk writes
- **Pagination & grouping** — `select()` returns `PaginatedResult<T>`, `groupBy()` returns `GroupResult<T>`
- **Zero runtime dependencies** — Bundles into a single `Code.js` via Webpack

## Architecture

```
src/
  core/types.ts          — All interfaces, types, constants
  core/SheetRepository.ts — Generic repository with CRUD, batch, hooks, cache
  utils/uuid.ts          — UUID v4 generation (GAS / fallback)
  utils/cache.ts         — MemoryCache (ICacheProvider)
  utils/serialization.ts — Row ↔ Entity conversion, header management
  storage/GoogleSheetsAdapter.ts — ISheetAdapter / ISpreadsheetAdapter wrappers
  query/QueryEngine.ts   — filter, sort, paginate, group pipeline
  query/QueryBuilder.ts  — Fluent query builder
  index/IndexStore.ts    — Secondary index management
  schema/SchemaMigrator.ts — Schema versioning & migrations
  SheetORM.ts            — Facade: register schemas, get repositories
  index.ts               — Barrel exports + GAS trigger stubs
```

## Quick Start

### 1. Install dependencies

```bash
npm install
```

### 2. Build

```bash
npm run build
```

This compiles TypeScript (`tsc`) and bundles via Webpack into a single `Code.js` file.

### 3. Deploy to Google Apps Script

```bash
npm run login   # once per device
npm run push    # build + push to GAS
```

### 4. Use in your GAS project

```ts
import { SheetORM, TableSchema } from './SheetORM';

interface User {
  __id: string;
  __createdAt: string;
  __updatedAt: string;
  name: string;
  email: string;
  age: number;
}

const userSchema: TableSchema = {
  tableName: 'Users',
  fields: {
    name:  { type: 'string', required: true },
    email: { type: 'string', required: true, unique: true },
    age:   { type: 'number', required: false, defaultValue: 0 },
  },
  indexes: [
    { fields: ['email'], unique: true },
  ],
};

// Initialize
const ss = SpreadsheetApp.getActiveSpreadsheet();
const orm = SheetORM.create(ss);
orm.register(userSchema);

// CRUD
const users = orm.getRepository<User>('Users');
const user = users.save({ name: 'Alice', email: 'alice@example.com', age: 30 } as any);

const found = users.findById(user.__id);
const adults = users.query()
  .where('age', '>=', 18)
  .orderBy('name', 'asc')
  .limit(10)
  .execute();

users.delete(user.__id);
```

## API Overview

### SheetORM (Facade)

| Method | Description |
|--------|-------------|
| `SheetORM.create(spreadsheet)` | Create an ORM instance |
| `register(schema)` | Register a table schema |
| `getRepository<T>(tableName)` | Get a typed repository |
| `getMigrator()` | Access the schema migrator |
| `getIndexStore()` | Access the index store |
| `clearCache()` | Clear all cached data |

### SheetRepository\<T\>

| Method | Description |
|--------|-------------|
| `save(entity)` | Insert or update an entity |
| `saveAll(entities)` | Bulk insert |
| `findById(id)` | Find by primary key |
| `find(filters?, sort?, options?)` | Find with filters and sorting |
| `findOne(filters)` | Find first matching entity |
| `delete(id)` | Delete by ID |
| `deleteAll()` | Remove all rows |
| `count(filters?)` | Count matching entities |
| `select(options)` | Paginated query → `PaginatedResult<T>` |
| `groupBy(field, filters?)` | Group → `GroupResult<T>` |
| `query()` | Start a fluent `QueryBuilder<T>` |
| `beginBatch()` / `commitBatch()` / `rollbackBatch()` | Batch operations |

### QueryBuilder\<T\>

```ts
repo.query()
  .where('status', '=', 'active')
  .and('age', '>=', 18)
  .or('role', '=', 'admin')
  .orderBy('name', 'asc')
  .limit(20)
  .offset(40)
  .execute();       // Entity[]
  // .first()       // Entity | null
  // .count()       // number
  // .groupBy('field') // GroupResult<T>
```

### Filter Operators

`=`, `!=`, `<`, `>`, `<=`, `>=`, `contains`, `startsWith`, `in`

## Testing

```bash
npm test
```

Runs 109 unit tests across 9 test suites using Jest + ts-jest with in-memory mock adapters:

- `uuid.test.ts` — UUID generation
- `cache.test.ts` — MemoryCache TTL behavior
- `serialization.test.ts` — Row ↔ Entity conversion
- `query-engine.test.ts` — Filter, sort, paginate, group
- `query-builder.test.ts` — Fluent builder API
- `index-store.test.ts` — Secondary index CRUD
- `schema-migrator.test.ts` — Schema versioning
- `repository.test.ts` — Full repository CRUD + batch + hooks
- `sheetorm.test.ts` — Facade integration

## CI

GitHub Actions workflow at `.github/workflows/ci.yml` runs:

1. TypeScript type-check (`tsc --noEmit`)
2. Unit tests (`npm test`)
3. Production build (`npm run build`)
4. Verify `Code.js` output exists

Matrix: Node 18, 20, 22.

## Available Scripts

| Script | Description |
|--------|-------------|
| `npm run build` | Clean + compile TypeScript + bundle via Webpack |
| `npm test` | Run all Jest tests |
| `npm run lint` | Lint with ESLint |
| `npm run format` | Format with Prettier |
| `npm run login` | Authenticate with Google Apps Script (once) |
| `npm run push` | Build + push to GAS |
| `npm run deploy` | Build + push + create versioned deployment |

## Sheet Layout

Each registered table occupies one sheet. Row 1 contains headers: `__id`, `__createdAt`, `__updatedAt`, followed by schema-defined fields. Data starts at row 2.

Special sheets:
- `_meta` — Schema metadata (tableName, schemaJson, version)
- `_idx_{table}_{field}` — Secondary index sheets (fieldValue → entityId mapping)

## Development Notes

### Exposing Functions to GAS

Only functions exported from `index.ts` via `export { name }` syntax are available in Google Apps Script. Due to a gas-webpack-plugin limitation, `export function` and `export const` forms do not work.

```ts
// ✅ Works
function myFunction() { /* ... */ }
export { myFunction };

// ❌ Does NOT work
export function myFunction() { /* ... */ }
```

### Circular Dependencies

Circular dependencies (files that depend on each other in a circular manner) can cause unexpected issues like
"X is not a function" or "X is not defined". If you are seeing these errors in your project and you know they
are wrong, try checking for circular dependencies using [`madge`](https://github.com/pahen/madge) (not
included in this template):

1. Install `madge` globally with `npm i --global madge`.
2. Check for circular dependencies with `madge src/index.ts --circular`.

## License

GPL-2.0 — see [license.md](license.md).
