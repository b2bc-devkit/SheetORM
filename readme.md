# SheetORM

A TypeScript ORM for Google Sheets running in Google Apps Script (GAS). SheetORM brings a structured,
type-safe persistence layer to spreadsheet-based applications with an **ActiveRecord** API — define a class,
extend `Record`, and everything just works.

## Features

- **ActiveRecord pattern** — Extend `Record`, define fields, and call `save()` / `find()` / `delete()`
  directly on instances and classes
- **Zero configuration** — Tables, schemas, indexes, and repositories are auto-created on first use
- **Fluent query builder** — `where()`, `and()`, `or()`, `orderBy()`, `limit()`, `offset()`
- **`QueryBuilder.from()`** — Start queries from a class reference or string name
- **Secondary indexes** — Stored in dedicated sheets for fast lookup by indexed fields
- **Schema migrations** — Tracked in `_meta` sheet with addField / removeField support
- **In-memory caching** — Configurable TTL cache to reduce sheet reads
- **Lifecycle hooks** — `beforeSave`, `afterSave`, `beforeDelete`, `afterDelete`
- **Batch operations** — `beginBatch` / `commitBatch` / `rollbackBatch` for safe bulk writes
- **Pagination & grouping** — `select()` returns `PaginatedResult<T>`, `groupBy()` returns `GroupResult<T>`
- **Zero runtime dependencies** — Bundles into a single `Code.js` via Webpack

## Quick Start

### 1. Install & build

```bash
npm install
npm run build
```

### 2. Deploy to Google Apps Script

```bash
npm run login   # once per device
npm run push    # build + push to GAS
```

### 3. Define a model

```ts
class Car extends Record {
  static tableName = "Cars";
  static fields: FieldDefinition[] = [
    { name: "make", type: "string", required: true },
    { name: "model", type: "string", required: true },
    { name: "year", type: "number" },
    { name: "color", type: "string" },
  ];
  static indexes: IndexDefinition[] = [{ field: "make" }];

  declare make: string;
  declare model: string;
  declare year: number;
  declare color: string;
}
```

### 4. Use it

```ts
// Create — table auto-created on first save
const car = new Car();
car.make = "Toyota";
car.model = "Corolla";
car.year = 2024;
car.color = "blue";
car.save();

// Fluent set + save (chainable)
new Car().set("make", "Honda").set("model", "Civic").set("year", 2023).save();

// Static queries — return typed Car[]
const toyotas = Car.where("make", "=", "Toyota").execute();
const found = Car.findById(car.__id);
const all = Car.find();

// QueryBuilder.from() — class ref (typed) or string
const recent = QueryBuilder.from(Car).where("year", ">=", 2023).orderBy("year", "desc").limit(10).execute();

// Update
car.color = "red";
car.save();

// Delete
car.delete();

// Count, pagination, grouping
Car.count();
Car.select(0, 10);
Car.groupBy("make");
```

> See [`examples/cars-crud.ts`](examples/cars-crud.ts) for a complete runnable example.

## Architecture

```
src/
  core/Record.ts          — ActiveRecord base class (primary API)
  core/Registry.ts        — Global singleton: adapter, repos, class map
  core/SheetRepository.ts — Generic repository: CRUD, batch, hooks, cache
  core/types.ts           — All interfaces, types, constants
  query/QueryBuilder.ts   — Fluent query builder + QueryBuilder.from()
  query/QueryEngine.ts    — filter, sort, paginate, group pipeline
  index/IndexStore.ts     — Secondary index management
  schema/SchemaMigrator.ts— Schema versioning & migrations
  storage/GoogleSheetsAdapter.ts — ISheetAdapter / ISpreadsheetAdapter wrappers
  utils/uuid.ts           — UUID v4 generation (GAS / fallback)
  utils/cache.ts          — MemoryCache (ICacheProvider)
  utils/serialization.ts  — Row ↔ Entity conversion
  testing/                — Runtime parity test suite
  SheetORM.ts             — Legacy facade (still supported)
  index.ts                — Barrel exports + GAS trigger stubs
examples/
  cars-crud.ts            — Full ActiveRecord example
```

## API Reference

### Record (ActiveRecord base class)

Extend `Record` to define a model. Override three static properties:

| Static property | Type                | Description                  |
| --------------- | ------------------- | ---------------------------- |
| `tableName`     | `string`            | Sheet name (required)        |
| `fields`        | `FieldDefinition[]` | Column definitions           |
| `indexes`       | `IndexDefinition[]` | Secondary indexes (optional) |

#### Instance methods

| Method              | Returns   | Description                   |
| ------------------- | --------- | ----------------------------- |
| `save()`            | `this`    | Insert or update (chainable)  |
| `delete()`          | `boolean` | Delete from sheet             |
| `set(field, value)` | `this`    | Set a field value (chainable) |
| `get(field)`        | `unknown` | Get a field value             |
| `toJSON()`          | `object`  | Plain object with all fields  |

#### Static methods

| Method                            | Returns              | Description                           |
| --------------------------------- | -------------------- | ------------------------------------- |
| `findById(id)`                    | `T \| null`          | Find by primary key                   |
| `find(options?)`                  | `T[]`                | Find all (with optional filters/sort) |
| `findOne(options?)`               | `T \| null`          | Find first matching entity            |
| `where(field, op, value)`         | `QueryBuilder<T>`    | Start a filtered query chain          |
| `query()`                         | `QueryBuilder<T>`    | Start an empty query chain            |
| `count(options?)`                 | `number`             | Count matching entities               |
| `deleteAll(options?)`             | `number`             | Delete matching entities              |
| `select(offset, limit, options?)` | `PaginatedResult<T>` | Paginated query                       |
| `groupBy(field, options?)`        | `GroupResult<T>[]`   | Group by field                        |

### QueryBuilder\<T\>

```ts
Car.where("make", "=", "Toyota")
  .and("year", ">=", 2020)
  .or("color", "=", "red")
  .orderBy("year", "desc")
  .limit(20)
  .offset(40)
  .execute(); // T[]
// .first()      // T | null
// .count()      // number
// .groupBy("field") // GroupResult<T>[]
```

Start from any class:

```ts
QueryBuilder.from(Car).where("year", ">=", 2023).execute();
QueryBuilder.from("Car").where("make", "=", "Toyota").first();
```

### Filter operators

`=`, `!=`, `<`, `>`, `<=`, `>=`, `contains`, `startsWith`, `in`

### Field types

`string`, `number`, `boolean`, `json`, `date`, `reference`

### SheetORM (legacy facade)

The original `SheetORM` facade still works for manual repository management:

| Method                        | Description                |
| ----------------------------- | -------------------------- |
| `SheetORM.create(options?)`   | Create an ORM instance     |
| `register(schema)`            | Register a table schema    |
| `getRepository<T>(tableName)` | Get a typed repository     |
| `getMigrator()`               | Access the schema migrator |
| `getIndexStore()`             | Access the index store     |
| `clearCache()`                | Clear all cached data      |

### SheetRepository\<T\> (advanced)

| Method                                               | Description                            |
| ---------------------------------------------------- | -------------------------------------- |
| `save(entity)`                                       | Insert or update an entity             |
| `saveAll(entities)`                                  | Bulk insert                            |
| `findById(id)`                                       | Find by primary key                    |
| `find(options?)`                                     | Find with filters and sorting          |
| `findOne(options?)`                                  | Find first matching entity             |
| `delete(id)`                                         | Delete by ID                           |
| `deleteAll(options?)`                                | Remove matching rows                   |
| `count(options?)`                                    | Count matching entities                |
| `select(offset, limit, options?)`                    | Paginated query → `PaginatedResult<T>` |
| `groupBy(field, options?)`                           | Group → `GroupResult<T>[]`             |
| `query()`                                            | Start a fluent `QueryBuilder<T>`       |
| `beginBatch()` / `commitBatch()` / `rollbackBatch()` | Batch operations                       |

## Testing

```bash
npm test
```

Runs **146 unit tests** across 11 test suites using Jest + ts-jest with in-memory mock adapters:

| Suite                      | Tests | Description                              |
| -------------------------- | ----- | ---------------------------------------- |
| `record.test.ts`           | 34    | ActiveRecord API (save, find, query, QB) |
| `repository.test.ts`       | 21    | Full repository CRUD + batch + hooks     |
| `query-engine.test.ts`     | 21    | Filter, sort, paginate, group            |
| `serialization.test.ts`    | 14    | Row ↔ Entity conversion                  |
| `schema-migrator.test.ts`  | 12    | Schema versioning                        |
| `query-builder.test.ts`    | 11    | Fluent builder API                       |
| `index-store.test.ts`      | 11    | Secondary index CRUD                     |
| `sheetorm.test.ts`         | 9     | Legacy facade integration                |
| `cache.test.ts`            | 8     | MemoryCache TTL behavior                 |
| `uuid.test.ts`             | 2     | UUID generation                          |
| `parity-validator.test.ts` | 3     | Jest ↔ GAS runtime parity check          |

### Jest ↔ GAS Runtime Parity (1:1)

Every Jest test has a matching handler in the GAS runtime parity suite. This ensures the library works
identically with real Google Sheets:

- `src/testing/parityCatalog.ts` — canonical list of all Jest test cases
- `src/testing/runtimeParity.ts` — runtime suite executing against real Sheets API
- `tests/parity-validator.test.ts` — fails when Jest and runtime cases diverge

Run locally (mock adapters):

```bash
npm test
```

Run in Google Apps Script (real Sheets API):

- `runSheetOrmRuntimeParity()` — executes full runtime parity suite against the active spreadsheet
- `validateSheetOrmRuntimeParity()` — validates mapping only (fast drift check)

## CI

GitHub Actions workflow at `.github/workflows/ci.yml`:

1. TypeScript type-check (`tsc --noEmit`)
2. Unit tests (`npm test`)
3. Production build (`npm run build`)
4. Verify `Code.js` output exists

Matrix: Node 18, 20, 22.

## License

[GPL](license.md)

## Available Scripts

| Script           | Description                                     |
| ---------------- | ----------------------------------------------- |
| `npm run build`  | Clean + compile TypeScript + bundle via Webpack |
| `npm test`       | Run all Jest tests                              |
| `npm run lint`   | Lint with ESLint                                |
| `npm run format` | Format with Prettier                            |
| `npm run login`  | Authenticate with Google Apps Script (once)     |
| `npm run push`   | Build + push to GAS                             |
| `npm run deploy` | Build + push + create versioned deployment      |

## Sheet Layout

Each registered table occupies one sheet. Row 1 contains headers: `__id`, `__createdAt`, `__updatedAt`,
followed by schema-defined fields. Data starts at row 2.

Special sheets:

- `_meta` — Schema metadata (tableName, schemaJson, version)
- `_idx_{table}_{field}` — Secondary index sheets (fieldValue → entityId mapping)

## Development Notes

### Exposing Functions to GAS

Only functions exported from `index.ts` via `export { name }` syntax are available in Google Apps Script. Due
to a gas-webpack-plugin limitation, `export function` and `export const` forms do not work.

```ts
// ✅ Works
function myFunction() {
  /* ... */
}
export { myFunction };

// ❌ Does NOT work
export function myFunction() {
  /* ... */
}
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
