# SheetORM

A TypeScript ORM for Google Sheets running in Google Apps Script (GAS). SheetORM brings a structured,
type-safe persistence layer to spreadsheet-based applications with an **ActiveRecord** API — define a class,
extend `Record`, and everything just works.

## Features

- **ActiveRecord pattern** — Extend `Record`, define fields, and call `save()` / `find()` / `delete()`
  directly on instances and classes
- **Zero configuration** — Tables, schemas, indexes, and repositories are auto-created on first use
- **Fluent query builder** — `where()`, `and()`, `or()`, `orderBy()`, `limit()`, `offset()`
- **`Query.from()`** — Start queries from a class reference or string name
- **Secondary indexes** — Stored in dedicated sheets for fast lookup by indexed fields
- **In-memory caching** — Configurable TTL cache to reduce sheet reads
- **Lifecycle hooks** — `beforeSave`, `afterSave`, `beforeDelete`, `afterDelete`
- **Batch operations** — `beginBatch` / `commitBatch` / `rollbackBatch` for safe bulk writes
- **Pagination & grouping** — `select()` returns `PaginatedResult<T>`, `groupBy()` returns `GroupResult<T>`
- **Zero runtime dependencies** — Bundles into a single `Code.js` via Vite

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

Extend `Record` and declare fields as plain TypeScript properties. Use `@Indexed()` for indexed fields,
`@Required()` for required fields, and `@Field()` for extra options. Undecorated properties are
auto-discovered as fields.

```ts
import { Record, Indexed, Required } from "sheetorm";

class Car extends Record {
  @Indexed()
  make: string;

  @Required()
  model: string;

  year: number;
  color: string;
}
```

- No `tableName` needed in the common case — the sheet name defaults to the class name (`Car` → `Cars`)
- `@Indexed()` — marks a field as a secondary index (also implies `@Field`)
- `@Required()` — marks a field as required using concise decorator syntax
- `@Field(...)` — adds extra field options like `type`, `defaultValue`, or `referenceTable`
- Plain properties (`year`, `color`) — auto-discovered as schema fields with type inferred at runtime

If you want a custom sheet name, override the static getter:

```ts
class ArchivedCar extends Record {
  static override get tableName() {
    return "ArchivedCars";
  }
}
```

### Available decorators

SheetORM supports three TypeScript property decorators on `Record` models.

#### `@Required()`

Use `@Required()` when a field must be present before `save()` succeeds.

```ts
class Car extends Record {
  @Required()
  model: string;
}
```

This decorator is shorthand for `@Field({ required: true })`.

#### `@Field(options?)`

Use `@Field()` when you want to describe schema metadata explicitly.

```ts
class Car extends Record {
  @Field({ type: "date" })
  purchasedAt: Date;

  @Field({ type: "reference", referenceTable: "Owners" })
  ownerId: string;
}
```

Supported options:

| Option           | Type        | Description                              |
| ---------------- | ----------- | ---------------------------------------- |
| `required`       | `boolean`   | Rejects save when the field is missing   |
| `type`           | `FieldType` | Overrides runtime type inference         |
| `defaultValue`   | `unknown`   | Value used when the field is empty       |
| `referenceTable` | `string`    | Target table name for `reference` fields |

#### `@Indexed(options?)`

Use `@Indexed()` when the field should have a secondary index for faster lookups.

```ts
class Car extends Record {
  @Indexed()
  make: string;

  @Indexed({ unique: true })
  vin: string;

  @Indexed({ type: "date" })
  registeredAt: Date;
}
```

Supported options:

| Option   | Type                             | Description                      |
| -------- | -------------------------------- | -------------------------------- |
| `unique` | `boolean`                        | Enforces uniqueness in the index |
| `type`   | `"string" \| "number" \| "date"` | Controls index value typing      |

#### Plain properties without annotations

Not everything needs a decorator. Plain class properties are still discovered automatically and become normal
schema fields:

```ts
class Car extends Record {
  make: string;
  model: string;
  year: number;
}
```

Use decorators only when you need schema metadata or indexing behaviour.

### 4. Use it

```ts
// Create — table auto-created on first save
const car = new Car();
car.make = "Toyota";
car.model = "Corolla";
car.year = 2024;
car.color = "blue";
car.save();

// Or use the static factory
const civic = Car.create({ make: "Honda", model: "Civic", year: 2023, color: "white" });
civic.save();

// Fluent set + save (chainable)
new Car().set("make", "Honda").set("model", "Civic").set("year", 2023).save();

// Static queries — return typed Car[]
const toyotas = Car.where("make", "=", "Toyota").execute();
const found = Car.findById(car.__id);
const all = Car.find();

// Query.from() — class ref (typed) or string
const recent = Query.from(Car).where("year", ">=", 2023).orderBy("year", "desc").limit(10).execute();

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

See [`examples/cars-crud.ts`](examples/cars-crud.ts) for a complete runnable example.

## Architecture

```
src/
  core/Record.ts          — ActiveRecord base class (primary API)
  core/Registry.ts        — Global singleton: adapter, repos, class map
  core/SheetRepository.ts — Generic repository: CRUD, batch, hooks, cache
  core/types.ts           — All interfaces, types, constants
  query/Query.ts          — Fluent query API + Query.from()
  query/QueryEngine.ts    — filter, sort, paginate, group pipeline
  index/IndexStore.ts     — Secondary index management
  storage/GoogleSheetsAdapter.ts — ISheetAdapter / ISpreadsheetAdapter wrappers
  utils/uuid.ts           — UUID v4 generation (GAS / fallback)
  utils/cache.ts          — MemoryCache (ICacheProvider)
  utils/serialization.ts  — Row ↔ Entity conversion
  testing/                — Runtime parity test suite
  index.ts                — Barrel exports + GAS trigger stubs
examples/
  cars-crud.ts            — Full ActiveRecord example
```

## API Reference

### Record (ActiveRecord base class)

Extend `Record` to define a model. Declare fields as plain class properties — they are auto-discovered. Use
decorators and an optional static property to customize behavior:

| Decorator / property     | Description                                                   |
| ------------------------ | ------------------------------------------------------------- |
| `@Required()`            | Shorthand for marking a field as required                     |
| `@Field(options?)`       | Explicit field with options (required, type, defaultValue)    |
| `@Indexed(options?)`     | Secondary index (implies `@Field`)                            |
| `static get tableName()` | Sheet name override (optional — defaults to class name + "s") |

#### `@Required()`

Use `@Required()` when a value must be present before saving.

| Behavior            | Description                                                                |
| ------------------- | -------------------------------------------------------------------------- |
| Required validation | Rejects `save()` when the field is `undefined`, `null`, or an empty string |
| Equivalent form     | Same as `@Field({ required: true })`                                       |

#### `@Field` options

| Option           | Type        | Default     | Description                                                                |
| ---------------- | ----------- | ----------- | -------------------------------------------------------------------------- |
| `required`       | `boolean`   | `false`     | Reject saves when the value is missing                                     |
| `type`           | `FieldType` | auto-infer  | Explicit type (`string`, `number`, `boolean`, `date`, `json`, `reference`) |
| `defaultValue`   | `any`       | `undefined` | Value used when the field is empty                                         |
| `referenceTable` | `string`    | `undefined` | Target table for `reference` type fields                                   |

#### `@Indexed` options

| Option   | Type        | Default    | Description                     |
| -------- | ----------- | ---------- | ------------------------------- |
| `unique` | `boolean`   | `false`    | Enforce uniqueness in the index |
| `type`   | `FieldType` | auto-infer | Index value type                |

#### Auto-discovered fields

Any property declared on a `Record` subclass that is **not** a system column (`__id`, `__createdAt`,
`__updatedAt`) and is **not** a function is automatically treated as a schema field. Its type is inferred at
runtime from the value (`typeof`). You only need `@Field()` when you want to set options like `required` or an
explicit type.

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
| `create(data)`                    | `T`                  | Factory: create instance with data    |
| `findById(id)`                    | `T \| null`          | Find by primary key                   |
| `find(options?)`                  | `T[]`                | Find all (with optional filters/sort) |
| `findOne(options?)`               | `T \| null`          | Find first matching entity            |
| `where(field, op, value)`         | `Query<T>`           | Start a filtered query chain          |
| `query()`                         | `Query<T>`           | Start an empty query chain            |
| `count(options?)`                 | `number`             | Count matching entities               |
| `deleteAll(options?)`             | `number`             | Delete matching entities              |
| `select(offset, limit, options?)` | `PaginatedResult<T>` | Paginated query                       |
| `groupBy(field, options?)`        | `GroupResult<T>[]`   | Group by field                        |

### Query\<T\>

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
Query.from(Car).where("year", ">=", 2023).execute();
Query.from("Car").where("make", "=", "Toyota").first();
```

### Filter operators

`=`, `!=`, `<`, `>`, `<=`, `>=`, `contains`, `startsWith`, `in`

### Field types

`string`, `number`, `boolean`, `json`, `date`, `reference`

## Testing

```bash
npm test
```

Runs **104 unit tests** across 8 test suites using Jest + ts-jest with in-memory mock adapters:

| Suite                      | Tests | Description                                 |
| -------------------------- | ----- | ------------------------------------------- |
| `record.test.ts`           | 34    | ActiveRecord API (save, find, query, Query) |
| `query-engine.test.ts`     | 21    | Filter, sort, paginate, group               |
| `serialization.test.ts`    | 14    | Row ↔ Entity conversion                     |
| `query.test.ts`            | 11    | Fluent query API                            |
| `index-store.test.ts`      | 11    | Secondary index CRUD                        |
| `cache.test.ts`            | 8     | MemoryCache TTL behavior                    |
| `uuid.test.ts`             | 2     | UUID generation                             |
| `parity-validator.test.ts` | 3     | Jest ↔ GAS runtime parity check             |

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

## Available Scripts

| Script           | Description                                     |
| ---------------- | ----------------------------------------------- |
| `npm run build`  | Clean + compile TypeScript + bundle via Vite    |
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
