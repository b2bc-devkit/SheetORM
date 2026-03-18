import { Entity, FieldDefinition, Filter, QueryOptions, SortClause, TableSchema } from "../core/types";
import { SheetRepository } from "../core/SheetRepository";
import { SheetORM } from "../SheetORM";
import { IndexStore } from "../index/IndexStore";
import { QueryBuilder } from "../query/QueryBuilder";
import { Record as BaseRecord } from "../core/Record";
import {
  executeQuery,
  filterEntities,
  groupEntities,
  paginateEntities,
  sortEntities,
} from "../query/QueryEngine";
import { SchemaMigrator } from "../schema/SchemaMigrator";
import { GoogleSpreadsheetAdapter } from "../storage/GoogleSheetsAdapter";
import { MemoryCache } from "../utils/cache";
import {
  buildHeaders,
  deserializeValue,
  entityToRow,
  rowToEntity,
  serializeValue,
} from "../utils/serialization";
import { generateUUID } from "../utils/uuid";
import { PARITY_CASE_IDS, PARITY_SUITES, toParityCaseId } from "./parityCatalog";

interface RuntimeCaseContext {
  state: RuntimeParityState;
}

type RuntimeCaseHandler = (ctx: RuntimeCaseContext) => void;

type RuntimeSuiteHandlers = Record<string, Record<string, RuntimeCaseHandler>>;

interface RuntimeCaseResult {
  id: string;
  ok: boolean;
  error?: string;
}

class RuntimeParityState {
  private readonly runId = Date.now();
  private sequence = 0;
  private spreadsheet: GoogleAppsScript.Spreadsheet.Spreadsheet | null = null;

  getSpreadsheet(): GoogleAppsScript.Spreadsheet.Spreadsheet {
    if (this.spreadsheet) return this.spreadsheet;
    if (typeof SpreadsheetApp === "undefined") {
      throw new Error("SpreadsheetApp is not available. Run this function in Google Apps Script runtime.");
    }
    this.spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    return this.spreadsheet;
  }

  getAdapter(): GoogleSpreadsheetAdapter {
    return new GoogleSpreadsheetAdapter(this.getSpreadsheet());
  }

  nextTableName(baseName: string): string {
    this.sequence += 1;
    return `${baseName}_${this.runId}_${this.sequence}`;
  }
}

function fail(message: string): never {
  throw new Error(message);
}

function assertTrue(condition: boolean, message: string): void {
  if (!condition) fail(message);
}

function assertEqual<T>(actual: T, expected: T, message: string): void {
  if (actual !== expected) {
    fail(`${message}. Expected: ${String(expected)}, actual: ${String(actual)}`);
  }
}

function assertDeepEqual(actual: unknown, expected: unknown, message: string): void {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    fail(`${message}. Expected: ${expectedJson}, actual: ${actualJson}`);
  }
}

function assertThrows(run: () => void, pattern: RegExp, message: string): void {
  try {
    run();
  } catch (error) {
    const text = error instanceof Error ? error.message : String(error);
    if (!pattern.test(text)) {
      fail(`${message}. Error did not match pattern ${pattern.toString()}. Got: ${text}`);
    }
    return;
  }
  fail(`${message}. Expected function to throw.`);
}

function sleepMs(milliseconds: number): void {
  if (typeof Utilities !== "undefined" && typeof Utilities.sleep === "function") {
    Utilities.sleep(milliseconds);
    return;
  }

  const start = Date.now();
  while (Date.now() - start < milliseconds) {
    // busy wait for non-GAS fallback
  }
}

interface TestItem extends Entity {
  name: string;
  price: number;
  category: string;
}

const queryBuilderItems: TestItem[] = [
  { __id: "1", name: "Apple", price: 1.5, category: "fruit" },
  { __id: "2", name: "Banana", price: 0.8, category: "fruit" },
  { __id: "3", name: "Carrot", price: 1.2, category: "vegetable" },
  { __id: "4", name: "Donut", price: 2.5, category: "pastry" },
  { __id: "5", name: "Eggplant", price: 3.0, category: "vegetable" },
];

function createBuilder(): QueryBuilder<TestItem> {
  return new QueryBuilder(() => [...queryBuilderItems]);
}

interface TestUser extends Entity {
  name: string;
  age: number;
  active: boolean;
  city: string;
}

const queryEngineUsers: TestUser[] = [
  { __id: "1", name: "Anna", age: 28, active: true, city: "Warszawa" },
  { __id: "2", name: "Jan", age: 35, active: true, city: "Kraków" },
  { __id: "3", name: "Piotr", age: 45, active: false, city: "Warszawa" },
  { __id: "4", name: "Maria", age: 22, active: true, city: "Gdańsk" },
  { __id: "5", name: "Zofia", age: 60, active: false, city: "Kraków" },
];

interface RepoUser extends Entity {
  name: string;
  email: string;
  age: number;
  active: boolean;
}

function createUserSchema(tableName: string): TableSchema {
  return {
    tableName,
    fields: [
      { name: "name", type: "string", required: true },
      { name: "email", type: "string", required: true },
      { name: "age", type: "number" },
      { name: "active", type: "boolean", defaultValue: true },
    ],
    indexes: [{ field: "email", unique: true }],
  };
}

function createRepo(ctx: RuntimeCaseContext): SheetRepository<RepoUser> {
  const adapter = ctx.state.getAdapter();
  const tableName = ctx.state.nextTableName("Users");
  const schema = createUserSchema(tableName);
  const cache = new MemoryCache();
  const indexStore = new IndexStore(adapter, cache);
  const migrator = new SchemaMigrator(adapter, indexStore);
  migrator.initialize(schema);
  return new SheetRepository<RepoUser>(adapter, schema, indexStore, cache);
}

function createSheetOrmProductSchema(tableName: string): TableSchema {
  return {
    tableName,
    fields: [
      { name: "name", type: "string", required: true },
      { name: "price", type: "number", required: true },
      { name: "category", type: "string" },
    ],
    indexes: [{ field: "category" }],
  };
}

const runtimeSuiteHandlers: RuntimeSuiteHandlers = {
  "cache.test.ts": {
    "stores and retrieves values": () => {
      const cache = new MemoryCache(1000);
      cache.set("key1", "value1");
      assertEqual(cache.get<string>("key1"), "value1", "cache should return stored value");
    },
    "returns null for missing keys": () => {
      const cache = new MemoryCache(1000);
      assertEqual(cache.get("nonexistent"), null, "cache miss should return null");
    },
    "has() returns true for existing keys": () => {
      const cache = new MemoryCache(1000);
      cache.set("key1", 42);
      assertTrue(cache.has("key1"), "cache should have key1");
      assertTrue(!cache.has("nonexistent"), "cache should not have nonexistent key");
    },
    "delete() removes a key": () => {
      const cache = new MemoryCache(1000);
      cache.set("key1", "val");
      cache.delete("key1");
      assertEqual(cache.get("key1"), null, "deleted key should be missing");
    },
    "clear() removes all keys": () => {
      const cache = new MemoryCache(1000);
      cache.set("a", 1);
      cache.set("b", 2);
      cache.clear();
      assertTrue(!cache.has("a"), "cache should not have key a after clear");
      assertTrue(!cache.has("b"), "cache should not have key b after clear");
    },
    "expires entries after TTL": () => {
      const cache = new MemoryCache(40);
      cache.set("key1", "value1");
      assertEqual(cache.get<string>("key1"), "value1", "value should exist before TTL");
      sleepMs(70);
      assertEqual(cache.get<string>("key1"), null, "value should expire after TTL");
    },
    "allows per-key TTL override": () => {
      const cache = new MemoryCache(1000);
      cache.set("short", "val", 30);
      cache.set("long", "val", 500);
      sleepMs(60);
      assertEqual(cache.get("short"), null, "short TTL key should expire first");
      assertEqual(cache.get<string>("long"), "val", "long TTL key should still exist");
    },
    "stores complex objects": () => {
      const cache = new MemoryCache(1000);
      const obj = { name: "test", items: [1, 2, 3] };
      cache.set("obj", obj);
      assertDeepEqual(cache.get("obj"), obj, "cache should preserve complex object");
    },
  },
  "index-store.test.ts": {
    "creates an index sheet": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "email", { unique: true });
      assertTrue(adapter.getSheetNames().includes(`_idx_${table}_email`), "index sheet should be created");
    },
    "adds and looks up entries": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "email");
      indexStore.registerIndex(table, "email", false);
      indexStore.add(table, "email", "jan@example.com", "user-001");
      indexStore.add(table, "email", "anna@example.com", "user-002");
      assertDeepEqual(
        indexStore.lookup(table, "email", "jan@example.com"),
        ["user-001"],
        "lookup should return matching entity id",
      );
    },
    "enforces unique index": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "email", { unique: true });
      indexStore.registerIndex(table, "email", true);
      indexStore.add(table, "email", "jan@example.com", "user-001");
      assertThrows(
        () => indexStore.add(table, "email", "jan@example.com", "user-002"),
        /Unique index violation/,
        "unique index should reject duplicated values for different entities",
      );
    },
    "allows same entity to re-index with same value (unique)": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "email", { unique: true });
      indexStore.registerIndex(table, "email", true);
      indexStore.add(table, "email", "jan@example.com", "user-001");
      indexStore.add(table, "email", "jan@example.com", "user-001");
      assertDeepEqual(
        indexStore.lookup(table, "email", "jan@example.com"),
        ["user-001"],
        "same entity/value reindex should stay valid",
      );
    },
    "removes entries": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "email");
      indexStore.registerIndex(table, "email", false);
      indexStore.add(table, "email", "jan@example.com", "user-001");
      indexStore.remove(table, "email", "jan@example.com", "user-001");
      assertDeepEqual(
        indexStore.lookup(table, "email", "jan@example.com"),
        [],
        "removed index entry should not be found",
      );
    },
    "removes all entries for an entity": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "email");
      indexStore.createIndex(table, "name");
      indexStore.registerIndex(table, "email", false);
      indexStore.registerIndex(table, "name", false);
      indexStore.add(table, "email", "jan@example.com", "user-001");
      indexStore.add(table, "name", "Jan", "user-001");
      indexStore.removeAllForEntity(table, "user-001");
      assertDeepEqual(
        indexStore.lookup(table, "email", "jan@example.com"),
        [],
        "email index entries should be removed for entity",
      );
      assertDeepEqual(
        indexStore.lookup(table, "name", "Jan"),
        [],
        "name index entries should be removed for entity",
      );
    },
    "updates entries when value changes": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "email");
      indexStore.registerIndex(table, "email", false);
      indexStore.add(table, "email", "old@example.com", "user-001");
      indexStore.updateForEntity(
        table,
        "user-001",
        { email: "old@example.com" },
        { email: "new@example.com" },
      );
      assertDeepEqual(
        indexStore.lookup(table, "email", "old@example.com"),
        [],
        "old value should be removed from index",
      );
      assertDeepEqual(
        indexStore.lookup(table, "email", "new@example.com"),
        ["user-001"],
        "new value should be indexed",
      );
    },
    "rebuilds index from entity data": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "name");
      indexStore.registerIndex(table, "name", false);
      indexStore.add(table, "name", "stale-data", "user-xxx");
      indexStore.rebuild(table, "name", [
        { id: "user-001", value: "Jan" },
        { id: "user-002", value: "Anna" },
      ]);
      assertDeepEqual(
        indexStore.lookup(table, "name", "stale-data"),
        [],
        "stale index rows should be removed by rebuild",
      );
      assertDeepEqual(indexStore.lookup(table, "name", "Jan"), ["user-001"], "rebuild should add Jan");
      assertDeepEqual(indexStore.lookup(table, "name", "Anna"), ["user-002"], "rebuild should add Anna");
    },
    "drops an index": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.createIndex(table, "email");
      indexStore.dropIndex(table, "email");
      assertTrue(
        !adapter.getSheetNames().includes(`_idx_${table}_email`),
        "dropped index sheet should be removed",
      );
    },
    "exists() checks for index sheet": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      assertTrue(!indexStore.exists(table, "email"), "index should not exist before create");
      indexStore.createIndex(table, "email");
      assertTrue(indexStore.exists(table, "email"), "index should exist after create");
    },
    "getIndexedFields() returns registered fields": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const table = ctx.state.nextTableName("Users");
      indexStore.registerIndex(table, "email", true);
      indexStore.registerIndex(table, "name", false);
      const fields = indexStore.getIndexedFields(table);
      assertEqual(fields.length, 2, "there should be two registered indexed fields");
      assertDeepEqual(
        fields.map((f) => f.field).sort(),
        ["email", "name"],
        "indexed field names should match",
      );
    },
  },
  "query-builder.test.ts": {
    "filters with where()": () => {
      const result = createBuilder().where("category", "=", "fruit").execute();
      assertEqual(result.length, 2, "where should filter by category");
    },
    "chains multiple where() as AND": () => {
      const result = createBuilder().where("category", "=", "fruit").and("price", ">", 1).execute();
      assertEqual(result.length, 1, "where+and should behave as AND");
      assertEqual(result[0].name, "Apple", "only Apple should match");
    },
    "sorts results": () => {
      const result = createBuilder().orderBy("price", "desc").execute();
      assertEqual(result[0].name, "Eggplant", "highest price should be first");
      assertEqual(result[4].name, "Banana", "lowest price should be last");
    },
    "limits results": () => {
      const result = createBuilder().orderBy("price", "asc").limit(2).execute();
      assertEqual(result.length, 2, "limit should cut result set");
      assertEqual(result[0].name, "Banana", "first sorted item should be Banana");
    },
    "applies offset": () => {
      const result = createBuilder().orderBy("price", "asc").offset(2).limit(2).execute();
      assertEqual(result.length, 2, "offset+limit should produce 2 items");
      assertEqual(result[0].name, "Apple", "offset should skip first two items");
    },
    "first() returns the first match": () => {
      const result = createBuilder().where("category", "=", "vegetable").orderBy("price", "asc").first();
      assertTrue(result !== null, "first should return an entity when match exists");
      assertEqual(result?.name, "Carrot", "first vegetable by price should be Carrot");
    },
    "first() returns null when no match": () => {
      const result = createBuilder().where("category", "=", "nonexistent").first();
      assertEqual(result, null, "first should return null for empty result");
    },
    "count() returns matching count": () => {
      const count = createBuilder().where("category", "=", "fruit").count();
      assertEqual(count, 2, "count should return number of matching rows");
    },
    "select() returns paginated result": () => {
      const result = createBuilder().where("category", "=", "vegetable").select(0, 10);
      assertEqual(result.total, 2, "pagination total should match filtered set");
      assertEqual(result.items.length, 2, "items length should match filtered set");
      assertTrue(!result.hasNext, "hasNext should be false for complete page");
    },
    "groupBy() groups results": () => {
      const groups = createBuilder().groupBy("category");
      assertEqual(groups.length, 3, "groupBy should produce 3 groups");
      const fruit = groups.find((g) => g.key === "fruit");
      assertTrue(Boolean(fruit), "fruit group should exist");
      assertEqual(fruit?.count, 2, "fruit group count should be 2");
    },
    "build() returns query options": () => {
      const query = createBuilder()
        .where("name", "startsWith", "A")
        .orderBy("price", "asc")
        .limit(5)
        .offset(0)
        .build();
      assertEqual(query.where?.length, 1, "build should include one where clause");
      assertEqual(query.orderBy?.length, 1, "build should include one orderBy clause");
      assertEqual(query.limit, 5, "build should preserve limit");
      assertEqual(query.offset, 0, "build should preserve offset");
    },
  },
  "query-engine.test.ts": {
    "filters with = operator": () => {
      const filters: Filter[] = [{ field: "city", operator: "=", value: "Kraków" }];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "equals filter should match two users");
      assertDeepEqual(
        result.map((u) => u.name),
        ["Jan", "Zofia"],
        "matching users should be Jan and Zofia",
      );
    },
    "filters with != operator": () => {
      const filters: Filter[] = [{ field: "active", operator: "!=", value: false }];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 3, "not-equal filter should match three users");
    },
    "filters with > operator": () => {
      const filters: Filter[] = [{ field: "age", operator: ">", value: 40 }];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "greater-than filter should match two users");
    },
    "filters with < operator": () => {
      const filters: Filter[] = [{ field: "age", operator: "<", value: 30 }];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "less-than filter should match two users");
    },
    "filters with >= and <= operators": () => {
      const filters: Filter[] = [
        { field: "age", operator: ">=", value: 28 },
        { field: "age", operator: "<=", value: 45 },
      ];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 3, "range filters should match three users");
    },
    "filters with contains operator": () => {
      const filters: Filter[] = [{ field: "name", operator: "contains", value: "an" }];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "contains filter should be case-insensitive and match two users");
    },
    "filters with startsWith operator": () => {
      const filters: Filter[] = [{ field: "name", operator: "startsWith", value: "A" }];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 1, "startsWith should match one user");
    },
    "filters with in operator": () => {
      const filters: Filter[] = [{ field: "city", operator: "in", value: ["Gdańsk", "Kraków"] }];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 3, "in operator should match users in both cities");
    },
    "applies multiple filters as AND": () => {
      const filters: Filter[] = [
        { field: "active", operator: "=", value: true },
        { field: "age", operator: ">", value: 25 },
      ];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "multiple filters should combine with AND");
    },
    "returns all when no filters": () => {
      assertEqual(filterEntities(queryEngineUsers, []).length, 5, "empty filters should return all users");
    },
    "sorts ascending by number": () => {
      const sorts: SortClause[] = [{ field: "age", direction: "asc" }];
      const result = sortEntities(queryEngineUsers, sorts);
      assertDeepEqual(
        result.map((u) => u.age),
        [22, 28, 35, 45, 60],
        "ascending numeric sort should match expected order",
      );
    },
    "sorts descending by number": () => {
      const sorts: SortClause[] = [{ field: "age", direction: "desc" }];
      const result = sortEntities(queryEngineUsers, sorts);
      assertDeepEqual(
        result.map((u) => u.age),
        [60, 45, 35, 28, 22],
        "descending numeric sort should match expected order",
      );
    },
    "sorts by string": () => {
      const sorts: SortClause[] = [{ field: "name", direction: "asc" }];
      const result = sortEntities(queryEngineUsers, sorts);
      assertDeepEqual(
        result.map((u) => u.name),
        ["Anna", "Jan", "Maria", "Piotr", "Zofia"],
        "string sort should match expected order",
      );
    },
    "sorts by multiple fields": () => {
      const sorts: SortClause[] = [
        { field: "city", direction: "asc" },
        { field: "age", direction: "desc" },
      ];
      const result = sortEntities(queryEngineUsers, sorts);
      assertDeepEqual(
        result.map((u) => u.name),
        ["Maria", "Zofia", "Jan", "Piotr", "Anna"],
        "multi-sort order should match expected",
      );
    },
    "does not mutate original array": () => {
      const original = [...queryEngineUsers];
      sortEntities(queryEngineUsers, [{ field: "age", direction: "asc" }]);
      assertDeepEqual(queryEngineUsers, original, "sortEntities should not mutate input array");
    },
    "returns first page": () => {
      const result = paginateEntities(queryEngineUsers, 0, 2);
      assertEqual(result.items.length, 2, "first page should contain two items");
      assertEqual(result.total, 5, "total should be full collection size");
      assertEqual(result.offset, 0, "offset should match input");
      assertEqual(result.limit, 2, "limit should match input");
      assertTrue(result.hasNext, "first page should have next page");
    },
    "returns last page": () => {
      const result = paginateEntities(queryEngineUsers, 4, 2);
      assertEqual(result.items.length, 1, "last page should contain single item");
      assertTrue(!result.hasNext, "last page should not have next page");
    },
    "returns empty if offset exceeds total": () => {
      const result = paginateEntities(queryEngineUsers, 10, 2);
      assertEqual(result.items.length, 0, "offset beyond total should return empty page");
      assertTrue(!result.hasNext, "empty out-of-range page should not have next");
    },
    "groups by field": () => {
      const groups = groupEntities(queryEngineUsers, "city");
      assertEqual(groups.length, 3, "city grouping should produce three groups");
      const waw = groups.find((g) => g.key === "Warszawa");
      assertEqual(waw?.count, 2, "Warszawa group should contain two users");
    },
    "groups by boolean": () => {
      const groups = groupEntities(queryEngineUsers, "active");
      assertEqual(groups.length, 2, "boolean grouping should produce two groups");
      const active = groups.find((g) => g.key === true);
      assertEqual(active?.count, 3, "active=true group should contain three users");
    },
    "combines filter + sort + pagination": () => {
      const options: QueryOptions = {
        where: [{ field: "active", operator: "=", value: true }],
        orderBy: [{ field: "age", direction: "asc" }],
        offset: 1,
        limit: 1,
      };
      const result = executeQuery(queryEngineUsers, options);
      assertEqual(result.length, 1, "combined query should return one entity");
      assertEqual(result[0].name, "Anna", "combined query should return Anna");
    },
  },
  "repository.test.ts": {
    "creates a new entity with auto-generated ID": (ctx) => {
      const repo = createRepo(ctx);
      const user = repo.save({ name: "Jan", email: "jan@test.com", age: 30 } as Partial<RepoUser>);
      assertTrue(Boolean(user.__id), "save should generate id");
      assertTrue(Boolean(user.__createdAt), "save should set created timestamp");
      assertEqual(user.name, "Jan", "saved name should match");
      assertEqual(user.active, true, "default active should be true");
    },
    "retrieves by ID": (ctx) => {
      const repo = createRepo(ctx);
      const user = repo.save({ name: "Anna", email: "anna@test.com", age: 28 } as Partial<RepoUser>);
      const found = repo.findById(user.__id);
      assertTrue(found !== null, "findById should return entity");
      assertEqual(found?.name, "Anna", "findById should return matching user");
    },
    "updates an existing entity": (ctx) => {
      const repo = createRepo(ctx);
      const user = repo.save({ name: "Jan", email: "jan@test.com", age: 30 } as Partial<RepoUser>);
      const updated = repo.save({
        __id: user.__id,
        name: "Jan Updated",
        email: "jan@test.com",
        age: 31,
      } as Partial<RepoUser> & { __id: string });
      assertEqual(updated.__id, user.__id, "update should preserve entity id");
      assertEqual(updated.name, "Jan Updated", "update should modify name");
      assertEqual(updated.age, 31, "update should modify age");
      assertEqual(updated.__createdAt, user.__createdAt, "update should preserve createdAt");
    },
    "throws on missing required field": (ctx) => {
      const repo = createRepo(ctx);
      assertThrows(
        () => repo.save({ name: "Jan" } as Partial<RepoUser>),
        /Required field "email"/,
        "save should fail when required field is missing",
      );
    },
    "finds all entities": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<RepoUser>);
      repo.save({ name: "C", email: "c@test.com", age: 40 } as Partial<RepoUser>);
      assertEqual(repo.find().length, 3, "find without options should return all entities");
    },
    "find with filter": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "Young", email: "y@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "Old", email: "o@test.com", age: 50 } as Partial<RepoUser>);
      const result = repo.find({ where: [{ field: "age", operator: ">", value: 30 }] });
      assertEqual(result.length, 1, "find with filter should return one entity");
      assertEqual(result[0].name, "Old", "matching entity should be Old");
    },
    "findOne returns first match": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<RepoUser>);
      const one = repo.findOne({ where: [{ field: "name", operator: "=", value: "B" }] });
      assertTrue(one !== null, "findOne should return entity when match exists");
      assertEqual(one?.name, "B", "findOne should return the matching entity");
    },
    "findOne returns null when no match": (ctx) => {
      const repo = createRepo(ctx);
      const one = repo.findOne({ where: [{ field: "name", operator: "=", value: "Nobody" }] });
      assertEqual(one, null, "findOne should return null when no entities match");
    },
    "deletes by ID": (ctx) => {
      const repo = createRepo(ctx);
      const user = repo.save({ name: "Del", email: "del@test.com", age: 30 } as Partial<RepoUser>);
      const result = repo.delete(user.__id);
      assertEqual(result, true, "delete should return true for existing id");
      assertEqual(repo.findById(user.__id), null, "deleted entity should not be found");
    },
    "returns false for non-existent ID": (ctx) => {
      const repo = createRepo(ctx);
      assertEqual(repo.delete("non-existent"), false, "delete should return false for missing id");
    },
    "deleteAll removes matching entities": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "B", email: "b@test.com", age: 50 } as Partial<RepoUser>);
      repo.save({ name: "C", email: "c@test.com", age: 60 } as Partial<RepoUser>);
      const count = repo.deleteAll({ where: [{ field: "age", operator: ">", value: 30 }] });
      assertEqual(count, 2, "deleteAll should remove two matching entities");
      assertEqual(repo.count(), 1, "one entity should remain after deleteAll");
    },
    "counts all entities": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<RepoUser>);
      assertEqual(repo.count(), 2, "count without filters should return all entities");
    },
    "counts with filter": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<RepoUser>);
      assertEqual(
        repo.count({ where: [{ field: "age", operator: ">", value: 25 }] }),
        1,
        "count with filter should return matching count",
      );
    },
    "select returns paginated result": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<RepoUser>);
      repo.save({ name: "C", email: "c@test.com", age: 40 } as Partial<RepoUser>);
      const page = repo.select(0, 2);
      assertEqual(page.items.length, 2, "select should return two entities for limit=2");
      assertEqual(page.total, 3, "select total should be 3");
      assertEqual(page.hasNext, true, "select should indicate next page");
    },
    "returns a QueryBuilder that works": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<RepoUser>);
      repo.save({ name: "C", email: "c@test.com", age: 40 } as Partial<RepoUser>);
      const result = repo.query().where("age", ">=", 30).orderBy("age", "desc").execute();
      assertEqual(result.length, 2, "query builder should return two entities");
      assertEqual(result[0].name, "C", "first query result should be C");
      assertEqual(result[1].name, "B", "second query result should be B");
    },
    "groups entities by field": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "A", email: "a@test.com", age: 20, active: true } as Partial<RepoUser>);
      repo.save({ name: "B", email: "b@test.com", age: 30, active: false } as Partial<RepoUser>);
      repo.save({ name: "C", email: "c@test.com", age: 40, active: true } as Partial<RepoUser>);
      const groups = repo.groupBy("active");
      assertEqual(groups.length, 2, "groupBy should return two groups");
    },
    "calls beforeSave and afterSave": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("Users");
      const schema = createUserSchema(tableName);
      const cache = new MemoryCache();
      const indexStore = new IndexStore(adapter, cache);
      const migrator = new SchemaMigrator(adapter, indexStore);
      migrator.initialize(schema);
      const beforeCalls: boolean[] = [];
      const afterCalls: boolean[] = [];
      const repo = new SheetRepository<RepoUser>(adapter, schema, indexStore, cache, {
        beforeSave: (_entity, isNew) => {
          beforeCalls.push(isNew);
        },
        afterSave: (_entity, isNew) => {
          afterCalls.push(isNew);
        },
      });
      repo.save({ name: "Hook", email: "hook@test.com", age: 25 } as Partial<RepoUser>);
      assertDeepEqual(beforeCalls, [true], "beforeSave should be called with isNew=true");
      assertDeepEqual(afterCalls, [true], "afterSave should be called with isNew=true");
    },
    "calls onValidate and rejects on errors": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("Users");
      const schema = createUserSchema(tableName);
      const cache = new MemoryCache();
      const indexStore = new IndexStore(adapter, cache);
      const migrator = new SchemaMigrator(adapter, indexStore);
      migrator.initialize(schema);
      const repo = new SheetRepository<RepoUser>(adapter, schema, indexStore, cache, {
        onValidate: (entity) => {
          if (entity.age !== undefined && Number(entity.age) < 18) return ["Must be 18+"];
          return undefined;
        },
      });
      assertThrows(
        () => repo.save({ name: "Kid", email: "kid@test.com", age: 10 } as Partial<RepoUser>),
        /Must be 18/,
        "onValidate errors should reject save",
      );
    },
    "calls beforeDelete and can cancel": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("Users");
      const schema = createUserSchema(tableName);
      const cache = new MemoryCache();
      const indexStore = new IndexStore(adapter, cache);
      const migrator = new SchemaMigrator(adapter, indexStore);
      migrator.initialize(schema);
      const repo = new SheetRepository<RepoUser>(adapter, schema, indexStore, cache, {
        beforeDelete: () => false,
      });
      const user = repo.save({ name: "Protected", email: "p@test.com", age: 30 } as Partial<RepoUser>);
      assertEqual(repo.delete(user.__id), false, "beforeDelete=false should cancel delete");
      assertTrue(repo.findById(user.__id) !== null, "entity should still exist when delete canceled");
    },
    "buffers and commits": (ctx) => {
      const repo = createRepo(ctx);
      repo.beginBatch();
      repo.save({ name: "Batch1", email: "b1@test.com", age: 20 } as Partial<RepoUser>);
      repo.save({ name: "Batch2", email: "b2@test.com", age: 30 } as Partial<RepoUser>);
      assertEqual(repo.isBatchActive(), true, "batch should be active before commit");
      repo.commitBatch();
      assertEqual(repo.isBatchActive(), false, "batch should be inactive after commit");
      assertEqual(repo.count(), 2, "commit should persist buffered saves");
    },
    "rollback discards buffered operations": (ctx) => {
      const repo = createRepo(ctx);
      repo.save({ name: "Existing", email: "e@test.com", age: 20 } as Partial<RepoUser>);
      repo.beginBatch();
      repo.save({ name: "Discarded", email: "d@test.com", age: 30 } as Partial<RepoUser>);
      repo.rollbackBatch();
      assertEqual(repo.count(), 1, "rollback should discard buffered operations");
    },
  },
  "schema-migrator.test.ts": {
    "initializes meta sheet and data sheet": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      migrator.initialize(createUserSchema(tableName));
      const names = adapter.getSheetNames();
      assertTrue(names.includes("_meta"), "_meta sheet should exist");
      assertTrue(names.includes(tableName), "data sheet should exist");
    },
    "sets headers on data sheet": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      migrator.initialize(createUserSchema(tableName));
      const sheet = adapter.getSheetByName(tableName);
      assertTrue(sheet !== null, "data sheet should exist");
      assertDeepEqual(
        sheet?.getHeaders(),
        ["__id", "__createdAt", "__updatedAt", "name", "email", "age", "active"],
        "data sheet headers should match schema + system columns",
      );
    },
    "creates indexes during initialization": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      migrator.initialize(createUserSchema(tableName));
      assertEqual(
        indexStore.exists(tableName, "email"),
        true,
        "email index should be created during initialize",
      );
    },
    "stores schema in _meta sheet": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      migrator.initialize(createUserSchema(tableName));
      const schema = migrator.getSchema(tableName);
      assertTrue(schema !== null, "schema should be stored in _meta");
      assertEqual(schema?.tableName, tableName, "stored schema should have matching tableName");
      assertEqual(schema?.fields.length, 4, "stored schema should have expected field count");
    },
    "tableExists returns correct value": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      assertEqual(migrator.tableExists(tableName), false, "tableExists should be false before initialize");
      migrator.initialize(createUserSchema(tableName));
      assertEqual(migrator.tableExists(tableName), true, "tableExists should be true after initialize");
    },
    "addField adds a column to the schema": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      migrator.initialize(createUserSchema(tableName));
      migrator.addField(tableName, { name: "phone", type: "string" });
      const schema = migrator.getSchema(tableName);
      assertEqual(schema?.fields.length, 5, "addField should increase field count");
      assertTrue(
        Boolean(schema?.fields.find((f) => f.name === "phone")),
        "new field should be present in schema",
      );
    },
    "addField is idempotent for existing fields": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      migrator.initialize(createUserSchema(tableName));
      migrator.addField(tableName, { name: "email", type: "string" });
      const schema = migrator.getSchema(tableName);
      const emailCount = schema?.fields.filter((f) => f.name === "email").length ?? 0;
      assertEqual(emailCount, 1, "existing field should not be duplicated");
    },
    "addField throws for unknown table": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      assertThrows(
        () => migrator.addField(ctx.state.nextTableName("NonExistent"), { name: "x", type: "string" }),
        /not found/,
        "addField should fail for unknown table",
      );
    },
    "removeField removes a field from schema": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      migrator.initialize(createUserSchema(tableName));
      migrator.removeField(tableName, "age");
      const schema = migrator.getSchema(tableName);
      assertTrue(
        !(schema?.fields.map((f) => f.name).includes("age") ?? false),
        "age should be removed from schema",
      );
    },
    "sync initializes if table does not exist": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      migrator.sync(createUserSchema(tableName));
      assertEqual(migrator.tableExists(tableName), true, "sync should initialize missing table");
    },
    "sync adds missing fields to existing table": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      const schema = createUserSchema(tableName);
      migrator.initialize(schema);
      const updatedSchema: TableSchema = {
        ...schema,
        fields: [...schema.fields, { name: "phone", type: "string" }],
      };
      migrator.sync(updatedSchema);
      const synced = migrator.getSchema(tableName);
      assertTrue(Boolean(synced?.fields.find((f) => f.name === "phone")), "sync should add missing field");
    },
    "sync adds missing indexes": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const migrator = new SchemaMigrator(adapter, indexStore);
      const tableName = ctx.state.nextTableName("Users");
      const schema = createUserSchema(tableName);
      migrator.initialize(schema);
      const updatedSchema: TableSchema = {
        ...schema,
        indexes: [...schema.indexes, { field: "name" }],
      };
      migrator.sync(updatedSchema);
      assertEqual(indexStore.exists(tableName, "name"), true, "sync should create missing index");
    },
  },
  "serialization.test.ts": {
    "serializes string": () => {
      const fd: FieldDefinition = { name: "x", type: "string" };
      assertEqual(serializeValue("hello", fd), "hello", "string serialization should preserve string value");
      assertEqual(serializeValue(123, fd), "123", "string serialization should coerce number to string");
      assertEqual(serializeValue(null, fd), "", "string serialization should map null to empty string");
    },
    "serializes number": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(serializeValue(42, fd), 42, "number serialization should preserve number");
      assertEqual(serializeValue("7", fd), 7, "number serialization should coerce numeric string");
    },
    "serializes boolean": () => {
      const fd: FieldDefinition = { name: "x", type: "boolean" };
      assertEqual(serializeValue(true, fd), true, "boolean serialization should preserve boolean");
      assertEqual(serializeValue("true", fd), true, "boolean serialization should parse true string");
      assertEqual(serializeValue("false", fd), false, "boolean serialization should parse false string");
    },
    "serializes json": () => {
      const fd: FieldDefinition = { name: "x", type: "json" };
      assertEqual(serializeValue({ a: 1 }, fd), '{"a":1}', "json serialization should stringify object");
      assertEqual(
        serializeValue("already string", fd),
        "already string",
        "json serialization should keep string untouched",
      );
    },
    "serializes date": () => {
      const fd: FieldDefinition = { name: "x", type: "date" };
      const date = new Date("2024-01-15T10:00:00.000Z");
      assertEqual(
        serializeValue(date, fd),
        "2024-01-15T10:00:00.000Z",
        "date serialization should use ISO format",
      );
    },
    "serializes reference": () => {
      const fd: FieldDefinition = { name: "x", type: "reference" };
      assertEqual(
        serializeValue("user-001", fd),
        "user-001",
        "reference serialization should keep id string",
      );
    },
    "deserializes string": () => {
      const fd: FieldDefinition = { name: "x", type: "string" };
      assertEqual(deserializeValue("hello", fd), "hello", "string deserialization should preserve text");
      assertEqual(deserializeValue("", fd), null, "empty string should deserialize to null without default");
    },
    "applies defaultValue when empty": () => {
      const fd: FieldDefinition = { name: "x", type: "string", defaultValue: "default" };
      assertEqual(deserializeValue("", fd), "default", "empty value should use defaultValue");
    },
    "deserializes number": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(deserializeValue(42, fd), 42, "number deserialization should preserve number");
      assertEqual(deserializeValue("3.14", fd), 3.14, "number deserialization should parse decimal string");
      assertEqual(deserializeValue("abc", fd), null, "invalid number should deserialize to null");
    },
    "deserializes boolean": () => {
      const fd: FieldDefinition = { name: "x", type: "boolean" };
      assertEqual(deserializeValue(true, fd), true, "boolean deserialization should preserve boolean");
      assertEqual(deserializeValue("true", fd), true, "boolean deserialization should parse true string");
      assertEqual(deserializeValue("false", fd), false, "boolean deserialization should parse false string");
    },
    "deserializes json": () => {
      const fd: FieldDefinition = { name: "x", type: "json" };
      assertDeepEqual(
        deserializeValue('{"a":1}', fd),
        { a: 1 },
        "json deserialization should parse valid json",
      );
      assertEqual(deserializeValue("invalid json", fd), null, "invalid json should deserialize to null");
    },
    "prepends system columns": () => {
      const fields: FieldDefinition[] = [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
      ];
      assertDeepEqual(
        buildHeaders(fields),
        ["__id", "__createdAt", "__updatedAt", "name", "age"],
        "buildHeaders should prepend system columns",
      );
    },
    "round-trips an entity": () => {
      const fields: FieldDefinition[] = [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
        { name: "active", type: "boolean" },
      ];
      const headers = buildHeaders(fields);
      const entity: Entity = {
        __id: "id-1",
        __createdAt: "2024-01-01T00:00:00.000Z",
        __updatedAt: "2024-01-02T00:00:00.000Z",
        name: "Jan",
        age: 30,
        active: true,
      };
      const row = entityToRow(entity, fields, headers);
      assertDeepEqual(
        row,
        ["id-1", "2024-01-01T00:00:00.000Z", "2024-01-02T00:00:00.000Z", "Jan", 30, true],
        "entityToRow should serialize in header order",
      );
      const restored = rowToEntity<Entity>(row, headers, fields);
      assertEqual(restored.__id, "id-1", "rowToEntity should restore __id");
      assertEqual(restored.name, "Jan", "rowToEntity should restore string field");
      assertEqual(restored.age, 30, "rowToEntity should restore number field");
      assertEqual(restored.active, true, "rowToEntity should restore boolean field");
    },
    "handles missing optional fields": () => {
      const fields: FieldDefinition[] = [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
        { name: "active", type: "boolean" },
      ];
      const headers = buildHeaders(fields);
      const entity: Entity = {
        __id: "id-2",
        name: "Anna",
        age: 25,
        active: false,
      };
      const row = entityToRow(entity, fields, headers);
      assertEqual(row[1], "", "missing __createdAt should serialize to empty string");
      assertEqual(row[2], "", "missing __updatedAt should serialize to empty string");
      const restored = rowToEntity<Entity>(row, headers, fields);
      assertEqual(restored.__createdAt, undefined, "missing __createdAt should restore as undefined");
    },
  },
  "sheetorm.test.ts": {
    "registers a schema and creates sheets": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const orm = new SheetORM({ adapter, cache: new MemoryCache() });
      const tableName = ctx.state.nextTableName("Products");
      const schema = createSheetOrmProductSchema(tableName);
      orm.register(schema);
      const names = adapter.getSheetNames();
      assertTrue(names.includes(tableName), "register should create data sheet");
      assertTrue(names.includes("_meta"), "register should ensure _meta exists");
    },
    "getRepository returns a working repo": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const orm = new SheetORM({ adapter, cache: new MemoryCache() });
      const tableName = ctx.state.nextTableName("Products");
      orm.register(createSheetOrmProductSchema(tableName));
      const repo = orm.getRepository<Entity>(tableName);
      const saved = repo.save({ name: "Widget", price: 9.99, category: "tools" } as Partial<Entity>);
      assertTrue(Boolean(saved.__id), "saved product should have id");
      const found = repo.findById(saved.__id);
      assertTrue(found !== null, "saved product should be found by id");
      assertEqual(found?.name, "Widget", "found product should match saved data");
    },
    "getRepository caches instances": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const orm = new SheetORM({ adapter, cache: new MemoryCache() });
      const tableName = ctx.state.nextTableName("Products");
      orm.register(createSheetOrmProductSchema(tableName));
      const repo1 = orm.getRepository<Entity>(tableName);
      const repo2 = orm.getRepository<Entity>(tableName);
      assertTrue(repo1 === repo2, "getRepository should return cached instance for same table");
    },
    "throws when getting repo for unregistered table": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const orm = new SheetORM({ adapter, cache: new MemoryCache() });
      assertThrows(
        () => orm.getRepository(ctx.state.nextTableName("Unknown")),
        /not registered/,
        "getRepository should throw for unregistered table",
      );
    },
    "static create() works": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const instance = SheetORM.create({ adapter });
      assertTrue(instance instanceof SheetORM, "SheetORM.create should return instance");
    },
    "clearCache() clears the cache": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const orm = new SheetORM({ adapter, cache: new MemoryCache() });
      const tableName = ctx.state.nextTableName("Products");
      orm.register(createSheetOrmProductSchema(tableName));
      const repo = orm.getRepository<Entity>(tableName);
      repo.save({ name: "A", price: 1, category: "x" } as Partial<Entity>);
      orm.clearCache();
      assertTrue(true, "clearCache should complete without throwing");
    },
    "getMigrator() returns the migrator": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const orm = new SheetORM({ adapter, cache: new MemoryCache() });
      assertTrue(Boolean(orm.getMigrator()), "getMigrator should return migrator instance");
    },
    "getIndexStore() returns the index store": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const orm = new SheetORM({ adapter, cache: new MemoryCache() });
      assertTrue(Boolean(orm.getIndexStore()), "getIndexStore should return index store instance");
    },
    "full workflow: register → save → query → delete": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const orm = new SheetORM({ adapter, cache: new MemoryCache() });
      const tableName = ctx.state.nextTableName("Products");
      orm.register(createSheetOrmProductSchema(tableName));
      const repo = orm.getRepository<Entity>(tableName);

      repo.save({ name: "Apple", price: 1.5, category: "fruit" } as Partial<Entity>);
      repo.save({ name: "Banana", price: 0.8, category: "fruit" } as Partial<Entity>);
      repo.save({ name: "Hammer", price: 15.0, category: "tools" } as Partial<Entity>);

      const fruits = repo.query().where("category", "=", "fruit").orderBy("price", "asc").execute();
      assertEqual(fruits.length, 2, "query should return two fruits");
      assertEqual(fruits[0].name, "Banana", "fruits should be sorted by price asc");

      repo.delete(fruits[0].__id);
      assertEqual(repo.count(), 2, "delete should reduce count to 2");

      const page = repo.select(0, 1);
      assertEqual(page.items.length, 1, "pagination should return one item with limit=1");
      assertEqual(page.total, 2, "pagination total should match remaining entities");
    },
  },
  "uuid.test.ts": {
    "returns a string of UUID v4 format": () => {
      const uuid = generateUUID();
      assertTrue(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(uuid),
        "generated UUID should match v4 format",
      );
    },
    "generates unique values": () => {
      const uuids = new Set(Array.from({ length: 100 }, () => generateUUID()));
      assertEqual(uuids.size, 100, "100 generated UUIDs should be unique");
    },
  },
  "record.test.ts": (() => {
    // Build Record subclasses inside a factory to avoid top-level side-effects
    function createRecordClasses(adapter: GoogleSpreadsheetAdapter, suffix: string) {
      SheetORM.reset();
      SheetORM.initialize({ adapter });

      class Car extends BaseRecord {
        static tableName = `Cars_${suffix}`;
        static fields: FieldDefinition[] = [
          { name: "make", type: "string", required: true },
          { name: "model", type: "string", required: true },
          { name: "year", type: "number" },
          { name: "color", type: "string" },
        ];
        static indexes = [{ field: "make" }];
        declare make: string;
        declare model: string;
        declare year: number;
        declare color: string;
      }

      class Product extends BaseRecord {
        static tableName = `Products_${suffix}`;
        static fields: FieldDefinition[] = [
          { name: "name", type: "string", required: true },
          { name: "price", type: "number", required: true },
          { name: "category", type: "string" },
        ];
        static indexes = [{ field: "category" }];
        declare name: string;
        declare price: number;
        declare category: string;
      }

      return { Car, Product };
    }

    function setup(ctx: RuntimeCaseContext) {
      const adapter = ctx.state.getAdapter();
      const suffix = ctx.state.nextTableName("rec");
      return createRecordClasses(adapter, suffix);
    }

    return {
      "creates a new entity with auto-generated ID": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.year = 2024;
        car.save();
        assertTrue(Boolean(car.__id), "car should have __id after save");
        assertTrue(Boolean(car.__createdAt), "car should have __createdAt");
        assertTrue(Boolean(car.__updatedAt), "car should have __updatedAt");
        assertEqual(car.make, "Toyota", "make should be Toyota");
      },
      "auto-creates the table on first save": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Honda";
        car.model = "Civic";
        car.save();
        assertTrue(Boolean(car.__id), "table should be auto-created on save");
      },
      "updates an existing entity": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.year = 2024;
        car.color = "blue";
        car.save();
        car.color = "red";
        car.save();
        assertEqual(car.color, "red", "color should be updated to red");
      },
      "returns this for chaining": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        const result = car.save();
        assertTrue(result === car, "save() should return this");
      },
      "persists update via findById round-trip": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.color = "blue";
        car.save();
        const loaded = Car.findById(car.__id);
        assertTrue(loaded !== null, "should find car by id");
        assertEqual(loaded!.color, "blue", "loaded color should be blue");
        loaded!.color = "red";
        loaded!.save();
        const reloaded = Car.findById(car.__id);
        assertEqual(reloaded!.color, "red", "reloaded color should be red after update");
      },
      "throws on missing required field": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.color = "blue";
        assertThrows(() => car.save(), /required/i, "should throw for missing required field");
      },
      "sets a field value and returns this": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        const result = car.set("make", "BMW");
        assertTrue(result === car, "set() should return this");
        assertEqual(car.make, "BMW", "field should be set");
      },
      "supports chaining set calls": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.set("make", "BMW").set("model", "M3").set("year", 2024);
        assertEqual(car.make, "BMW", "make should be BMW");
        assertEqual(car.model, "M3", "model should be M3");
        assertEqual(car.year, 2024, "year should be 2024");
      },
      "get() retrieves a field value": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        assertEqual(car.get("make") as string, "Toyota", "get should return field value");
      },
      "deletes a saved entity": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        const id = car.__id;
        const deleted = car.delete();
        assertTrue(deleted, "delete should return true");
        const found = Car.findById(id);
        assertTrue(found === null, "deleted entity should not be found");
      },
      "returns false for unsaved entity": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        const deleted = car.delete();
        assertTrue(!deleted, "delete should return false for unsaved entity");
      },
      "returns a plain object with all fields": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.year = 2024;
        car.color = "blue";
        car.save();
        const json = car.toJSON();
        assertEqual(json.make, "Toyota", "toJSON make should match");
        assertEqual(json.model, "Corolla", "toJSON model should match");
        assertEqual(json.year, 2024, "toJSON year should match");
        assertTrue(Boolean(json.__id), "toJSON should include __id");
      },
      "initializes fields from data object": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car({ make: "BMW", model: "X5", year: 2023 });
        assertEqual(car.make, "BMW", "constructor should set make");
        assertEqual(car.model, "X5", "constructor should set model");
      },
      "finds a saved entity by ID": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        const found = Car.findById(car.__id);
        assertTrue(found !== null, "findById should return the entity");
        assertEqual(found!.make, "Toyota", "found entity should match");
      },
      "returns null for non-existent ID": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        // Ensure table exists
        const car = new Car();
        car.make = "Test";
        car.model = "Test";
        car.save();
        const found = Car.findById("non-existent-id");
        assertTrue(found === null, "findById should return null for non-existent id");
      },
      "returns all entities": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.save();
        const all = Car.find();
        assertEqual(all.length, 2, "find should return 2 entities");
      },
      "returns entities matching query": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.save();
        const toyotas = Car.find({ where: [{ field: "make", operator: "=", value: "Toyota" }] });
        assertEqual(toyotas.length, 1, "find with filter should return 1");
        assertEqual(toyotas[0].make, "Toyota", "filtered entity should be Toyota");
      },
      "returns first matching entity": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Toyota";
        c2.model = "Camry";
        c2.save();
        const found = Car.findOne({ where: [{ field: "make", operator: "=", value: "Toyota" }] });
        assertTrue(found !== null, "findOne should find a match");
        assertEqual(found!.make, "Toyota", "findOne should return Toyota");
      },
      "returns null when no match": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c = new Car();
        c.make = "Toyota";
        c.model = "Corolla";
        c.save();
        const found = Car.findOne({ where: [{ field: "make", operator: "=", value: "BMW" }] });
        assertTrue(found === null, "findOne should return null when no match");
      },
      "returns a QueryBuilder and chains": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.year = 2020;
        c1.save();
        const c2 = new Car();
        c2.make = "Toyota";
        c2.model = "Camry";
        c2.year = 2024;
        c2.save();
        const results = Car.where("make", "=", "Toyota").orderBy("year", "desc").execute();
        assertEqual(results.length, 2, "where query should return 2 results");
        assertEqual(results[0].year, 2024, "first result should be 2024");
      },
      "returns a QueryBuilder": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c = new Car();
        c.make = "Toyota";
        c.model = "Corolla";
        c.save();
        const qb = Car.query();
        const results = qb.execute();
        assertEqual(results.length, 1, "query() should return builder that executes");
      },
      "counts all entities": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        assertEqual(Car.count(), 1, "count should be 1");
      },
      "counts with filter": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.save();
        assertEqual(
          Car.count({ where: [{ field: "make", operator: "=", value: "Toyota" }] }),
          1,
          "count with filter should be 1",
        );
      },
      "deletes matching entities": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.save();
        const deleted = Car.deleteAll({ where: [{ field: "make", operator: "=", value: "Toyota" }] });
        assertEqual(deleted, 1, "deleteAll should return 1");
        assertEqual(Car.count(), 1, "count after deleteAll should be 1");
      },
      "returns paginated results": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        for (let i = 0; i < 5; i++) {
          const c = new Car();
          c.make = "Make" + i;
          c.model = "Model" + i;
          c.save();
        }
        const page = Car.select(0, 2);
        assertEqual(page.items.length, 2, "select should return 2 items");
        assertEqual(page.total, 5, "select total should be 5");
      },
      "groups entities by field": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Toyota";
        c2.model = "Camry";
        c2.save();
        const c3 = new Car();
        c3.make = "Honda";
        c3.model = "Civic";
        c3.save();
        const groups = Car.groupBy("make");
        assertTrue(groups.length >= 2, "groupBy should return at least 2 groups");
      },
      "creates separate tables for each class": (ctx: RuntimeCaseContext) => {
        const { Car, Product } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        const prod = new Product();
        prod.name = "Widget";
        prod.price = 9.99;
        prod.save();
        assertEqual(Car.count(), 1, "car count should be 1");
        assertEqual(Product.count(), 1, "product count should be 1");
      },
      "works with class reference (typed)": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c = new Car();
        c.make = "Toyota";
        c.model = "Corolla";
        c.save();
        const results = QueryBuilder.from(Car).execute();
        assertEqual(results.length, 1, "QueryBuilder.from(class) should work");
      },
      "works with string name": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c = new Car();
        c.make = "Toyota";
        c.model = "Corolla";
        c.save();
        const results = QueryBuilder.from("Car").execute();
        assertEqual(results.length, 1, "QueryBuilder.from(string) should work");
      },
      "works with table name string": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c = new Car();
        c.make = "Toyota";
        c.model = "Corolla";
        c.save();
        const results = QueryBuilder.from(Car.tableName).execute();
        assertEqual(results.length, 1, "QueryBuilder.from(tableName) should work");
      },
      "supports full fluent chain": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.year = 2020;
        c1.save();
        const c2 = new Car();
        c2.make = "Toyota";
        c2.model = "Camry";
        c2.year = 2024;
        c2.save();
        const c3 = new Car();
        c3.make = "Honda";
        c3.model = "Civic";
        c3.year = 2022;
        c3.save();
        const results = QueryBuilder.from(Car)
          .where("make", "=", "Toyota")
          .orderBy("year", "desc")
          .limit(1)
          .execute();
        assertEqual(results.length, 1, "fluent chain should return 1 result");
        assertEqual(results[0].year, 2024, "fluent chain should return newest Toyota");
      },
      "throws for unknown class name": (ctx: RuntimeCaseContext) => {
        setup(ctx);
        assertThrows(
          () => QueryBuilder.from("UnknownClassName"),
          /unknown|not found|not registered/i,
          "QueryBuilder.from with unknown name should throw",
        );
      },
      "create → query → update → delete cycle": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.year = 2024;
        car.color = "blue";
        car.save();
        const found = Car.findById(car.__id);
        assertTrue(found !== null, "should find created car");
        assertEqual(found!.color, "blue", "color should be blue");
        found!.color = "red";
        found!.save();
        const updated = Car.findById(car.__id);
        assertEqual(updated!.color, "red", "color should be updated to red");
        updated!.delete();
        const deleted = Car.findById(car.__id);
        assertTrue(deleted === null, "deleted car should not be found");
      },
      "works with QueryBuilder.from() end-to-end": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.year = 2024;
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.year = 2023;
        c2.save();
        const results = QueryBuilder.from(Car).where("make", "=", "Toyota").execute();
        assertEqual(results.length, 1, "QueryBuilder.from e2e should return 1");
        assertEqual(results[0].make, "Toyota", "result should be Toyota");
      },
    } as Record<string, RuntimeCaseHandler>;
  })(),
};

function getRuntimeCaseHandler(id: string): RuntimeCaseHandler {
  const separator = "::";
  const separatorIndex = id.indexOf(separator);
  if (separatorIndex < 0) {
    throw new Error(`Invalid parity case ID format: ${id}`);
  }
  const file = id.slice(0, separatorIndex);
  const testName = id.slice(separatorIndex + separator.length);
  const suiteHandlers = runtimeSuiteHandlers[file];
  const caseHandler = suiteHandlers?.[testName];
  if (!caseHandler) {
    throw new Error(`No runtime handler found for parity case: ${id}`);
  }
  return caseHandler;
}

export const RUNTIME_PARITY_CASE_IDS: string[] = Object.entries(runtimeSuiteHandlers)
  .flatMap(([file, testMap]) => Object.keys(testMap).map((testName) => toParityCaseId(file, testName)))
  .sort();

export function validateSheetOrmRuntimeParity(): void {
  const expected = new Set(PARITY_CASE_IDS);
  const actual = new Set(RUNTIME_PARITY_CASE_IDS);

  const missingInRuntime = PARITY_CASE_IDS.filter((id) => !actual.has(id));
  const extraInRuntime = RUNTIME_PARITY_CASE_IDS.filter((id) => !expected.has(id));

  if (missingInRuntime.length > 0 || extraInRuntime.length > 0) {
    const parts: string[] = [];
    if (missingInRuntime.length > 0) {
      parts.push(`Missing in runtime (${missingInRuntime.length}): ${missingInRuntime.join(" | ")}`);
    }
    if (extraInRuntime.length > 0) {
      parts.push(`Extra in runtime (${extraInRuntime.length}): ${extraInRuntime.join(" | ")}`);
    }
    throw new Error(`Jest/runtime parity drift detected. ${parts.join(" ; ")}`);
  }
}

export function runSheetOrmRuntimeParity(): string {
  validateSheetOrmRuntimeParity();

  const state = new RuntimeParityState();
  const results: RuntimeCaseResult[] = [];
  const total = PARITY_CASE_IDS.length;

  const log = (msg: string): void => {
    if (typeof Logger !== "undefined" && typeof Logger.log === "function") {
      Logger.log(msg);
    }
  };

  log(`[SheetORM] Starting parity suite — ${total} test cases`);

  for (const suite of PARITY_SUITES) {
    log(`[Suite] ${suite.file} (${suite.tests.length} tests)`);

    for (const testName of suite.tests) {
      const id = toParityCaseId(suite.file, testName);
      const num = results.length + 1;
      try {
        const handler = getRuntimeCaseHandler(id);
        handler({ state });
        results.push({ id, ok: true });
        log(`  PASS [${num}/${total}] ${testName}`);
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : String(error);
        results.push({ id, ok: false, error: errMsg });
        log(`  FAIL [${num}/${total}] ${testName}`);
        log(`       ${errMsg}`);
      }
    }
  }

  const failures = results.filter((result) => !result.ok);
  const passed = results.length - failures.length;

  log(`[SheetORM] Done — ${passed}/${total} passed, ${failures.length} failed`);

  if (failures.length > 0) {
    const summary = failures
      .slice(0, 10)
      .map((f) => `${f.id} => ${f.error}`)
      .join(" || ");
    throw new Error(
      `Runtime parity failed: ${failures.length}/${results.length} cases failed (${passed} passed). ${summary}`,
    );
  }

  const spreadsheetUrl = state.getSpreadsheet().getUrl();
  const report = {
    status: "ok",
    total: results.length,
    passed,
    failed: 0,
    spreadsheetUrl,
  };

  log(JSON.stringify(report));
  return JSON.stringify(report);
}
