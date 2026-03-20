import { Entity, FieldDefinition, Filter, QueryOptions, SortClause } from "../core/types";
import { Registry } from "../core/Registry";
import { IndexStore } from "../index/IndexStore";
import { Query } from "../query/Query";
import { Record as BaseRecord } from "../core/Record";
import { Indexed, Required, resetDecoratorCaches } from "../core/decorators";
import {
  executeQuery,
  filterEntities,
  groupEntities,
  paginateEntities,
  sortEntities,
} from "../query/QueryEngine";
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
  durationMs: number;
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

  clearAllSheets(log?: (msg: string) => void): void {
    const emit = log ?? (() => {});
    const spreadsheet = this.getSpreadsheet();
    const originalSheets = spreadsheet.getSheets();
    emit(`[SheetORM] Sheets found before cleanup: ${originalSheets.length}`);

    if (originalSheets.length === 0) {
      const keeper = spreadsheet.insertSheet("Sheet1");
      keeper.clear();
      emit('[SheetORM] No sheets existed. Created and prepared keeper: "Sheet1"');
      return;
    }

    // Reuse first existing sheet as keeper to avoid temporarily increasing cell count
    // (insertSheet can fail when spreadsheet is close to 10M-cell limit).
    const keeper = originalSheets[0];
    emit(`[SheetORM] Keeper sheet: "${keeper.getName()}"`);

    let remainingToDelete = originalSheets.length - 1;
    for (let i = 1; i < originalSheets.length; i += 1) {
      const sheetToDelete = originalSheets[i];
      emit(
        `[SheetORM] Deleting sheet: "${sheetToDelete.getName()}" | pozostało do usunięcia: ${remainingToDelete}`,
      );
      spreadsheet.deleteSheet(sheetToDelete);
      remainingToDelete -= 1;
      emit(`[SheetORM] Usunięto. Pozostało do usunięcia: ${remainingToDelete}`);
    }

    // Keep one clean, minimal sheet so subsequent test sheets fit under cell limits.
    keeper.clear();

    const maxRows = keeper.getMaxRows();
    if (maxRows > 1) {
      keeper.deleteRows(2, maxRows - 1);
    }

    const maxColumns = keeper.getMaxColumns();
    if (maxColumns > 1) {
      keeper.deleteColumns(2, maxColumns - 1);
    }

    if (keeper.getName() !== "Sheet1") {
      keeper.setName("Sheet1");
    }

    emit('[SheetORM] Cleanup finished. Remaining sheet: "Sheet1"');
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

const queryItems: TestItem[] = [
  { __id: "1", name: "Apple", price: 1.5, category: "fruit" },
  { __id: "2", name: "Banana", price: 0.8, category: "fruit" },
  { __id: "3", name: "Carrot", price: 1.2, category: "vegetable" },
  { __id: "4", name: "Donut", price: 2.5, category: "pastry" },
  { __id: "5", name: "Eggplant", price: 3.0, category: "vegetable" },
];

function createBuilder(): Query<TestItem> {
  return new Query(() => [...queryItems]);
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
    "creates a combined index sheet": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      assertTrue(adapter.getSheetNames().includes(indexTable), "combined index sheet should be created");
    },
    "adds and looks up entries": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);
      indexStore.addToCombined(indexTable, "email", "jan@example.com", "user-001");
      indexStore.addToCombined(indexTable, "email", "anna@example.com", "user-002");
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "jan@example.com"),
        ["user-001"],
        "lookup should return matching entity id",
      );
    },
    "enforces unique index": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", true);
      indexStore.addToCombined(indexTable, "email", "jan@example.com", "user-001");
      assertThrows(
        () => indexStore.addToCombined(indexTable, "email", "jan@example.com", "user-002"),
        /Unique index violation/,
        "unique index should reject duplicated values for different entities",
      );
    },
    "allows same entity to re-index with same value (unique)": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", true);
      indexStore.addToCombined(indexTable, "email", "jan@example.com", "user-001");
      indexStore.addToCombined(indexTable, "email", "jan@example.com", "user-001");
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "jan@example.com"),
        ["user-001"],
        "same entity/value reindex should stay valid",
      );
    },
    "removes entries when value is cleared in update": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);
      indexStore.addToCombined(indexTable, "email", "jan@example.com", "user-001");
      indexStore.updateInCombined(indexTable, "user-001", { email: "jan@example.com" }, { email: "" });
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "jan@example.com"),
        [],
        "removed index entry should not be found",
      );
    },
    "removes all entries for an entity": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);
      indexStore.registerIndex(indexTable, "name", false);
      indexStore.addToCombined(indexTable, "email", "jan@example.com", "user-001");
      indexStore.addToCombined(indexTable, "name", "Jan", "user-001");
      indexStore.removeAllFromCombined(indexTable, "user-001");
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "jan@example.com"),
        [],
        "email index entries should be removed for entity",
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "name", "Jan"),
        [],
        "name index entries should be removed for entity",
      );
    },
    "updates entries when value changes": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);
      indexStore.addToCombined(indexTable, "email", "old@example.com", "user-001");
      indexStore.updateInCombined(
        indexTable,
        "user-001",
        { email: "old@example.com" },
        { email: "new@example.com" },
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "old@example.com"),
        [],
        "old value should be removed from index",
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "new@example.com"),
        ["user-001"],
        "new value should be indexed",
      );
    },
    "supports independent lookups per indexed field": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "name", false);
      indexStore.registerIndex(indexTable, "city", false);
      indexStore.addToCombined(indexTable, "name", "Jan", "user-001");
      indexStore.addToCombined(indexTable, "city", "Warszawa", "user-001");
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "name", "Jan"),
        ["user-001"],
        "name lookup should return matching entity",
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "city", "Warszawa"),
        ["user-001"],
        "city lookup should return matching entity",
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "name", "Warszawa"),
        [],
        "lookups must stay scoped to field",
      );
    },
    "drops a combined index": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.dropCombinedIndex(indexTable);
      assertTrue(!adapter.getSheetNames().includes(indexTable), "dropped index sheet should be removed");
    },
    "existsCombined() checks for index sheet": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      assertTrue(!indexStore.existsCombined(indexTable), "index should not exist before create");
      indexStore.createCombinedIndex(indexTable);
      assertTrue(indexStore.existsCombined(indexTable), "index should exist after create");
    },
    "getIndexedFields() returns registered fields": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.registerIndex(indexTable, "email", true);
      indexStore.registerIndex(indexTable, "name", false);
      const fields = indexStore.getIndexedFields(indexTable);
      assertEqual(fields.length, 2, "there should be two registered indexed fields");
      assertDeepEqual(
        fields.map((f) => f.field).sort(),
        ["email", "name"],
        "indexed field names should match",
      );
    },
    // ─── N-gram search (Solr-like) ─────────────────
    "searchCombined (n-gram) > finds exact token match": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      indexStore.addToCombined(indexTable, "model", "Mercedes-Benz C200", "car-002");
      const ids = indexStore.searchCombined(indexTable, "model", "BMW");
      assertDeepEqual(ids, ["car-001"], "exact token 'BMW' should match car-001");
    },
    "searchCombined (n-gram) > finds partial match via trigrams": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      const ids = indexStore.searchCombined(indexTable, "model", "320");
      assertDeepEqual(ids, ["car-001"], "partial '320' should match via trigrams");
    },
    "searchCombined (n-gram) > is case insensitive": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      const ids = indexStore.searchCombined(indexTable, "model", "bmw");
      assertDeepEqual(ids, ["car-001"], "case-insensitive search should work");
    },
    "searchCombined (n-gram) > handles multi-token query (intersection)": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      indexStore.addToCombined(indexTable, "model", "BMW X5", "car-002");
      const ids = indexStore.searchCombined(indexTable, "model", "BMW 320");
      assertDeepEqual(ids, ["car-001"], "multi-token intersection should return only car-001");
    },
    "searchCombined (n-gram) > returns empty for no match": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      const ids = indexStore.searchCombined(indexTable, "model", "Volvo");
      assertDeepEqual(ids, [], "unmatched query should return empty array");
    },
    "searchCombined (n-gram) > respects limit parameter": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      indexStore.addToCombined(indexTable, "model", "BMW X5", "car-002");
      const ids = indexStore.searchCombined(indexTable, "model", "BMW", 1);
      assertEqual(ids.length, 1, "limit should restrict number of results");
    },
    "searchCombined (n-gram) > finds match through normalized separators": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "Mercedes-Benz C200", "car-001");
      const ids = indexStore.searchCombined(indexTable, "model", "Mercedes Benz");
      assertDeepEqual(ids, ["car-001"], "dash-normalized query should match");
    },
    "searchCombined (n-gram) > invalidates search index cache on data change": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      assertDeepEqual(indexStore.searchCombined(indexTable, "model", "Volvo"), [], "no Volvo yet");
      indexStore.addToCombined(indexTable, "model", "Volvo S60", "car-002");
      const ids = indexStore.searchCombined(indexTable, "model", "Volvo");
      assertDeepEqual(ids, ["car-002"], "search should pick up newly added entry");
    },
    "searchCombined (n-gram) > returns empty for empty query": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      assertDeepEqual(indexStore.searchCombined(indexTable, "model", ""), [], "empty query should return []");
    },
    "searchCombined (n-gram) > finds substring within a token via trigrams": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "Toyota Corolla", "car-001");
      const ids = indexStore.searchCombined(indexTable, "model", "Corol");
      assertDeepEqual(ids, ["car-001"], "substring 'Corol' should match via trigrams");
    },
    "normalizeForSearch > lowercases and trims": () => {
      assertEqual(IndexStore.normalizeForSearch("  BMW 320i  "), "bmw 320i", "should lowercase and trim");
    },
    "normalizeForSearch > normalizes dashes to spaces": () => {
      assertEqual(
        IndexStore.normalizeForSearch("Mercedes-Benz"),
        "mercedes benz",
        "dash should become space",
      );
    },
    "normalizeForSearch > collapses whitespace": () => {
      assertEqual(IndexStore.normalizeForSearch("a   b   c"), "a b c", "multiple spaces should collapse");
    },
    "normalizeForSearch > returns empty for null-ish input": () => {
      assertEqual(IndexStore.normalizeForSearch(""), "", "empty string should return empty");
    },
    "ngrams > generates trigrams": () => {
      const ngs = IndexStore.ngrams("abcde", 3);
      assertTrue(ngs.size === 3, "should have 3 trigrams");
      assertTrue(ngs.has("abc"), "should contain abc");
      assertTrue(ngs.has("bcd"), "should contain bcd");
      assertTrue(ngs.has("cde"), "should contain cde");
    },
    "ngrams > returns empty for short input": () => {
      const ngs = IndexStore.ngrams("ab", 3);
      assertEqual(ngs.size, 0, "input shorter than n should produce empty set");
    },
    "ngrams > strips whitespace before generating": () => {
      const ngs = IndexStore.ngrams("a b c d e", 3);
      assertTrue(ngs.size === 3, "whitespace should be stripped first");
      assertTrue(ngs.has("abc"), "should contain abc after stripping spaces");
    },
  },
  "query.test.ts": {
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
    "filters with search operator (substring match)": () => {
      const filters: Filter[] = [{ field: "name", operator: "search", value: "an" }];
      const result = filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "search operator should match Anna and Jan");
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
      Registry.reset();
      resetDecoratorCaches();
      Registry.getInstance().configure({ adapter });

      class Car extends BaseRecord {
        static override get tableName() {
          return `Cars_${suffix}`;
        }

        @Indexed()
        make: string;

        @Required()
        model: string;

        year: number;
        color: string;
      }

      class Product extends BaseRecord {
        static override get tableName() {
          return `Products_${suffix}`;
        }

        name: string;

        @Required()
        price: number;

        @Indexed()
        category: string;
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
      "creates instance with data via static create()": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = Car.create({ make: "BMW", model: "X5", year: 2023 });
        assertEqual(car.make, "BMW", "create should set make");
        assertEqual(car.model, "X5", "create should set model");
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
      "returns a Query and chains": (ctx: RuntimeCaseContext) => {
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
      "returns a Query": (ctx: RuntimeCaseContext) => {
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
        const results = Query.from(Car).execute();
        assertEqual(results.length, 1, "Query.from(class) should work");
      },
      "works with string name": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c = new Car();
        c.make = "Toyota";
        c.model = "Corolla";
        c.save();
        const results = Query.from("Car").execute();
        assertEqual(results.length, 1, "Query.from(string) should work");
      },
      "works with table name string": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c = new Car();
        c.make = "Toyota";
        c.model = "Corolla";
        c.save();
        const results = Query.from(Car.tableName).execute();
        assertEqual(results.length, 1, "Query.from(tableName) should work");
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
        const results = Query.from(Car)
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
          () => Query.from("UnknownClassName"),
          /unknown|not found|not registered/i,
          "Query.from with unknown name should throw",
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
      "works with Query.from() end-to-end": (ctx: RuntimeCaseContext) => {
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
        const results = Query.from(Car).where("make", "=", "Toyota").execute();
        assertEqual(results.length, 1, "Query.from e2e should return 1");
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

export function validateTests(): void {
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

export function runTests(): string {
  validateTests();

  const runStartedAt = Date.now();
  const state = new RuntimeParityState();
  const results: RuntimeCaseResult[] = [];
  const total = PARITY_CASE_IDS.length;

  const log = (msg: string): void => {
    if (typeof Logger !== "undefined" && typeof Logger.log === "function") {
      Logger.log(msg);
    }
  };

  log(`[SheetORM] Starting parity suite — ${total} test cases`);
  log("[SheetORM] Clearing all existing sheets from active spreadsheet before test run");
  state.clearAllSheets(log);

  for (const suite of PARITY_SUITES) {
    log(`[Suite] ${suite.file} (${suite.tests.length} tests)`);

    for (const testName of suite.tests) {
      const id = toParityCaseId(suite.file, testName);
      const num = results.length + 1;
      const startedAt = Date.now();
      try {
        const handler = getRuntimeCaseHandler(id);
        handler({ state });
        const durationMs = Date.now() - startedAt;
        results.push({ id, ok: true, durationMs });
        log(`  PASS [${num}/${total}] ${testName} (${durationMs} ms)`);
      } catch (error) {
        const durationMs = Date.now() - startedAt;
        const errMsg = error instanceof Error ? error.message : String(error);
        results.push({ id, ok: false, durationMs, error: errMsg });
        log(`  FAIL [${num}/${total}] ${testName} (${durationMs} ms)`);
        log(`       ${errMsg}`);
      }
    }
  }

  const failures = results.filter((result) => !result.ok);
  const passed = results.length - failures.length;
  const totalDurationMs = Date.now() - runStartedAt;
  const sumCaseDurationMs = results.reduce((sum, result) => sum + result.durationMs, 0);
  const avgDurationMs = results.length > 0 ? sumCaseDurationMs / results.length : 0;

  const fastest = results.reduce<RuntimeCaseResult | null>((best, current) => {
    if (!best) return current;
    return current.durationMs < best.durationMs ? current : best;
  }, null);

  const slowest = results.reduce<RuntimeCaseResult | null>((worst, current) => {
    if (!worst) return current;
    return current.durationMs > worst.durationMs ? current : worst;
  }, null);

  log(`[SheetORM] Done — ${passed}/${total} passed, ${failures.length} failed`);
  log(
    `[SheetORM] Timing summary — total: ${totalDurationMs} ms, avg/test: ${avgDurationMs.toFixed(2)} ms, fastest: ${fastest?.id ?? "n/a"} (${fastest?.durationMs ?? 0} ms), slowest: ${slowest?.id ?? "n/a"} (${slowest?.durationMs ?? 0} ms)`,
  );

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
    timing: {
      totalDurationMs,
      averagePerTestMs: Number(avgDurationMs.toFixed(2)),
      fastest: fastest
        ? {
            id: fastest.id,
            durationMs: fastest.durationMs,
          }
        : null,
      slowest: slowest
        ? {
            id: slowest.id,
            durationMs: slowest.durationMs,
          }
        : null,
    },
    spreadsheetUrl,
  };

  log(JSON.stringify(report));
  return JSON.stringify(report);
}
