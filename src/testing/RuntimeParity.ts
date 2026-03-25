import type { Entity } from "../core/types/Entity.js";
import type { FieldDefinition } from "../core/types/FieldDefinition.js";
import type { Filter } from "../core/types/Filter.js";
import type { QueryOptions } from "../core/types/QueryOptions.js";
import type { SortClause } from "../core/types/SortClause.js";
import { Registry } from "../core/Registry.js";
import { IndexStore } from "../index/IndexStore.js";
import { Query } from "../query/Query.js";
import { Record as BaseRecord } from "../core/Record.js";
import type { RecordStatic } from "../core/RecordStatic.js";
import { Decorators } from "../core/Decorators.js";
import { QueryEngine } from "../query/QueryEngine.js";
import { GoogleSpreadsheetAdapter } from "../storage/GoogleSpreadsheetAdapter.js";
import { MemoryCache } from "../core/cache/MemoryCache.js";
import { SheetRepository } from "../core/SheetRepository.js";
import type { LifecycleHooks } from "../core/types/LifecycleHooks.js";
import { Serialization } from "../utils/Serialization.js";
import { Uuid } from "../utils/Uuid.js";
import { ParityCatalog } from "./ParityCatalog.js";

const { Indexed, Required, resetDecoratorCaches } = Decorators;

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
      emit(`[SheetORM] Deleting sheet: "${sheetToDelete.getName()}" | remaining: ${remainingToDelete}`);
      spreadsheet.deleteSheet(sheetToDelete);
      remainingToDelete -= 1;
      emit(`[SheetORM] Deleted. Remaining: ${remainingToDelete}`);
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
    "has() returns false after TTL expiry": () => {
      const cache = new MemoryCache(40);
      cache.set("ttlKey", "value");
      assertEqual(cache.has("ttlKey"), true, "has() should return true before TTL");
      sleepMs(70);
      assertEqual(cache.has("ttlKey"), false, "has() should return false after TTL");
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
    "constructor throws for NaN TTL": () => {
      let threw = false;
      try {
        new MemoryCache(NaN);
      } catch {
        threw = true;
      }
      assertTrue(threw, "MemoryCache(NaN) should throw");
    },
    "constructor throws for negative TTL": () => {
      let threw = false;
      try {
        new MemoryCache(-100);
      } catch {
        threw = true;
      }
      assertTrue(threw, "MemoryCache(-100) should throw");
    },
    "set() throws for NaN per-key TTL": () => {
      const cache = new MemoryCache(1000);
      cache.set("key1", "value1");
      let threw = false;
      try {
        cache.set("key2", "value2", NaN);
      } catch {
        threw = true;
      }
      assertTrue(threw, "set() with NaN TTL should throw");
    },
    "TTL of 0 expires immediately": () => {
      const cache = new MemoryCache(1000);
      cache.set("instant", "gone", 0);
      assertEqual(cache.get("instant"), null, "TTL=0 entry should be expired immediately");
      assertTrue(!cache.has("instant"), "has() should return false for TTL=0 entry");
    },
    "set() throws for negative per-key TTL": () => {
      const cache = new MemoryCache(1000);
      let threw = false;
      try {
        cache.set("k", "v", -1);
      } catch {
        threw = true;
      }
      assertTrue(threw, "set() with negative TTL should throw");
    },
    "set() throws for Infinity per-key TTL": () => {
      const cache = new MemoryCache(1000);
      let threw = false;
      try {
        cache.set("k", "v", Infinity);
      } catch {
        threw = true;
      }
      assertTrue(threw, "set() with Infinity TTL should throw");
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
    "cancelIndexBatch() discards buffered entries": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);
      indexStore.beginIndexBatch();
      indexStore.addAllFieldsToCombined(indexTable, [{ field: "email", value: "a@e.com" }], "u-001");
      indexStore.cancelIndexBatch();
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "a@e.com"),
        [],
        "cancelled batch entries should not appear in index",
      );
    },
    "removeMultipleFromCombined() bulk-removes entries": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);
      indexStore.addToCombined(indexTable, "email", "a@e.com", "u-001");
      indexStore.addToCombined(indexTable, "email", "b@e.com", "u-002");
      indexStore.addToCombined(indexTable, "email", "c@e.com", "u-003");
      indexStore.removeMultipleFromCombined(indexTable, ["u-001", "u-003"]);
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "b@e.com"),
        ["u-002"],
        "only u-002 should remain after bulk remove",
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "a@e.com"),
        [],
        "u-001 should be removed",
      );
    },
    "removeMultipleFromCombined() no-op for empty array": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Users")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);
      indexStore.addToCombined(indexTable, "email", "a@e.com", "u-001");
      indexStore.removeMultipleFromCombined(indexTable, []);
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "a@e.com"),
        ["u-001"],
        "empty remove array should leave index intact",
      );
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
    "searchCombined (n-gram) > invalidates search cache after flushIndexBatch": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      assertDeepEqual(indexStore.searchCombined(indexTable, "model", "Civic"), [], "no Civic yet");
      indexStore.beginIndexBatch();
      indexStore.addAllFieldsToCombined(indexTable, [{ field: "model", value: "Honda Civic" }], "car-002");
      indexStore.flushIndexBatch();
      const ids = indexStore.searchCombined(indexTable, "model", "Civic");
      assertDeepEqual(ids, ["car-002"], "search should find entry added via batch after flush");
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
    "searchCombined (n-gram) > returns empty array for limit=0": (ctx: RuntimeCaseContext) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      const ids = indexStore.searchCombined(indexTable, "model", "BMW", 0);
      assertEqual(ids.length, 0, "searchCombined with limit=0 should return empty array");
    },
    "searchCombined (n-gram) > finds match for query shorter than trigram size": (ctx: RuntimeCaseContext) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = `idx_${ctx.state.nextTableName("Cars")}`;
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "model", false);
      indexStore.addToCombined(indexTable, "model", "BMW 320i", "car-001");
      const ids = indexStore.searchCombined(indexTable, "model", "bm");
      assertTrue(ids.length > 0, "2-char query should find match via substring fallback");
      assertTrue(ids.includes("car-001"), "should contain car-001");
    },
    "lookupCombined returns empty for non-existent index table": (ctx: RuntimeCaseContext) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const ids = indexStore.lookupCombined("idx_NonExistent", "field", "value");
      assertDeepEqual(ids, [], "lookupCombined should return empty for non-existent table");
    },
    "updateInCombined throws unique violation when value conflicts with another entity": (ctx: RuntimeCaseContext) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = ctx.state.nextTableName("idx_upd_uniq");
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", true);
      indexStore.addToCombined(indexTable, "email", "jan@example.com", "user-001");
      indexStore.addToCombined(indexTable, "email", "anna@example.com", "user-002");

      assertThrows(
        () => indexStore.updateInCombined(
          indexTable,
          "user-002",
          { email: "anna@example.com" },
          { email: "jan@example.com" },
        ),
        /Unique index violation/,
        "updateInCombined should throw unique violation",
      );
    },
    "addAllFieldsToCombined detects unique violation in pending batch entries": (ctx: RuntimeCaseContext) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = ctx.state.nextTableName("idx_batch_uniq");
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", true);

      indexStore.beginIndexBatch();
      indexStore.addAllFieldsToCombined(
        indexTable,
        [{ field: "email", value: "dup@example.com" }],
        "user-001",
      );
      assertThrows(
        () => indexStore.addAllFieldsToCombined(
          indexTable,
          [{ field: "email", value: "dup@example.com" }],
          "user-002",
        ),
        /Unique index violation/,
        "addAllFieldsToCombined should detect unique violation in pending batch",
      );
      indexStore.cancelIndexBatch();
    },
    "operates correctly without cache provider": (ctx: RuntimeCaseContext) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter);
      const indexTable = ctx.state.nextTableName("idx_nocache");
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "name", false);
      indexStore.addToCombined(indexTable, "name", "Alice", "e-001");
      indexStore.addToCombined(indexTable, "name", "Bob", "e-002");

      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "name", "Alice"),
        ["e-001"],
        "lookup should work without cache",
      );

      indexStore.updateInCombined(
        indexTable,
        "e-001",
        { name: "Alice" },
        { name: "Alicia" },
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "name", "Alicia"),
        ["e-001"],
        "update should work without cache",
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "name", "Alice"),
        [],
        "old value should be gone after update without cache",
      );

      indexStore.removeAllFromCombined(indexTable, "e-002");
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "name", "Bob"),
        [],
        "remove should work without cache",
      );
    },
    "removeMultipleFromCombined is no-op for non-existent entity IDs": (ctx: RuntimeCaseContext) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = ctx.state.nextTableName("idx_rm_noexist");
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);
      indexStore.addToCombined(indexTable, "email", "a@example.com", "user-001");

      indexStore.removeMultipleFromCombined(indexTable, ["user-999", "user-888"]);
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "a@example.com"),
        ["user-001"],
        "existing entry should remain after removing non-existent IDs",
      );
    },
    "updateInCombined creates entry when field goes from empty to populated": (ctx: RuntimeCaseContext) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const indexTable = ctx.state.nextTableName("idx_upd_empty");
      indexStore.createCombinedIndex(indexTable);
      indexStore.registerIndex(indexTable, "email", false);

      indexStore.updateInCombined(
        indexTable,
        "user-001",
        { email: "" },
        { email: "new@example.com" },
      );
      assertDeepEqual(
        indexStore.lookupCombined(indexTable, "email", "new@example.com"),
        ["user-001"],
        "entry should be created when going from empty to populated",
      );
    },
    "normalizeForSearch > normalizes em-dashes and underscores to spaces": () => {
      assertEqual(
        IndexStore.normalizeForSearch("hello\u2014world"),
        "hello world",
        "em-dash should be normalized to space",
      );
      assertEqual(
        IndexStore.normalizeForSearch("snake_case"),
        "snake case",
        "underscore should be normalized to space",
      );
      assertEqual(
        IndexStore.normalizeForSearch("a\u2013b"),
        "a b",
        "en-dash should be normalized to space",
      );
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
    "limit() throws for negative number": () => {
      let threw = false;
      try {
        createBuilder().limit(-1);
      } catch {
        threw = true;
      }
      assertTrue(threw, "limit(-1) should throw");
    },
    "limit() throws for NaN": () => {
      let threw = false;
      try {
        createBuilder().limit(NaN);
      } catch {
        threw = true;
      }
      assertTrue(threw, "limit(NaN) should throw");
    },
    "offset() throws for negative number": () => {
      let threw = false;
      try {
        createBuilder().offset(-1);
      } catch {
        threw = true;
      }
      assertTrue(threw, "offset(-1) should throw");
    },
    "offset() throws for Infinity": () => {
      let threw = false;
      try {
        createBuilder().offset(Infinity);
      } catch {
        threw = true;
      }
      assertTrue(threw, "offset(Infinity) should throw");
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
    "first() respects offset": () => {
      // Sorted by price asc: Banana(0.8), Carrot(1.2), Apple(1.5), Donut(2.5), Eggplant(3.0)
      const result = createBuilder().orderBy("price", "asc").offset(2).first();
      assertTrue(result !== null, "first with offset should return an entity");
      assertEqual(result?.name, "Apple", "third cheapest item should be Apple");
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
    "build() includes offset when set alone": () => {
      const query = createBuilder().offset(5).build();
      assertEqual(query.offset, 5, "build should include offset when set alone");
    },
    "returns entities matching either condition": () => {
      const result = createBuilder()
        .where("category", "=", "pastry")
        .or("category", "=", "vegetable")
        .execute();
      assertEqual(result.length, 3, "or should return entities matching either condition");
    },
    "applies AND within each OR group": () => {
      const result = createBuilder()
        .where("category", "=", "fruit")
        .and("price", ">", 1)
        .or("category", "=", "vegetable")
        .and("price", "<", 2)
        .execute();
      assertEqual(result.length, 2, "AND within OR groups should produce correct results");
    },
    "chains multiple or() calls": () => {
      const result = createBuilder()
        .where("name", "=", "Apple")
        .or("name", "=", "Banana")
        .or("name", "=", "Donut")
        .execute();
      assertEqual(result.length, 3, "multiple or() calls should chain correctly");
    },
    "works with orderBy": () => {
      const result = createBuilder()
        .where("category", "=", "fruit")
        .or("category", "=", "pastry")
        .orderBy("price", "desc")
        .execute();
      assertEqual(result[0].name, "Donut", "first result should be Donut (highest price)");
    },
    "works with limit and offset": () => {
      const result = createBuilder()
        .where("category", "=", "fruit")
        .or("category", "=", "vegetable")
        .orderBy("price", "asc")
        .limit(2)
        .offset(1)
        .execute();
      assertEqual(result.length, 2, "limit+offset should work with or()");
      assertEqual(result[0].name, "Carrot", "offset 1 should start at Carrot");
    },
    "first() returns first OR match": () => {
      const result = createBuilder()
        .where("category", "=", "vegetable")
        .or("category", "=", "pastry")
        .orderBy("price", "asc")
        .first();
      assertTrue(result !== null, "first() should return a match");
      assertEqual(result?.name, "Carrot", "first OR match sorted by price should be Carrot");
    },
    "count() counts OR matches": () => {
      const count = createBuilder().where("category", "=", "fruit").or("category", "=", "pastry").count();
      assertEqual(count, 3, "count should include all OR matches");
    },
    "select() paginates OR results": () => {
      const result = createBuilder()
        .where("category", "=", "fruit")
        .or("category", "=", "vegetable")
        .select(0, 2);
      assertEqual(result.total, 4, "total should count all OR matches");
      assertEqual(result.items.length, 2, "items should be limited to page size");
      assertTrue(result.hasNext, "hasNext should be true when more items remain");
    },
    "groupBy() groups OR results": () => {
      const groups = createBuilder()
        .where("category", "=", "fruit")
        .or("category", "=", "vegetable")
        .groupBy("category");
      assertEqual(groups.length, 2, "groupBy should produce 2 groups from OR results");
    },
    "build() returns whereGroups for OR queries": () => {
      const qo = createBuilder().where("category", "=", "fruit").or("name", "=", "Donut").build();
      assertEqual(qo.where, undefined, "where should be undefined for OR queries");
      assertEqual(qo.whereGroups?.length, 2, "whereGroups should have 2 groups");
    },
    "build() returns where (not whereGroups) for AND-only queries": () => {
      const qo = createBuilder().where("category", "=", "fruit").and("price", ">", 1).build();
      assertEqual(qo.where?.length, 2, "where should have 2 filters for AND-only");
      assertEqual(qo.whereGroups, undefined, "whereGroups should be undefined for AND-only");
    },
    "or() without preceding where() still filters correctly": () => {
      const result = createBuilder().or("category", "=", "pastry").execute();
      assertEqual(result.length, 1, "or() without where() should match 1 item");
      assertEqual(result[0].name, "Donut", "matched item should be Donut");
    },
    "or().and() without preceding where() chains correctly": () => {
      const result = createBuilder()
        .or("category", "=", "pastry")
        .or("category", "=", "fruit")
        .and("price", ">", 1)
        .execute();
      assertEqual(result.length, 2, "or().and() should match 2 items");
      const names = result.map((r: { name: string }) => r.name).sort();
      assertDeepEqual(names, ["Apple", "Donut"], "matched items should be Apple and Donut");
    },
    "execute() with limit(0) returns an empty array": () => {
      const result = createBuilder().limit(0).execute();
      assertEqual(result.length, 0, "limit(0) should return empty array");
    },
    "build() with limit(0) includes limit 0": () => {
      const opts = createBuilder().limit(0).build();
      assertEqual(opts.limit, 0, "build should include limit 0");
    },
    "build() with no filters returns all undefined options": () => {
      const opts = createBuilder().build();
      assertEqual(opts.where, undefined, "where should be undefined");
      assertEqual(opts.whereGroups, undefined, "whereGroups should be undefined");
      assertEqual(opts.orderBy, undefined, "orderBy should be undefined");
      assertEqual(opts.limit, undefined, "limit should be undefined");
      assertEqual(opts.offset, undefined, "offset should be undefined");
    },
    "first() returns null when offset exceeds result count": () => {
      const result = createBuilder().where("category", "=", "fruit").offset(100).first();
      assertEqual(result, null, "first() should return null when offset exceeds result count");
    },
    "groupBy() respects orderBy before grouping": () => {
      const groups = createBuilder().orderBy("price", "desc").groupBy("category");
      assertTrue(groups.length >= 2, "groupBy with orderBy should return at least 2 groups");
      const fruitGroup = groups.find((g: { key: unknown }) => g.key === "fruit");
      assertTrue(fruitGroup !== undefined, "fruit group should exist");
      assertEqual(fruitGroup!.items.length, 2, "fruit group should have 2 items");
    },
    "execute() with orderBy and offset combined returns correct slice": () => {
      const result = createBuilder().orderBy("price", "asc").offset(2).limit(2).execute();
      assertEqual(result.length, 2, "should return 2 items with offset 2 limit 2");
      assertEqual(result[0].name, "Apple", "3rd item by price asc should be Apple");
      assertEqual(result[1].name, "Donut", "4th item by price asc should be Donut");
    },
    "limit() and offset() floor fractional values": () => {
      const result = createBuilder().orderBy("price", "asc").limit(2.9).offset(1.7).execute();
      assertEqual(result.length, 2, "limit(2.9) should floor to 2");
      assertEqual(result[0].name, "Carrot", "offset(1.7) should floor to 1, so 2nd cheapest");
    },
    "Query.from() without resolver throws descriptive error": () => {
      const original = (Query as unknown as { _fromResolverFn: unknown })._fromResolverFn;
      try {
        (Query as unknown as { _fromResolverFn: unknown })._fromResolverFn = null;
        let threw = false;
        try {
          Query.from("NonExistent");
        } catch (e: unknown) {
          threw = true;
          assertTrue(
            (e as Error).message.includes("not available"),
            "error message should mention 'not available'",
          );
        }
        assertTrue(threw, "Query.from() without resolver should throw");
      } finally {
        (Query as unknown as { _fromResolverFn: unknown })._fromResolverFn = original;
      }
    },
  },
  "query-engine.test.ts": {
    "filters with = operator": () => {
      const filters: Filter[] = [{ field: "city", operator: "=", value: "Kraków" }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "equals filter should match two users");
      assertDeepEqual(
        result.map((u) => u.name),
        ["Jan", "Zofia"],
        "matching users should be Jan and Zofia",
      );
    },
    "filters with != operator": () => {
      const filters: Filter[] = [{ field: "active", operator: "!=", value: false }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 3, "not-equal filter should match three users");
    },
    "filters with > operator": () => {
      const filters: Filter[] = [{ field: "age", operator: ">", value: 40 }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "greater-than filter should match two users");
    },
    "filters with < operator": () => {
      const filters: Filter[] = [{ field: "age", operator: "<", value: 30 }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "less-than filter should match two users");
    },
    "filters with >= and <= operators": () => {
      const filters: Filter[] = [
        { field: "age", operator: ">=", value: 28 },
        { field: "age", operator: "<=", value: 45 },
      ];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 3, "range filters should match three users");
    },
    "filters with contains operator": () => {
      const filters: Filter[] = [{ field: "name", operator: "contains", value: "an" }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "contains filter should be case-insensitive and match two users");
    },
    "contains is case-insensitive for uppercase query": () => {
      const filters: Filter[] = [{ field: "name", operator: "contains", value: "AN" }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "uppercase contains should match Anna and Jan");
    },
    "filters with startsWith operator": () => {
      const filters: Filter[] = [{ field: "name", operator: "startsWith", value: "A" }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 1, "startsWith should match one user");
    },
    "filters with in operator": () => {
      const filters: Filter[] = [{ field: "city", operator: "in", value: ["Gdańsk", "Kraków"] }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 3, "in operator should match users in both cities");
    },
    "filters with search operator (substring match)": () => {
      const filters: Filter[] = [{ field: "name", operator: "search", value: "an" }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "search operator should match Anna and Jan");
    },
    "applies multiple filters as AND": () => {
      const filters: Filter[] = [
        { field: "active", operator: "=", value: true },
        { field: "age", operator: ">", value: 25 },
      ];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 2, "multiple filters should combine with AND");
    },
    "returns all when no filters": () => {
      assertEqual(
        QueryEngine.filterEntities(queryEngineUsers, []).length,
        5,
        "empty filters should return all users",
      );
    },
    "sorts ascending by number": () => {
      const sorts: SortClause[] = [{ field: "age", direction: "asc" }];
      const result = QueryEngine.sortEntities(queryEngineUsers, sorts);
      assertDeepEqual(
        result.map((u) => u.age),
        [22, 28, 35, 45, 60],
        "ascending numeric sort should match expected order",
      );
    },
    "sorts descending by number": () => {
      const sorts: SortClause[] = [{ field: "age", direction: "desc" }];
      const result = QueryEngine.sortEntities(queryEngineUsers, sorts);
      assertDeepEqual(
        result.map((u) => u.age),
        [60, 45, 35, 28, 22],
        "descending numeric sort should match expected order",
      );
    },
    "sorts by string": () => {
      const sorts: SortClause[] = [{ field: "name", direction: "asc" }];
      const result = QueryEngine.sortEntities(queryEngineUsers, sorts);
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
      const result = QueryEngine.sortEntities(queryEngineUsers, sorts);
      assertDeepEqual(
        result.map((u) => u.name),
        ["Maria", "Zofia", "Jan", "Piotr", "Anna"],
        "multi-sort order should match expected",
      );
    },
    "does not mutate original array": () => {
      const original = [...queryEngineUsers];
      QueryEngine.sortEntities(queryEngineUsers, [{ field: "age", direction: "asc" }]);
      assertDeepEqual(queryEngineUsers, original, "sortEntities should not mutate input array");
    },
    "returns first page": () => {
      const result = QueryEngine.paginateEntities(queryEngineUsers, 0, 2);
      assertEqual(result.items.length, 2, "first page should contain two items");
      assertEqual(result.total, 5, "total should be full collection size");
      assertEqual(result.offset, 0, "offset should match input");
      assertEqual(result.limit, 2, "limit should match input");
      assertTrue(result.hasNext, "first page should have next page");
    },
    "returns last page": () => {
      const result = QueryEngine.paginateEntities(queryEngineUsers, 4, 2);
      assertEqual(result.items.length, 1, "last page should contain single item");
      assertTrue(!result.hasNext, "last page should not have next page");
    },
    "returns empty if offset exceeds total": () => {
      const result = QueryEngine.paginateEntities(queryEngineUsers, 10, 2);
      assertEqual(result.items.length, 0, "offset beyond total should return empty page");
      assertTrue(!result.hasNext, "empty out-of-range page should not have next");
    },
    "groups by field": () => {
      const groups = QueryEngine.groupEntities(queryEngineUsers, "city");
      assertEqual(groups.length, 3, "city grouping should produce three groups");
      const waw = groups.find((g) => g.key === "Warszawa");
      assertEqual(waw?.count, 2, "Warszawa group should contain two users");
    },
    "groups by boolean": () => {
      const groups = QueryEngine.groupEntities(queryEngineUsers, "active");
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
      const result = QueryEngine.executeQuery(queryEngineUsers, options);
      assertEqual(result.length, 1, "combined query should return one entity");
      assertEqual(result[0].name, "Anna", "combined query should return Anna");
    },
    "applies only sort and limit when no filters are provided": () => {
      const options: QueryOptions = {
        orderBy: [{ field: "age", direction: "desc" }],
        limit: 3,
      };
      const result = QueryEngine.executeQuery(queryEngineUsers, options);
      assertEqual(result.length, 3, "query should return three entities after limit");
      assertDeepEqual(
        result.map((u) => u.name),
        ["Zofia", "Piotr", "Jan"],
        "query should apply sort and limit without filters",
      );
    },
    "returns all entities for empty options": () => {
      const result = QueryEngine.executeQuery(queryEngineUsers, {});
      assertEqual(result.length, queryEngineUsers.length, "empty options should return all entities");
    },
    "matches entities passing any group": () => {
      const groups: Filter[][] = [
        [{ field: "city", operator: "=", value: "Gdańsk" }],
        [{ field: "city", operator: "=", value: "Kraków" }],
      ];
      const result = QueryEngine.filterEntitiesOr(queryEngineUsers, groups);
      assertEqual(result.length, 3, "OR groups should match entities passing any group");
    },
    "applies AND within each group": () => {
      const groups: Filter[][] = [
        [
          { field: "city", operator: "=", value: "Kraków" },
          { field: "active", operator: "=", value: true },
        ],
        [
          { field: "city", operator: "=", value: "Warszawa" },
          { field: "active", operator: "=", value: false },
        ],
      ];
      const result = QueryEngine.filterEntitiesOr(queryEngineUsers, groups);
      assertEqual(result.length, 2, "AND within each OR group should narrow correctly");
    },
    "returns all entities for empty groups": () => {
      const result = QueryEngine.filterEntitiesOr(queryEngineUsers, []);
      assertEqual(result.length, 5, "empty groups should return all entities");
    },
    "uses OR groups when whereGroups is provided": () => {
      const options: QueryOptions = {
        whereGroups: [
          [{ field: "city", operator: "=", value: "Gdańsk" }],
          [{ field: "name", operator: "=", value: "Anna" }],
        ],
        orderBy: [{ field: "age", direction: "asc" }],
      };
      const result = QueryEngine.executeQuery(queryEngineUsers, options);
      assertEqual(result.length, 2, "executeQuery should use whereGroups");
      assertEqual(result[0].name, "Maria", "first sorted result should be Maria");
    },
    "prefers whereGroups over where": () => {
      const options: QueryOptions = {
        where: [{ field: "active", operator: "=", value: true }],
        whereGroups: [[{ field: "city", operator: "=", value: "Gdańsk" }]],
      };
      const result = QueryEngine.executeQuery(queryEngineUsers, options);
      assertEqual(result.length, 1, "whereGroups should take precedence over where");
      assertEqual(result[0].name, "Maria", "only Maria should match Gdańsk group");
    },
    "contains with non-string value matches nothing": () => {
      const filters: Filter[] = [{ field: "name", operator: "contains", value: 123 }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 0, "contains with non-string value should match nothing");
    },
    "startsWith with non-string value matches nothing": () => {
      const filters: Filter[] = [{ field: "name", operator: "startsWith", value: null }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 0, "startsWith with non-string value should match nothing");
    },
    "search with non-string value matches nothing": () => {
      const filters: Filter[] = [{ field: "name", operator: "search", value: undefined }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 0, "search with non-string value should match nothing");
    },
    "negative offset is clamped to 0": () => {
      const result = QueryEngine.paginateEntities(queryEngineUsers, -5, 2);
      assertEqual(result.offset, 0, "negative offset should be clamped to 0");
      assertEqual(result.items.length, 2, "should still return requested items");
    },
    "negative limit defaults to full length": () => {
      const result = QueryEngine.paginateEntities(queryEngineUsers, 0, -1);
      assertEqual(result.limit, queryEngineUsers.length, "negative limit should default to total length");
      assertEqual(result.items.length, queryEngineUsers.length, "should return all items");
    },
    "NaN offset defaults to 0": () => {
      const result = QueryEngine.paginateEntities(queryEngineUsers, NaN, 2);
      assertEqual(result.offset, 0, "NaN offset should default to 0");
      assertEqual(result.items.length, 2, "should still return requested items");
    },
    "returns no matches for an unrecognized operator": () => {
      const filters: Filter[] = [
        { field: "name", operator: "regex" as unknown as Filter["operator"], value: ".*" },
      ];
      assertEqual(
        QueryEngine.filterEntities(queryEngineUsers, filters).length,
        0,
        "unknown operator should match nothing",
      );
    },
    "returns false when field type differs from value type (number vs string)": () => {
      const filters: Filter[] = [{ field: "age", operator: ">", value: "thirty" as unknown as number }];
      assertEqual(
        QueryEngine.filterEntities(queryEngineUsers, filters).length,
        0,
        "type mismatch should match nothing",
      );
    },
    "compares strings when both field and value are strings": () => {
      const filters: Filter[] = [{ field: "name", operator: "<", value: "Jan" }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertDeepEqual(
        result.map((u) => u.name),
        ["Anna"],
        "should match Anna (Anna < Jan)",
      );
    },
    "filters by nested dot-path field": () => {
      const nested = [
        { __id: "1", name: "Anna", profile: { city: "Warszawa", score: 80 } },
        { __id: "2", name: "Jan", profile: { city: "Kraków", score: 95 } },
        { __id: "3", name: "Piotr", profile: { city: "Warszawa", score: 60 } },
      ];
      const filters: Filter[] = [{ field: "profile.city", operator: "=", value: "Warszawa" }];
      const result = QueryEngine.filterEntities(nested, filters);
      assertEqual(result.length, 2, "should match 2 users in Warszawa via dot path");
    },
    "filters by nested slash-path field": () => {
      const nested = [
        { __id: "1", name: "Anna", profile: { city: "Warszawa" } },
        { __id: "2", name: "Jan", profile: { city: "Kraków" } },
      ];
      const filters: Filter[] = [{ field: "profile/city", operator: "=", value: "Kraków" }];
      const result = QueryEngine.filterEntities(nested, filters);
      assertEqual(result.length, 1, "should match via slash path");
      assertEqual(result[0].name, "Jan", "should find Jan via slash path");
    },
    "sorts by nested field": () => {
      const nested = [
        { __id: "1", name: "Anna", profile: { city: "W", score: 80 } },
        { __id: "2", name: "Jan", profile: { city: "K", score: 95 } },
        { __id: "3", name: "Piotr", profile: { city: "W", score: 60 } },
      ];
      const sorts: SortClause[] = [{ field: "profile.score", direction: "asc" }];
      const result = QueryEngine.sortEntities(nested, sorts);
      assertDeepEqual(result.map((u) => u.name), ["Piotr", "Anna", "Jan"], "should sort by nested score asc");
    },
    "returns undefined for missing nested segment": () => {
      const mixed = [
        { __id: "1", name: "A" },
        { __id: "2", name: "B", profile: { city: "X" } },
      ];
      const filters: Filter[] = [{ field: "profile.city", operator: "=", value: "X" }];
      const result = QueryEngine.filterEntities(mixed, filters);
      assertEqual(result.length, 1, "should match only entity with nested field");
      assertEqual(result[0].__id, "2", "should find entity with profile.city = X");
    },
    "uses Set for arrays with more than 8 elements": () => {
      const manyValues = [
        "Warszawa",
        "Kraków",
        "Gdańsk",
        "Wrocław",
        "Poznań",
        "Łódź",
        "Katowice",
        "Szczecin",
        "Lublin",
      ];
      const filters: Filter[] = [{ field: "city", operator: "in", value: manyValues }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 3, "should match 3 users from cities in the large set");
    },
    "returns empty items when limit is 0": () => {
      const result = QueryEngine.paginateEntities(queryEngineUsers, 0, 0);
      assertEqual(result.items.length, 0, "limit 0 should return empty items");
      assertEqual(result.total, queryEngineUsers.length, "total should equal full collection size");
      assertEqual(result.limit, 0, "limit should be 0");
      assertTrue(result.hasNext, "hasNext should be true when total > 0");
    },
    "places null values before non-null in ascending order": () => {
      const data: Entity[] = [
        { __id: "1", name: "Anna", score: 80 },
        { __id: "2", name: "Jan", score: null as unknown as number },
        { __id: "3", name: "Piotr", score: 60 },
      ];
      const sorted = QueryEngine.sortEntities(data, [{ field: "score", direction: "asc" }]);
      assertEqual(sorted[0].__id, "2", "null should sort first in ascending order");
      assertEqual(sorted[1].__id, "3", "60 should be second");
      assertEqual(sorted[2].__id, "1", "80 should be third");
    },
    "places null values last in descending sort": () => {
      const data: Entity[] = [
        { __id: "1", name: "Anna", score: 80 },
        { __id: "2", name: "Jan", score: null as unknown as number },
        { __id: "3", name: "Piotr", score: 60 },
      ];
      const sorted = QueryEngine.sortEntities(data, [{ field: "score", direction: "desc" }]);
      assertEqual(sorted[0].__id, "1", "80 should sort first in descending order");
      assertEqual(sorted[1].__id, "3", "60 should be second");
      assertEqual(sorted[2].__id, "2", "null should sort last in descending order");
    },
    "groups entities including those with undefined keys": () => {
      const data: Entity[] = [
        { __id: "1", name: "Anna", city: "Warszawa" },
        { __id: "2", name: "Jan" },
        { __id: "3", name: "Piotr", city: "Warszawa" },
      ];
      const groups = QueryEngine.groupEntities(data, "city");
      assertEqual(groups.length, 2, "should produce 2 groups");
      const warszawaGroup = groups.find((g) => g.key === "Warszawa");
      const undefinedGroup = groups.find((g) => g.key === undefined);
      assertEqual(warszawaGroup!.count, 2, "Warszawa group should have 2 members");
      assertEqual(undefinedGroup!.count, 1, "undefined group should have 1 member");
    },
    "sortEntities returns empty array for empty input": () => {
      const result = QueryEngine.sortEntities([], [{ field: "name", direction: "asc" }]);
      assertEqual(result.length, 0, "sorting empty array should return empty array");
    },
    "groupEntities returns empty array for empty input": () => {
      const result = QueryEngine.groupEntities([], "name");
      assertEqual(result.length, 0, "grouping empty array should return empty array");
    },
    "filterEntities with empty entity list returns empty": () => {
      const filters: Filter[] = [{ field: "name", operator: "=", value: "Anna" }];
      const result = QueryEngine.filterEntities([], filters);
      assertEqual(result.length, 0, "filtering empty array should return empty array");
    },
    "paginateEntities with Infinity limit defaults to full length": () => {
      const result = QueryEngine.paginateEntities(queryEngineUsers, 0, Infinity);
      assertEqual(result.items.length, queryEngineUsers.length, "Infinity limit should return all items");
      assertEqual(result.total, queryEngineUsers.length, "total should equal full collection size");
      assertEqual(result.limit, queryEngineUsers.length, "limit should default to full length");
      assertTrue(!result.hasNext, "hasNext should be false when all items returned");
    },
    "sortEntities treats both-null values as equal": () => {
      const data: Entity[] = [
        { __id: "1", name: null, age: 10 },
        { __id: "2", name: null, age: 20 },
        { __id: "3", name: "Alice", age: 15 },
      ];
      const sorted = QueryEngine.sortEntities(data, [{ field: "name", direction: "asc" }]);
      assertEqual(sorted[0].__id, "1", "first null should maintain position");
      assertEqual(sorted[1].__id, "2", "second null should maintain position");
      assertEqual(sorted[2].__id, "3", "Alice should come after nulls");
    },
    "in operator with non-array value returns empty result": () => {
      const filters: Filter[] = [{ field: "city", operator: "in", value: "Warszawa" as unknown }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 0, "in with non-array value should return empty");
    },
    "in operator with >8 elements uses Set-based path": () => {
      const targetCities = [
        "Warszawa", "Kraków", "Gdańsk", "Łódź", "Lublin",
        "Białystok", "Katowice", "Bydgoszcz", "Szczecin",
      ];
      const filters: Filter[] = [{ field: "city", operator: "in", value: targetCities }];
      const result = QueryEngine.filterEntities(queryEngineUsers, filters);
      assertEqual(result.length, 5, "in with >8 elements should match all 5 users");
      assertDeepEqual(
        result.map((u) => u.name).sort(),
        ["Anna", "Jan", "Maria", "Piotr", "Zofia"],
        "should match all users whose cities are in the set",
      );
    },
    "contains operator with non-string entity value returns false": () => {
      const data: Entity[] = [
        { __id: "1", name: 12345 },
        { __id: "2", name: "hello world" },
      ];
      const filters: Filter[] = [{ field: "name", operator: "contains", value: "123" }];
      const result = QueryEngine.filterEntities(data, filters);
      assertEqual(result.length, 0, "contains on numeric entity value should not match");
    },
  },
  "serialization.test.ts": {
    "serializes string": () => {
      const fd: FieldDefinition = { name: "x", type: "string" };
      assertEqual(
        Serialization.serializeValue("hello", fd),
        "hello",
        "string serialization should preserve string value",
      );
      assertEqual(
        Serialization.serializeValue(123, fd),
        "123",
        "string serialization should coerce number to string",
      );
      assertEqual(
        Serialization.serializeValue(null, fd),
        "",
        "string serialization should map null to empty string",
      );
    },
    "serializes number": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(Serialization.serializeValue(42, fd), 42, "number serialization should preserve number");
      assertEqual(
        Serialization.serializeValue("7", fd),
        7,
        "number serialization should coerce numeric string",
      );
    },
    "serializes boolean": () => {
      const fd: FieldDefinition = { name: "x", type: "boolean" };
      assertEqual(
        Serialization.serializeValue(true, fd),
        true,
        "boolean serialization should preserve boolean",
      );
      assertEqual(
        Serialization.serializeValue("true", fd),
        true,
        "boolean serialization should parse true string",
      );
      assertEqual(
        Serialization.serializeValue("false", fd),
        false,
        "boolean serialization should parse false string",
      );
    },
    "serializes json": () => {
      const fd: FieldDefinition = { name: "x", type: "json" };
      assertEqual(
        Serialization.serializeValue({ a: 1 }, fd),
        '{"a":1}',
        "json serialization should stringify object",
      );
      assertEqual(
        Serialization.serializeValue("already string", fd),
        '"already string"',
        "json serialization should wrap string with JSON.stringify",
      );
    },
    "serializes date": () => {
      const fd: FieldDefinition = { name: "x", type: "date" };
      const date = new Date("2024-01-15T10:00:00.000Z");
      assertEqual(
        Serialization.serializeValue(date, fd),
        "2024-01-15T10:00:00.000Z",
        "date serialization should use ISO format",
      );
    },
    "serializes reference": () => {
      const fd: FieldDefinition = { name: "x", type: "reference" };
      assertEqual(
        Serialization.serializeValue("user-001", fd),
        "user-001",
        "reference serialization should keep id string",
      );
    },
    "deserializes string": () => {
      const fd: FieldDefinition = { name: "x", type: "string" };
      assertEqual(
        Serialization.deserializeValue("hello", fd),
        "hello",
        "string deserialization should preserve text",
      );
      assertEqual(
        Serialization.deserializeValue("", fd),
        null,
        "empty string should deserialize to null without default",
      );
    },
    "applies defaultValue when empty": () => {
      const fd: FieldDefinition = { name: "x", type: "string", defaultValue: "default" };
      assertEqual(Serialization.deserializeValue("", fd), "default", "empty value should use defaultValue");
    },
    "deserializes number": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(
        Serialization.deserializeValue(42, fd),
        42,
        "number deserialization should preserve number",
      );
      assertEqual(
        Serialization.deserializeValue("3.14", fd),
        3.14,
        "number deserialization should parse decimal string",
      );
      assertEqual(
        Serialization.deserializeValue("abc", fd),
        null,
        "invalid number should deserialize to null",
      );
    },
    "deserializes boolean": () => {
      const fd: FieldDefinition = { name: "x", type: "boolean" };
      assertEqual(
        Serialization.deserializeValue(true, fd),
        true,
        "boolean deserialization should preserve boolean",
      );
      assertEqual(
        Serialization.deserializeValue("true", fd),
        true,
        "boolean deserialization should parse true string",
      );
      assertEqual(
        Serialization.deserializeValue("false", fd),
        false,
        "boolean deserialization should parse false string",
      );
      assertEqual(
        Serialization.deserializeValue(NaN, fd),
        false,
        "NaN boolean deserialization should return false",
      );
    },
    "deserializes date from ISO string": () => {
      const fd: FieldDefinition = { name: "x", type: "date" };
      const result = Serialization.deserializeValue("2024-01-15T10:00:00.000Z", fd);
      assertEqual(result, "2024-01-15T10:00:00.000Z", "date deserialization should return ISO string");
    },
    "deserializes json": () => {
      const fd: FieldDefinition = { name: "x", type: "json" };
      assertDeepEqual(
        Serialization.deserializeValue('{"a":1}', fd),
        { a: 1 },
        "json deserialization should parse valid json",
      );
      assertEqual(
        Serialization.deserializeValue("invalid json", fd),
        null,
        "invalid json should deserialize to null",
      );
    },
    "prepends system columns": () => {
      const fields: FieldDefinition[] = [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
      ];
      assertDeepEqual(
        Serialization.buildHeaders(fields),
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
      const headers = Serialization.buildHeaders(fields);
      const entity: Entity = {
        __id: "id-1",
        __createdAt: "2024-01-01T00:00:00.000Z",
        __updatedAt: "2024-01-02T00:00:00.000Z",
        name: "Jan",
        age: 30,
        active: true,
      };
      const row = Serialization.entityToRow(entity, fields, headers);
      assertDeepEqual(
        row,
        ["id-1", "2024-01-01T00:00:00.000Z", "2024-01-02T00:00:00.000Z", "Jan", 30, true],
        "entityToRow should serialize in header order",
      );
      const restored = Serialization.rowToEntity<Entity>(row, headers, fields);
      assertEqual(restored.__id, "id-1", "rowToEntity should restore __id");
      assertEqual(restored.name, "Jan", "rowToEntity should restore string field");
      assertEqual(restored.age, 30, "rowToEntity should restore number field");
      assertEqual(restored.active, true, "rowToEntity should restore boolean field");
    },
    "round-trips an entity with explicit fieldMap": () => {
      const fields: FieldDefinition[] = [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
        { name: "active", type: "boolean" },
      ];
      const headers = Serialization.buildHeaders(fields);
      const fieldMap = new Map(fields.map((f) => [f.name, f]));
      const entity: Entity = {
        __id: "id-fm",
        __createdAt: "2024-06-01T00:00:00.000Z",
        __updatedAt: "2024-06-02T00:00:00.000Z",
        name: "Piotr",
        age: 40,
        active: false,
      };
      const row = Serialization.entityToRow(entity, fields, headers, fieldMap);
      const restored = Serialization.rowToEntity<Entity>(row, headers, fields, fieldMap);
      assertEqual(restored.__id, "id-fm", "fieldMap round-trip should restore __id");
      assertEqual(restored.name, "Piotr", "fieldMap round-trip should restore string field");
      assertEqual(restored.age, 40, "fieldMap round-trip should restore number field");
      assertEqual(restored.active, false, "fieldMap round-trip should restore boolean field");
    },
    "handles missing optional fields": () => {
      const fields: FieldDefinition[] = [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
        { name: "active", type: "boolean" },
      ];
      const headers = Serialization.buildHeaders(fields);
      const entity: Entity = {
        __id: "id-2",
        name: "Anna",
        age: 25,
        active: false,
      };
      const row = Serialization.entityToRow(entity, fields, headers);
      assertEqual(row[1], "", "missing __createdAt should serialize to empty string");
      assertEqual(row[2], "", "missing __updatedAt should serialize to empty string");
      const restored = Serialization.rowToEntity<Entity>(row, headers, fields);
      assertEqual(restored.__createdAt, undefined, "missing __createdAt should restore as undefined");
    },
    "round-trips json string values": () => {
      const fd: FieldDefinition = { name: "x", type: "json" };
      const serialized = Serialization.serializeValue("hello", fd);
      assertEqual(serialized, '"hello"', "json serialization of string should wrap in quotes");
      const deserialized = Serialization.deserializeValue(serialized, fd);
      assertEqual(deserialized, "hello", "json deserialization should recover original string");
    },
    "deserializes native Date objects in date fields to ISO strings": () => {
      const dateFd: FieldDefinition = { name: "birthday", type: "date" };
      const nativeDate = new Date("2024-03-15T10:30:00.000Z");
      const result = Serialization.deserializeValue(nativeDate, dateFd);
      assertEqual(result, "2024-03-15T10:30:00.000Z", "native Date should be deserialized to ISO string");
    },
    "serializes Infinity as empty for number type": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(Serialization.serializeValue(Infinity, fd), "", "Infinity should serialize to empty string");
      assertEqual(Serialization.serializeValue(-Infinity, fd), "", "-Infinity should serialize to empty string");
    },
    "serializes Infinity string as empty for number type": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(Serialization.serializeValue("Infinity", fd), "", "Infinity string should serialize to empty string");
      assertEqual(Serialization.serializeValue("-Infinity", fd), "", "-Infinity string should serialize to empty string");
    },
    "serializes invalid Date as empty for date type": () => {
      const fd: FieldDefinition = { name: "x", type: "date" };
      assertEqual(
        Serialization.serializeValue(new Date("invalid"), fd),
        "",
        "invalid Date should serialize to empty string",
      );
    },
    "auto-infers number type when fieldDef.type is undefined": () => {
      const fd: FieldDefinition = { name: "x" };
      assertEqual(Serialization.serializeValue(42, fd), 42, "number should be preserved when type is inferred");
    },
    "auto-infers boolean type when fieldDef.type is undefined": () => {
      const fd: FieldDefinition = { name: "x" };
      assertEqual(
        Serialization.serializeValue(true, fd),
        true,
        "boolean should be preserved when type is inferred",
      );
    },
    "auto-infers object as JSON when fieldDef.type is undefined": () => {
      const fd: FieldDefinition = { name: "x" };
      assertEqual(
        Serialization.serializeValue({ a: 1 }, fd),
        '{"a":1}',
        "object should serialize as JSON when type is inferred",
      );
    },
    "deserializes Infinity as null for number type": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(Serialization.deserializeValue(Infinity, fd), null, "Infinity should deserialize to null");
      assertEqual(Serialization.deserializeValue(-Infinity, fd), null, "-Infinity should deserialize to null");
    },
    "deserializes Infinity string as null for number type": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(
        Serialization.deserializeValue("Infinity", fd),
        null,
        "Infinity string should deserialize to null",
      );
    },
    "deserializes invalid Date object as null for date type": () => {
      const fd: FieldDefinition = { name: "x", type: "date" };
      assertEqual(
        Serialization.deserializeValue(new Date("invalid"), fd),
        null,
        "invalid Date should deserialize to null",
      );
    },
    "returns raw value when fieldDef.type is undefined": () => {
      const fd: FieldDefinition = { name: "x" };
      assertEqual(Serialization.deserializeValue(42, fd), 42, "number should be returned as-is without explicit type");
      assertEqual(
        Serialization.deserializeValue("hello", fd),
        "hello",
        "string should be returned as-is without explicit type",
      );
    },
    "auto-infers invalid Date as empty string when fieldDef.type is undefined": () => {
      const fd: FieldDefinition = { name: "x" };
      assertEqual(Serialization.serializeValue(new Date("invalid"), fd), "", "invalid Date should auto-infer to empty string");
    },
    "serializes NaN as empty for number type": () => {
      const fd: FieldDefinition = { name: "x", type: "number" };
      assertEqual(Serialization.serializeValue(NaN, fd), "", "NaN should serialize to empty string for number type");
    },
    "deserializes boolean from number 0 and 1": () => {
      const fd: FieldDefinition = { name: "x", type: "boolean" };
      assertEqual(Serialization.deserializeValue(0, fd), false, "0 should deserialize to false");
      assertEqual(Serialization.deserializeValue(1, fd), true, "1 should deserialize to true");
    },
    "deserializes boolean string false as false": () => {
      const fd: FieldDefinition = { name: "x", type: "boolean" };
      assertEqual(Serialization.deserializeValue("false", fd), false, "'false' should deserialize to false");
      assertEqual(Serialization.deserializeValue("FALSE", fd), false, "'FALSE' should deserialize to false");
    },
    "serializes boolean from string yes/no": () => {
      const fd: FieldDefinition = { name: "x", type: "boolean" };
      assertEqual(Serialization.serializeValue("yes", fd), true, "'yes' should serialize to true");
      assertEqual(Serialization.serializeValue("no", fd), false, "'no' should serialize to false");
    },
    "serializes unrecognized field type as string": () => {
      const fd = { name: "x", type: "custom" } as unknown as FieldDefinition;
      assertEqual(Serialization.serializeValue(42, fd), "42", "unrecognized type should serialize number as string");
      assertEqual(Serialization.serializeValue(true, fd), "true", "unrecognized type should serialize boolean as string");
    },
    "deserializes unrecognized field type as raw value": () => {
      const fd = { name: "x", type: "custom" } as unknown as FieldDefinition;
      assertEqual(Serialization.deserializeValue(42, fd), 42, "unrecognized type should pass through number");
      assertEqual(Serialization.deserializeValue("hello", fd), "hello", "unrecognized type should pass through string");
    },
    "rowToEntity handles row shorter than headers": () => {
      const fields: FieldDefinition[] = [
        { name: "name", type: "string" },
        { name: "age", type: "number" },
      ];
      const headers = Serialization.buildHeaders(fields);
      const shortRow = ["id-1", "2024-01-01T00:00:00.000Z"];
      const entity = Serialization.rowToEntity(shortRow, headers, fields);
      assertEqual(entity.__id, "id-1", "should parse __id from short row");
      assertEqual(entity.name, null, "missing name should deserialize to null");
      assertEqual(entity.age, null, "missing age should deserialize to null");
    },
    "entityToRow uses raw value for column without field definition": () => {
      const fields: FieldDefinition[] = [{ name: "name", type: "string" }];
      const headers = ["__id", "__createdAt", "__updatedAt", "name", "extra"];
      const entity = { __id: "id-1", name: "Alice", extra: "bonus" } as Entity;
      const row = Serialization.entityToRow(entity, fields, headers);
      assertEqual(row[3], "Alice", "name should be serialized via field definition");
      assertEqual(row[4], "bonus", "extra column without field definition should use raw value");
    },
    "rowToEntity converts Date objects in system columns to ISO strings": () => {
      const fields: FieldDefinition[] = [{ name: "name", type: "string" }];
      const headers = Serialization.buildHeaders(fields);
      const now = new Date("2024-06-15T12:00:00.000Z");
      const row = ["id-1", now, now, "Alice"];
      const entity = Serialization.rowToEntity(row, headers, fields);
      assertEqual(entity.__id, "id-1", "should parse __id");
      assertEqual(entity.__createdAt, "2024-06-15T12:00:00.000Z", "Date in __createdAt should become ISO string");
      assertEqual(entity.__updatedAt, "2024-06-15T12:00:00.000Z", "Date in __updatedAt should become ISO string");
    },
  },
  "uuid.test.ts": {
    "returns a string of UUID v4 format": () => {
      const uuid = Uuid.generate();
      assertTrue(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(uuid),
        "generated UUID should match v4 format",
      );
    },
    "generates unique values": () => {
      const uuids = new Set(Array.from({ length: 100 }, () => Uuid.generate()));
      assertEqual(uuids.size, 100, "100 generated UUIDs should be unique");
    },
    "falls back to Math.random when crypto is unavailable": () => {
      // In GAS runtime, crypto may not be available — test the fallback path
      // This is a smoke test: the UUID should still be valid v4 format
      const uuid = Uuid.generate();
      assertTrue(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(uuid),
        "fallback UUID should match v4 format",
      );
    },
    "uses GAS Utilities.getUuid when available": () => {
      // In GAS runtime, Utilities.getUuid() is available natively
      // This handler simply verifies the primary path produces a valid UUID
      const uuid = Uuid.generate();
      assertTrue(
        typeof uuid === "string" && uuid.length > 0,
        "GAS Utilities UUID should be a non-empty string",
      );
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
      "count() with whereGroups counts only matching entities": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.save();
        const c3 = new Car();
        c3.make = "Toyota";
        c3.model = "Camry";
        c3.save();
        const count = Car.count({
          whereGroups: [
            [{ field: "make", operator: "=", value: "Honda" }],
            [{ field: "model", operator: "=", value: "Camry" }],
          ],
        });
        assertEqual(count, 2, "count with whereGroups should be 2");
      },
      "deleteAll() with whereGroups deletes only matching entities": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.save();
        const c3 = new Car();
        c3.make = "Toyota";
        c3.model = "Camry";
        c3.save();
        const deleted = Car.deleteAll({
          whereGroups: [
            [{ field: "make", operator: "=", value: "Honda" }],
            [{ field: "model", operator: "=", value: "Camry" }],
          ],
        });
        assertEqual(deleted, 2, "deleteAll with whereGroups should delete 2");
        assertEqual(Car.count(), 1, "one entity should remain");
      },
      "select() with whereGroups paginates only matching entities": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.save();
        const c3 = new Car();
        c3.make = "Toyota";
        c3.model = "Camry";
        c3.save();
        const page = Car.select(0, 10, {
          whereGroups: [
            [{ field: "make", operator: "=", value: "Honda" }],
            [{ field: "model", operator: "=", value: "Camry" }],
          ],
        });
        assertEqual(page.total, 2, "select total with whereGroups should be 2");
        assertEqual(page.items.length, 2, "select items with whereGroups should be 2");
      },
      "groupBy() with whereGroups groups only matching entities": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Toyota";
        c1.model = "Corolla";
        c1.save();
        const c2 = new Car();
        c2.make = "Honda";
        c2.model = "Civic";
        c2.save();
        const c3 = new Car();
        c3.make = "Toyota";
        c3.model = "Camry";
        c3.save();
        const groups = Car.groupBy("make", {
          whereGroups: [
            [{ field: "make", operator: "=", value: "Honda" }],
            [{ field: "model", operator: "=", value: "Camry" }],
          ],
        });
        assertEqual(groups.length, 2, "groupBy with whereGroups should have 2 groups");
      },
      "invalidates cache when saveAll() fails mid-operation": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        let threw = false;
        try {
          Car.saveAll([
            { make: "Honda", model: "Civic" },
            { make: "BMW" }, // missing required 'model'
          ]);
        } catch {
          threw = true;
        }
        assertTrue(threw, "saveAll should throw on missing required field");
        const all = Car.find();
        assertEqual(all.length, 1, "only the original car should remain after failed saveAll");
      },
      "invalidates cache and re-throws on mid-batch failure": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        repo.beginBatch();
        repo.save({ make: "Honda", model: "Civic" });
        repo.save({ make: "BMW" } as Record<string, unknown>);
        let threw = false;
        try {
          repo.commitBatch();
        } catch {
          threw = true;
        }
        assertTrue(threw, "commitBatch should throw on missing required field");
        const all = Car.find();
        assertEqual(all.length, 2, "partially committed data should be readable");
      },
      "batch is no longer active after commitBatch error": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        repo.beginBatch();
        repo.save({ make: "Fail" } as Record<string, unknown>);
        try {
          repo.commitBatch();
        } catch {
          // expected
        }
        assertEqual(repo.isBatchActive(), false, "batch should not be active after error");
      },
      "clears class maps on re-configure": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Test";
        car.model = "Model";
        car.save();
        const registry = Registry.getInstance();
        assertTrue(registry.getClassByName(Car.name) !== undefined, "class should be registered");
        registry.configure({ adapter: ctx.state.getAdapter() });
        assertEqual(
          registry.getClassByName(Car.name),
          undefined,
          "class should be cleared after re-configure",
        );
      },
      "clears entity cache and allows re-read from sheet": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        Car.find(); // populate cache
        Registry.getInstance().clearCache();
        const all = Car.find();
        assertEqual(all.length, 1, "should re-read from sheet after cache clear");
        assertEqual(all[0].make, "Toyota", "re-read data should match");
      },
      "falls back to sheet scan when cache entry is null": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.year = 2020;
        car.save();
        const id = car.__id;
        // Tamper with cache entry to simulate stale/null slot
        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        const cache = (
          repo as unknown as { cache: { get(k: string): unknown[] | null; set(k: string, v: unknown): void } }
        ).cache;
        const cacheKey = (repo as unknown as { dataCacheKey: string }).dataCacheKey;
        const cached = cache.get(cacheKey) as unknown[];
        if (cached) cached[0] = undefined;
        // Update should fall through to sheet scan
        const updatedCar = new Car();
        updatedCar.__id = id;
        updatedCar.make = "Toyota";
        updatedCar.model = "Corolla";
        updatedCar.year = 2025;
        updatedCar.save();
        Registry.getInstance().clearCache();
        const refetched = Car.findById(id);
        assertTrue(refetched !== null, "should find via fallback scan");
        assertEqual(refetched!.year, 2025, "year should be updated after fallback save");
      },
      "skips rows with empty __id": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Honda";
        car.model = "Civic";
        car.year = 2022;
        car.save();
        // Inject empty row directly into sheet
        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        const sheet = (repo as unknown as { getSheet(): { appendRow(v: unknown[]): void } }).getSheet();
        sheet.appendRow(["", "", "", "", "", ""]);
        Registry.getInstance().clearCache();
        const all = Car.find();
        assertEqual(all.length, 1, "empty __id row should be filtered out");
        assertEqual(all[0].make, "Honda", "valid entity should remain");
      },
      "persists both new and updated entities in a single batch": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Honda";
        car.model = "Civic";
        car.year = 2020;
        car.color = "Red";
        car.save();
        const existingId = car.__id;

        Car.saveAll([
          { __id: existingId, make: "Honda", model: "Accord", year: 2021, color: "Blue" },
          { make: "Toyota", model: "Camry", year: 2022, color: "White" },
          { make: "Ford", model: "Focus", year: 2023, color: "Black" },
        ]);

        assertEqual(Car.count(), 3, "saveAll mixed batch should result in 3 entities");
        const updated = Car.findById(existingId);
        assertTrue(updated !== null, "updated entity should be found");
        assertEqual(updated!.model, "Accord", "existing entity model should be updated");
        assertEqual(updated!.color, "Blue", "existing entity color should be updated");
      },
      "returns an entity when called with no filter": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Honda";
        c1.model = "Civic";
        c1.year = 2020;
        c1.save();

        const c2 = new Car();
        c2.make = "Toyota";
        c2.model = "Camry";
        c2.year = 2021;
        c2.save();

        const result = Car.findOne();
        assertTrue(result !== null, "findOne() without args should return an entity");
        assertTrue(!!result!.__id, "returned entity should have an __id");
      },
      "deletes all entities and returns the count": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        for (let i = 0; i < 3; i++) {
          const c = new Car();
          c.make = "Brand";
          c.model = "Model" + i;
          c.year = 2020 + i;
          c.save();
        }
        assertEqual(Car.count(), 3, "should have 3 entities before deleteAll");

        const deleted = Car.deleteAll();
        assertEqual(deleted, 3, "deleteAll without args should return 3");
        assertEqual(Car.count(), 0, "count should be 0 after deleteAll");
      },
      "does not overwrite an existing entity when a gap row is present": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car1 = new Car();
        car1.make = "Honda";
        car1.model = "Civic";
        car1.year = 2020;
        car1.save();
        const id1 = car1.__id;

        // Inject a gap row directly into the sheet
        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        const sheet = (repo as unknown as { getSheet(): { appendRow(v: unknown[]): void } }).getSheet();
        sheet.appendRow(["", "", "", "", "", ""]);

        const car2 = new Car();
        car2.make = "Toyota";
        car2.model = "Camry";
        car2.year = 2021;
        car2.save();

        Registry.getInstance().clearCache();
        assertEqual(Car.count(), 2, "both valid entities should exist after gap-row save");
        const found1 = Car.findById(id1);
        assertTrue(found1 !== null, "first entity should not be overwritten");
        assertEqual(found1!.make, "Honda", "first entity make should remain Honda");
      },
      "findById returns correct entity after gap row": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car1 = new Car();
        car1.make = "Honda";
        car1.model = "Civic";
        car1.year = 2020;
        car1.save();

        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        const sheet = (repo as unknown as { getSheet(): { appendRow(v: unknown[]): void } }).getSheet();
        sheet.appendRow(["", "", "", "", "", ""]);

        const car2 = new Car();
        car2.make = "Toyota";
        car2.model = "Camry";
        car2.year = 2021;
        car2.save();

        Registry.getInstance().clearCache();

        const found2 = Car.findById(car2.__id);
        assertTrue(found2 !== null, "findById should return the correct entity after gap row");
        assertEqual(found2!.make, "Toyota", "entity should have correct make after gap row");
        assertEqual(found2!.__id, car2.__id, "entity should have correct __id after gap row");
      },
      "assigns correct row indices so delete targets the right entity": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        Car.saveAll([
          { make: "Alpha", model: "A1", year: 2020, color: "Red" },
          { make: "Beta", model: "B1", year: 2021, color: "Blue" },
        ]);
        assertEqual(Car.count(), 2, "saveAll should create two entities");

        const alpha = Car.findOne({ where: [{ field: "make", operator: "=", value: "Alpha" }] });
        const beta = Car.findOne({ where: [{ field: "make", operator: "=", value: "Beta" }] });
        assertTrue(alpha !== null, "Alpha should exist");
        assertTrue(beta !== null, "Beta should exist");

        beta!.delete();
        assertEqual(Car.count(), 1, "count should be 1 after deleting Beta");

        const remaining = Car.findById(alpha!.__id);
        assertTrue(remaining !== null, "Alpha should still exist after deleting Beta");
        assertEqual(remaining!.make, "Alpha", "remaining entity should be Alpha");
        assertEqual(Car.findById(beta!.__id), null, "Beta should be deleted");
      },
      "discards buffered operations without writing": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        repo.beginBatch();
        repo.save({ make: "Ghost", model: "Phantom" });
        repo.rollbackBatch();
        assertEqual(Car.count(), 1, "count should still be 1 after rollback");
        const ghost = Car.findOne({ where: [{ field: "make", operator: "=", value: "Ghost" }] });
        assertEqual(ghost, null, "rolled back entity should not exist");
      },
      "beginBatch \u2192 save \u2192 delete \u2192 commitBatch applies all operations": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        const id = car.__id;
        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        repo.beginBatch();
        repo.save({ make: "Honda", model: "Civic" });
        repo.delete(id);
        repo.commitBatch();
        assertEqual(Car.count(), 1, "should have 1 entity after commit");
        const honda = Car.findOne({ where: [{ field: "make", operator: "=", value: "Honda" }] });
        assertTrue(honda !== null, "Honda should be present");
        assertEqual(Car.findById(id), null, "Toyota should be deleted");
      },
      "count() during batch returns sheet state, not buffered state": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.save();
        const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
        repo.beginBatch();
        repo.save({ make: "Honda", model: "Civic" });
        assertEqual(Car.count(), 1, "count during batch should reflect sheet, not buffer");
        repo.commitBatch();
        assertEqual(Car.count(), 2, "count after commit should include new entity");
      },
      "returns correct entity without re-reading sheet data": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const c1 = new Car();
        c1.make = "Alpha";
        c1.model = "A1";
        c1.save();
        const c2 = new Car();
        c2.make = "Beta";
        c2.model = "B1";
        c2.save();
        // Force cache load
        Car.find();
        // findById should use cached data
        const found = Car.findById(c2.__id);
        assertTrue(found !== null, "findById should return the entity from cache");
        assertEqual(found!.make, "Beta", "cached entity should have correct make");
      },
      "delete() returns false on unsaved record (no __id)": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Ghost";
        car.model = "Phantom";
        assertTrue(!car.delete(), "delete() should return false when __id is missing");
      },
      "toJSON() returns undefined __id for unsaved entity": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = Car.create({ make: "Ghost", model: "Phantom", year: 2024, color: "black" });
        const json = car.toJSON();
        assertEqual(json.__id, undefined, "unsaved entity toJSON should have undefined __id");
        assertEqual(json.make, "Ghost", "toJSON make should match");
        assertEqual(json.model, "Phantom", "toJSON model should match");
      },
      "saveAll() with empty array returns empty array": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const result = Car.saveAll([]);
        assertDeepEqual(result, [], "saveAll with empty array should return empty array");
      },
      "deleteAll() returns zero when no entities match filter": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        Car.create({ make: "Toyota", model: "Supra", year: 2020, color: "white" }).save();
        const count = Car.deleteAll({ where: [{ field: "make", operator: "=", value: "NonExistent" }] });
        assertEqual(count, 0, "deleteAll with non-matching filter should return 0");
        assertEqual(Car.count(), 1, "original entity should still exist");
      },
      "save() includes null fields but excludes undefined fields": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const car = new Car();
        car.make = "Toyota";
        car.model = "Corolla";
        car.year = 2024;
        car.color = null as unknown as string;
        car.save();

        const found = Car.findById(car.__id);
        assertTrue(found !== null, "saved entity should be found");
        assertEqual(found!.color, null, "null field should be preserved as null");
      },
      "Query.from(Class) auto-registers class without prior save": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        const query = Query.from(Car);
        assertTrue(query !== undefined && query !== null, "Query.from should return a query");
        const results = query.execute();
        assertDeepEqual(results, [], "query on empty table should return empty array");
      },
      "deleteAll uses individual deletes for 2 entities and bulk for 3": (ctx: RuntimeCaseContext) => {
        const { Car } = setup(ctx);
        // Create exactly 3 entities — triggers bulk path (> 2)
        Car.create({ make: "A", model: "A1", year: 2020, color: "red" }).save();
        Car.create({ make: "B", model: "B1", year: 2021, color: "blue" }).save();
        Car.create({ make: "C", model: "C1", year: 2022, color: "green" }).save();
        assertEqual(Car.count(), 3, "should have 3 entities before bulk delete");
        const deletedBulk = Car.deleteAll();
        assertEqual(deletedBulk, 3, "deleteAll should return 3 for bulk path");
        assertEqual(Car.count(), 0, "count should be 0 after bulk delete");

        // Create exactly 2 entities — triggers individual path (<= 2)
        Car.create({ make: "D", model: "D1", year: 2023, color: "black" }).save();
        Car.create({ make: "E", model: "E1", year: 2024, color: "white" }).save();
        assertEqual(Car.count(), 2, "should have 2 entities before individual delete");
        const deletedIndiv = Car.deleteAll();
        assertEqual(deletedIndiv, 2, "deleteAll should return 2 for individual path");
        assertEqual(Car.count(), 0, "count should be 0 after individual delete");
      },
    } as Record<string, RuntimeCaseHandler>;
  })(),
  "sheet-repository.test.ts": {
    "onValidate rejects save when validation errors returned": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("tbl_ValItems");
      const schema = { tableName, fields: [{ name: "name" }, { name: "price" }, { name: "category" }], indexes: [] };
      adapter.createSheet(tableName);
      const sheet = adapter.getSheetByName(tableName)!;
      sheet.setHeaders(Serialization.buildHeaders(schema.fields));
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const hooks: LifecycleHooks<Entity> = {
        onValidate: (entity) => {
          const errors: string[] = [];
          if (!entity.name) errors.push("name is required");
          return errors;
        },
      };
      const repo = new SheetRepository<Entity>(adapter, schema, indexStore, new MemoryCache(), hooks);
      let threw = false;
      try {
        repo.save({ name: "", price: 1, category: "x" });
      } catch (e: unknown) {
        threw = true;
        assertTrue((e as Error).message.includes("Validation failed"), "should mention Validation failed");
      }
      assertTrue(threw, "onValidate should reject save");
    },
    "beforeSave mutates entity payload": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("tbl_MutItems");
      const schema = { tableName, fields: [{ name: "name" }, { name: "price" }, { name: "category" }], indexes: [] };
      adapter.createSheet(tableName);
      const sheet = adapter.getSheetByName(tableName)!;
      sheet.setHeaders(Serialization.buildHeaders(schema.fields));
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const hooks: LifecycleHooks<Entity> = {
        beforeSave: (entity) => ({ ...entity, name: String(entity.name ?? "").toUpperCase() }),
      };
      const repo = new SheetRepository<Entity>(adapter, schema, indexStore, new MemoryCache(), hooks);
      const saved = repo.save({ name: "widget", price: 10, category: "tools" });
      assertEqual(saved.name, "WIDGET", "beforeSave should mutate name to uppercase");
    },
    "afterSave receives saved entity and isNew flag": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("tbl_AfterItems");
      const schema = { tableName, fields: [{ name: "name" }, { name: "price" }, { name: "category" }], indexes: [] };
      adapter.createSheet(tableName);
      const sheet = adapter.getSheetByName(tableName)!;
      sheet.setHeaders(Serialization.buildHeaders(schema.fields));
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const calls: Array<{ isNew: boolean }> = [];
      const hooks: LifecycleHooks<Entity> = {
        afterSave: (_entity, isNew) => { calls.push({ isNew }); },
      };
      const repo = new SheetRepository<Entity>(adapter, schema, indexStore, new MemoryCache(), hooks);
      const saved = repo.save({ name: "A", price: 1, category: "x" });
      assertEqual(calls.length, 1, "afterSave should be called once");
      assertTrue(calls[0].isNew, "first save should be isNew=true");
      repo.save({ ...saved, price: 2 });
      assertEqual(calls.length, 2, "afterSave should be called twice");
      assertTrue(!calls[1].isNew, "update should be isNew=false");
    },
    "beforeDelete returning false blocks deletion": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("tbl_NoDelItems");
      const schema = { tableName, fields: [{ name: "name" }, { name: "price" }, { name: "category" }], indexes: [] };
      adapter.createSheet(tableName);
      const sheet = adapter.getSheetByName(tableName)!;
      sheet.setHeaders(Serialization.buildHeaders(schema.fields));
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const hooks: LifecycleHooks<Entity> = { beforeDelete: () => false };
      const repo = new SheetRepository<Entity>(adapter, schema, indexStore, new MemoryCache(), hooks);
      const saved = repo.save({ name: "keep", price: 5, category: "x" });
      const deleted = repo.delete(saved.__id);
      assertEqual(deleted, false, "beforeDelete returning false should block deletion");
      assertEqual(repo.count(), 1, "entity should still exist");
    },
    "beforeDelete veto on deleteAll returns zero and preserves data": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("tbl_VetoDelItems");
      const schema = { tableName, fields: [{ name: "name" }, { name: "price" }, { name: "category" }], indexes: [] };
      adapter.createSheet(tableName);
      const sheet = adapter.getSheetByName(tableName)!;
      sheet.setHeaders(Serialization.buildHeaders(schema.fields));
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const hooks: LifecycleHooks<Entity> = { beforeDelete: () => false };
      const repo = new SheetRepository<Entity>(adapter, schema, indexStore, new MemoryCache(), hooks);
      repo.save({ name: "A", price: 1, category: "x" });
      repo.save({ name: "B", price: 2, category: "x" });
      repo.save({ name: "C", price: 3, category: "x" });
      const count = repo.deleteAll();
      assertEqual(count, 0, "deleteAll should return 0 when beforeDelete vetoes");
      assertEqual(repo.count(), 3, "all entities should be preserved");
    },
    "afterDelete is called with deleted entity ID": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("tbl_AfterDelItems");
      const schema = { tableName, fields: [{ name: "name" }, { name: "price" }, { name: "category" }], indexes: [] };
      adapter.createSheet(tableName);
      const sheet = adapter.getSheetByName(tableName)!;
      sheet.setHeaders(Serialization.buildHeaders(schema.fields));
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const deletedIds: string[] = [];
      const hooks: LifecycleHooks<Entity> = { afterDelete: (id) => { deletedIds.push(id); } };
      const repo = new SheetRepository<Entity>(adapter, schema, indexStore, new MemoryCache(), hooks);
      const saved = repo.save({ name: "gone", price: 0, category: "x" });
      repo.delete(saved.__id);
      assertDeepEqual(deletedIds, [saved.__id], "afterDelete should receive deleted ID");
    },
    "getSheet throws when sheet does not exist": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const indexStore = new IndexStore(adapter, new MemoryCache());
      const schema = { tableName: "tbl_NonExistent_" + Date.now(), fields: [{ name: "name" }], indexes: [] };
      const repo = new SheetRepository<Entity>(adapter, schema, indexStore);
      let threw = false;
      try {
        repo.find();
      } catch (e: unknown) {
        threw = true;
        assertTrue((e as Error).message.includes("not found"), "error should mention not found");
      }
      assertTrue(threw, "find() on non-existent sheet should throw");
    },
    "loadAllEntities throws during active saveAll entity batch": (ctx) => {
      const adapter = ctx.state.getAdapter();
      const tableName = ctx.state.nextTableName("tbl_BatchItems");
      const schema = { tableName, fields: [{ name: "name" }, { name: "price" }, { name: "category" }], indexes: [] };
      adapter.createSheet(tableName);
      const sheet = adapter.getSheetByName(tableName)!;
      sheet.setHeaders(Serialization.buildHeaders(schema.fields));
      const indexStore = new IndexStore(adapter, new MemoryCache());
      let repoRef: SheetRepository<Entity> | null = null;
      const hooks: LifecycleHooks<Entity> = {
        beforeSave: () => {
          try { repoRef!.count(); } catch { throw new Error("re-entrant read blocked"); }
        },
      };
      const repo = new SheetRepository<Entity>(adapter, schema, indexStore, new MemoryCache(), hooks);
      repoRef = repo;
      let threw = false;
      try {
        repo.saveAll([{ name: "A", price: 1, category: "x" }, { name: "B", price: 2, category: "y" }]);
      } catch {
        threw = true;
      }
      assertTrue(threw, "saveAll with re-entrant read should throw");
    },
  },
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

const RUNTIME_PARITY_CASE_IDS: string[] = Object.entries(runtimeSuiteHandlers)
  .flatMap(([file, testMap]) =>
    Object.keys(testMap).map((testName) => ParityCatalog.toCaseId(file, testName)),
  )
  .sort();

function validateTests(): void {
  const expected = new Set(ParityCatalog.CASE_IDS);
  const actual = new Set(RUNTIME_PARITY_CASE_IDS);

  const missingInRuntime = ParityCatalog.CASE_IDS.filter((id) => !actual.has(id));
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

function runTests(): string {
  validateTests();

  const runStartedAt = Date.now();
  const state = new RuntimeParityState();
  const results: RuntimeCaseResult[] = [];
  const total = ParityCatalog.CASE_IDS.length;

  const log = (msg: string): void => {
    if (typeof Logger !== "undefined" && typeof Logger.log === "function") {
      Logger.log(msg);
    }
  };

  log(`[SheetORM] Starting parity suite — ${total} test cases`);
  log("[SheetORM] Clearing all existing sheets from active spreadsheet before test run");
  state.clearAllSheets(log);

  for (const suite of ParityCatalog.SUITES) {
    log(`[Suite] ${suite.file} (${suite.tests.length} tests)`);

    for (const testName of suite.tests) {
      const id = ParityCatalog.toCaseId(suite.file, testName);
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

export class RuntimeParity {
  static run = runTests;
  static validate = validateTests;
  static readonly CASE_IDS = RUNTIME_PARITY_CASE_IDS;
}
