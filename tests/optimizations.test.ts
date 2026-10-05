/**
 * Focused tests for the deep-optimisation pass:
 *   1. count() without full deserialization
 *   2. QueryEngine selectivity ordering + limit early-exit
 *   3. Sparse dirty-column updates (updateRowSparse)
 *   4. Adaptive delete strategy (batched deleteRows vs replaceAllData)
 *   5. GasCacheProvider gzip shards
 *   6. Persisted derived structures (dix: row-index map)
 *   7. Schema fingerprints (PropertiesService / local fallback)
 *   8. warmUpSheets graceful fallback without UrlFetchApp
 *   9. Index-driven equality query planning (sparse row reads)
 */

import { SheetRepository } from "../src/core/SheetRepository";
import { IndexStore } from "../src/index/IndexStore";
import { MemoryCache } from "../src/core/cache/MemoryCache";
import { GasCacheProvider } from "../src/core/cache/GasCacheProvider";
import { Registry } from "../src/core/Registry";
import { Record } from "../src/core/Record";
import { Decorators } from "../src/core/Decorators";
import { QueryEngine } from "../src/query/QueryEngine";
import { MockSpreadsheetAdapter } from "./MockSpreadsheetAdapter";
import { MockSheetAdapter } from "./MockSheetAdapter";
import { Serialization } from "../src/utils/Serialization";
import {
  schemaFingerprint,
  fingerprintMatches,
  writeFingerprint,
  dropFingerprint,
  _clearLocalFingerprints,
} from "../src/storage/SchemaFingerprint";
import type { Entity } from "../src/core/types/Entity";
import type { TableSchema } from "../src/core/types/TableSchema";
import type { Filter } from "../src/core/types/Filter";

const { Field, Indexed } = Decorators;

// ─────────────────────────────────────────────────────────────────────────────
// Shared fixtures
// ─────────────────────────────────────────────────────────────────────────────

interface Item extends Entity {
  name: string;
  price: number;
  category: string;
}

const itemSchema: TableSchema = {
  tableName: "tbl_Items",
  fields: [{ name: "name" }, { name: "price" }, { name: "category" }],
  indexes: [],
};

function createRepo(
  adapter: MockSpreadsheetAdapter,
  cache?: MemoryCache,
): { repo: SheetRepository<Item>; sheet: MockSheetAdapter } {
  const sheet = adapter.createSheet(itemSchema.tableName) as MockSheetAdapter;
  const indexStore = new IndexStore(adapter);
  const repo = new SheetRepository<Item>(adapter, itemSchema, indexStore, cache ?? new MemoryCache());
  sheet.setHeaders(Serialization.buildHeaders(itemSchema.fields));
  return { repo, sheet };
}

/** In-memory CacheService stub (mirrors gas-cache-provider.test.ts). */
class FakeGasCache {
  store = new Map<string, { v: string; exp: number }>();
  get(key: string) {
    const e = this.store.get(key);
    return e && Date.now() < e.exp ? e.v : null;
  }
  getAll(keys: string[]) {
    const out: { [k: string]: string } = {};
    for (const k of keys) {
      const v = this.get(k);
      if (v !== null) out[k] = v;
    }
    return out;
  }
  put(key: string, value: string, sec?: number) {
    this.store.set(key, { v: value, exp: Date.now() + (sec ?? 600) * 1000 });
  }
  putAll(values: { [k: string]: string }, sec?: number) {
    for (const [k, v] of Object.entries(values)) this.put(k, v, sec);
  }
  remove(key: string) {
    this.store.delete(key);
  }
  removeAll(keys: string[]) {
    for (const k of keys) this.store.delete(k);
  }
}

function bindFakeCacheService(): FakeGasCache {
  const fake = new FakeGasCache();
  (globalThis as { CacheService?: unknown }).CacheService = {
    getScriptCache: () => fake,
    getUserCache: () => fake,
    getDocumentCache: () => fake,
  };
  return fake;
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. count() without deserialization
// ─────────────────────────────────────────────────────────────────────────────

describe("count() fast paths", () => {
  let adapter: MockSpreadsheetAdapter;
  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
  });

  it("returns entity count from the complete row index — zero sheet reads", () => {
    const { repo, sheet } = createRepo(adapter);
    repo.saveAll([
      { name: "a", price: 1, category: "x" },
      { name: "b", price: 2, category: "y" },
      { name: "c", price: 3, category: "x" },
    ]);

    const spy = jest.spyOn(sheet, "getAllData");
    expect(repo.count()).toBe(3);
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it("filtered count still evaluates predicates correctly", () => {
    const { repo } = createRepo(adapter);
    repo.saveAll([
      { name: "a", price: 1, category: "x" },
      { name: "b", price: 2, category: "y" },
      { name: "c", price: 3, category: "x" },
    ]);
    expect(repo.count({ where: [{ field: "category", operator: "=", value: "x" }] })).toBe(2);
  });

  it("cold count falls back to the adapter row count", () => {
    const { repo, sheet } = createRepo(adapter);
    // Seed sheet directly (bypasses repo state) — physical rows exist but
    // no cache/rowIndex state.
    sheet.appendRows([
      Serialization.entityToRow(
        { __id: "i1", name: "a", price: 1, category: "x" } as Item,
        itemSchema.fields,
        sheet.getHeaders(),
      ),
    ]);
    expect(repo.count()).toBe(1);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. QueryEngine selectivity ordering + early exit
// ─────────────────────────────────────────────────────────────────────────────

describe("QueryEngine predicate ordering + early exit", () => {
  const entities: Item[] = Array.from({ length: 50 }, (_, i) => ({
    __id: `e${i}`,
    __createdAt: "t",
    __updatedAt: "t",
    name: `n${i}`,
    price: i,
    category: i % 3 === 0 ? "x" : "y",
  }));

  it("reordered AND predicates produce identical results", () => {
    const where: Filter[] = [
      { field: "name", operator: "contains", value: "n" }, // expensive, low selectivity
      { field: "category", operator: "=", value: "x" }, // cheap, selective
      { field: "price", operator: ">", value: 5 },
    ];
    const res = QueryEngine.executeQuery(entities, { where });
    const expected = entities.filter((e) => e.name.includes("n") && e.category === "x" && e.price > 5);
    expect(res).toEqual(expected);
    expect(res.length).toBe(expected.length);
  });

  it("limit without orderBy returns the FIRST matches in array order", () => {
    const res = QueryEngine.executeQuery(entities, {
      where: [{ field: "category", operator: "=", value: "x" }],
      limit: 3,
    });
    const expected = entities.filter((e) => e.category === "x").slice(0, 3);
    expect(res).toEqual(expected);
  });

  it("offset+limit without orderBy matches slice-of-filtered semantics", () => {
    const res = QueryEngine.executeQuery(entities, {
      where: [{ field: "category", operator: "=", value: "x" }],
      offset: 2,
      limit: 4,
    });
    const expected = entities.filter((e) => e.category === "x").slice(2, 6);
    expect(res).toEqual(expected);
  });

  it("whereGroups + early-exit preserve OR semantics and order", () => {
    const res = QueryEngine.executeQuery(entities, {
      whereGroups: [
        [{ field: "category", operator: "=", value: "x" }],
        [{ field: "price", operator: "=", value: 1 }],
      ],
      limit: 4,
    });
    const expected = entities.filter((e) => e.category === "x" || e.price === 1).slice(0, 4);
    expect(res).toEqual(expected);
  });

  it("sorted queries still scan fully (correctness over early exit)", () => {
    const res = QueryEngine.executeQuery(entities, {
      where: [{ field: "category", operator: "=", value: "x" }],
      orderBy: [{ field: "price", direction: "desc" }],
      limit: 2,
    });
    const expected = entities
      .filter((e) => e.category === "x")
      .sort((a, b) => b.price - a.price)
      .slice(0, 2);
    expect(res).toEqual(expected);
  });

  it("invalid limit values disable early exit gracefully", () => {
    const res = QueryEngine.executeQuery(entities, {
      where: [{ field: "category", operator: "=", value: "x" }],
      limit: NaN,
    });
    expect(res).toEqual(entities.filter((e) => e.category === "x"));
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. Sparse dirty-column updates
// ─────────────────────────────────────────────────────────────────────────────

describe("sparse dirty-column updates", () => {
  let adapter: MockSpreadsheetAdapter;
  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
  });

  it("update writes only changed cells + __updatedAt", () => {
    const { repo, sheet } = createRepo(adapter);
    const saved = repo.save({ name: "a", price: 1, category: "x" });

    const sparseSpy = jest.spyOn(sheet, "updateRowSparse");
    const fullSpy = jest.spyOn(sheet, "updateRow");

    repo.save({ ...saved, price: 42 }); // only `price` changes

    expect(sparseSpy).toHaveBeenCalled();
    expect(fullSpy).not.toHaveBeenCalled();
    const [, cells] = sparseSpy.mock.calls[0] as [number, Array<readonly [number, unknown]>];
    const cols = cells.map(([c]) => sheet.getHeaders()[c]);
    // price changed → always present; __updatedAt present unless the two
    // saves landed in the same millisecond (identical stamp → not dirty).
    expect(cols).toContain("price");
    for (const c of cols) expect(c === "price" || c === "__updatedAt").toBe(true);
    expect(cols).not.toContain("name");
    expect(cols).not.toContain("category");
    expect(cols).not.toContain("__id");
    expect(cols).not.toContain("__createdAt");

    expect(repo.findById(saved.__id)!.price).toBe(42);
    sparseSpy.mockRestore();
    fullSpy.mockRestore();
  });

  it("no-op field save writes at most __updatedAt", () => {
    const { repo, sheet } = createRepo(adapter);
    const saved = repo.save({ name: "a", price: 1, category: "x" });

    const sparseSpy = jest.spyOn(sheet, "updateRowSparse");
    repo.save({ ...saved }); // identical fields
    // Same-ms saves can even collapse __updatedAt → zero cells is valid too.
    expect(sparseSpy).toHaveBeenCalledTimes(1);
    const [, cells] = sparseSpy.mock.calls[0] as [number, Array<readonly [number, unknown]>];
    const cols = cells.map(([c]) => sheet.getHeaders()[c]);
    expect(cols.every((c) => c === "__updatedAt")).toBe(true);
    expect(cols.length).toBeLessThanOrEqual(1);
    sparseSpy.mockRestore();
  });

  it("saveAll batch updates still write full rows (span-merged)", () => {
    const { repo, sheet } = createRepo(adapter);
    const saved = repo.save({ name: "a", price: 1, category: "x" });
    const sparseSpy = jest.spyOn(sheet, "updateRowSparse");
    repo.saveAll([{ ...saved, price: 9 }]);
    expect(sparseSpy).not.toHaveBeenCalled();
    expect(repo.findById(saved.__id)!.price).toBe(9);
    sparseSpy.mockRestore();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. Adaptive delete strategy
// ─────────────────────────────────────────────────────────────────────────────

describe("adaptive delete strategy", () => {
  let adapter: MockSpreadsheetAdapter;
  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
  });

  it("sparse deletions use batched deleteRows (not replaceAllData)", () => {
    const { repo, sheet } = createRepo(adapter);
    const saved = repo.saveAll(
      Array.from({ length: 10 }, (_, i) => ({ name: `n${i}`, price: i, category: i < 8 ? "a" : "b" })),
    );
    const delSpy = jest.spyOn(sheet, "deleteRows");
    const repSpy = jest.spyOn(sheet, "replaceAllData");

    // Delete 2 of 10 → sparse
    const victims = saved.slice(-2); // category "b"
    repo.deleteAll({ where: [{ field: "category", operator: "=", value: "b" }] });

    expect(delSpy).toHaveBeenCalledTimes(1);
    expect(delSpy.mock.calls[0][0].sort((a, b) => a - b)).toEqual(
      victims.map((_, i) => 8 + i).sort((a, b) => a - b),
    );
    expect(repSpy).not.toHaveBeenCalled();
    expect(repo.count()).toBe(8);
    delSpy.mockRestore();
    repSpy.mockRestore();
  });

  it("dense deletions use replaceAllData", () => {
    const { repo, sheet } = createRepo(adapter);
    repo.saveAll(
      Array.from({ length: 10 }, (_, i) => ({ name: `n${i}`, price: i, category: i < 8 ? "a" : "b" })),
    );
    const delSpy = jest.spyOn(sheet, "deleteRows");
    const repSpy = jest.spyOn(sheet, "replaceAllData");

    // Delete 8 of 10 → dense
    repo.deleteAll({ where: [{ field: "category", operator: "=", value: "a" }] });

    expect(repSpy).toHaveBeenCalledTimes(1);
    expect(delSpy).not.toHaveBeenCalled();
    expect(repo.count()).toBe(2);
    delSpy.mockRestore();
    repSpy.mockRestore();
  });

  it("row indexes of survivors shift correctly after sparse delete", () => {
    const { repo } = createRepo(adapter);
    const saved = repo.saveAll(
      Array.from({ length: 6 }, (_, i) => ({ name: `n${i}`, price: i, category: i % 2 === 0 ? "e" : "o" })),
    );
    // Delete rows 0,2,4 (evens) — sparse: 3 < 3? equal → dense. Use 2 deletes:
    repo.deleteAll({
      where: [{ field: "name", operator: "in", value: ["n0", "n2"] }],
    });
    expect(repo.count()).toBe(4);
    // Verify findById still resolves every survivor through the row index
    for (const s of saved.slice(3)) {
      const f = repo.findById(s.__id);
      expect(f).not.toBeNull();
      expect(f!.__id).toBe(s.__id);
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. GasCacheProvider gzip
// ─────────────────────────────────────────────────────────────────────────────

describe("GasCacheProvider gzip", () => {
  // Minimal real zlib-backed Utilities mock — exercises the true round-trip.
  function bindFakeUtilities(): void {
    const zlib = jest.requireActual<typeof import("zlib")>("zlib");
    interface FakeBlob {
      getBytes(): number[];
      getDataAsString(): string;
    }
    const mk = (data: string | number[]): FakeBlob => ({
      getBytes: () => (typeof data === "string" ? [...Buffer.from(data)] : data),
      getDataAsString: () => Buffer.from(data as number[]).toString(),
    });
    (globalThis as { Utilities?: unknown }).Utilities = {
      newBlob: mk,
      gzip: (b: FakeBlob) => mk([...zlib.gzipSync(Buffer.from(b.getBytes()))]),
      ungzip: (b: FakeBlob) => mk(zlib.gunzipSync(Buffer.from(b.getBytes())).toString()),
      base64Encode: (bytes: number[]) => Buffer.from(bytes).toString("base64"),
      base64Decode: (s: string) => [...Buffer.from(s, "base64")],
    };
  }

  afterEach(() => {
    delete (globalThis as { CacheService?: unknown }).CacheService;
    delete (globalThis as { Utilities?: unknown }).Utilities;
  });

  it("compresses large payloads and round-trips them", () => {
    bindFakeCacheService();
    bindFakeUtilities();
    const c = new GasCacheProvider();
    const big = Array.from({ length: 400 }, (_, i) => ({ id: i, name: `item-${i % 10}` }));
    c.set("gz", big);
    // Fresh provider — forces a remote (decompressing) read.
    const fresh = new GasCacheProvider();
    expect(fresh.get<typeof big>("gz")).toEqual(big);
  });

  it("highly compressible large payloads stay single-envelope with z=1", () => {
    const fake = bindFakeCacheService();
    bindFakeUtilities();
    const c = new GasCacheProvider();
    const big = { blob: "ab".repeat(120 * 1024) }; // ~240 KB → ~2 KB gzipped
    c.set("big", big);
    const env = JSON.parse(fake.store.get("big")!.v) as { z?: number; v: unknown };
    expect(env.z).toBe(1);
    expect(typeof env.v).toBe("string");
    expect(new GasCacheProvider().get("big")).toEqual(big);
  });

  it("incompressible payloads still shard regardless of compression", () => {
    const fake = bindFakeCacheService();
    bindFakeUtilities();
    const c = new GasCacheProvider();
    // True entropy: ~267 KB of base64 noise — gzip cannot shrink it below
    // the shard threshold, so the sharded layout must still be used.
    const blob = jest
      .requireActual<typeof import("crypto")>("crypto")
      .randomBytes(200 * 1024)
      .toString("base64");
    const big = { blob };
    c.set("big", big);
    expect(fake.store.has("big_meta")).toBe(true);
    const meta = JSON.parse(fake.store.get("big_meta")!.v) as { c: number; z?: number };
    expect(meta.c).toBeGreaterThan(2);
    expect(new GasCacheProvider().get("big")).toEqual(big);
    fake.remove("big_p1");
    expect(new GasCacheProvider().get("big")).toBeNull();
  });

  it("small payloads stay uncompressed (no envelope z flag)", () => {
    const fake = bindFakeCacheService();
    bindFakeUtilities();
    const c = new GasCacheProvider();
    c.set("tiny", { a: 1 });
    const env = JSON.parse(fake.store.get("tiny")!.v) as { z?: number };
    expect(env.z).toBeUndefined();
  });

  it("works without Utilities (L1-only fallback keeps functioning)", () => {
    const c = new GasCacheProvider(); // no CacheService, no Utilities
    c.set("k", { x: "y".repeat(5000) });
    expect(c.get<{ x: string }>("k")?.x.length).toBe(5000);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 6+9. Persisted dix map + index-driven equality planning
// ─────────────────────────────────────────────────────────────────────────────

describe("index-driven equality planning + persisted row index", () => {
  class EqCar extends Record {
    @Field({ required: true }) make!: string;
    @Indexed({ unique: true }) vin!: string;
    @Indexed() color!: string;
  }

  beforeEach(() => {
    Registry.reset();
    Registry.getInstance().configure({ adapter: new MockSpreadsheetAdapter() });
    _clearLocalFingerprints();
  });
  afterEach(() => {
    Registry.reset();
    delete (globalThis as { CacheService?: unknown }).CacheService;
    delete (globalThis as { PropertiesService?: unknown }).PropertiesService;
  });

  it("find with = on an indexed field returns the entity", () => {
    const c1 = new EqCar();
    c1.make = "Toyota";
    c1.vin = "V1";
    c1.color = "red";
    c1.save();
    const c2 = new EqCar();
    c2.make = "Honda";
    c2.vin = "V2";
    c2.color = "blue";
    c2.save();

    const res = EqCar.find({ where: [{ field: "vin", operator: "=", value: "V1" }] });
    expect(res).toHaveLength(1);
    expect(res[0].make).toBe("Toyota");
  });

  it("sparse path never reads the entity grid (getAllData unused)", () => {
    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    const c1 = new EqCar();
    c1.make = "Toyota";
    c1.vin = "V1";
    c1.color = "red";
    c1.save();

    const entitySheet = adapter._getSheet(EqCar.tableName)!;
    const spy = jest.spyOn(entitySheet, "getAllData");

    const res = EqCar.find({ where: [{ field: "vin", operator: "=", value: "V1" }] });
    expect(res).toHaveLength(1);
    expect(res[0].__id).toBe(c1.__id);
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it("persists dix: and hydrates it on a fresh execution", () => {
    bindFakeCacheService();
    Registry.reset();
    const gasCache = new GasCacheProvider();
    const adapter = new MockSpreadsheetAdapter();
    Registry.getInstance().configure({ adapter, cache: gasCache });

    const c = new EqCar();
    c.make = "Volvo";
    c.vin = "VX";
    c.color = "gray";
    c.save();
    // Force a full scan → builds + persists dix:
    EqCar.find();

    const remote = gasCache.get<{ r: number; m: [string, number][] }>(`dix:${EqCar.tableName}`);
    expect(remote).not.toBeNull();
    expect(remote!.m.length).toBe(1);

    // Fresh "execution": new Registry + new provider (empty L1), same remote.
    Registry.reset();
    Registry.getInstance().configure({ adapter, cache: new GasCacheProvider() });
    const entitySheet = adapter._getSheet(EqCar.tableName)!;
    const spy = jest.spyOn(entitySheet, "getAllData");
    const res = EqCar.find({ where: [{ field: "vin", operator: "=", value: "VX" }] });
    expect(res).toHaveLength(1);
    expect(res[0].make).toBe("Volvo");
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it("stale dix (row count drift) falls back safely and re-scans", () => {
    const cache = new MemoryCache();
    const adapter = new MockSpreadsheetAdapter();
    Registry.reset();
    Registry.getInstance().configure({ adapter, cache });

    const c = new EqCar();
    c.make = "Volvo";
    c.vin = "VX";
    c.color = "gray";
    c.save();
    EqCar.find(); // populate dix

    // Simulate external append (row count changed) — dix must be rejected.
    const sheet = adapter._getSheet(EqCar.tableName)!;
    sheet.appendRows([["external-id", "t", "t", "Opel", "VZ", "green"]]);

    const res = EqCar.find({ where: [{ field: "vin", operator: "=", value: "VX" }] });
    expect(res).toHaveLength(1);
    expect(res[0].vin).toBe("VX");
  });

  it("in operator on indexed field unions postings", () => {
    const makes = ["A", "B", "C"];
    makes.forEach((m, i) => {
      const c = new EqCar();
      c.make = m;
      c.vin = `V${i}`;
      c.color = i < 2 ? "warm" : "cold";
      c.save();
    });
    const res = EqCar.find({ where: [{ field: "vin", operator: "in", value: ["V0", "V2"] }] });
    expect(res.map((r) => r.make).sort()).toEqual(["A", "C"]);
  });

  it("=null on indexed field bypasses the index and keeps engine semantics", () => {
    const c = new EqCar();
    c.make = "Nully";
    c.vin = "VN";
    // color left undefined → not indexed; engine `=` is strict: undefined ≠ null
    c.save();
    const res = EqCar.find({ where: [{ field: "color", operator: "=", value: null }] });
    expect(res).toHaveLength(0); // pre-existing strict-equality semantics, not a recall bug
    const res2 = EqCar.find({ where: [{ field: "color", operator: "=", value: "absent" }] });
    expect(res2).toHaveLength(0);
  });

  it("combined equality + residual filter stays correct", () => {
    [
      { make: "A", vin: "V1", color: "red" },
      { make: "B", vin: "V2", color: "red" },
      { make: "C", vin: "V3", color: "blue" },
    ].forEach((d) => {
      const c = new EqCar();
      Object.assign(c, d);
      c.save();
    });
    const res = EqCar.find({
      where: [
        { field: "color", operator: "=", value: "red" }, // indexed equality
        { field: "make", operator: "=", value: "B" }, // non-indexed residual
      ],
    });
    expect(res).toHaveLength(1);
    expect(res[0].vin).toBe("V2");
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. Schema fingerprint
// ─────────────────────────────────────────────────────────────────────────────

describe("schema fingerprint", () => {
  class FpCar extends Record {
    @Field({ required: true }) make!: string;
  }

  beforeEach(() => {
    _clearLocalFingerprints();
    Registry.reset();
    Registry.getInstance().configure({ adapter: new MockSpreadsheetAdapter() });
  });
  afterEach(() => Registry.reset());

  it("writes a fingerprint after ensureTable and matches on second call", () => {
    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    FpCar.find(); // first ensureTable → writes fp
    const sheet = adapter._getSheet(FpCar.tableName)!;
    const schema: TableSchema = {
      tableName: FpCar.tableName,
      fields: Decorators.getFields(FpCar),
      indexes: [],
    };
    expect(fingerprintMatches("mock-spreadsheet", schema, sheet.getSheetId!())).toBe(true);
  });

  it("fp hit skips the header verification read on the next execution", () => {
    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    FpCar.find(); // execution 1 — writes fingerprint

    // New "execution": fresh Registry (repos + caches cleared)
    Registry.reset();
    Registry.getInstance().configure({ adapter });

    const sheet = adapter._getSheet(FpCar.tableName)!;
    const headerSpy = jest.spyOn(sheet, "getHeaders");
    FpCar.find(); // execution 2 — fingerprint hit skips getHeaders
    expect(headerSpy).not.toHaveBeenCalled();
    headerSpy.mockRestore();
  });

  it("a recreated sheet (new sheetId) invalidates the fingerprint", () => {
    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    FpCar.find();
    const schema: TableSchema = {
      tableName: FpCar.tableName,
      fields: Decorators.getFields(FpCar),
      indexes: [],
    };
    // Recreate the tab → new sheetId
    adapter.deleteSheet(FpCar.tableName);
    adapter.createSheet(FpCar.tableName);
    const sheet = adapter._getSheet(FpCar.tableName)!;
    expect(fingerprintMatches("mock-spreadsheet", schema, sheet.getSheetId!())).toBe(false);
  });

  it("schema change produces a different fingerprint", () => {
    const schemaA: TableSchema = {
      tableName: "t",
      fields: [{ name: "a" }],
      indexes: [],
    };
    const schemaB: TableSchema = {
      tableName: "t",
      fields: [{ name: "a" }, { name: "b" }],
      indexes: [],
    };
    expect(schemaFingerprint(schemaA)).not.toBe(schemaFingerprint(schemaB));
  });

  it("dropFingerprint removes the entry", () => {
    const schema: TableSchema = { tableName: "t", fields: [], indexes: [] };
    writeFingerprint("ss", schema, 42);
    expect(fingerprintMatches("ss", schema, 42)).toBe(true);
    dropFingerprint("ss", "t");
    expect(fingerprintMatches("ss", schema, 42)).toBe(false);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. warmUpSheets graceful fallback
// ─────────────────────────────────────────────────────────────────────────────

describe("warmUpSheets fallback", () => {
  it("mock adapter reports false (no UrlFetchApp) and prefetch still works", () => {
    const adapter = new MockSpreadsheetAdapter();
    adapter.createSheet("tbl_X");
    expect(adapter.warmUpSheets(["tbl_X"])).toBe(false);
    // ensureTable-style fallback: prefetchSheets is a no-op mock — no throw.
    expect(() => adapter.prefetchSheets(["tbl_X"])).not.toThrow();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 10. Persisted combined hash (chash:) — derived-structure warm start
// ─────────────────────────────────────────────────────────────────────────────

describe("persisted combined hash (chash:)", () => {
  it("persists the hash map under chash: after a data-driven build", () => {
    const adapter = new MockSpreadsheetAdapter();
    const cache = new MemoryCache();
    const store = new IndexStore(adapter, cache);
    store.createCombinedIndex("idx_T");
    store.registerIndex("idx_T", "color", false);
    store.addAllFieldsToCombined("idx_T", [{ field: "color", value: "red" }], "id1");
    store.addAllFieldsToCombined("idx_T", [{ field: "color", value: "blue" }], "id2");

    expect(store.lookupCombined("idx_T", "color", "red")).toEqual(["id1"]);
    const persisted = cache.get<{ n: number; e: [string, string, string[]][] }>("chash:idx_T");
    expect(persisted).not.toBeNull();
    expect(persisted!.n).toBe(2);
    expect(persisted!.e.length).toBe(2);
  });

  it("a fresh IndexStore answers lookups from chash: alone (cidx gone)", () => {
    const adapter = new MockSpreadsheetAdapter();
    const cache = new MemoryCache();
    const store = new IndexStore(adapter, cache);
    store.createCombinedIndex("idx_T");
    store.registerIndex("idx_T", "color", false);
    store.addAllFieldsToCombined("idx_T", [{ field: "color", value: "red" }], "id1");
    store.lookupCombined("idx_T", "color", "red"); // build + persist

    // Simulate a new execution: fresh in-memory IndexStore over the same cache.
    const store2 = new IndexStore(adapter, cache);
    store2.registerIndex("idx_T", "color", false);
    cache.delete("cidx:idx_T"); // prove the answer came from chash, not rows
    expect(store2.lookupCombined("idx_T", "color", "red")).toEqual(["id1"]);
    expect(store2.lookupCombined("idx_T", "color", "green")).toEqual([]);
  });

  it("mutations invalidate the persisted hash before the next lookup", () => {
    const adapter = new MockSpreadsheetAdapter();
    const cache = new MemoryCache();
    const store = new IndexStore(adapter, cache);
    store.createCombinedIndex("idx_T");
    store.registerIndex("idx_T", "color", false);
    store.addAllFieldsToCombined("idx_T", [{ field: "color", value: "red" }], "id1");
    store.lookupCombined("idx_T", "color", "red");
    expect(cache.get("chash:idx_T")).not.toBeNull();

    store.addAllFieldsToCombined("idx_T", [{ field: "color", value: "blue" }], "id2");
    expect(cache.get("chash:idx_T")).toBeNull(); // dropped by commitCombined

    // Rebuilt on next lookup — sees the new row.
    expect(store.lookupCombined("idx_T", "color", "blue")).toEqual(["id2"]);
    const persisted = cache.get<{ n: number }>("chash:idx_T");
    expect(persisted!.n).toBe(2);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 11. Tombstone deletes (opt-in)
// ─────────────────────────────────────────────────────────────────────────────

describe("tombstone deletes", () => {
  class TombThing extends Record {
    static override tombstoneDeletes(): boolean {
      return true;
    }
    @Field({ required: true }) name!: string;
  }

  beforeEach(() => {
    Registry.reset();
    Registry.getInstance().configure({ adapter: new MockSpreadsheetAdapter() });
    _clearLocalFingerprints();
  });
  afterEach(() => {
    Registry.reset();
  });

  it("delete marks __id instead of shifting rows; reads skip the dead row", () => {
    const names = ["a", "b", "c"];
    const items = names.map((n) => {
      const t = new TombThing();
      t.name = n;
      return t.save();
    });
    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    const sheet = adapter._getSheet(TombThing.tableName)!;

    items[1].delete();

    // Physical row survives — only its __id cell became the marker.
    expect(sheet.getRowCount()).toBe(3);
    expect(sheet.getRow(1)[0]).toBe("#TOMB#");
    // Logical view: 2 live entities, the middle one gone.
    expect(TombThing.find().map((t) => t.name)).toEqual(["a", "c"]);
    expect(TombThing.count()).toBe(2);
    expect(TombThing.findById(items[1].__id)).toBeNull();
  });

  it("count() without load stays exact via the ids-column read", () => {
    const t1 = new TombThing();
    t1.name = "x";
    t1.save();
    const t2 = new TombThing();
    t2.name = "y";
    t2.save();
    t2.delete();
    // count() must not count the tombstoned row.
    expect(TombThing.count()).toBe(1);
    expect(t1.__id).toBeTruthy();
  });

  it("save after a tombstone delete appends at the physical end", () => {
    const t1 = new TombThing();
    t1.name = "one";
    t1.save();
    const t2 = new TombThing();
    t2.name = "two";
    t2.save();
    t1.delete();
    const t3 = new TombThing();
    t3.name = "three";
    t3.save();
    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    const sheet = adapter._getSheet(TombThing.tableName)!;
    expect(sheet.getRowCount()).toBe(3); // tomb + live + new
    expect(TombThing.find().map((t) => t.name)).toEqual(["two", "three"]);
  });

  it("compaction reclaims dead rows past the threshold", () => {
    const items: TombThing[] = [];
    for (let i = 0; i < 6; i++) {
      const t = new TombThing();
      t.name = `n${i}`;
      items.push(t.save());
    }
    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    const sheet = adapter._getSheet(TombThing.tableName)!;

    // Deletes 0–3 → tombstones hit 4 ≥ 25% of 6 → compaction fires, leaving
    // 2 live rows (n4, n5).  Delete n4 → one fresh tombstone, below threshold.
    for (let i = 0; i < 5; i++) items[i].delete();
    expect(sheet.getRowCount()).toBe(2); // n5 live + n4 tombstone
    expect(TombThing.find().map((t) => t.name)).toEqual(["n5"]);
    expect(TombThing.count()).toBe(1);

    // Last live row gone → all-dead triggers compaction → physically empty.
    items[5].delete();
    expect(sheet.getRowCount()).toBe(0);
    expect(TombThing.count()).toBe(0);
  });

  it("deleteAll on a subset tombstones; on everything it clears physically", () => {
    const items: TombThing[] = [];
    for (let i = 0; i < 4; i++) {
      const t = new TombThing();
      t.name = `n${i}`;
      items.push(t.save());
    }
    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    const sheet = adapter._getSheet(TombThing.tableName)!;

    TombThing.deleteAll({ where: [{ field: "name", operator: "=", value: "n1" }] });
    expect(sheet.getRowCount()).toBe(4); // marked, not removed
    expect(
      TombThing.find()
        .map((t) => t.name)
        .sort(),
    ).toEqual(["n0", "n2", "n3"]);

    TombThing.deleteAll(); // whole table → physical clear, no tombstones left
    expect(sheet.getRowCount()).toBe(0);
    expect(TombThing.count()).toBe(0);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 12. Packed-row storage (opt-in)
// ─────────────────────────────────────────────────────────────────────────────

describe("packed storage", () => {
  class PackedItem extends Record {
    static override packedStorage(): boolean {
      return true;
    }
    @Field({ required: true }) name!: string;
    @Field() price!: number;
    @Indexed({ unique: true }) sku!: string;
  }

  beforeEach(() => {
    Registry.reset();
    Registry.getInstance().configure({ adapter: new MockSpreadsheetAdapter() });
    _clearLocalFingerprints();
  });
  afterEach(() => {
    Registry.reset();
  });

  it("writes a 2-cell row and reads the entity back losslessly", () => {
    const p = new PackedItem();
    p.name = "widget";
    p.price = 9.5;
    p.sku = "SKU-1";
    p.save();

    const adapter = Registry.getInstance()["adapter"] as MockSpreadsheetAdapter;
    const sheet = adapter._getSheet(PackedItem.tableName)!;
    expect(sheet.getHeaders()).toEqual(["__id", "__data"]);
    const row = sheet.getRow(0);
    expect(row).toHaveLength(2);
    expect(row[0]).toBe(p.__id);
    const payload = JSON.parse(String(row[1])) as { [k: string]: unknown };
    expect(payload.name).toBe("widget");
    expect(payload.price).toBe(9.5);
    expect(payload.sku).toBe("SKU-1");

    const found = PackedItem.findById(p.__id)!;
    expect(found.name).toBe("widget");
    expect(found.price).toBe(9.5);
    expect(found.sku).toBe("SKU-1");
  });

  it("update round-trips through the packed payload", () => {
    const p = new PackedItem();
    p.name = "a";
    p.price = 1;
    p.sku = "S1";
    p.save();
    p.price = 42;
    p.save();

    const found = PackedItem.find({ where: [{ field: "price", operator: "=", value: 42 }] });
    expect(found).toHaveLength(1);
    expect(found[0].name).toBe("a");
    expect(PackedItem.count()).toBe(1);
  });

  it("indexed equality planning works against packed rows", () => {
    for (const [name, sku] of [
      ["x", "S1"],
      ["y", "S2"],
    ] as const) {
      const p = new PackedItem();
      p.name = name;
      p.price = 1;
      p.sku = sku;
      p.save();
    }
    const res = PackedItem.find({ where: [{ field: "sku", operator: "=", value: "S2" }] });
    expect(res).toHaveLength(1);
    expect(res[0].name).toBe("y");
  });

  it("delete + saveAll work on packed rows", () => {
    PackedItem.saveAll([
      { name: "a", price: 1, sku: "A" },
      { name: "b", price: 2, sku: "B" },
    ]);
    expect(PackedItem.count()).toBe(2);
    const b = PackedItem.find({ where: [{ field: "sku", operator: "=", value: "B" }] })[0];
    b.delete();
    expect(PackedItem.count()).toBe(1);
    expect(PackedItem.find()[0].name).toBe("a");
  });
});
