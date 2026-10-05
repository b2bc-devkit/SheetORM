import { GasCacheProvider } from "../src/core/cache/GasCacheProvider";
import { MemoryCache } from "../src/core/cache/MemoryCache";
import { Registry } from "../src/core/Registry";
import { Record } from "../src/core/Record";
import { Decorators } from "../src/core/Decorators";
const { Field, Indexed } = Decorators;
import { MockSpreadsheetAdapter } from "./MockSpreadsheetAdapter";
import type { ICacheProvider } from "../src/core/types/ICacheProvider";

/** In-memory stub of the GAS CacheService surface. */
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

describe("GasCacheProvider", () => {
  afterEach(() => {
    delete (globalThis as { CacheService?: unknown }).CacheService;
    jest.useRealTimers();
  });

  describe("without CacheService (non-GAS / L1-only)", () => {
    it("stores and retrieves values", () => {
      const c = new GasCacheProvider();
      c.set("k", { a: 1 });
      expect(c.get<{ a: number }>("k")).toEqual({ a: 1 });
    });

    it("honours per-entry TTL", () => {
      jest.useFakeTimers();
      const c = new GasCacheProvider();
      c.set("k", "v", 500);
      jest.advanceTimersByTime(600);
      expect(c.get("k")).toBeNull();
    });

    it("delete/clear/has work", () => {
      const c = new GasCacheProvider();
      c.set("a", 1);
      c.set("b", 2);
      expect(c.has("a")).toBe(true);
      c.delete("a");
      expect(c.has("a")).toBe(false);
      c.clear();
      expect(c.has("b")).toBe(false);
    });

    it("rejects invalid TTL", () => {
      const c = new GasCacheProvider();
      expect(() => c.set("k", "v", -1)).toThrow(/non-negative/);
      expect(() => new GasCacheProvider({ defaultTtlMs: NaN })).toThrow();
    });
  });

  describe("with CacheService bound", () => {
    it("round-trips small values through the remote store", () => {
      const fake = bindFakeCacheService();
      const c = new GasCacheProvider();
      c.set("k", [1, 2, 3]);
      const fresh = new GasCacheProvider(); // empty L1 — forces remote read
      expect(fresh.get<number[]>("k")).toEqual([1, 2, 3]);
      expect(fake.store.has("k")).toBe(true);
    });

    it("shards payloads over ~90KB and reassembles them", () => {
      const fake = bindFakeCacheService();
      const c = new GasCacheProvider();
      const big = { blob: "x".repeat(200 * 1024) };
      c.set("big", big);
      expect(fake.store.has("big_meta")).toBe(true);
      const meta = JSON.parse(fake.store.get("big_meta")!.v) as { c: number };
      expect(meta.c).toBeGreaterThan(2);
      const fresh = new GasCacheProvider();
      expect(fresh.get<{ blob: string }>("big")).toEqual(big);
    });

    it("returns null when a shard is missing", () => {
      const fake = bindFakeCacheService();
      const c = new GasCacheProvider();
      c.set("big", { blob: "y".repeat(200 * 1024) });
      fake.remove("big_p1");
      const fresh = new GasCacheProvider();
      expect(fresh.get("big")).toBeNull();
    });

    it("honours remote expiry embedded in the envelope", () => {
      jest.useFakeTimers();
      const fake = bindFakeCacheService();
      const c = new GasCacheProvider();
      c.set("k", "v", 1000);
      jest.advanceTimersByTime(2000);
      fake.store.clear(); // simulate service TTL sweep
      const fresh = new GasCacheProvider();
      expect(fresh.get("k")).toBeNull();
    });

    it("delete() removes the key, its meta, and shards", () => {
      const fake = bindFakeCacheService();
      const c = new GasCacheProvider();
      c.set("big", { blob: "z".repeat(200 * 1024) });
      c.delete("big");
      expect([...fake.store.keys()].filter((k) => k.startsWith("big"))).toHaveLength(0);
      expect(new GasCacheProvider().get("big")).toBeNull();
    });

    it("clamps TTL seconds into the service bounds", () => {
      const fake = bindFakeCacheService();
      const c = new GasCacheProvider();
      c.set("k", "v", 999999999); // > 6h → clamped to 21600s
      const exp = fake.store.get("k")!.exp;
      expect(exp - Date.now()).toBeLessThanOrEqual(21600 * 1000 + 500);
    });
  });
});

describe("per-class cacheTtlMs() propagation", () => {
  class TtlCar extends Record {
    @Field({ required: true }) make!: string;
    static override cacheTtlMs(): number {
      return 5 * 60_000;
    }
  }
  class DefaultCar extends Record {
    @Field({ required: true }) make!: string;
  }
  class IndexedTtlCar extends Record {
    @Field({ required: true }) make!: string;
    @Indexed({ unique: true }) vin!: string;
    static override cacheTtlMs(): number {
      return 7 * 60_000;
    }
  }

  function spyCache(): { cache: ICacheProvider; calls: Array<[string, unknown, number?]> } {
    const calls: Array<[string, unknown, number?]> = [];
    const inner = new MemoryCache(60_000);
    const cache: ICacheProvider = {
      get: (k) => inner.get(k),
      set: (k, v, ttl) => {
        calls.push([k, v, ttl]);
        inner.set(k, v, ttl);
      },
      delete: (k) => inner.delete(k),
      clear: () => inner.clear(),
      has: (k) => inner.has(k),
    };
    return { cache, calls };
  }

  beforeEach(() => {
    Registry.reset();
    Registry.getInstance().configure({ adapter: new MockSpreadsheetAdapter() });
  });
  afterEach(() => Registry.reset());

  it("passes the overridden TTL into data: cache writes", () => {
    const { cache, calls } = spyCache();
    Registry.reset();
    Registry.getInstance().configure({ adapter: new MockSpreadsheetAdapter(), cache });
    const car = new TtlCar();
    car.make = "Toyota";
    car.save();
    TtlCar.find(); // triggers a data: cache population
    const writes = calls.filter(([k]) => k === `data:${TtlCar.tableName}`);
    expect(writes.length).toBeGreaterThan(0);
    for (const w of writes) expect(w[2]).toBe(5 * 60_000);
  });

  it("uses the default TTL for non-overriding classes", () => {
    const { cache, calls } = spyCache();
    Registry.reset();
    Registry.getInstance().configure({ adapter: new MockSpreadsheetAdapter(), cache });
    const car = new DefaultCar();
    car.make = "Honda";
    car.save();
    DefaultCar.find();
    const writes = calls.filter(([k]) => k === `data:${DefaultCar.tableName}`);
    expect(writes.length).toBeGreaterThan(0);
    for (const w of writes) expect(w[2]).toBe(60_000);
  });

  it("passes the class TTL into cidx: index cache writes", () => {
    const { cache, calls } = spyCache();
    Registry.reset();
    Registry.getInstance().configure({ adapter: new MockSpreadsheetAdapter(), cache });
    const car = new IndexedTtlCar();
    car.make = "Volvo";
    car.vin = "VIN-1";
    car.save(); // unique-index check runs getCombinedData → cidx: write
    const idxKey = `cidx:${IndexedTtlCar.indexTableName}`;
    const writes = calls.filter(([k]) => k === idxKey);
    expect(writes.length).toBeGreaterThan(0);
    for (const w of writes) expect(w[2]).toBe(7 * 60_000);
  });
});

describe("write-through coherence across executions", () => {
  class CohCar extends Record {
    @Field({ required: true }) make!: string;
    @Indexed({ unique: true }) tag!: string;
  }

  type CohRow = { __id: string; make: string; tag: string };
  const dataKey = () => `data:${CohCar.tableName}`;
  const idxKey = () => `cidx:${CohCar.indexTableName}`;

  /**
   * Simulate a fresh GAS execution: new Registry + a new GasCacheProvider
   * (empty L1) over the same shared remote store.  The MockSpreadsheetAdapter
   * instance persists — like the real spreadsheet survives across runs.
   */
  function newExecution(adapter: MockSpreadsheetAdapter): GasCacheProvider {
    Registry.reset();
    const cache = new GasCacheProvider();
    Registry.getInstance().configure({ adapter, cache });
    return cache;
  }

  afterEach(() => {
    Registry.reset();
    delete (globalThis as { CacheService?: unknown }).CacheService;
  });

  it("create → a later execution sees the entity via the remote snapshot", () => {
    bindFakeCacheService();
    const adapter = new MockSpreadsheetAdapter();
    newExecution(adapter);
    const car = new CohCar();
    car.make = "A";
    car.tag = "t1";
    car.save();

    const exec2 = newExecution(adapter);
    const remote = exec2.get<CohRow[]>(dataKey());
    expect(remote?.map((e) => e.make)).toEqual(["A"]);
    expect(CohCar.find().map((c) => c.make)).toEqual(["A"]);
  });

  it("update → a later execution sees the new field value", () => {
    bindFakeCacheService();
    const adapter = new MockSpreadsheetAdapter();
    newExecution(adapter);
    const car = new CohCar();
    car.make = "A";
    car.tag = "t1";
    car.save();

    newExecution(adapter);
    const fetched = CohCar.find()[0];
    fetched.make = "B";
    fetched.save();

    const exec3 = newExecution(adapter);
    const remote = exec3.get<CohRow[]>(dataKey());
    expect(remote).toHaveLength(1);
    expect(remote![0].make).toBe("B");
  });

  it("delete → a later execution sees the row gone", () => {
    bindFakeCacheService();
    const adapter = new MockSpreadsheetAdapter();
    newExecution(adapter);
    const car = new CohCar();
    car.make = "A";
    car.tag = "t1";
    car.save();

    newExecution(adapter);
    CohCar.find()[0].delete();

    const exec3 = newExecution(adapter);
    expect(exec3.get<CohRow[]>(dataKey())).toEqual([]);
    expect(CohCar.find()).toHaveLength(0);
  });

  it("saveAll → remote holds the complete final array", () => {
    bindFakeCacheService();
    const adapter = new MockSpreadsheetAdapter();
    newExecution(adapter);
    const cars = ["A", "B", "C"].map((make) => {
      const c = new CohCar();
      c.make = make;
      c.tag = `t-${make}`;
      return c;
    });
    CohCar.saveAll(cars);

    const exec2 = newExecution(adapter);
    const remote = exec2.get<CohRow[]>(dataKey());
    expect(remote?.map((e) => e.make).sort()).toEqual(["A", "B", "C"]);
  });

  it("index insert/update/delete propagate to the remote cidx: snapshot", () => {
    bindFakeCacheService();
    const adapter = new MockSpreadsheetAdapter();
    newExecution(adapter);
    const car = new CohCar();
    car.make = "A";
    car.tag = "t1";
    car.save();

    // Execution 2: remote index holds the inserted entry
    let exec = newExecution(adapter);
    let idx = exec.get<unknown[][]>(idxKey());
    expect(idx?.some((r) => r[0] === "tag" && r[1] === "t1")).toBe(true);

    // Execution 2 → update the indexed field → execution 3 sees the new value
    const fetched = CohCar.find()[0];
    fetched.tag = "t2";
    fetched.save();
    exec = newExecution(adapter);
    idx = exec.get<unknown[][]>(idxKey());
    expect(idx?.some((r) => r[0] === "tag" && r[1] === "t2")).toBe(true);
    expect(idx?.some((r) => r[0] === "tag" && r[1] === "t1")).toBe(false);

    // Execution 3 → delete → execution 4 sees the entry removed
    CohCar.find()[0].delete();
    exec = newExecution(adapter);
    idx = exec.get<unknown[][]>(idxKey());
    expect(idx?.some((r) => r[0] === "tag")).toBe(false);
  });
});
