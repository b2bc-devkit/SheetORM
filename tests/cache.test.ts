import { MemoryCache } from "../src/core/cache/MemoryCache";

describe("MemoryCache", () => {
  let cache: MemoryCache;

  beforeEach(() => {
    cache = new MemoryCache(1000); // 1 second TTL
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("stores and retrieves values", () => {
    cache.set("key1", "value1");
    expect(cache.get<string>("key1")).toBe("value1");
  });

  it("returns null for missing keys", () => {
    expect(cache.get("nonexistent")).toBeNull();
  });

  it("has() returns true for existing keys", () => {
    cache.set("key1", 42);
    expect(cache.has("key1")).toBe(true);
    expect(cache.has("nonexistent")).toBe(false);
  });

  it("delete() removes a key", () => {
    cache.set("key1", "val");
    cache.delete("key1");
    expect(cache.get("key1")).toBeNull();
  });

  it("clear() removes all keys", () => {
    cache.set("a", 1);
    cache.set("b", 2);
    cache.clear();
    expect(cache.has("a")).toBe(false);
    expect(cache.has("b")).toBe(false);
  });

  it("expires entries after TTL", () => {
    jest.useFakeTimers();
    cache.set("key1", "value1");
    expect(cache.get<string>("key1")).toBe("value1");

    jest.advanceTimersByTime(1500);
    expect(cache.get<string>("key1")).toBeNull();
    jest.useRealTimers();
  });

  it("has() returns false after TTL expiry", () => {
    jest.useFakeTimers();
    cache.set("ttlKey", "value");
    expect(cache.has("ttlKey")).toBe(true);

    jest.advanceTimersByTime(1500);
    expect(cache.has("ttlKey")).toBe(false);
    jest.useRealTimers();
  });

  it("allows per-key TTL override", () => {
    jest.useFakeTimers();
    cache.set("short", "val", 200);
    cache.set("long", "val", 5000);

    jest.advanceTimersByTime(300);
    expect(cache.get("short")).toBeNull();
    expect(cache.get<string>("long")).toBe("val");
    jest.useRealTimers();
  });

  it("stores complex objects", () => {
    const obj = { name: "test", items: [1, 2, 3] };
    cache.set("obj", obj);
    expect(cache.get<typeof obj>("obj")).toEqual(obj);
  });

  it("constructor throws for NaN TTL", () => {
    expect(() => new MemoryCache(NaN)).toThrow();
  });

  it("constructor throws for negative TTL", () => {
    expect(() => new MemoryCache(-100)).toThrow();
  });

  it("set() throws for NaN per-key TTL", () => {
    expect(() => cache.set("key", "value", NaN)).toThrow();
  });

  it("TTL of 0 expires immediately", () => {
    cache.set("instant", "gone", 0);
    expect(cache.get<string>("instant")).toBeNull();
    expect(cache.has("instant")).toBe(false);
  });

  it("set() throws for negative per-key TTL", () => {
    expect(() => cache.set("key", "value", -1)).toThrow("non-negative finite number");
  });

  it("set() throws for Infinity per-key TTL", () => {
    expect(() => cache.set("key", "value", Infinity)).toThrow("non-negative finite number");
  });
});
