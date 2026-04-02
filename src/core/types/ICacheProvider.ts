/**
 * Generic cache provider interface used throughout SheetORM.
 *
 * The built-in implementation is MemoryCache (in-memory Map with TTL).
 * Consumers can supply a custom implementation (e.g. CacheService in GAS)
 * via Registry.configure({ cache: myProvider }).
 */
export interface ICacheProvider {
  /** Retrieve a cached value by key, or null if missing / expired. */
  get<T>(key: string): T | null;

  /** Store a value under the given key with an optional per-entry TTL in milliseconds. */
  set<T>(key: string, value: T, ttlMs?: number): void;

  /** Remove a single cached entry. */
  delete(key: string): void;

  /** Remove all cached entries. */
  clear(): void;

  /** Check whether a non-expired entry exists for the given key. */
  has(key: string): boolean;
}
