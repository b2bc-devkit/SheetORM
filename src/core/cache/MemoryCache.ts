/**
 * In-memory cache with per-entry TTL (time-to-live) support.
 *
 * Implements the {@link ICacheProvider} interface used by SheetORM to
 * avoid redundant Google Sheets API calls (e.g. caching `getAllData()`
 * results, index sheet data, and header rows).
 *
 * Entries expire lazily: they are evicted on the next `get()` or `has()`
 * call after their TTL has elapsed — there is no background sweep timer.
 *
 * @module MemoryCache
 */

import type { ICacheProvider } from "../types/ICacheProvider.js";
import { SheetOrmLogger } from "../../utils/SheetOrmLogger.js";

/** Internal wrapper holding the cached data and its absolute expiration time. */
interface CacheEntry<T> {
  /** The cached payload. */
  data: T;
  /** Absolute timestamp (ms since epoch) at which the entry becomes stale. */
  expiresAt: number;
}

/** Default time-to-live for cache entries: 60 seconds. */
const DEFAULT_TTL_MS = 60_000;

/**
 * Simple in-memory cache backed by a `Map`.
 *
 * Suitable for the single-threaded GAS runtime where no cross-request
 * persistence is needed — each GAS execution starts with a fresh cache.
 */
export class MemoryCache implements ICacheProvider {
  /** Internal storage mapping cache keys to their entries. */
  private store = new Map<string, CacheEntry<unknown>>();

  /** TTL applied when `set()` is called without an explicit per-entry TTL. */
  private defaultTtlMs: number;

  /**
   * Create a new MemoryCache instance.
   *
   * @param defaultTtlMs - Default TTL in milliseconds (must be ≥ 0).
   * @throws If defaultTtlMs is negative or non-finite.
   */
  constructor(defaultTtlMs: number = DEFAULT_TTL_MS) {
    if (!Number.isFinite(defaultTtlMs) || defaultTtlMs < 0) {
      throw new Error(`MemoryCache: defaultTtlMs must be a non-negative finite number, got ${defaultTtlMs}`);
    }
    this.defaultTtlMs = defaultTtlMs;
  }

  /**
   * Retrieve a cached value by key.
   *
   * Returns null on cache miss or if the entry has expired (lazy eviction).
   *
   * @param key - Cache key.
   * @returns The cached value, or null.
   */
  get<T>(key: string): T | null {
    const entry = this.store.get(key);
    if (!entry) {
      SheetOrmLogger.log(`[Cache] MISS  "${key}"`);
      return null;
    }
    // Lazy expiration: remove stale entries on access
    if (Date.now() >= entry.expiresAt) {
      this.store.delete(key);
      SheetOrmLogger.log(`[Cache] EXPIRED "${key}"`);
      return null;
    }
    SheetOrmLogger.log(`[Cache] HIT   "${key}"`);
    return entry.data as T;
  }

  /**
   * Store a value under the given key with an optional per-entry TTL.
   *
   * @param key   - Cache key.
   * @param value - Value to cache.
   * @param ttlMs - TTL in milliseconds; falls back to the default TTL.
   * @throws If ttlMs is negative or non-finite.
   */
  set<T>(key: string, value: T, ttlMs?: number): void {
    const ttl = ttlMs ?? this.defaultTtlMs;
    if (!Number.isFinite(ttl) || ttl < 0) {
      throw new Error(`MemoryCache.set: ttlMs must be a non-negative finite number, got ${ttl}`);
    }
    SheetOrmLogger.log(`[Cache] SET   "${key}" ttl=${ttl}ms`);
    this.store.set(key, {
      data: value,
      expiresAt: Date.now() + ttl,
    });
  }

  /**
   * Remove a single entry from the cache.
   *
   * @param key - Cache key to remove.
   */
  delete(key: string): void {
    SheetOrmLogger.log(`[Cache] DELETE "${key}"`);
    this.store.delete(key);
  }

  /** Remove all entries from the cache. */
  clear(): void {
    SheetOrmLogger.log(`[Cache] CLEAR  (${this.store.size} entries)`);
    this.store.clear();
  }

  /**
   * Check whether a non-expired entry exists for the given key.
   * Expired entries are lazily evicted.
   *
   * @param key - Cache key.
   * @returns true if a valid (non-expired) entry exists.
   */
  has(key: string): boolean {
    const entry = this.store.get(key);
    if (!entry) return false;
    // Lazy eviction on has() check
    if (Date.now() >= entry.expiresAt) {
      this.store.delete(key);
      return false;
    }
    return true;
  }
}
