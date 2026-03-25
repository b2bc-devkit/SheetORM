// SheetORM — In-memory cache with TTL support

import type { ICacheProvider } from "../types/ICacheProvider.js";

interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

const DEFAULT_TTL_MS = 60_000; // 60 seconds

export class MemoryCache implements ICacheProvider {
  private store = new Map<string, CacheEntry<unknown>>();
  private defaultTtlMs: number;

  constructor(defaultTtlMs: number = DEFAULT_TTL_MS) {
    if (!Number.isFinite(defaultTtlMs) || defaultTtlMs < 0) {
      throw new Error(`MemoryCache: defaultTtlMs must be a non-negative finite number, got ${defaultTtlMs}`);
    }
    this.defaultTtlMs = defaultTtlMs;
  }

  get<T>(key: string): T | null {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      this.store.delete(key);
      return null;
    }
    return entry.data as T;
  }

  set<T>(key: string, value: T, ttlMs?: number): void {
    const ttl = ttlMs ?? this.defaultTtlMs;
    if (!Number.isFinite(ttl) || ttl < 0) {
      throw new Error(`MemoryCache.set: ttlMs must be a non-negative finite number, got ${ttl}`);
    }
    this.store.set(key, {
      data: value,
      expiresAt: Date.now() + ttl,
    });
  }

  delete(key: string): void {
    this.store.delete(key);
  }

  clear(): void {
    this.store.clear();
  }

  has(key: string): boolean {
    const entry = this.store.get(key);
    if (!entry) return false;
    if (Date.now() > entry.expiresAt) {
      this.store.delete(key);
      return false;
    }
    return true;
  }
}
