/**
 * Persistent cache provider backed by GAS `CacheService` with sharding.
 *
 * Implements {@link ICacheProvider} so a SheetORM deployment can keep entity
 * and index data warm ACROSS executions — a fresh GAS run can serve reads
 * from cache (~3 ms) instead of re-reading whole sheets (~150–3000 ms).
 *
 * Design (after the "Sharded Cache-Engine" pattern):
 *  - Values whose JSON fits under ~90 KB are stored under the key as a
 *    `{e: expiresAt, v: value}` envelope — the embedded expiry lets a later
 *    execution honour the writer's exact TTL.
 *  - Larger payloads are split into `{key}_p0..pN` shards plus a `{key}_meta`
 *    descriptor `{c: shardCount, e: expiresAt}` and written in ONE `putAll`
 *    call; reads take one `getAll` for the descriptor then one `getAll` for
 *    the shards — two CacheService RPCs regardless of payload size.
 *  - A per-execution L1 `Map` fronts the service: repeated `get()`s inside
 *    one run are free, and entries written this run never need a round-trip.
 *    L1 expiry mirrors the embedded `expiresAt`, so L1 never outlives the
 *    remote entry.
 *  - `CacheService` cannot enumerate keys, so `clear()` removes only keys
 *    written through this instance; cross-execution leftovers expire with
 *    their TTL.
 *
 * TTL: `CacheService` expirations are in whole seconds clamped to [1, 21600]
 * (6 h service maximum); `ttlMs` converts via `Math.ceil`, so `0` lands on a
 * 1-second expiry — matching the "no real caching" intent.
 *
 * Outside GAS (e.g. unit tests) the service global is absent and every call
 * degrades to the in-memory L1 — the provider stays usable and deterministic.
 *
 * @module GasCacheProvider
 */

import type { ICacheProvider } from "../types/ICacheProvider.js";
import { DEFAULT_CACHE_TTL_MS } from "../types/ICacheProvider.js";
import { SheetOrmLogger } from "../../utils/SheetOrmLogger.js";

/** Minimal structural subset of `GoogleAppsScript.Cache.Cache`. */
interface GasCache {
  get(key: string): string | null;
  getAll(keys: string[]): { [key: string]: string };
  put(key: string, value: string, expirationInSeconds?: number): void;
  putAll(values: { [key: string]: string }, expirationInSeconds?: number): void;
  remove(key: string): void;
  removeAll(keys: string[]): void;
}

/** Structural subset of the `CacheService` facade. */
interface GasCacheService {
  getScriptCache(): GasCache;
  getUserCache(): GasCache;
  getDocumentCache(): GasCache | null;
}

/** Which GAS cache scope the provider binds to. */
export type GasCacheScope = "script" | "user" | "document";

/** Safe shard size — the service hard-limit is 100 KB per entry. */
const CHUNK_SIZE = 90 * 1024;
/** Service maximum expiration: 6 hours. */
const MAX_TTL_SEC = 21600;
/** Suffixes for sharded payloads (`key_meta` + `key_p0..pN-1`). */
const META_SUFFIX = "_meta";
const PART_SUFFIX = "_p";
/**
 * JSON payloads above this size are gzip-compressed before storage
 * (`Utilities.gzip` → base64).  Repetitive entity JSON typically compresses
 * 3–5×, so a "shard" effectively holds ~300–450 KB of source data.  Below
 * the threshold gzip overhead (CPU + base64 growth) isn't worth it.
 */
const COMPRESS_THRESHOLD = 1024;

/** Minimal structural subset of `GoogleAppsScript.Utilities`. */
interface GasUtilities {
  newBlob(data: string | number[]): GoogleAppsScript.Base.Blob;
  gzip(blob: GoogleAppsScript.Base.Blob): GoogleAppsScript.Base.Blob;
  ungzip(blob: GoogleAppsScript.Base.Blob): GoogleAppsScript.Base.Blob;
  base64Encode(bytes: number[]): string;
  base64Decode(encoded: string): number[];
}

function gasUtilities(): GasUtilities | null {
  const u = (globalThis as Record<string, unknown>)["Utilities"] as GasUtilities | undefined;
  return u !== undefined && typeof u.gzip === "function" ? u : null;
}

/** gzip → base64, or `null` when the compressed form isn't smaller / available. */
function compressForWire(json: string): string | null {
  if (json.length <= COMPRESS_THRESHOLD) return null;
  const u = gasUtilities();
  if (!u) return null;
  try {
    const b64 = u.base64Encode(u.gzip(u.newBlob(json)).getBytes());
    return b64.length < json.length ? b64 : null; // incompressible → keep plain
  } catch {
    return null;
  }
}

function decompressFromWire(b64: string): string {
  const u = gasUtilities();
  if (!u) throw new Error("GasCacheProvider: compressed entry read outside GAS");
  return u.ungzip(u.newBlob(u.base64Decode(b64))).getDataAsString();
}

interface CacheEntry {
  data: unknown;
  expiresAt: number;
}

/** Envelope written under `key` for small (unsharded) values. */
interface Envelope {
  /** Absolute expiry in epoch ms — the writer's TTL made explicit. */
  e: number;
  /** Value — or a base64-gzip JSON string when `z` is set. */
  v: unknown;
  /** 1 when `v` holds compressed JSON (backward-compatible: absent → plain). */
  z?: 1;
}

/** Descriptor written under `key_meta` for sharded values. */
interface ShardMeta {
  c: number;
  e: number;
  /** 1 when shards hold base64-gzip of the source JSON. */
  z?: 1;
}

export class GasCacheProvider implements ICacheProvider {
  /** Per-execution L1 — repeated gets in one run never hit the service. */
  private l1 = new Map<string, CacheEntry>();
  /** Keys written through this instance — needed because the service has no key enumeration. */
  private knownKeys = new Set<string>();
  /** Bound service cache (null outside GAS → L1-only behaviour). */
  private remote: GasCache | null;
  /** TTL applied when `set()` is called without a per-entry TTL. */
  private defaultTtlMs: number;

  /**
   * @param opts.defaultTtlMs - Default TTL in ms (must be ≥ 0, finite).
   * @param opts.scope        - "script" (default) | "user" | "document".
   */
  constructor(opts: { defaultTtlMs?: number; scope?: GasCacheScope } = {}) {
    const ttl = opts.defaultTtlMs ?? DEFAULT_CACHE_TTL_MS;
    if (!Number.isFinite(ttl) || ttl < 0) {
      throw new Error(`GasCacheProvider: defaultTtlMs must be a non-negative finite number, got ${ttl}`);
    }
    this.defaultTtlMs = ttl;
    const svc = (globalThis as Record<string, unknown>)["CacheService"] as GasCacheService | undefined;
    const scope = opts.scope ?? "script";
    this.remote =
      svc === undefined
        ? null
        : scope === "user"
          ? svc.getUserCache()
          : scope === "document"
            ? svc.getDocumentCache()
            : svc.getScriptCache();
  }

  get<T>(key: string): T | null {
    const hit = this.l1.get(key);
    if (hit) {
      if (Date.now() < hit.expiresAt) return hit.data as T;
      this.l1.delete(key);
      return null;
    }
    const remote = this.remote;
    if (!remote) return null;
    try {
      // One RPC answers both layouts: direct envelope on `key`, or the
      // shard descriptor at `key_meta`.
      const probe = remote.getAll([key, key + META_SUFFIX]);
      const direct = probe[key];
      if (direct !== undefined) {
        const env = JSON.parse(direct) as Envelope;
        if (Date.now() >= env.e) {
          SheetOrmLogger.log(`[GasCache] EXPIRED "${key}"`);
          return null;
        }
        const value = (env.z === 1 ? JSON.parse(decompressFromWire(env.v as string)) : env.v) as T;
        this.l1.set(key, { data: value, expiresAt: env.e });
        return value;
      }
      const metaRaw = probe[key + META_SUFFIX];
      if (!metaRaw) {
        SheetOrmLogger.log(`[GasCache] MISS  "${key}"`);
        return null;
      }
      const meta = JSON.parse(metaRaw) as ShardMeta;
      if (Date.now() >= meta.e) {
        SheetOrmLogger.log(`[GasCache] EXPIRED "${key}" (sharded)`);
        return null;
      }
      const shardKeys = Array.from({ length: meta.c }, (_, i) => `${key}${PART_SUFFIX}${i}`);
      const shards = remote.getAll(shardKeys);
      let wire = "";
      for (const k of shardKeys) {
        const part = shards[k];
        // Any missing shard means a torn/expired write — treat as a miss.
        if (part === undefined) return null;
        wire += part;
      }
      const value = JSON.parse(meta.z === 1 ? decompressFromWire(wire) : wire) as T;
      this.l1.set(key, { data: value, expiresAt: meta.e });
      SheetOrmLogger.log(`[GasCache] HIT   "${key}" (${meta.c} shard(s))`);
      return value;
    } catch (e) {
      SheetOrmLogger.log(`[GasCache] get("${key}") failed: ${String(e)}`);
      return null;
    }
  }

  set<T>(key: string, value: T, ttlMs?: number): void {
    const ttl = ttlMs ?? this.defaultTtlMs;
    if (!Number.isFinite(ttl) || ttl < 0) {
      throw new Error(`GasCacheProvider.set: ttlMs must be a non-negative finite number, got ${ttl}`);
    }
    const expiresAt = Date.now() + ttl;
    this.l1.set(key, { data: value, expiresAt });
    this.knownKeys.add(key);
    const remote = this.remote;
    if (!remote) return;
    const ttlSec = Math.min(MAX_TTL_SEC, Math.max(1, Math.ceil(ttl / 1000)));
    try {
      const json = JSON.stringify(value);
      // gzip when worthwhile — repetitive entity JSON compresses ~3–5×, so
      // one 90 KB shard effectively holds several hundred KB of source data.
      const compressed = compressForWire(json);
      const wire = compressed ?? json;
      const zipped = compressed !== null;
      if (wire.length <= CHUNK_SIZE - 64 /* room for the envelope wrapper */) {
        const env: Envelope = zipped ? { e: expiresAt, z: 1, v: wire } : { e: expiresAt, v: value };
        remote.put(key, JSON.stringify(env), ttlSec);
        return;
      }
      // Sharded payload: meta descriptor + 90 KB parts in ONE putAll.
      const payload: { [k: string]: string } = {};
      let count = 0;
      for (let i = 0; i < wire.length; i += CHUNK_SIZE) {
        payload[`${key}${PART_SUFFIX}${count}`] = wire.substring(i, i + CHUNK_SIZE);
        count++;
      }
      payload[key + META_SUFFIX] = JSON.stringify({
        c: count,
        e: expiresAt,
        ...(zipped ? { z: 1 as const } : {}),
      } satisfies ShardMeta);
      remote.putAll(payload, ttlSec);
      SheetOrmLogger.log(
        `[GasCache] SET   "${key}" → ${count} shard(s)${zipped ? " gzipped" : ""}, ttl=${ttlSec}s`,
      );
    } catch (e) {
      // A remote failure must not break the ORM — L1 still holds the value.
      SheetOrmLogger.log(`[GasCache] set("${key}") failed: ${String(e)}`);
    }
  }

  delete(key: string): void {
    this.l1.delete(key);
    this.knownKeys.delete(key);
    const remote = this.remote;
    if (!remote) return;
    try {
      const metaRaw = remote.get(key + META_SUFFIX);
      const keys = [key, key + META_SUFFIX];
      if (metaRaw) {
        const count = (JSON.parse(metaRaw) as Partial<ShardMeta>).c ?? 0;
        for (let i = 0; i < count; i++) keys.push(`${key}${PART_SUFFIX}${i}`);
      }
      remote.removeAll(keys);
    } catch {
      remote.remove(key);
    }
  }

  /**
   * Remove every key written through THIS instance.  CacheService offers no
   * key enumeration, so entries written by earlier executions can only
   * expire naturally (bounded by TTL).
   */
  clear(): void {
    this.l1.clear();
    const remote = this.remote;
    if (remote && this.knownKeys.size > 0) {
      try {
        const keys: string[] = [];
        const metaKeys = [...this.knownKeys].map((k) => k + META_SUFFIX);
        const metas = remote.getAll(metaKeys);
        for (const k of this.knownKeys) {
          keys.push(k, k + META_SUFFIX);
          const metaRaw = metas[k + META_SUFFIX];
          if (metaRaw) {
            const count = (JSON.parse(metaRaw) as Partial<ShardMeta>).c ?? 0;
            for (let i = 0; i < count; i++) keys.push(`${k}${PART_SUFFIX}${i}`);
          }
        }
        remote.removeAll(keys);
      } catch (e) {
        SheetOrmLogger.log(`[GasCache] clear() failed: ${String(e)}`);
      }
    }
    this.knownKeys.clear();
  }

  has(key: string): boolean {
    const hit = this.l1.get(key);
    if (hit) {
      if (Date.now() < hit.expiresAt) return true;
      this.l1.delete(key);
      return false;
    }
    const remote = this.remote;
    if (!remote) return false;
    try {
      const probe = remote.getAll([key, key + META_SUFFIX]);
      const direct = probe[key];
      if (direct !== undefined) return Date.now() < (JSON.parse(direct) as Envelope).e;
      const metaRaw = probe[key + META_SUFFIX];
      return metaRaw !== undefined && Date.now() < (JSON.parse(metaRaw) as ShardMeta).e;
    } catch {
      return false;
    }
  }
}
