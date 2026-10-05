/**
 * Schema fingerprints persisted in GAS `PropertiesService`.
 *
 * Purpose: on a cold execution `Registry.ensureTable` can skip the
 * header-verification read when it can PROVE the on-sheet schema already
 * matches the class schema.  The proof object is a per-(spreadsheet, table)
 * record holding:
 *
 *   fp      — FNV-1a hash of the canonical schema form
 *             (tableName + field names/types/options + indexes).
 *   sheetId — the tab's numeric Sheets ID.  A tab deleted and recreated
 *             under the same name gets a NEW sheetId → the fingerprint
 *             invalidates automatically.
 *   exp     — absolute expiry (epoch ms).  Bounds the staleness window for
 *             manual header edits, which neither fp nor sheetId can see.
 *
 * Correctness contract: a fingerprint is ONLY written after ensureTable has
 * actually verified or written the expected headers — a hit therefore means
 * "this exact tab carried this exact schema within the TTL window".
 *
 * Outside GAS (tests) the provider degrades to a process-local Map —
 * same semantics, no persistence.
 *
 * @module SchemaFingerprint
 */

import type { TableSchema } from "../core/types/TableSchema.js";

/** Fingerprint record TTL — bounds the manual-edit staleness window. */
const FP_TTL_MS = 6 * 60 * 60 * 1000; // 6 h, mirrors CacheService max

const PROP_KEY_PREFIX = "som_schema:";

/** Minimal structural subset of `GoogleAppsScript.Properties.Properties`. */
interface GasProperties {
  getProperty(key: string): string | null;
  setProperty(key: string, value: string): void;
  deleteProperty(key: string): void;
}

interface GasPropertiesService {
  getScriptProperties(): GasProperties;
}

interface FingerprintEntry {
  /** Schema content hash (FNV-1a hex). */
  fp: string;
  /** Numeric Sheets sheetId of the tab the fingerprint was verified on. */
  sid: number;
  /** Absolute expiry, epoch ms. */
  exp: number;
}

/** Process-local fallback store (tests / non-GAS runtimes). */
const localStore = new Map<string, string>();

function propsService(): GasProperties | null {
  const svc = (globalThis as Record<string, unknown>)["PropertiesService"] as
    GasPropertiesService | undefined;
  if (svc === undefined) return null;
  try {
    return svc.getScriptProperties();
  } catch {
    return null;
  }
}

/** FNV-1a 32-bit — fast, deterministic, dependency-free string hash. */
function fnv1a(input: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  return h.toString(16);
}

/**
 * Compute the canonical fingerprint for a table schema.
 * Covers everything that determines the on-sheet layout and index
 * semantics: table name, ordered field definitions (name + type + options
 * that affect serialisation), and index definitions.
 */
export function schemaFingerprint(schema: TableSchema): string {
  const parts: string[] = [
    schema.tableName,
    ";",
    // Storage-mode flags change the physical layout — they must be part of
    // the fingerprint so toggling them invalidates stale verifications.
    schema.packed ? "P" : "",
    schema.tombstones ? "T" : "",
    ";",
  ];
  for (const f of schema.fields) {
    parts.push(
      f.name,
      ":",
      String(f.type ?? ""),
      f.required ? "!r" : "",
      f.defaultValue !== undefined ? "=d" : "",
      ";",
    );
  }
  if (schema.indexTableName) parts.push("@", schema.indexTableName, ";");
  for (const idx of schema.indexes) {
    parts.push("^", idx.field, idx.unique ? "u" : "", ";");
  }
  return fnv1a(parts.join(""));
}

/**
 * Look up the stored fingerprint for (spreadsheet, table) and report whether
 * it is valid for the given schema hash + live sheetId.
 */
export function fingerprintMatches(
  spreadsheetId: string | null,
  schema: TableSchema,
  sheetId: number | null,
): boolean {
  if (spreadsheetId === null || sheetId === null) return false;
  const key = PROP_KEY_PREFIX + spreadsheetId;
  let raw: string | null;
  const props = propsService();
  try {
    raw = props ? props.getProperty(key) : (localStore.get(key) ?? null);
  } catch {
    return false;
  }
  if (!raw) return false;
  try {
    const table = JSON.parse(raw) as Record<string, FingerprintEntry>;
    const entry = table[schema.tableName];
    if (!entry || Date.now() >= entry.exp) return false;
    return entry.sid === sheetId && entry.fp === schemaFingerprint(schema);
  } catch {
    return false;
  }
}

/**
 * Persist a verified fingerprint for (spreadsheet, table).
 * Reads-modify-writes the single JSON property for the spreadsheet — one
 * PropertiesService call (~10 ms) per ensured table, far cheaper than the
 * header verification it replaces (~150 ms RPC on a cold grid).
 */
export function writeFingerprint(
  spreadsheetId: string | null,
  schema: TableSchema,
  sheetId: number | null,
): void {
  if (spreadsheetId === null || sheetId === null) return;
  const key = PROP_KEY_PREFIX + spreadsheetId;
  const props = propsService();
  try {
    let table: Record<string, FingerprintEntry> = {};
    const raw = props ? props.getProperty(key) : (localStore.get(key) ?? null);
    if (raw) table = JSON.parse(raw) as Record<string, FingerprintEntry>;
    table[schema.tableName] = {
      fp: schemaFingerprint(schema),
      sid: sheetId,
      exp: Date.now() + FP_TTL_MS,
    };
    const json = JSON.stringify(table);
    if (props) props.setProperty(key, json);
    else localStore.set(key, json);
  } catch {
    // Fingerprinting is a hint — never break ensureTable on it.
  }
}

/** Drop a table's fingerprint (called when the tab is deleted/recreated). */
export function dropFingerprint(spreadsheetId: string | null, tableName: string): void {
  if (spreadsheetId === null) return;
  const key = PROP_KEY_PREFIX + spreadsheetId;
  const props = propsService();
  try {
    const raw = props ? props.getProperty(key) : (localStore.get(key) ?? null);
    if (!raw) return;
    const table = JSON.parse(raw) as Record<string, FingerprintEntry>;
    if (!(tableName in table)) return;
    delete table[tableName];
    const json = JSON.stringify(table);
    if (props) props.setProperty(key, json);
    else localStore.set(key, json);
  } catch {
    /* hint only */
  }
}

/** Test hook: wipe the process-local fallback store. */
export function _clearLocalFingerprints(): void {
  localStore.clear();
}
