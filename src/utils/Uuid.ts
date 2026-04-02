/**
 * UUID v4 generation with a three-tier fallback strategy:
 *
 * 1. **GAS Utilities.getUuid()** — native, crypto-safe, fastest in Apps Script.
 * 2. **Web Crypto API (globalThis.crypto)** — available in Node 19+, browsers,
 *    Deno, Bun, and other modern runtimes.
 * 3. **Math.random()** — last resort; not cryptographically secure but
 *    adequate for non-security-critical record identifiers in dev/test.
 *
 * All generated UUIDs conform to RFC 4122 v4 (random).
 */

/**
 * Pre-computed lookup table mapping byte values (0–255) to their two-character
 * hexadecimal representations. Avoids per-byte toString(16) + padStart calls
 * during UUID formatting.
 */
const HEX: string[] = new Array(256);
for (let i = 0; i < 256; i++) {
  // (i + 0x100) produces a 3-char hex string; substring(1) drops the leading "1"
  HEX[i] = (i + 0x100).toString(16).substring(1);
}

/** Shape of globalThis when Web Crypto API is available. */
interface CryptoHost {
  crypto?: {
    getRandomValues?: (buf: Uint8Array) => Uint8Array;
  };
}

/**
 * Generate a RFC 4122 v4 (random) UUID string.
 *
 * @returns A lowercase UUID in the canonical `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx` format.
 */
function generateUUID(): string {
  // --- Tier 1: GAS native UUID ---
  // Google Apps Script V8 runtime exposes Utilities.getUuid(), which delegates
  // to Java's java.util.UUID.randomUUID() — crypto-safe and zero-overhead.
  if (typeof Utilities !== "undefined" && typeof Utilities.getUuid === "function") {
    return Utilities.getUuid();
  }

  // --- Tier 2 & 3: manual RFC 4122 v4 construction ---
  // Allocate 16 random bytes (128 bits); 6 bits are then overwritten for version/variant.
  const r = new Uint8Array(16);

  // Attempt to fill with crypto-safe random values via Web Crypto API
  const globalHost = globalThis as typeof globalThis & CryptoHost;
  const g = typeof globalThis !== "undefined" ? globalHost.crypto : undefined;
  if (g && typeof g.getRandomValues === "function") {
    // Tier 2: Web Crypto (Node 19+, browsers, Deno)
    g.getRandomValues(r);
  } else {
    // Tier 3: Math.random fallback (non-crypto, dev/test only)
    for (let i = 0; i < 16; i++) {
      r[i] = (Math.random() * 256) | 0;
    }
  }

  // Set the version nibble (bits 48–51) to 0100 → UUID version 4
  r[6] = (r[6] & 0x0f) | 0x40;
  // Set the variant bits (bits 64–65) to 10 → RFC 4122 variant
  r[8] = (r[8] & 0x3f) | 0x80;

  // Format as canonical UUID string: 8-4-4-4-12 hexadecimal characters
  return (
    HEX[r[0]] +
    HEX[r[1]] +
    HEX[r[2]] +
    HEX[r[3]] +
    "-" +
    HEX[r[4]] +
    HEX[r[5]] +
    "-" +
    HEX[r[6]] +
    HEX[r[7]] +
    "-" +
    HEX[r[8]] +
    HEX[r[9]] +
    "-" +
    HEX[r[10]] +
    HEX[r[11]] +
    HEX[r[12]] +
    HEX[r[13]] +
    HEX[r[14]] +
    HEX[r[15]]
  );
}

/**
 * Public UUID utility class.
 *
 * Exposes a single static method `generate()` that returns a new UUID v4 string.
 * Wrapped in a class to follow the SheetORM one-export-per-file convention.
 */
export class Uuid {
  /** Generate a new RFC 4122 v4 UUID string. */
  static generate = generateUUID;
}
