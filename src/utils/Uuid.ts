// SheetORM — UUID generation utility for Google Apps Script
// Uses crypto-safe random when available, falls back to Math.random

// Pre-computed hex lookup table for fast byte-to-hex conversion
const HEX: string[] = new Array(256);
for (let i = 0; i < 256; i++) {
  HEX[i] = (i + 0x100).toString(16).substring(1);
}

interface CryptoHost {
  crypto?: {
    getRandomValues?: (buf: Uint8Array) => Uint8Array;
  };
}

function generateUUID(): string {
  // GAS V8 runtime supports Utilities.getUuid() but we provide a fallback
  // for testing environments and npm usage
  if (typeof Utilities !== "undefined" && typeof Utilities.getUuid === "function") {
    return Utilities.getUuid();
  }
  // RFC 4122 v4 UUID — direct array approach, avoids per-char regex replace
  const r = new Uint8Array(16);
  // Use Web Crypto API when available (Node.js, browsers); fall back to Math.random
  const globalHost = globalThis as typeof globalThis & CryptoHost;
  const g = typeof globalThis !== "undefined" ? globalHost.crypto : undefined;
  if (g && typeof g.getRandomValues === "function") {
    g.getRandomValues(r);
  } else {
    // Last resort fallback (non-cryptographic) for environments without Web Crypto
    for (let i = 0; i < 16; i++) {
      r[i] = (Math.random() * 256) | 0;
    }
  }
  r[6] = (r[6] & 0x0f) | 0x40; // version 4
  r[8] = (r[8] & 0x3f) | 0x80; // variant 10xx

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

export class Uuid {
  static generate = generateUUID;
}
