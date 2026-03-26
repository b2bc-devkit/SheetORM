import { Uuid } from "../src/utils/Uuid";

describe("Uuid.generate", () => {
  it("returns a string of UUID v4 format", () => {
    const uuid = Uuid.generate();
    expect(uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
  });

  it("generates unique values", () => {
    const uuids = new Set(Array.from({ length: 100 }, () => Uuid.generate()));
    expect(uuids.size).toBe(100);
  });

  it("falls back to Math.random when crypto is unavailable", () => {
    const originalCrypto = globalThis.crypto;
    // Remove crypto to force the Math.random fallback
    Object.defineProperty(globalThis, "crypto", { value: undefined, writable: true, configurable: true });
    try {
      const uuid = Uuid.generate();
      expect(uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    } finally {
      Object.defineProperty(globalThis, "crypto", {
        value: originalCrypto,
        writable: true,
        configurable: true,
      });
    }
  });

  it("uses GAS Utilities.getUuid when available", () => {
    const fakeUuid = "aaaaaaaa-bbbb-4ccc-9ddd-eeeeeeeeeeee";
    (globalThis as unknown as { Utilities: { getUuid: () => string } }).Utilities = {
      getUuid: () => fakeUuid,
    };
    try {
      expect(Uuid.generate()).toBe(fakeUuid);
    } finally {
      delete (globalThis as unknown as { Utilities?: unknown }).Utilities;
    }
  });

  it("uses crypto.getRandomValues when available", () => {
    const originalCrypto = globalThis.crypto;
    const fakeCrypto = {
      getRandomValues: (buf: Uint8Array) => {
        for (let i = 0; i < buf.length; i++) buf[i] = i;
        return buf;
      },
    };

    Object.defineProperty(globalThis, "crypto", {
      value: fakeCrypto,
      writable: true,
      configurable: true,
    });
    try {
      const uuid = Uuid.generate();
      // bytes 0..15 with RFC4122 v4 adjustments: r[6]=0x46, r[8]=0x88
      expect(uuid).toBe("00010203-0405-4607-8809-0a0b0c0d0e0f");
    } finally {
      Object.defineProperty(globalThis, "crypto", {
        value: originalCrypto,
        writable: true,
        configurable: true,
      });
    }
  });
});
