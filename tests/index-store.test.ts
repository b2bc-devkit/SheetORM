import { MockSpreadsheetAdapter } from "./mocks";
import { IndexStore } from "../src/index/IndexStore";
import { MemoryCache } from "../src/utils/cache";

describe("IndexStore", () => {
  let adapter: MockSpreadsheetAdapter;
  let cache: MemoryCache;
  let indexStore: IndexStore;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
    cache = new MemoryCache();
    indexStore = new IndexStore(adapter, cache);
  });

  it("creates a combined index sheet", () => {
    indexStore.createCombinedIndex("idx_Users");
    expect(adapter.getSheetNames()).toContain("idx_Users");
  });

  it("adds and looks up entries", () => {
    indexStore.createCombinedIndex("idx_Users");
    indexStore.registerIndex("idx_Users", "email", false);
    indexStore.addToCombined("idx_Users", "email", "jan@example.com", "user-001");
    indexStore.addToCombined("idx_Users", "email", "anna@example.com", "user-002");

    const ids = indexStore.lookupCombined("idx_Users", "email", "jan@example.com");
    expect(ids).toEqual(["user-001"]);
  });

  it("enforces unique index", () => {
    indexStore.createCombinedIndex("idx_Users");
    indexStore.registerIndex("idx_Users", "email", true);
    indexStore.addToCombined("idx_Users", "email", "jan@example.com", "user-001");

    expect(() => {
      indexStore.addToCombined("idx_Users", "email", "jan@example.com", "user-002");
    }).toThrow(/Unique index violation/);
  });

  it("allows same entity to re-index with same value (unique)", () => {
    indexStore.createCombinedIndex("idx_Users");
    indexStore.registerIndex("idx_Users", "email", true);
    indexStore.addToCombined("idx_Users", "email", "jan@example.com", "user-001");
    // Should not throw
    indexStore.addToCombined("idx_Users", "email", "jan@example.com", "user-001");
  });

  it("removes entries when value is cleared in update", () => {
    indexStore.createCombinedIndex("idx_Users");
    indexStore.registerIndex("idx_Users", "email", false);
    indexStore.addToCombined("idx_Users", "email", "jan@example.com", "user-001");
    indexStore.updateInCombined("idx_Users", "user-001", { email: "jan@example.com" }, { email: "" });

    const ids = indexStore.lookupCombined("idx_Users", "email", "jan@example.com");
    expect(ids).toEqual([]);
  });

  it("removes all entries for an entity", () => {
    indexStore.createCombinedIndex("idx_Users");
    indexStore.registerIndex("idx_Users", "email", false);
    indexStore.registerIndex("idx_Users", "name", false);

    indexStore.addToCombined("idx_Users", "email", "jan@example.com", "user-001");
    indexStore.addToCombined("idx_Users", "name", "Jan", "user-001");

    indexStore.removeAllFromCombined("idx_Users", "user-001");

    expect(indexStore.lookupCombined("idx_Users", "email", "jan@example.com")).toEqual([]);
    expect(indexStore.lookupCombined("idx_Users", "name", "Jan")).toEqual([]);
  });

  it("updates entries when value changes", () => {
    indexStore.createCombinedIndex("idx_Users");
    indexStore.registerIndex("idx_Users", "email", false);
    indexStore.addToCombined("idx_Users", "email", "old@example.com", "user-001");

    indexStore.updateInCombined(
      "idx_Users",
      "user-001",
      { email: "old@example.com" },
      { email: "new@example.com" },
    );

    expect(indexStore.lookupCombined("idx_Users", "email", "old@example.com")).toEqual([]);
    expect(indexStore.lookupCombined("idx_Users", "email", "new@example.com")).toEqual(["user-001"]);
  });

  it("supports independent lookups per indexed field", () => {
    indexStore.createCombinedIndex("idx_Users");
    indexStore.registerIndex("idx_Users", "name", false);
    indexStore.registerIndex("idx_Users", "city", false);

    indexStore.addToCombined("idx_Users", "name", "Jan", "user-001");
    indexStore.addToCombined("idx_Users", "city", "Warszawa", "user-001");

    expect(indexStore.lookupCombined("idx_Users", "name", "Jan")).toEqual(["user-001"]);
    expect(indexStore.lookupCombined("idx_Users", "city", "Warszawa")).toEqual(["user-001"]);
    expect(indexStore.lookupCombined("idx_Users", "name", "Warszawa")).toEqual([]);
  });

  it("drops a combined index", () => {
    indexStore.createCombinedIndex("idx_Users");
    indexStore.dropCombinedIndex("idx_Users");
    expect(adapter.getSheetNames()).not.toContain("idx_Users");
  });

  it("existsCombined() checks for index sheet", () => {
    expect(indexStore.existsCombined("idx_Users")).toBe(false);
    indexStore.createCombinedIndex("idx_Users");
    expect(indexStore.existsCombined("idx_Users")).toBe(true);
  });

  it("getIndexedFields() returns registered fields", () => {
    indexStore.registerIndex("idx_Users", "email", true);
    indexStore.registerIndex("idx_Users", "name", false);

    const fields = indexStore.getIndexedFields("idx_Users");
    expect(fields).toHaveLength(2);
    expect(fields.map((f) => f.field).sort()).toEqual(["email", "name"]);
  });
  // ─── N-gram search (Solr-like) ──────────────────────────────────────────────

  describe("searchCombined (n-gram)", () => {
    beforeEach(() => {
      indexStore.createCombinedIndex("idx_Cars");
      indexStore.registerIndex("idx_Cars", "model", false);
      indexStore.addToCombined("idx_Cars", "model", "BMW 320i", "car-001");
      indexStore.addToCombined("idx_Cars", "model", "Mercedes-Benz C200", "car-002");
      indexStore.addToCombined("idx_Cars", "model", "Audi A4 Avant", "car-003");
      indexStore.addToCombined("idx_Cars", "model", "Peugeot 205 GTI", "car-004");
      indexStore.addToCombined("idx_Cars", "model", "Toyota Corolla", "car-005");
    });

    it("searchCombined (n-gram) > finds exact token match", () => {
      const ids = indexStore.searchCombined("idx_Cars", "model", "BMW");
      expect(ids).toEqual(["car-001"]);
    });

    it("searchCombined (n-gram) > finds partial match via trigrams", () => {
      const ids = indexStore.searchCombined("idx_Cars", "model", "320");
      expect(ids).toEqual(["car-001"]);
    });

    it("searchCombined (n-gram) > is case insensitive", () => {
      const ids = indexStore.searchCombined("idx_Cars", "model", "bmw");
      expect(ids).toEqual(["car-001"]);
    });

    it("searchCombined (n-gram) > handles multi-token query (intersection)", () => {
      const ids = indexStore.searchCombined("idx_Cars", "model", "BMW 320");
      expect(ids).toEqual(["car-001"]);
    });

    it("searchCombined (n-gram) > returns empty for no match", () => {
      const ids = indexStore.searchCombined("idx_Cars", "model", "Volvo");
      expect(ids).toEqual([]);
    });

    it("searchCombined (n-gram) > respects limit parameter", () => {
      // Add another BMW to get multiple hits
      indexStore.addToCombined("idx_Cars", "model", "BMW X5", "car-006");
      const ids = indexStore.searchCombined("idx_Cars", "model", "BMW", 1);
      expect(ids).toHaveLength(1);
    });

    it("searchCombined (n-gram) > finds match through normalized separators", () => {
      // "Mercedes-Benz" is normalized to "mercedes benz"
      const ids = indexStore.searchCombined("idx_Cars", "model", "Mercedes Benz");
      expect(ids).toEqual(["car-002"]);
    });

    it("searchCombined (n-gram) > invalidates search index cache on data change", () => {
      expect(indexStore.searchCombined("idx_Cars", "model", "Volvo")).toEqual([]);
      indexStore.addToCombined("idx_Cars", "model", "Volvo S60", "car-006");
      const ids = indexStore.searchCombined("idx_Cars", "model", "Volvo");
      expect(ids).toEqual(["car-006"]);
    });

    it("searchCombined (n-gram) > returns empty for empty query", () => {
      expect(indexStore.searchCombined("idx_Cars", "model", "")).toEqual([]);
    });

    it("searchCombined (n-gram) > finds substring within a token via trigrams", () => {
      // "Corolla" contains trigrams: cor, oro, rol, oll, lla
      const ids = indexStore.searchCombined("idx_Cars", "model", "Corol");
      expect(ids).toEqual(["car-005"]);
    });
  });

  describe("normalizeForSearch", () => {
    it("normalizeForSearch > lowercases and trims", () => {
      expect(IndexStore.normalizeForSearch("  BMW 320i  ")).toBe("bmw 320i");
    });

    it("normalizeForSearch > normalizes dashes to spaces", () => {
      expect(IndexStore.normalizeForSearch("Mercedes-Benz")).toBe("mercedes benz");
    });

    it("normalizeForSearch > collapses whitespace", () => {
      expect(IndexStore.normalizeForSearch("a   b   c")).toBe("a b c");
    });

    it("normalizeForSearch > returns empty for null-ish input", () => {
      expect(IndexStore.normalizeForSearch("")).toBe("");
    });
  });

  describe("ngrams", () => {
    it("ngrams > generates trigrams", () => {
      const ngs = IndexStore.ngrams("abcde", 3);
      expect(ngs).toEqual(new Set(["abc", "bcd", "cde"]));
    });

    it("ngrams > returns empty for short input", () => {
      expect(IndexStore.ngrams("ab", 3)).toEqual(new Set());
    });

    it("ngrams > strips whitespace before generating", () => {
      const ngs = IndexStore.ngrams("a b c d e", 3);
      expect(ngs).toEqual(new Set(["abc", "bcd", "cde"]));
    });
  });
});
