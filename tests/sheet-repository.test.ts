import { SheetRepository } from "../src/core/SheetRepository";
import { IndexStore } from "../src/index/IndexStore";
import { MemoryCache } from "../src/core/cache/MemoryCache";
import { MockSpreadsheetAdapter } from "./MockSpreadsheetAdapter";
import type { Entity } from "../src/core/types/Entity";
import type { TableSchema } from "../src/core/types/TableSchema";
import { Serialization } from "../src/utils/Serialization";
import type { LifecycleHooks } from "../src/core/types/LifecycleHooks";

interface Item extends Entity {
  name: string;
  price: number;
  category: string;
}

const itemSchema: TableSchema = {
  tableName: "tbl_Items",
  fields: [{ name: "name" }, { name: "price" }, { name: "category" }],
  indexes: [],
};

function createRepo(adapter: MockSpreadsheetAdapter, hooks?: LifecycleHooks<Item>): SheetRepository<Item> {
  const sheet = adapter.createSheet(itemSchema.tableName);
  const indexStore = new IndexStore(adapter);
  const cache = new MemoryCache();
  const repo = new SheetRepository<Item>(adapter, itemSchema, indexStore, cache, hooks);
  // SheetRepository expects headers already set on the sheet
  sheet.setHeaders(Serialization.buildHeaders(itemSchema.fields));
  return repo;
}

describe("SheetRepository lifecycle hooks", () => {
  let adapter: MockSpreadsheetAdapter;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
  });

  it("onValidate rejects save when validation errors returned", () => {
    const repo = createRepo(adapter, {
      onValidate: (entity) => {
        const errors: string[] = [];
        if (!entity.name) errors.push("name is required");
        if ((entity.price ?? 0) < 0) errors.push("price must be non-negative");
        return errors;
      },
    });

    expect(() => repo.save({ name: "", price: -1, category: "x" })).toThrow("Validation failed");
  });

  it("beforeSave mutates entity payload", () => {
    const repo = createRepo(adapter, {
      beforeSave: (entity) => ({
        ...entity,
        name: (entity.name ?? "").toUpperCase(),
      }),
    });

    const saved = repo.save({ name: "widget", price: 10, category: "tools" });
    expect(saved.name).toBe("WIDGET");
    const found = repo.findById(saved.__id);
    expect(found!.name).toBe("WIDGET");
  });

  it("afterSave receives saved entity and isNew flag", () => {
    const calls: Array<{ entity: Item; isNew: boolean }> = [];
    const repo = createRepo(adapter, {
      afterSave: (entity, isNew) => {
        calls.push({ entity: { ...entity } as Item, isNew });
      },
    });

    const saved = repo.save({ name: "A", price: 1, category: "x" });
    expect(calls).toHaveLength(1);
    expect(calls[0].isNew).toBe(true);
    expect(calls[0].entity.__id).toBe(saved.__id);

    repo.save({ ...saved, price: 2 });
    expect(calls).toHaveLength(2);
    expect(calls[1].isNew).toBe(false);
  });

  it("beforeDelete returning false blocks deletion", () => {
    const repo = createRepo(adapter, {
      beforeDelete: () => false,
    });

    const saved = repo.save({ name: "keep", price: 5, category: "x" });
    const deleted = repo.delete(saved.__id);
    expect(deleted).toBe(false);
    expect(repo.count()).toBe(1);
  });

  it("beforeDelete veto on deleteAll returns zero and preserves data", () => {
    const repo = createRepo(adapter, {
      beforeDelete: () => false,
    });

    repo.save({ name: "A", price: 1, category: "x" });
    repo.save({ name: "B", price: 2, category: "x" });
    repo.save({ name: "C", price: 3, category: "x" });

    const count = repo.deleteAll();
    expect(count).toBe(0);
    expect(repo.count()).toBe(3);
  });

  it("afterDelete is called with deleted entity ID", () => {
    const deletedIds: string[] = [];
    const repo = createRepo(adapter, {
      afterDelete: (id) => {
        deletedIds.push(id);
      },
    });

    const saved = repo.save({ name: "gone", price: 0, category: "x" });
    repo.delete(saved.__id);
    expect(deletedIds).toEqual([saved.__id]);
  });
});

describe("SheetRepository error paths", () => {
  let adapter: MockSpreadsheetAdapter;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
  });

  it("getSheet throws when sheet does not exist", () => {
    const indexStore = new IndexStore(adapter);
    // Create repo pointing to a table that does NOT exist in adapter
    const repo = new SheetRepository<Item>(
      adapter,
      { ...itemSchema, tableName: "tbl_NonExistent" },
      indexStore,
    );

    expect(() => repo.find()).toThrow('Sheet "tbl_NonExistent" not found');
  });

  it("loadAllEntities throws during active saveAll entity batch", () => {
    const repo = createRepo(adapter, {
      beforeSave: () => {
        // Attempt a read (count → loadAllEntities) during saveAll
        try {
          repo.count();
        } catch {
          // re-throw to propagate
          throw new Error("re-entrant read blocked");
        }
      },
    });

    expect(() =>
      repo.saveAll([
        { name: "A", price: 1, category: "x" },
        { name: "B", price: 2, category: "y" },
      ]),
    ).toThrow();
  });
});
