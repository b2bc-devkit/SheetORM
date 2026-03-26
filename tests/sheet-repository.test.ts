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

  it("beforeDelete veto on deleteAll can preserve selected entities", () => {
    let vetoId: string | null = null;
    const repo = createRepo(adapter, {
      beforeDelete: (id) => id !== vetoId,
    });

    const a = repo.save({ name: "A", price: 1, category: "x" });
    const b = repo.save({ name: "B", price: 2, category: "x" });
    const c = repo.save({ name: "C", price: 3, category: "x" });
    vetoId = b.__id;

    const count = repo.deleteAll();
    expect(count).toBe(2);
    expect(repo.count()).toBe(1);
    const remaining = repo.find();
    expect(remaining).toHaveLength(1);
    expect(remaining[0].__id).toBe(vetoId);
    expect([a.__id, b.__id, c.__id]).toContain(remaining[0].__id);
  });

  it("beforeDelete partial veto works for deleteAll small-batch path", () => {
    let vetoId: string | null = null;
    const deletedIds: string[] = [];
    const repo = createRepo(adapter, {
      beforeDelete: (id) => id !== vetoId,
      afterDelete: (id) => {
        deletedIds.push(id);
      },
    });

    const a = repo.save({ name: "A", price: 1, category: "x" });
    const b = repo.save({ name: "B", price: 2, category: "x" });
    vetoId = b.__id;

    // Exactly 2 entities triggers the individual-delete path (<=2)
    const count = repo.deleteAll();
    expect(count).toBe(1);
    expect(repo.count()).toBe(1);

    const remaining = repo.find();
    expect(remaining).toHaveLength(1);
    expect(remaining[0].__id).toBe(vetoId);
    expect(new Set(deletedIds)).toEqual(new Set([a.__id]));
    expect(deletedIds).toHaveLength(1);
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

  it("afterDelete is called for each entity removed by deleteAll", () => {
    const deletedIds: string[] = [];
    const repo = createRepo(adapter, {
      afterDelete: (id) => {
        deletedIds.push(id);
      },
    });

    const a = repo.save({ name: "A", price: 1, category: "x" });
    const b = repo.save({ name: "B", price: 2, category: "x" });
    const c = repo.save({ name: "C", price: 3, category: "x" });

    const count = repo.deleteAll();
    expect(count).toBe(3);
    expect(repo.count()).toBe(0);
    expect(new Set(deletedIds)).toEqual(new Set([a.__id, b.__id, c.__id]));
    expect(deletedIds).toHaveLength(3);
  });

  it("afterDelete is called for each entity removed by deleteAll small-batch path", () => {
    const deletedIds: string[] = [];
    const repo = createRepo(adapter, {
      afterDelete: (id) => {
        deletedIds.push(id);
      },
    });

    const a = repo.save({ name: "A", price: 1, category: "x" });
    const b = repo.save({ name: "B", price: 2, category: "x" });

    // Exactly 2 entities uses the individual-delete path (<=2)
    const count = repo.deleteAll();
    expect(count).toBe(2);
    expect(repo.count()).toBe(0);
    expect(new Set(deletedIds)).toEqual(new Set([a.__id, b.__id]));
    expect(deletedIds).toHaveLength(2);
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

describe("SheetRepository detached read results", () => {
  let adapter: MockSpreadsheetAdapter;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
  });

  it("findById returns a detached copy from cache", () => {
    const repo = createRepo(adapter);
    const saved = repo.save({ name: "Widget", price: 10, category: "tools" });

    const found = repo.findById(saved.__id)!;
    found.name = "CHANGED";

    const reread = repo.findById(saved.__id)!;
    expect(reread.name).toBe("Widget");
  });

  it("query() returns detached copies from cache-backed reads", () => {
    const repo = createRepo(adapter);
    const saved = repo.save({ name: "Widget", price: 10, category: "tools" });

    const first = repo.query().first()!;
    first.name = "CHANGED";

    const reread = repo.findById(saved.__id)!;
    expect(reread.name).toBe("Widget");
  });

  it("save() in batch mode with explicit __id sets __createdAt for new entity", () => {
    const repo = createRepo(adapter);
    repo.beginBatch();
    // Entity has an explicit __id but no __createdAt — it is a new entity with a caller-supplied ID
    const placeholder = repo.save({ __id: "brand-new-id", name: "X", price: 5, category: "tools" });
    expect(placeholder.__createdAt).toBeDefined();
    repo.rollbackBatch();
  });

  it("commitBatch persists queued save with explicit __id for new entity", () => {
    const repo = createRepo(adapter);
    repo.beginBatch();
    repo.save({ __id: "brand-new-id", name: "X", price: 5, category: "tools" });

    // Deferred before commit
    expect(repo.count()).toBe(0);

    repo.commitBatch();

    expect(repo.count()).toBe(1);
    const found = repo.findById("brand-new-id");
    expect(found).not.toBeNull();
    expect(found!.__id).toBe("brand-new-id");
    expect(found!.name).toBe("X");
    expect(found!.__createdAt).toBeDefined();
  });

  it("two queued saves with same explicit __id produce one updated entity after commit", () => {
    const repo = createRepo(adapter);

    repo.beginBatch();
    repo.save({ __id: "same-id", name: "First", price: 1, category: "x" });
    repo.save({
      __id: "same-id",
      name: "Second",
      price: 2,
      category: "y",
      __createdAt: "2020-01-01T00:00:00.000Z",
    });

    // Deferred write before commit
    expect(repo.count()).toBe(0);

    repo.commitBatch();

    expect(repo.count()).toBe(1);
    const found = repo.findById("same-id");
    expect(found).not.toBeNull();
    expect(found!.name).toBe("Second");
    expect(found!.price).toBe(2);
    expect(found!.category).toBe("y");
    expect(found!.__createdAt).toBeDefined();
    const all = repo.find();
    expect(all).toHaveLength(1);
    expect(all[0].__id).toBe("same-id");
  });

  it("two queued saves with same explicit __id trigger afterSave as create then update", () => {
    const calls: Array<{ isNew: boolean; name: string }> = [];
    const repo = createRepo(adapter, {
      afterSave: (entity, isNew) => {
        calls.push({ isNew, name: String(entity.name ?? "") });
      },
    });

    repo.beginBatch();
    repo.save({ __id: "same-id", name: "First", price: 1, category: "x" });
    repo.save({
      __id: "same-id",
      name: "Second",
      price: 2,
      category: "y",
      __createdAt: "2020-01-01T00:00:00.000Z",
    });
    expect(calls).toHaveLength(0);

    repo.commitBatch();

    expect(calls).toHaveLength(2);
    expect(calls[0].isNew).toBe(true);
    expect(calls[0].name).toBe("First");
    expect(calls[1].isNew).toBe(false);
    expect(calls[1].name).toBe("Second");
  });
});

describe("SheetRepository batch no-op paths", () => {
  let adapter: MockSpreadsheetAdapter;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
  });

  it("commitBatch() is a no-op when no batch is active", () => {
    const repo = createRepo(adapter);
    expect(repo.isBatchActive()).toBe(false);
    expect(() => repo.commitBatch()).not.toThrow();
    expect(repo.isBatchActive()).toBe(false);
  });

  it("rollbackBatch() is a no-op when no batch is active", () => {
    const repo = createRepo(adapter);
    expect(repo.isBatchActive()).toBe(false);
    expect(() => repo.rollbackBatch()).not.toThrow();
    expect(repo.isBatchActive()).toBe(false);
  });

  it("rollbackBatch discards queued operations and does not trigger save/delete hooks", () => {
    const afterSaveCalls: string[] = [];
    const afterDeleteCalls: string[] = [];
    const repo = createRepo(adapter, {
      afterSave: (entity) => {
        afterSaveCalls.push(entity.__id);
      },
      afterDelete: (id) => {
        afterDeleteCalls.push(id);
      },
    });

    const existing = repo.save({ name: "Existing", price: 1, category: "x" });
    // Ignore hooks from setup save; we only assert rollback side effects.
    afterSaveCalls.length = 0;
    afterDeleteCalls.length = 0;

    repo.beginBatch();
    repo.save({ name: "Queued", price: 2, category: "y" });
    repo.delete(existing.__id);
    expect(repo.count()).toBe(1);

    repo.rollbackBatch();

    expect(repo.count()).toBe(1);
    const found = repo.findById(existing.__id);
    expect(found).not.toBeNull();
    expect(found!.name).toBe("Existing");
    expect(afterSaveCalls).toHaveLength(0);
    expect(afterDeleteCalls).toHaveLength(0);
  });

  it("beginBatch() resets the buffer when batch is already active", () => {
    const repo = createRepo(adapter);
    repo.beginBatch();
    repo.save({ name: "First", price: 1, category: "x" }); // buffered, not yet written
    repo.beginBatch(); // resets the buffer, discarding the buffered save
    repo.commitBatch(); // commits an empty buffer
    expect(repo.count()).toBe(0); // First entity was discarded by the reset
  });

  it("queued save applies beforeSave and triggers afterSave on commit", () => {
    const calls: Array<{ isNew: boolean; name: string }> = [];
    const repo = createRepo(adapter, {
      beforeSave: (entity) => ({
        ...entity,
        name: String(entity.name ?? "").toUpperCase(),
      }),
      afterSave: (entity, isNew) => {
        calls.push({ isNew, name: String(entity.name ?? "") });
      },
    });

    repo.beginBatch();
    repo.save({ name: "alpha", price: 1, category: "x" });

    // Deferred: no write and no afterSave before commit
    expect(repo.count()).toBe(0);
    expect(calls).toHaveLength(0);

    repo.commitBatch();

    expect(repo.count()).toBe(1);
    const all = repo.find();
    expect(all).toHaveLength(1);
    expect(all[0].name).toBe("ALPHA");
    expect(calls).toHaveLength(1);
    expect(calls[0].isNew).toBe(true);
    expect(calls[0].name).toBe("ALPHA");
  });

  it("queued update applies beforeSave and triggers afterSave with isNew=false", () => {
    const calls: Array<{ isNew: boolean; name: string }> = [];
    const repo = createRepo(adapter, {
      beforeSave: (entity) => ({
        ...entity,
        name: String(entity.name ?? "").toUpperCase(),
      }),
      afterSave: (entity, isNew) => {
        calls.push({ isNew, name: String(entity.name ?? "") });
      },
    });

    const original = repo.save({ name: "alpha", price: 1, category: "x" });
    calls.length = 0;

    repo.beginBatch();
    repo.save({ ...original, name: "beta" });

    // Deferred update
    expect(repo.findById(original.__id)!.name).toBe("ALPHA");
    expect(calls).toHaveLength(0);

    repo.commitBatch();

    const updated = repo.findById(original.__id);
    expect(updated).not.toBeNull();
    expect(updated!.name).toBe("BETA");
    expect(calls).toHaveLength(1);
    expect(calls[0].isNew).toBe(false);
    expect(calls[0].name).toBe("BETA");
  });

  it("queued update by __id without __createdAt updates existing entity and preserves createdAt", () => {
    const repo = createRepo(adapter);
    const original = repo.save({ name: "alpha", price: 1, category: "x" });
    const originalCreatedAt = original.__createdAt;

    repo.beginBatch();
    const placeholder = repo.save({ __id: original.__id, name: "beta", price: 2, category: "x" });

    // Heuristic may treat this as likely new in placeholder, but commit must update existing row.
    expect(placeholder.__createdAt).toBeDefined();
    expect(repo.count()).toBe(1);

    repo.commitBatch();

    expect(repo.count()).toBe(1);
    const updated = repo.findById(original.__id);
    expect(updated).not.toBeNull();
    expect(updated!.name).toBe("beta");
    expect(updated!.price).toBe(2);
    expect(updated!.__createdAt).toBe(originalCreatedAt);

    const all = repo.find();
    expect(all).toHaveLength(1);
    expect(all[0].__id).toBe(original.__id);
  });
});

describe("SheetRepository batch deleteAll", () => {
  let adapter: MockSpreadsheetAdapter;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
  });

  it("deleteAll() in batch mode queues deletes for deferred execution", () => {
    const repo = createRepo(adapter);
    repo.save({ name: "Alpha", price: 1, category: "x" });
    repo.save({ name: "Beta", price: 2, category: "x" });
    expect(repo.count()).toBe(2);

    repo.beginBatch();
    const queued = repo.deleteAll();

    // Returns the number of entities that will be deleted
    expect(queued).toBe(2);
    // Sheet is not yet modified — count still reads persisted state
    expect(repo.count()).toBe(2);

    repo.commitBatch();
    // After commit all deletes applied
    expect(repo.count()).toBe(0);
  });

  it("deleteAll(options) in batch mode queues only matching deletes", () => {
    const repo = createRepo(adapter);
    repo.save({ name: "Alpha", price: 1, category: "x" });
    repo.save({ name: "Beta", price: 2, category: "y" });
    repo.save({ name: "Gamma", price: 3, category: "x" });
    expect(repo.count()).toBe(3);

    repo.beginBatch();
    const queued = repo.deleteAll({ where: [{ field: "category", operator: "=", value: "x" }] });
    expect(queued).toBe(2);
    // Deferred mode: persisted state unchanged until commit
    expect(repo.count()).toBe(3);

    repo.commitBatch();
    const remaining = repo.find();
    expect(remaining).toHaveLength(1);
    expect(remaining[0].name).toBe("Beta");
  });

  it("delete() in batch mode with non-existent ID is a deferred no-op after commit", () => {
    const repo = createRepo(adapter);
    repo.save({ name: "Alpha", price: 1, category: "x" });
    expect(repo.count()).toBe(1);

    repo.beginBatch();
    const accepted = repo.delete("missing-id");
    // In batch mode delete() is accepted and deferred
    expect(accepted).toBe(true);
    expect(repo.count()).toBe(1);

    repo.commitBatch();
    // Missing ID delete becomes an effective no-op after commit
    expect(repo.count()).toBe(1);
    expect(repo.find()[0].name).toBe("Alpha");
  });

  it("afterDelete is called for queued delete when batch is committed", () => {
    const deletedIds: string[] = [];
    const repo = createRepo(adapter, {
      afterDelete: (id) => {
        deletedIds.push(id);
      },
    });
    const saved = repo.save({ name: "Alpha", price: 1, category: "x" });

    repo.beginBatch();
    repo.delete(saved.__id);
    // Operation is deferred until commit
    expect(deletedIds).toHaveLength(0);

    repo.commitBatch();
    expect(deletedIds).toEqual([saved.__id]);
    expect(repo.count()).toBe(0);
  });

  it("afterDelete is not called for missing queued delete on commit", () => {
    const deletedIds: string[] = [];
    const repo = createRepo(adapter, {
      afterDelete: (id) => {
        deletedIds.push(id);
      },
    });
    repo.save({ name: "Alpha", price: 1, category: "x" });

    repo.beginBatch();
    repo.delete("missing-id");
    expect(deletedIds).toHaveLength(0);

    repo.commitBatch();
    expect(deletedIds).toHaveLength(0);
    expect(repo.count()).toBe(1);
  });

  it("missing delete in batch does not block valid queued operations", () => {
    const repo = createRepo(adapter);
    const alpha = repo.save({ name: "Alpha", price: 1, category: "x" });
    expect(repo.count()).toBe(1);

    repo.beginBatch();
    repo.delete("missing-id");
    repo.save({ name: "Beta", price: 2, category: "y" });
    repo.delete(alpha.__id);

    repo.commitBatch();
    const all = repo.find();
    expect(all).toHaveLength(1);
    expect(all[0].name).toBe("Beta");
  });

  it("delete() outside batch returns false for non-existent ID", () => {
    const repo = createRepo(adapter);
    repo.save({ name: "Alpha", price: 1, category: "x" });
    expect(repo.count()).toBe(1);

    const deleted = repo.delete("missing-id");
    expect(deleted).toBe(false);
    expect(repo.count()).toBe(1);
    expect(repo.find()[0].name).toBe("Alpha");
  });

  it("delete() falls back to full scan when cached row index is stale", () => {
    const repo = createRepo(adapter);
    const alpha = repo.save({ name: "Alpha", price: 1, category: "x" });
    repo.save({ name: "Beta", price: 2, category: "y" });

    // Prime internal index map, then make it stale.
    expect(repo.findById(alpha.__id)).not.toBeNull();
    (repo as unknown as { idToRowIndex: Map<string, number> }).idToRowIndex.set(alpha.__id, 9999);

    const deleted = repo.delete(alpha.__id);
    expect(deleted).toBe(true);
    expect(repo.findById(alpha.__id)).toBeNull();
    expect(repo.count()).toBe(1);
    expect(repo.find()[0].name).toBe("Beta");
  });

  it("deleteAll(options) in batch mode returns zero when nothing matches", () => {
    const repo = createRepo(adapter);
    repo.save({ name: "Alpha", price: 1, category: "x" });
    repo.save({ name: "Beta", price: 2, category: "y" });
    expect(repo.count()).toBe(2);

    repo.beginBatch();
    const queued = repo.deleteAll({ where: [{ field: "category", operator: "=", value: "z" }] });
    expect(queued).toBe(0);
    expect(repo.count()).toBe(2);

    repo.commitBatch();
    expect(repo.count()).toBe(2);
  });

  it("beforeDelete veto is respected when queued deleteAll is committed", () => {
    let vetoId: string | null = null;
    const repo = createRepo(adapter, {
      beforeDelete: (id) => id !== vetoId,
    });

    const a = repo.save({ name: "Alpha", price: 1, category: "x" });
    const b = repo.save({ name: "Beta", price: 2, category: "x" });
    vetoId = b.__id;
    expect(repo.count()).toBe(2);

    repo.beginBatch();
    const queued = repo.deleteAll();
    expect(queued).toBe(2);
    // still deferred
    expect(repo.count()).toBe(2);

    repo.commitBatch();
    const remaining = repo.find();
    expect(remaining).toHaveLength(1);
    expect(remaining[0].__id).toBe(vetoId);
    expect([a.__id, b.__id]).toContain(remaining[0].__id);
  });
});
