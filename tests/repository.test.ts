import { MockSpreadsheetAdapter } from "./mocks";
import { Entity, TableSchema } from "../src/core/types";
import { SheetRepository } from "../src/core/SheetRepository";
import { IndexStore } from "../src/index/IndexStore";
import { SchemaMigrator } from "../src/schema/SchemaMigrator";
import { MemoryCache } from "../src/utils/cache";

interface User extends Entity {
  name: string;
  email: string;
  age: number;
  active: boolean;
}

const userSchema: TableSchema = {
  tableName: "Users",
  fields: [
    { name: "name", type: "string", required: true },
    { name: "email", type: "string", required: true },
    { name: "age", type: "number" },
    { name: "active", type: "boolean", defaultValue: true },
  ],
  indexes: [{ field: "email", unique: true }],
};

function createRepo(): { repo: SheetRepository<User>; adapter: MockSpreadsheetAdapter } {
  const adapter = new MockSpreadsheetAdapter();
  const cache = new MemoryCache();
  const indexStore = new IndexStore(adapter, cache);
  const migrator = new SchemaMigrator(adapter, indexStore);
  migrator.initialize(userSchema);

  const repo = new SheetRepository<User>(adapter, userSchema, indexStore, cache);
  return { repo, adapter };
}

describe("SheetRepository", () => {
  describe("save & findById", () => {
    it("creates a new entity with auto-generated ID", () => {
      const { repo } = createRepo();
      const user = repo.save({ name: "Jan", email: "jan@test.com", age: 30 } as Partial<User>);

      expect(user.__id).toBeDefined();
      expect(user.__createdAt).toBeDefined();
      expect(user.name).toBe("Jan");
      expect(user.active).toBe(true); // default value
    });

    it("retrieves by ID", () => {
      const { repo } = createRepo();
      const user = repo.save({ name: "Anna", email: "anna@test.com", age: 28 } as Partial<User>);

      const found = repo.findById(user.__id);
      expect(found).not.toBeNull();
      expect(found!.name).toBe("Anna");
    });

    it("updates an existing entity", () => {
      const { repo } = createRepo();
      const user = repo.save({ name: "Jan", email: "jan@test.com", age: 30 } as Partial<User>);
      const updated = repo.save({
        __id: user.__id,
        name: "Jan Updated",
        email: "jan@test.com",
        age: 31,
      } as Partial<User> & { __id: string });

      expect(updated.__id).toBe(user.__id);
      expect(updated.name).toBe("Jan Updated");
      expect(updated.age).toBe(31);
      // __createdAt is preserved (same second)
      expect(updated.__createdAt!.slice(0, 19)).toBe(user.__createdAt!.slice(0, 19));
    });

    it("throws on missing required field", () => {
      const { repo } = createRepo();
      expect(() => {
        repo.save({ name: "Jan" } as Partial<User>); // missing email
      }).toThrow(/Required field "email"/);
    });
  });

  describe("find & findOne", () => {
    it("finds all entities", () => {
      const { repo } = createRepo();
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<User>);
      repo.save({ name: "C", email: "c@test.com", age: 40 } as Partial<User>);

      const all = repo.find();
      expect(all).toHaveLength(3);
    });

    it("find with filter", () => {
      const { repo } = createRepo();
      repo.save({ name: "Young", email: "y@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "Old", email: "o@test.com", age: 50 } as Partial<User>);

      const result = repo.find({ where: [{ field: "age", operator: ">", value: 30 }] });
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe("Old");
    });

    it("findOne returns first match", () => {
      const { repo } = createRepo();
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<User>);

      const one = repo.findOne({ where: [{ field: "name", operator: "=", value: "B" }] });
      expect(one).not.toBeNull();
      expect(one!.name).toBe("B");
    });

    it("findOne returns null when no match", () => {
      const { repo } = createRepo();
      const one = repo.findOne({ where: [{ field: "name", operator: "=", value: "Nobody" }] });
      expect(one).toBeNull();
    });
  });

  describe("delete", () => {
    it("deletes by ID", () => {
      const { repo } = createRepo();
      const user = repo.save({ name: "Del", email: "del@test.com", age: 30 } as Partial<User>);

      const result = repo.delete(user.__id);
      expect(result).toBe(true);
      expect(repo.findById(user.__id)).toBeNull();
    });

    it("returns false for non-existent ID", () => {
      const { repo } = createRepo();
      expect(repo.delete("non-existent")).toBe(false);
    });

    it("deleteAll removes matching entities", () => {
      const { repo } = createRepo();
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "B", email: "b@test.com", age: 50 } as Partial<User>);
      repo.save({ name: "C", email: "c@test.com", age: 60 } as Partial<User>);

      const count = repo.deleteAll({ where: [{ field: "age", operator: ">", value: 30 }] });
      expect(count).toBe(2);
      expect(repo.count()).toBe(1);
    });
  });

  describe("count & select", () => {
    it("counts all entities", () => {
      const { repo } = createRepo();
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<User>);
      expect(repo.count()).toBe(2);
    });

    it("counts with filter", () => {
      const { repo } = createRepo();
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<User>);
      expect(repo.count({ where: [{ field: "age", operator: ">", value: 25 }] })).toBe(1);
    });

    it("select returns paginated result", () => {
      const { repo } = createRepo();
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<User>);
      repo.save({ name: "C", email: "c@test.com", age: 40 } as Partial<User>);

      const page = repo.select(0, 2);
      expect(page.items).toHaveLength(2);
      expect(page.total).toBe(3);
      expect(page.hasNext).toBe(true);
    });
  });

  describe("query builder", () => {
    it("returns a QueryBuilder that works", () => {
      const { repo } = createRepo();
      repo.save({ name: "A", email: "a@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "B", email: "b@test.com", age: 30 } as Partial<User>);
      repo.save({ name: "C", email: "c@test.com", age: 40 } as Partial<User>);

      const result = repo.query().where("age", ">=", 30).orderBy("age", "desc").execute();

      expect(result).toHaveLength(2);
      expect(result[0].name).toBe("C");
      expect(result[1].name).toBe("B");
    });
  });

  describe("groupBy", () => {
    it("groups entities by field", () => {
      const { repo } = createRepo();
      repo.save({ name: "A", email: "a@test.com", age: 20, active: true } as Partial<User>);
      repo.save({ name: "B", email: "b@test.com", age: 30, active: false } as Partial<User>);
      repo.save({ name: "C", email: "c@test.com", age: 40, active: true } as Partial<User>);

      const groups = repo.groupBy("active");
      expect(groups).toHaveLength(2);
    });
  });

  describe("lifecycle hooks", () => {
    it("calls beforeSave and afterSave", () => {
      const adapter = new MockSpreadsheetAdapter();
      const cache = new MemoryCache();
      const indexStore = new IndexStore(adapter, cache);
      const migrator = new SchemaMigrator(adapter, indexStore);
      migrator.initialize(userSchema);

      const beforeCalls: boolean[] = [];
      const afterCalls: boolean[] = [];

      const repo = new SheetRepository<User>(adapter, userSchema, indexStore, cache, {
        beforeSave: (_entity, isNew) => {
          beforeCalls.push(isNew);
        },
        afterSave: (_entity, isNew) => {
          afterCalls.push(isNew);
        },
      });

      repo.save({ name: "Hook", email: "hook@test.com", age: 25 } as Partial<User>);
      expect(beforeCalls).toEqual([true]);
      expect(afterCalls).toEqual([true]);
    });

    it("calls onValidate and rejects on errors", () => {
      const adapter = new MockSpreadsheetAdapter();
      const cache = new MemoryCache();
      const indexStore = new IndexStore(adapter, cache);
      const migrator = new SchemaMigrator(adapter, indexStore);
      migrator.initialize(userSchema);

      const repo = new SheetRepository<User>(adapter, userSchema, indexStore, cache, {
        onValidate: (entity) => {
          if (entity.age !== undefined && entity.age < 18) return ["Must be 18+"];
        },
      });

      expect(() => {
        repo.save({ name: "Kid", email: "kid@test.com", age: 10 } as Partial<User>);
      }).toThrow(/Must be 18/);
    });

    it("calls beforeDelete and can cancel", () => {
      const adapter = new MockSpreadsheetAdapter();
      const cache = new MemoryCache();
      const indexStore = new IndexStore(adapter, cache);
      const migrator = new SchemaMigrator(adapter, indexStore);
      migrator.initialize(userSchema);

      const repo = new SheetRepository<User>(adapter, userSchema, indexStore, cache, {
        beforeDelete: () => false,
      });

      const user = repo.save({ name: "Protected", email: "p@test.com", age: 30 } as Partial<User>);
      expect(repo.delete(user.__id)).toBe(false);
      expect(repo.findById(user.__id)).not.toBeNull();
    });
  });

  describe("batch operations", () => {
    it("buffers and commits", () => {
      const { repo } = createRepo();
      repo.beginBatch();
      repo.save({ name: "Batch1", email: "b1@test.com", age: 20 } as Partial<User>);
      repo.save({ name: "Batch2", email: "b2@test.com", age: 30 } as Partial<User>);

      // Before commit, data should not be persisted to sheet
      expect(repo.isBatchActive()).toBe(true);

      repo.commitBatch();
      expect(repo.isBatchActive()).toBe(false);
      expect(repo.count()).toBe(2);
    });

    it("rollback discards buffered operations", () => {
      const { repo } = createRepo();
      repo.save({ name: "Existing", email: "e@test.com", age: 20 } as Partial<User>);

      repo.beginBatch();
      repo.save({ name: "Discarded", email: "d@test.com", age: 30 } as Partial<User>);
      repo.rollbackBatch();

      expect(repo.count()).toBe(1);
    });
  });
});
