import { MockSpreadsheetAdapter } from "./MockSpreadsheetAdapter";
import { Record } from "../src/core/Record";
import { Decorators } from "../src/core/Decorators";
const { Indexed, Required, resetDecoratorCaches } = Decorators;
import { Query } from "../src/query/Query";
import { Registry } from "../src/core/Registry";
import type { RecordStatic } from "../src/core/RecordStatic";

// ─── Test Model Definitions ─────────────────────────

class Car extends Record {
  @Indexed()
  make: string;

  @Required()
  model: string;

  year: number;
  color: string;
}

class Product extends Record {
  name: string;

  @Required()
  price: number;

  @Indexed()
  category: string;
}

// ─── Tests ──────────────────────────────────────────

describe("Record ActiveRecord API", () => {
  let adapter: MockSpreadsheetAdapter;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
    Registry.getInstance().configure({ adapter });
  });

  afterEach(() => {
    Registry.reset();
    resetDecoratorCaches();
  });

  describe("save()", () => {
    it("creates a new entity with auto-generated ID", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.year = 2024;
      car.save();

      expect(car.__id).toBeDefined();
      expect(car.__createdAt).toBeDefined();
      expect(car.__updatedAt).toBeDefined();
      expect(car.make).toBe("Toyota");
    });

    it("auto-creates the table on first save", () => {
      const car = new Car();
      car.make = "Honda";
      car.model = "Civic";
      car.save();

      expect(adapter.getSheetNames()).toContain("tbl_Cars");
    });

    it("updates an existing entity", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.year = 2024;
      car.save();

      const originalId = car.__id;
      const originalCreated = car.__createdAt;

      car.color = "red";
      car.save();

      expect(car.__id).toBe(originalId);
      expect(car.__createdAt).toBe(originalCreated);
      expect(car.color).toBe("red");
    });

    it("returns this for chaining", () => {
      const car = new Car();
      car.make = "BMW";
      car.model = "X5";
      const result = car.save();
      expect(result).toBe(car);
    });

    it("persists update via findById round-trip", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.color = "blue";
      car.save();

      const savedId = car.__id;

      // Load fresh instance and update
      const loaded = Car.findById(savedId);
      expect(loaded!.color).toBe("blue");
      loaded!.color = "red";
      loaded!.save();

      // Verify raw sheet data — should update in place, not create a new row
      const sheet = adapter._getSheet("tbl_Cars")!;
      const rawData = sheet._getRawData();
      const headers = sheet.getHeaders();
      const colorIdx = headers.indexOf("color");

      expect(rawData.length).toBe(1);
      expect(rawData[0][colorIdx]).toBe("red");

      // Verify via findById
      const reloaded = Car.findById(savedId);
      expect(reloaded!.color).toBe("red");
    });

    it("throws on missing required field", () => {
      const car = new Car();
      car.make = "Toyota";
      // model is required but not set
      expect(() => car.save()).toThrow(/Required field "model"/);
    });
  });

  describe("set() and get()", () => {
    it("sets a field value and returns this", () => {
      const car = new Car();
      const result = car.set("make", "Ford");
      expect(result).toBe(car);
      expect(car.make).toBe("Ford");
    });

    it("supports chaining set calls", () => {
      const car = new Car();
      car.set("make", "Tesla").set("model", "Model 3").set("year", 2025);
      expect(car.make).toBe("Tesla");
      expect(car.model).toBe("Model 3");
      expect(car.year).toBe(2025);
    });

    it("get() retrieves a field value", () => {
      const car = new Car();
      car.make = "Toyota";
      expect(car.get("make")).toBe("Toyota");
    });
  });

  describe("delete()", () => {
    it("deletes a saved entity", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.save();

      const id = car.__id;
      const result = car.delete();

      expect(result).toBe(true);
      expect(Car.findById(id)).toBeNull();
    });

    it("returns false for unsaved entity", () => {
      const car = new Car();
      expect(car.delete()).toBe(false);
    });
  });

  describe("toJSON()", () => {
    it("returns a plain object with all fields", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.year = 2024;
      car.save();

      const json = car.toJSON();
      expect(json.__id).toBe(car.__id);
      expect(json.make).toBe("Toyota");
      expect(json.model).toBe("Corolla");
      expect(json.year).toBe(2024);
    });
  });

  describe("create() with data", () => {
    it("creates instance with data via static create()", () => {
      const car = Car.create({ make: "Toyota", model: "Corolla", year: 2024 });
      expect(car.make).toBe("Toyota");
      expect(car.model).toBe("Corolla");
      expect(car.year).toBe(2024);
    });
  });

  describe("static findById()", () => {
    it("finds a saved entity by ID", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.save();

      const found = Car.findById(car.__id);
      expect(found).not.toBeNull();
      expect(found).toBeInstanceOf(Car);
      expect(found!.make).toBe("Toyota");
    });

    it("returns null for non-existent ID", () => {
      // Ensure table exists
      Car.create({ make: "X", model: "Y" }).save();
      expect(Car.findById("nonexistent")).toBeNull();
    });
  });

  describe("static find()", () => {
    it("returns all entities", () => {
      Car.create({ make: "Toyota", model: "Corolla" }).save();
      Car.create({ make: "Honda", model: "Civic" }).save();

      const all = Car.find();
      expect(all).toHaveLength(2);
      expect(all[0]).toBeInstanceOf(Car);
    });

    it("returns entities matching query", () => {
      Car.create({ make: "Toyota", model: "Corolla" }).save();
      Car.create({ make: "Honda", model: "Civic" }).save();

      const toyotas = Car.find({
        where: [{ field: "make", operator: "=", value: "Toyota" }],
      });
      expect(toyotas).toHaveLength(1);
      expect(toyotas[0].make).toBe("Toyota");
    });
  });

  describe("static findOne()", () => {
    it("returns first matching entity", () => {
      Car.create({ make: "Toyota", model: "Corolla" }).save();
      Car.create({ make: "Honda", model: "Civic" }).save();

      const found = Car.findOne({
        where: [{ field: "make", operator: "=", value: "Honda" }],
      });
      expect(found).not.toBeNull();
      expect(found).toBeInstanceOf(Car);
      expect(found!.model).toBe("Civic");
    });

    it("returns null when no match", () => {
      Car.create({ make: "Toyota", model: "Corolla" }).save();
      const found = Car.findOne({
        where: [{ field: "make", operator: "=", value: "BMW" }],
      });
      expect(found).toBeNull();
    });
  });

  describe("static where()", () => {
    it("returns a Query and chains", () => {
      Car.create({ make: "Toyota", model: "Corolla", year: 2020 }).save();
      Car.create({ make: "Toyota", model: "Camry", year: 2024 }).save();
      Car.create({ make: "Honda", model: "Civic", year: 2023 }).save();

      const result = Car.where("make", "=", "Toyota").orderBy("year", "desc").execute();

      expect(result).toHaveLength(2);
      expect(result[0]).toBeInstanceOf(Car);
      expect(result[0].model).toBe("Camry");
    });
  });

  describe("static query()", () => {
    it("returns a Query", () => {
      Car.create({ make: "Toyota", model: "Corolla", year: 2020 }).save();
      Car.create({ make: "Honda", model: "Civic", year: 2023 }).save();
      Car.create({ make: "BMW", model: "X5", year: 2024 }).save();

      const result = Car.query().where("year", ">=", 2023).orderBy("year", "asc").execute();

      expect(result).toHaveLength(2);
      expect(result[0].make).toBe("Honda");
      expect(result[1].make).toBe("BMW");
    });
  });

  describe("static count()", () => {
    it("counts all entities", () => {
      Car.create({ make: "A", model: "B" }).save();
      Car.create({ make: "C", model: "D" }).save();
      expect(Car.count()).toBe(2);
    });

    it("counts with filter", () => {
      Car.create({ make: "Toyota", model: "A" }).save();
      Car.create({ make: "Honda", model: "B" }).save();
      expect(Car.count({ where: [{ field: "make", operator: "=", value: "Toyota" }] })).toBe(1);
    });
  });

  describe("static deleteAll()", () => {
    it("deletes matching entities", () => {
      Car.create({ make: "Toyota", model: "A" }).save();
      Car.create({ make: "Honda", model: "B" }).save();
      Car.create({ make: "Toyota", model: "C" }).save();

      const count = Car.deleteAll({
        where: [{ field: "make", operator: "=", value: "Toyota" }],
      });
      expect(count).toBe(2);
      expect(Car.count()).toBe(1);
    });
  });

  describe("static select()", () => {
    it("returns paginated results", () => {
      Car.create({ make: "A", model: "A" }).save();
      Car.create({ make: "B", model: "B" }).save();
      Car.create({ make: "C", model: "C" }).save();

      const page = Car.select(0, 2);
      expect(page.items).toHaveLength(2);
      expect(page.total).toBe(3);
      expect(page.hasNext).toBe(true);
      expect(page.items[0]).toBeInstanceOf(Car);
    });
  });

  describe("static groupBy()", () => {
    it("groups entities by field", () => {
      Car.create({ make: "Toyota", model: "Corolla" }).save();
      Car.create({ make: "Toyota", model: "Camry" }).save();
      Car.create({ make: "Honda", model: "Civic" }).save();

      const groups = Car.groupBy("make");
      expect(groups).toHaveLength(2);
      const toyota = groups.find((g) => g.key === "Toyota");
      expect(toyota!.count).toBe(2);
      expect(toyota!.items[0]).toBeInstanceOf(Car);
    });
  });

  describe("multiple Record classes", () => {
    it("creates separate tables for each class", () => {
      const car = Car.create({ make: "Toyota", model: "Corolla" });
      car.save();

      const product = Product.create({ name: "Widget", price: 9.99, category: "tools" });
      product.save();

      expect(adapter.getSheetNames()).toContain("tbl_Cars");
      expect(adapter.getSheetNames()).toContain("tbl_Products");
      expect(Car.count()).toBe(1);
      expect(Product.count()).toBe(1);
    });
  });

  describe("Query.from()", () => {
    it("works with class reference (typed)", () => {
      Car.create({ make: "Toyota", model: "Corolla", year: 2024 }).save();
      Car.create({ make: "Honda", model: "Civic", year: 2023 }).save();

      const result = Query.from(Car).where("make", "=", "Toyota").execute();

      expect(result).toHaveLength(1);
      expect(result[0]).toBeInstanceOf(Car);
    });

    it("works with string name", () => {
      // Car must be registered first (happens on save)
      Car.create({ make: "Toyota", model: "Corolla" }).save();

      const result = Query.from("Car").where("make", "=", "Toyota").execute();

      expect(result).toHaveLength(1);
    });

    it("works with table name string", () => {
      Car.create({ make: "Toyota", model: "Corolla" }).save();

      const result = Query.from("tbl_Cars").where("make", "=", "Toyota").execute();

      expect(result).toHaveLength(1);
    });

    it("supports full fluent chain", () => {
      Car.create({ make: "Toyota", model: "Corolla", year: 2020 }).save();
      Car.create({ make: "Toyota", model: "Camry", year: 2024 }).save();
      Car.create({ make: "Honda", model: "Civic", year: 2023 }).save();

      const result = Query.from(Car)
        .where("make", "=", "Toyota")
        .and("year", ">=", 2023)
        .orderBy("year", "desc")
        .limit(10)
        .offset(0)
        .execute();

      expect(result).toHaveLength(1);
      expect(result[0].model).toBe("Camry");
    });

    it("throws for unknown class name", () => {
      expect(() => Query.from("Unknown")).toThrow(/not found/);
    });
  });

  describe("full workflow", () => {
    it("create → query → update → delete cycle", () => {
      // Create
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.year = 2024;
      car.color = "blue";
      car.save();

      // Verify initial save persisted
      const check1 = Car.findById(car.__id);
      expect(check1!.color).toBe("blue");

      // Query
      const found = Car.where("make", "=", "Toyota").first();
      expect(found).not.toBeNull();
      expect(found!.model).toBe("Corolla");
      expect(found!.__id).toBe(car.__id);

      // Update
      found!.color = "red";
      found!.save();

      // Verify found instance updated
      expect(found!.color).toBe("red");

      const updated = Car.findById(car.__id);
      expect(updated!.color).toBe("red");

      // Delete
      updated!.delete();
      expect(Car.count()).toBe(0);
    });

    it("works with Query.from() end-to-end", () => {
      Product.create({ name: "Apple", price: 1.5, category: "fruit" }).save();
      Product.create({ name: "Banana", price: 0.8, category: "fruit" }).save();
      Product.create({ name: "Hammer", price: 15.0, category: "tools" }).save();

      const result = Query.from(Product).where("category", "=", "fruit").orderBy("price", "asc").execute();

      expect(result).toHaveLength(2);
      expect(result[0].name).toBe("Banana");
      expect(result[1].name).toBe("Apple");
    });
  });

  describe("OR support in repository methods", () => {
    beforeEach(() => {
      Car.create({ make: "Toyota", model: "Corolla", year: 2020 }).save();
      Car.create({ make: "Honda", model: "Civic", year: 2022 }).save();
      Car.create({ make: "BMW", model: "X5", year: 2024 }).save();
    });

    it("count() with whereGroups counts only matching entities", () => {
      const count = Car.count({
        whereGroups: [
          [{ field: "make", operator: "=", value: "Toyota" }],
          [{ field: "make", operator: "=", value: "BMW" }],
        ],
      });
      expect(count).toBe(2);
    });

    it("deleteAll() with whereGroups deletes only matching entities", () => {
      const deleted = Car.deleteAll({
        whereGroups: [
          [{ field: "make", operator: "=", value: "Toyota" }],
          [{ field: "make", operator: "=", value: "BMW" }],
        ],
      });
      expect(deleted).toBe(2);
      expect(Car.count()).toBe(1);
      const remaining = Car.find();
      expect(remaining[0].make).toBe("Honda");
    });

    it("select() with whereGroups paginates only matching entities", () => {
      const page = Car.select(0, 10, {
        whereGroups: [
          [{ field: "make", operator: "=", value: "Toyota" }],
          [{ field: "make", operator: "=", value: "Honda" }],
        ],
      });
      expect(page.total).toBe(2);
      expect(page.items).toHaveLength(2);
    });

    it("groupBy() with whereGroups groups only matching entities", () => {
      const groups = Car.groupBy("make", {
        whereGroups: [
          [{ field: "make", operator: "=", value: "Toyota" }],
          [{ field: "make", operator: "=", value: "Honda" }],
        ],
      });
      expect(groups).toHaveLength(2);
      expect(groups.every((g) => g.count === 1)).toBe(true);
    });
  });

  describe("saveAll() error-path cache invalidation", () => {
    it("invalidates cache when saveAll() fails mid-operation", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.year = 2024;
      car.save();

      expect(Car.count()).toBe(1);

      expect(() => {
        Car.saveAll([
          { make: "Honda", model: "Civic" },
          { make: "BMW" }, // missing required 'model'
        ]);
      }).toThrow(/Required field "model"/);

      // Cache invalidated — subsequent read re-loads from sheet (no partial writes)
      const all = Car.find();
      expect(all).toHaveLength(1);
      expect(all[0].make).toBe("Toyota");
    });
  });

  describe("commitBatch() error-path cache invalidation", () => {
    it("invalidates cache and re-throws on mid-batch failure", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.save();

      // Access the underlying repository for batch operations
      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);

      repo.beginBatch();
      repo.save({ make: "Honda", model: "Civic" }); // valid — will be written
      repo.save({ make: "BMW" } as unknown as Parameters<typeof repo.save>[0]); // missing required 'model'

      expect(() => repo.commitBatch()).toThrow(/Required field "model"/);

      // Cache invalidated — reads re-load from sheet; first save was committed
      const all = Car.find();
      expect(all).toHaveLength(2);
      expect(all.map((c) => c.make).sort()).toEqual(["Honda", "Toyota"]);
    });

    it("batch is no longer active after commitBatch error", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.save();

      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);

      repo.beginBatch();
      repo.save({ make: "Fail" } as unknown as Parameters<typeof repo.save>[0]);

      expect(() => repo.commitBatch()).toThrow();
      expect(repo.isBatchActive()).toBe(false);
    });
  });

  describe("Registry.configure() re-configuration", () => {
    it("clears class maps on re-configure", () => {
      // Register the Car class by doing a save
      const car = new Car();
      car.make = "Test";
      car.model = "Model";
      car.save();

      const registry = Registry.getInstance();
      expect(registry.getClassByName("Car")).toBeDefined();

      // Re-configure with same adapter
      registry.configure({ adapter });
      expect(registry.getClassByName("Car")).toBeUndefined();
    });
  });

  describe("Registry.clearCache() with index store", () => {
    it("clears entity cache and allows re-read from sheet", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.save();

      // Populate cache via find
      expect(Car.find()).toHaveLength(1);

      // Clear all caches
      Registry.getInstance().clearCache();

      // Subsequent find should re-read from sheet (still works)
      const all = Car.find();
      expect(all).toHaveLength(1);
      expect(all[0].make).toBe("Toyota");
    });
  });

  describe("doSave null-entity safety", () => {
    it("falls back to sheet scan when cache entry is null", () => {
      // Create and save an entity to populate cache
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.year = 2020;
      car.save();

      const id = car.__id;

      // Tamper with cache: set entity slot to undefined while keeping idToRowIndex
      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
      const cache = (
        repo as unknown as { cache: { get(k: string): unknown[] | null; set(k: string, v: unknown): void } }
      ).cache;
      const cacheKey = (repo as unknown as { dataCacheKey: string }).dataCacheKey;
      const cached = cache.get(cacheKey) as unknown[];
      if (cached) cached[0] = undefined;

      // Update should not throw — should fall through to sheet scan
      const updatedCar = new Car();
      updatedCar.__id = id;
      updatedCar.make = "Toyota";
      updatedCar.model = "Corolla";
      updatedCar.year = 2025;
      updatedCar.save();

      // Clear cache to verify via fresh sheet read
      Registry.getInstance().clearCache();
      const refetched = Car.findById(id);
      expect(refetched).not.toBeNull();
      expect(refetched!.year).toBe(2025);
    });
  });

  describe("loadAllEntities empty-row filtering", () => {
    it("skips rows with empty __id", () => {
      // Create a valid entity
      const car = new Car();
      car.make = "Honda";
      car.model = "Civic";
      car.year = 2022;
      car.save();

      // Inject an empty row directly into the sheet (simulating garbage/blank row)
      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
      const sheet = (repo as unknown as { getSheet(): { appendRow(v: unknown[]): void } }).getSheet();
      sheet.appendRow(["", "", "", "", "", ""]);

      // Clear cache so next read re-loads from sheet
      Registry.getInstance().clearCache();

      // Find should not include the empty-__id row
      const all = Car.find();
      expect(all).toHaveLength(1);
      expect(all[0].make).toBe("Honda");
    });
  });

  describe("saveAll() mixed create and update", () => {
    it("persists both new and updated entities in a single batch", () => {
      const car = new Car();
      car.make = "Honda";
      car.model = "Civic";
      car.year = 2020;
      car.color = "Red";
      car.save();
      const existingId = car.__id;

      Car.saveAll([
        { __id: existingId, make: "Honda", model: "Accord", year: 2021, color: "Blue" },
        { make: "Toyota", model: "Camry", year: 2022, color: "White" },
        { make: "Ford", model: "Focus", year: 2023, color: "Black" },
      ]);

      expect(Car.count()).toBe(3);
      const updated = Car.findById(existingId);
      expect(updated).not.toBeNull();
      expect(updated!.model).toBe("Accord");
      expect(updated!.color).toBe("Blue");
    });
  });

  describe("findOne() without arguments", () => {
    it("returns an entity when called with no filter", () => {
      const c1 = new Car();
      c1.make = "Honda";
      c1.model = "Civic";
      c1.year = 2020;
      c1.save();

      const c2 = new Car();
      c2.make = "Toyota";
      c2.model = "Camry";
      c2.year = 2021;
      c2.save();

      const result = Car.findOne();
      expect(result).not.toBeNull();
      expect(result!.__id).toBeDefined();
    });
  });

  describe("deleteAll() without arguments", () => {
    it("deletes all entities and returns the count", () => {
      for (let i = 0; i < 3; i++) {
        const c = new Car();
        c.make = "Brand";
        c.model = `Model${i}`;
        c.year = 2020 + i;
        c.save();
      }
      expect(Car.count()).toBe(3);

      const deleted = Car.deleteAll();
      expect(deleted).toBe(3);
      expect(Car.count()).toBe(0);
    });
  });

  describe("save after gap row", () => {
    it("does not overwrite an existing entity when a gap row is present", () => {
      const car1 = new Car();
      car1.make = "Honda";
      car1.model = "Civic";
      car1.year = 2020;
      car1.save();

      // Inject a gap row (empty __id) directly into the sheet
      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
      const sheet = (repo as unknown as { getSheet(): { appendRow(v: unknown[]): void } }).getSheet();
      sheet.appendRow(["", "", "", "", "", ""]);

      const car2 = new Car();
      car2.make = "Toyota";
      car2.model = "Camry";
      car2.year = 2021;
      car2.save();

      // Clear cache to force re-read from sheet
      Registry.getInstance().clearCache();

      // Both valid entities must still exist
      expect(Car.count()).toBe(2);
      const found1 = Car.findById(car1.__id);
      expect(found1).not.toBeNull();
      expect(found1!.make).toBe("Honda");
    });

    it("findById returns correct entity after gap row", () => {
      const car1 = new Car();
      car1.make = "Honda";
      car1.model = "Civic";
      car1.year = 2020;
      car1.save();

      // Inject empty gap row directly into the sheet
      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
      const sheet = (repo as unknown as { getSheet(): { appendRow(v: unknown[]): void } }).getSheet();
      sheet.appendRow(["", "", "", "", "", ""]);

      const car2 = new Car();
      car2.make = "Toyota";
      car2.model = "Camry";
      car2.year = 2021;
      car2.save();

      // Clear cache to force full reload (rebuilds idToRowIndex)
      Registry.getInstance().clearCache();

      // findById must return the correct entity despite gap row shifting cache indices
      const found2 = Car.findById(car2.__id);
      expect(found2).not.toBeNull();
      expect(found2!.make).toBe("Toyota");
      expect(found2!.__id).toBe(car2.__id);
    });
  });

  describe("rollbackBatch()", () => {
    it("discards buffered operations without writing", () => {
      Car.create({ make: "Toyota", model: "Corolla" }).save();
      expect(Car.count()).toBe(1);

      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
      repo.beginBatch();
      repo.save({ make: "Honda", model: "Civic" });
      repo.save({ make: "BMW", model: "X5" });
      repo.delete(Car.find()[0].__id);

      repo.rollbackBatch();

      expect(repo.isBatchActive()).toBe(false);
      expect(Car.count()).toBe(1);
      expect(Car.find()[0].make).toBe("Toyota");
    });
  });

  describe("batch happy-path lifecycle", () => {
    it("beginBatch → save → delete → commitBatch applies all operations", () => {
      const car1 = Car.create({ make: "Toyota", model: "Corolla" });
      car1.save();
      const car2 = Car.create({ make: "Honda", model: "Civic" });
      car2.save();
      expect(Car.count()).toBe(2);

      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
      expect(repo.isBatchActive()).toBe(false);

      repo.beginBatch();
      expect(repo.isBatchActive()).toBe(true);

      repo.save({ make: "BMW", model: "X5" });
      repo.delete(car1.__id);

      repo.commitBatch();

      expect(repo.isBatchActive()).toBe(false);
      expect(Car.count()).toBe(2);
      const remaining = Car.find();
      expect(remaining.map((c) => c.make).sort()).toEqual(["BMW", "Honda"]);
    });

    it("count() during batch returns sheet state, not buffered state", () => {
      Car.create({ make: "Toyota", model: "Corolla" }).save();
      expect(Car.count()).toBe(1);

      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
      repo.beginBatch();
      repo.save({ make: "Honda", model: "Civic" });

      // count() reads from sheet/cache — buffered save is not visible
      expect(Car.count()).toBe(1);

      repo.commitBatch();
      expect(Car.count()).toBe(2);
    });
  });

  describe("findById after gap row uses cached array scan", () => {
    it("returns correct entity without re-reading sheet data", () => {
      const car1 = new Car();
      car1.make = "Honda";
      car1.model = "Civic";
      car1.year = 2020;
      car1.save();

      const car2 = new Car();
      car2.make = "Toyota";
      car2.model = "Camry";
      car2.year = 2021;
      car2.save();

      // Inject gap row between the two entities
      const repo = Registry.getInstance().ensureRepository(Car as unknown as RecordStatic);
      const sheet = (repo as unknown as { getSheet(): { appendRow(v: unknown[]): void } }).getSheet();
      sheet.appendRow(["", "", "", "", "", ""]);

      // Clear cache to rebuild with gap row present
      Registry.getInstance().clearCache();

      // First find loads entities and builds idToRowIndex with raw row indices
      const all = Car.find();
      expect(all).toHaveLength(2);

      // findById for entity after gap row should return correct entity via cached array scan
      const found = Car.findById(car2.__id);
      expect(found).not.toBeNull();
      expect(found!.make).toBe("Toyota");
      expect(found!.__id).toBe(car2.__id);
    });
  });

  describe("saveAll() batch idToRowIndex integrity", () => {
    it("assigns correct row indices so delete targets the right entity", () => {
      // Create two entities in a single saveAll batch
      Car.saveAll([
        { make: "Alpha", model: "A1", year: 2020, color: "Red" },
        { make: "Beta", model: "B1", year: 2021, color: "Blue" },
      ]);
      expect(Car.count()).toBe(2);

      const alpha = Car.findOne({ where: [{ field: "make", operator: "=", value: "Alpha" }] });
      const beta = Car.findOne({ where: [{ field: "make", operator: "=", value: "Beta" }] });
      expect(alpha).not.toBeNull();
      expect(beta).not.toBeNull();

      // Delete Beta — must NOT affect Alpha
      beta!.delete();
      expect(Car.count()).toBe(1);

      const remaining = Car.findById(alpha!.__id);
      expect(remaining).not.toBeNull();
      expect(remaining!.make).toBe("Alpha");

      // Beta should be gone
      expect(Car.findById(beta!.__id)).toBeNull();
    });

    it("delete() returns false on unsaved record (no __id)", () => {
      const car = new Car();
      car.make = "Ghost";
      car.model = "Phantom";
      expect(car.delete()).toBe(false);
    });

    it("toJSON() returns undefined __id for unsaved entity", () => {
      const car = Car.create({ make: "Ghost", model: "Phantom", year: 2024, color: "black" });
      const json = car.toJSON();
      expect(json.__id).toBeUndefined();
      expect(json.make).toBe("Ghost");
      expect(json.model).toBe("Phantom");
    });

    it("saveAll() with empty array returns empty array", () => {
      const result = Car.saveAll([]);
      expect(result).toEqual([]);
    });

    it("deleteAll() returns zero when no entities match filter", () => {
      Car.create({ make: "Toyota", model: "Supra", year: 2020, color: "white" }).save();
      const count = Car.deleteAll({ where: [{ field: "make", operator: "=", value: "NonExistent" }] });
      expect(count).toBe(0);
      expect(Car.count()).toBe(1);
    });

    it("save() includes null fields but excludes undefined fields", () => {
      const car = new Car();
      car.make = "Toyota";
      car.model = "Corolla";
      car.year = 2024;
      car.color = null as unknown as string;
      car.save();

      const found = Car.findById(car.__id);
      expect(found).not.toBeNull();
      // null field should be included (serialized), undefined field excluded
      expect(found!.color).toBeNull();
    });

    it("Query.from(Class) auto-registers class without prior save", () => {
      // Query.from with class reference should auto-register without needing a save first
      const query = Query.from(Car);
      expect(query).toBeDefined();
      const results = query.execute();
      expect(results).toEqual([]);
    });

    it("deleteAll uses individual deletes for 2 entities and bulk for 3", () => {
      // Create exactly 3 entities
      Car.create({ make: "A", model: "A1", year: 2020, color: "red" }).save();
      Car.create({ make: "B", model: "B1", year: 2021, color: "blue" }).save();
      Car.create({ make: "C", model: "C1", year: 2022, color: "green" }).save();
      expect(Car.count()).toBe(3);

      // Delete all 3 — triggers bulk path (> 2)
      const deletedBulk = Car.deleteAll();
      expect(deletedBulk).toBe(3);
      expect(Car.count()).toBe(0);

      // Create exactly 2 entities
      Car.create({ make: "D", model: "D1", year: 2023, color: "black" }).save();
      Car.create({ make: "E", model: "E1", year: 2024, color: "white" }).save();
      expect(Car.count()).toBe(2);

      // Delete all 2 — triggers individual path (<= 2)
      const deletedIndiv = Car.deleteAll();
      expect(deletedIndiv).toBe(2);
      expect(Car.count()).toBe(0);
    });
  });
});
