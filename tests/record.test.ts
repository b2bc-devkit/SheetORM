import { MockSpreadsheetAdapter } from "./mocks";
import { Record } from "../src/core/Record";
import { Indexed, Required, resetDecoratorCaches } from "../src/core/decorators";
import { Query } from "../src/query/Query";
import { Registry } from "../src/core/Registry";

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
});
