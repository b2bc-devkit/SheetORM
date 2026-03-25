import type { Entity } from "../src/core/types/Entity";
import { Query } from "../src/query/Query";

interface TestItem extends Entity {
  name: string;
  price: number;
  category: string;
}

const items: TestItem[] = [
  { __id: "1", name: "Apple", price: 1.5, category: "fruit" },
  { __id: "2", name: "Banana", price: 0.8, category: "fruit" },
  { __id: "3", name: "Carrot", price: 1.2, category: "vegetable" },
  { __id: "4", name: "Donut", price: 2.5, category: "pastry" },
  { __id: "5", name: "Eggplant", price: 3.0, category: "vegetable" },
];

function createBuilder(): Query<TestItem> {
  return new Query(() => [...items]);
}

describe("Query", () => {
  it("filters with where()", () => {
    const result = createBuilder().where("category", "=", "fruit").execute();
    expect(result).toHaveLength(2);
    expect(result.map((r) => r.name).sort()).toEqual(["Apple", "Banana"]);
  });

  it("chains multiple where() as AND", () => {
    const result = createBuilder().where("category", "=", "fruit").and("price", ">", 1).execute();
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("Apple");
  });

  it("sorts results", () => {
    const result = createBuilder().orderBy("price", "desc").execute();
    expect(result[0].name).toBe("Eggplant");
    expect(result[4].name).toBe("Banana");
  });

  it("limits results", () => {
    const result = createBuilder().orderBy("price", "asc").limit(2).execute();
    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("Banana");
  });

  it("applies offset", () => {
    const result = createBuilder().orderBy("price", "asc").offset(2).limit(2).execute();
    expect(result).toHaveLength(2);
    // Sorted: Banana(0.8), Carrot(1.2), Apple(1.5), Donut(2.5), Eggplant(3.0)
    // offset 2 → Apple, Donut
    expect(result[0].name).toBe("Apple");
  });

  it("limit() throws for negative number", () => {
    expect(() => createBuilder().limit(-1)).toThrow();
  });

  it("limit() throws for NaN", () => {
    expect(() => createBuilder().limit(NaN)).toThrow();
  });

  it("offset() throws for negative number", () => {
    expect(() => createBuilder().offset(-1)).toThrow();
  });

  it("offset() throws for Infinity", () => {
    expect(() => createBuilder().offset(Infinity)).toThrow();
  });

  it("first() returns the first match", () => {
    const result = createBuilder().where("category", "=", "vegetable").orderBy("price", "asc").first();
    expect(result).not.toBeNull();
    expect(result!.name).toBe("Carrot");
  });

  it("first() returns null when no match", () => {
    const result = createBuilder().where("category", "=", "nonexistent").first();
    expect(result).toBeNull();
  });

  it("first() respects offset", () => {
    // Sorted by price asc: Banana(0.8), Carrot(1.2), Apple(1.5), Donut(2.5), Eggplant(3.0)
    const result = createBuilder().orderBy("price", "asc").offset(2).first();
    expect(result).not.toBeNull();
    expect(result!.name).toBe("Apple");
  });

  it("count() returns matching count", () => {
    const count = createBuilder().where("category", "=", "fruit").count();
    expect(count).toBe(2);
  });

  it("select() returns paginated result", () => {
    const result = createBuilder().where("category", "=", "vegetable").select(0, 10);
    expect(result.total).toBe(2);
    expect(result.items).toHaveLength(2);
    expect(result.hasNext).toBe(false);
  });

  it("groupBy() groups results", () => {
    const groups = createBuilder().groupBy("category");
    expect(groups).toHaveLength(3);
    const fruit = groups.find((g) => g.key === "fruit");
    expect(fruit!.count).toBe(2);
  });

  it("build() returns query options", () => {
    const qo = createBuilder()
      .where("name", "startsWith", "A")
      .orderBy("price", "asc")
      .limit(5)
      .offset(0)
      .build();

    expect(qo.where).toHaveLength(1);
    expect(qo.orderBy).toHaveLength(1);
    expect(qo.limit).toBe(5);
    expect(qo.offset).toBe(0);
  });

  it("build() includes offset when set alone", () => {
    const qo = createBuilder().offset(5).build();
    expect(qo.offset).toBe(5);
  });

  describe("or()", () => {
    it("returns entities matching either condition", () => {
      const result = createBuilder()
        .where("category", "=", "pastry")
        .or("category", "=", "vegetable")
        .execute();
      expect(result).toHaveLength(3);
      expect(result.map((r) => r.name).sort()).toEqual(["Carrot", "Donut", "Eggplant"]);
    });

    it("applies AND within each OR group", () => {
      // (category=fruit AND price>1) OR (category=vegetable AND price<2)
      const result = createBuilder()
        .where("category", "=", "fruit")
        .and("price", ">", 1)
        .or("category", "=", "vegetable")
        .and("price", "<", 2)
        .execute();
      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name).sort()).toEqual(["Apple", "Carrot"]);
    });

    it("chains multiple or() calls", () => {
      const result = createBuilder()
        .where("name", "=", "Apple")
        .or("name", "=", "Banana")
        .or("name", "=", "Donut")
        .execute();
      expect(result).toHaveLength(3);
      expect(result.map((r) => r.name).sort()).toEqual(["Apple", "Banana", "Donut"]);
    });

    it("works with orderBy", () => {
      const result = createBuilder()
        .where("category", "=", "fruit")
        .or("category", "=", "pastry")
        .orderBy("price", "desc")
        .execute();
      expect(result[0].name).toBe("Donut");
      expect(result[result.length - 1].name).toBe("Banana");
    });

    it("works with limit and offset", () => {
      const result = createBuilder()
        .where("category", "=", "fruit")
        .or("category", "=", "vegetable")
        .orderBy("price", "asc")
        .limit(2)
        .offset(1)
        .execute();
      expect(result).toHaveLength(2);
      expect(result[0].name).toBe("Carrot");
    });

    it("first() returns first OR match", () => {
      const result = createBuilder()
        .where("category", "=", "vegetable")
        .or("category", "=", "pastry")
        .orderBy("price", "asc")
        .first();
      expect(result).not.toBeNull();
      expect(result!.name).toBe("Carrot");
    });

    it("count() counts OR matches", () => {
      const count = createBuilder().where("category", "=", "fruit").or("category", "=", "pastry").count();
      expect(count).toBe(3);
    });

    it("select() paginates OR results", () => {
      const result = createBuilder()
        .where("category", "=", "fruit")
        .or("category", "=", "vegetable")
        .select(0, 2);
      expect(result.total).toBe(4);
      expect(result.items).toHaveLength(2);
      expect(result.hasNext).toBe(true);
    });

    it("groupBy() groups OR results", () => {
      const groups = createBuilder()
        .where("category", "=", "fruit")
        .or("category", "=", "vegetable")
        .groupBy("category");
      expect(groups).toHaveLength(2);
    });

    it("build() returns whereGroups for OR queries", () => {
      const qo = createBuilder().where("category", "=", "fruit").or("name", "=", "Donut").build();
      expect(qo.where).toBeUndefined();
      expect(qo.whereGroups).toHaveLength(2);
      expect(qo.whereGroups![0]).toHaveLength(1);
      expect(qo.whereGroups![1]).toHaveLength(1);
    });

    it("build() returns where (not whereGroups) for AND-only queries", () => {
      const qo = createBuilder().where("category", "=", "fruit").and("price", ">", 1).build();
      expect(qo.where).toHaveLength(2);
      expect(qo.whereGroups).toBeUndefined();
    });

    it("or() without preceding where() still filters correctly", () => {
      const result = createBuilder().or("category", "=", "pastry").execute();
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe("Donut");
    });

    it("or().and() without preceding where() chains correctly", () => {
      // First OR group: category=pastry, second OR group: category=fruit AND price>1
      const result = createBuilder()
        .or("category", "=", "pastry")
        .or("category", "=", "fruit")
        .and("price", ">", 1)
        .execute();
      // pastry → Donut; fruit AND price>1 → Apple
      expect(result).toHaveLength(2);
      expect(result.map((r) => r.name).sort()).toEqual(["Apple", "Donut"]);
    });
  });

  describe("limit(0) returns empty array", () => {
    it("execute() with limit(0) returns an empty array", () => {
      const result = createBuilder().limit(0).execute();
      expect(result).toHaveLength(0);
    });

    it("build() with limit(0) includes limit 0", () => {
      const opts = createBuilder().limit(0).build();
      expect(opts.limit).toBe(0);
    });
  });

  describe("edge cases", () => {
    it("build() with no filters returns all undefined options", () => {
      const opts = createBuilder().build();
      expect(opts.where).toBeUndefined();
      expect(opts.whereGroups).toBeUndefined();
      expect(opts.orderBy).toBeUndefined();
      expect(opts.limit).toBeUndefined();
      expect(opts.offset).toBeUndefined();
    });

    it("first() returns null when offset exceeds result count", () => {
      const result = createBuilder().where("category", "=", "fruit").offset(100).first();
      expect(result).toBeNull();
    });
  });

  it("groupBy() respects orderBy before grouping", () => {
    const groups = createBuilder().orderBy("price", "desc").groupBy("category");
    expect(groups.length).toBeGreaterThanOrEqual(2);
    const fruitGroup = groups.find((g) => g.key === "fruit");
    expect(fruitGroup).toBeDefined();
    expect(fruitGroup!.items.length).toBe(2);
  });

  it("execute() with orderBy and offset combined returns correct slice", () => {
    const result = createBuilder().orderBy("price", "asc").offset(2).limit(2).execute();
    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("Apple");  // price 1.5 (3rd after sort)
    expect(result[1].name).toBe("Donut");  // price 2.5 (4th after sort)
  });
});
