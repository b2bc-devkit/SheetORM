import { Entity } from "../src/core/types";
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

  it("first() returns the first match", () => {
    const result = createBuilder().where("category", "=", "vegetable").orderBy("price", "asc").first();
    expect(result).not.toBeNull();
    expect(result!.name).toBe("Carrot");
  });

  it("first() returns null when no match", () => {
    const result = createBuilder().where("category", "=", "nonexistent").first();
    expect(result).toBeNull();
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
});
