import type { Entity } from "../src/core/types/Entity";
import type { Filter } from "../src/core/types/Filter";
import type { SortClause } from "../src/core/types/SortClause";
import type { QueryOptions } from "../src/core/types/QueryOptions";
import { QueryEngine } from "../src/query/QueryEngine";

interface TestUser extends Entity {
  name: string;
  age: number;
  active: boolean;
  city: string;
}

const users: TestUser[] = [
  { __id: "1", name: "Anna", age: 28, active: true, city: "Warszawa" },
  { __id: "2", name: "Jan", age: 35, active: true, city: "Kraków" },
  { __id: "3", name: "Piotr", age: 45, active: false, city: "Warszawa" },
  { __id: "4", name: "Maria", age: 22, active: true, city: "Gdańsk" },
  { __id: "5", name: "Zofia", age: 60, active: false, city: "Kraków" },
];

describe("filterEntities", () => {
  it("filters with = operator", () => {
    const filters: Filter[] = [{ field: "city", operator: "=", value: "Kraków" }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2);
    expect(result.map((u) => u.name)).toEqual(["Jan", "Zofia"]);
  });

  it("filters with != operator", () => {
    const filters: Filter[] = [{ field: "active", operator: "!=", value: false }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(3);
  });

  it("filters with > operator", () => {
    const filters: Filter[] = [{ field: "age", operator: ">", value: 40 }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2);
  });

  it("filters with < operator", () => {
    const filters: Filter[] = [{ field: "age", operator: "<", value: 30 }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2);
  });

  it("filters with >= and <= operators", () => {
    const filters: Filter[] = [
      { field: "age", operator: ">=", value: 28 },
      { field: "age", operator: "<=", value: 45 },
    ];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(3);
  });

  it("filters with contains operator", () => {
    const filters: Filter[] = [{ field: "name", operator: "contains", value: "an" }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2); // Anna (case-insensitive "An") and Jan
  });

  it("filters with startsWith operator", () => {
    const filters: Filter[] = [{ field: "name", operator: "startsWith", value: "A" }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(1); // Anna
  });

  it("filters with in operator", () => {
    const filters: Filter[] = [{ field: "city", operator: "in", value: ["Gdańsk", "Kraków"] }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(3);
  });

  it("filters with search operator (substring match)", () => {
    const filters: Filter[] = [{ field: "name", operator: "search", value: "an" }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2); // Anna (case-insensitive "An") and Jan
  });

  it("applies multiple filters as AND", () => {
    const filters: Filter[] = [
      { field: "active", operator: "=", value: true },
      { field: "age", operator: ">", value: 25 },
    ];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2); // Anna (28) and Jan (35)
  });

  it("returns all when no filters", () => {
    expect(QueryEngine.filterEntities(users, [])).toHaveLength(5);
  });
});

describe("sortEntities", () => {
  it("sorts ascending by number", () => {
    const sorts: SortClause[] = [{ field: "age", direction: "asc" }];
    const result = QueryEngine.sortEntities(users, sorts);
    expect(result.map((u) => u.age)).toEqual([22, 28, 35, 45, 60]);
  });

  it("sorts descending by number", () => {
    const sorts: SortClause[] = [{ field: "age", direction: "desc" }];
    const result = QueryEngine.sortEntities(users, sorts);
    expect(result.map((u) => u.age)).toEqual([60, 45, 35, 28, 22]);
  });

  it("sorts by string", () => {
    const sorts: SortClause[] = [{ field: "name", direction: "asc" }];
    const result = QueryEngine.sortEntities(users, sorts);
    expect(result.map((u) => u.name)).toEqual(["Anna", "Jan", "Maria", "Piotr", "Zofia"]);
  });

  it("sorts by multiple fields", () => {
    const sorts: SortClause[] = [
      { field: "city", direction: "asc" },
      { field: "age", direction: "desc" },
    ];
    const result = QueryEngine.sortEntities(users, sorts);
    // Gdańsk (Maria:22), Kraków (Zofia:60, Jan:35), Warszawa (Piotr:45, Anna:28)
    expect(result.map((u) => u.name)).toEqual(["Maria", "Zofia", "Jan", "Piotr", "Anna"]);
  });

  it("does not mutate original array", () => {
    const original = [...users];
    QueryEngine.sortEntities(users, [{ field: "age", direction: "asc" }]);
    expect(users).toEqual(original);
  });
});

describe("paginateEntities", () => {
  it("returns first page", () => {
    const result = QueryEngine.paginateEntities(users, 0, 2);
    expect(result.items).toHaveLength(2);
    expect(result.total).toBe(5);
    expect(result.offset).toBe(0);
    expect(result.limit).toBe(2);
    expect(result.hasNext).toBe(true);
  });

  it("returns last page", () => {
    const result = QueryEngine.paginateEntities(users, 4, 2);
    expect(result.items).toHaveLength(1);
    expect(result.hasNext).toBe(false);
  });

  it("returns empty if offset exceeds total", () => {
    const result = QueryEngine.paginateEntities(users, 10, 2);
    expect(result.items).toHaveLength(0);
    expect(result.hasNext).toBe(false);
  });
});

describe("groupEntities", () => {
  it("groups by field", () => {
    const groups = QueryEngine.groupEntities(users, "city");
    expect(groups).toHaveLength(3);
    const waw = groups.find((g) => g.key === "Warszawa");
    expect(waw!.count).toBe(2);
  });

  it("groups by boolean", () => {
    const groups = QueryEngine.groupEntities(users, "active");
    expect(groups).toHaveLength(2);
    const active = groups.find((g) => g.key === true);
    expect(active!.count).toBe(3);
  });
});

describe("executeQuery", () => {
  it("combines filter + sort + pagination", () => {
    const options: QueryOptions = {
      where: [{ field: "active", operator: "=", value: true }],
      orderBy: [{ field: "age", direction: "asc" }],
      offset: 1,
      limit: 1,
    };
    const result = QueryEngine.executeQuery(users, options);
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("Anna"); // active sorted by age: Maria(22), Anna(28), Jan(35) → offset 1 = Anna
  });
});
