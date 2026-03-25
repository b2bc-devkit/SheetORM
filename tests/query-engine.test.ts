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
    expect(result.map((u) => u.__id).sort()).toEqual(["1", "2", "4"]);
  });

  it("filters with > operator", () => {
    const filters: Filter[] = [{ field: "age", operator: ">", value: 40 }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2);
    expect(result.map((u) => u.name).sort()).toEqual(["Piotr", "Zofia"]);
  });

  it("filters with < operator", () => {
    const filters: Filter[] = [{ field: "age", operator: "<", value: 30 }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2);
    expect(result.map((u) => u.name).sort()).toEqual(["Anna", "Maria"]);
  });

  it("filters with >= and <= operators", () => {
    const filters: Filter[] = [
      { field: "age", operator: ">=", value: 28 },
      { field: "age", operator: "<=", value: 45 },
    ];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(3);
    expect(result.map((u) => u.name).sort()).toEqual(["Anna", "Jan", "Piotr"]);
  });

  it("filters with contains operator", () => {
    const filters: Filter[] = [{ field: "name", operator: "contains", value: "an" }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2);
    expect(result.map((u) => u.name).sort()).toEqual(["Anna", "Jan"]);
  });

  it("contains is case-insensitive for uppercase query", () => {
    const filters: Filter[] = [{ field: "name", operator: "contains", value: "AN" }];
    const result = QueryEngine.filterEntities(users, filters);
    expect(result).toHaveLength(2);
    expect(result.map((u) => u.name).sort()).toEqual(["Anna", "Jan"]);
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
    expect(result.map((u) => u.__id).sort()).toEqual(["2", "4", "5"]);
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

  it("applies only sort and limit when no filters are provided", () => {
    const options: QueryOptions = {
      orderBy: [{ field: "age", direction: "desc" }],
      limit: 3,
    };
    const result = QueryEngine.executeQuery(users, options);
    expect(result).toHaveLength(3);
    expect(result.map((u) => u.name)).toEqual(["Zofia", "Piotr", "Jan"]);
  });

  it("returns all entities for empty options", () => {
    const result = QueryEngine.executeQuery(users, {});
    expect(result).toHaveLength(5);
  });
});

describe("filterEntitiesOr", () => {
  it("matches entities passing any group", () => {
    const groups: Filter[][] = [
      [{ field: "city", operator: "=", value: "Gdańsk" }],
      [{ field: "city", operator: "=", value: "Kraków" }],
    ];
    const result = QueryEngine.filterEntitiesOr(users, groups);
    expect(result).toHaveLength(3);
    expect(result.map((u) => u.name).sort()).toEqual(["Jan", "Maria", "Zofia"]);
  });

  it("applies AND within each group", () => {
    const groups: Filter[][] = [
      [
        { field: "city", operator: "=", value: "Kraków" },
        { field: "active", operator: "=", value: true },
      ],
      [
        { field: "city", operator: "=", value: "Warszawa" },
        { field: "active", operator: "=", value: false },
      ],
    ];
    const result = QueryEngine.filterEntitiesOr(users, groups);
    expect(result).toHaveLength(2);
    expect(result.map((u) => u.name).sort()).toEqual(["Jan", "Piotr"]);
  });

  it("returns all entities for empty groups", () => {
    const result = QueryEngine.filterEntitiesOr(users, []);
    expect(result).toHaveLength(5);
  });
});

describe("executeQuery with whereGroups", () => {
  it("uses OR groups when whereGroups is provided", () => {
    const options: QueryOptions = {
      whereGroups: [
        [{ field: "city", operator: "=", value: "Gdańsk" }],
        [{ field: "name", operator: "=", value: "Anna" }],
      ],
      orderBy: [{ field: "age", direction: "asc" }],
    };
    const result = QueryEngine.executeQuery(users, options);
    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("Maria");
    expect(result[1].name).toBe("Anna");
  });

  it("prefers whereGroups over where", () => {
    const options: QueryOptions = {
      where: [{ field: "active", operator: "=", value: true }],
      whereGroups: [[{ field: "city", operator: "=", value: "Gdańsk" }]],
    };
    const result = QueryEngine.executeQuery(users, options);
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("Maria");
  });
});

describe("string operators with non-string values", () => {
  it("contains with non-string value matches nothing", () => {
    const filters: Filter[] = [{ field: "name", operator: "contains", value: 123 }];
    expect(QueryEngine.filterEntities(users, filters)).toHaveLength(0);
  });

  it("startsWith with non-string value matches nothing", () => {
    const filters: Filter[] = [{ field: "name", operator: "startsWith", value: null }];
    expect(QueryEngine.filterEntities(users, filters)).toHaveLength(0);
  });

  it("search with non-string value matches nothing", () => {
    const filters: Filter[] = [{ field: "name", operator: "search", value: undefined }];
    expect(QueryEngine.filterEntities(users, filters)).toHaveLength(0);
  });
});

describe("pagination edge cases", () => {
  it("negative offset is clamped to 0", () => {
    const result = QueryEngine.paginateEntities(users, -5, 2);
    expect(result.offset).toBe(0);
    expect(result.items).toHaveLength(2);
  });

  it("negative limit defaults to full length", () => {
    const result = QueryEngine.paginateEntities(users, 0, -1);
    expect(result.limit).toBe(users.length);
    expect(result.items).toHaveLength(users.length);
  });

  it("NaN offset defaults to 0", () => {
    const result = QueryEngine.paginateEntities(users, NaN, 2);
    expect(result.offset).toBe(0);
    expect(result.items).toHaveLength(2);
  });
});

describe("unknown filter operator", () => {
  it("returns no matches for an unrecognized operator", () => {
    const filters: Filter[] = [
      { field: "name", operator: "regex" as unknown as Filter["operator"], value: ".*" },
    ];
    expect(QueryEngine.filterEntities(users, filters)).toHaveLength(0);
  });
});

describe("in operator with large array (Set optimization)", () => {
  it("uses Set for arrays with more than 8 elements", () => {
    const manyUsers: TestUser[] = Array.from({ length: 20 }, (_, i) => ({
      __id: `u${i}`,
      name: `User${i}`,
      age: 20 + i,
      active: true,
      city: `City${i}`,
    }));

    const targetCities = Array.from({ length: 10 }, (_, i) => `City${i}`);
    const filters: Filter[] = [{ field: "city", operator: "in", value: targetCities }];
    const result = QueryEngine.filterEntities(manyUsers, filters);
    expect(result).toHaveLength(10);
    expect(result.every((u) => targetCities.includes(u.city))).toBe(true);
  });
});

describe("relational operator type guards", () => {
  it("returns false when field type differs from value type (number vs string)", () => {
    const filters: Filter[] = [{ field: "age", operator: ">", value: "thirty" as unknown as number }];
    expect(QueryEngine.filterEntities(users, filters)).toHaveLength(0);
  });

  it("compares strings when both field and value are strings", () => {
    const filters: Filter[] = [{ field: "name", operator: "<", value: "Jan" }];
    const result = QueryEngine.filterEntities(users, filters);
    // Anna < Jan (lexicographic)
    expect(result.map((u) => u.name)).toEqual(["Anna"]);
  });
});

describe("nested field paths", () => {
  interface NestedUser extends Entity {
    name: string;
    profile: { city: string; score: number };
  }

  const nestedUsers: NestedUser[] = [
    { __id: "1", name: "Anna", profile: { city: "Warszawa", score: 80 } },
    { __id: "2", name: "Jan", profile: { city: "Kraków", score: 95 } },
    { __id: "3", name: "Piotr", profile: { city: "Warszawa", score: 60 } },
  ];

  it("filters by nested dot-path field", () => {
    const filters: Filter[] = [{ field: "profile.city", operator: "=", value: "Warszawa" }];
    const result = QueryEngine.filterEntities(nestedUsers, filters);
    expect(result).toHaveLength(2);
    expect(result.map((u) => u.name).sort()).toEqual(["Anna", "Piotr"]);
  });

  it("filters by nested slash-path field", () => {
    const filters: Filter[] = [{ field: "profile/city", operator: "=", value: "Kraków" }];
    const result = QueryEngine.filterEntities(nestedUsers, filters);
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("Jan");
  });

  it("sorts by nested field", () => {
    const sorts: SortClause[] = [{ field: "profile.score", direction: "asc" }];
    const result = QueryEngine.sortEntities(nestedUsers, sorts);
    expect(result.map((u) => u.name)).toEqual(["Piotr", "Anna", "Jan"]);
  });

  it("returns undefined for missing nested segment", () => {
    const usersWithMissing: Entity[] = [
      { __id: "1", name: "A" },
      { __id: "2", name: "B", profile: { city: "X" } },
    ];
    const filters: Filter[] = [{ field: "profile.city", operator: "=", value: "X" }];
    const result = QueryEngine.filterEntities(usersWithMissing, filters);
    expect(result).toHaveLength(1);
    expect(result[0].__id).toBe("2");
  });
});

describe("paginateEntities with limit=0", () => {
  it("returns empty items when limit is 0", () => {
    const result = QueryEngine.paginateEntities(users, 0, 0);
    expect(result.items).toHaveLength(0);
    expect(result.total).toBe(users.length);
    expect(result.limit).toBe(0);
    expect(result.hasNext).toBe(true);
  });
});

describe("sortEntities with null values", () => {
  it("places null values before non-null in ascending order", () => {
    const data: Entity[] = [
      { __id: "1", name: "Anna", score: 80 },
      { __id: "2", name: "Jan", score: null as unknown as number },
      { __id: "3", name: "Piotr", score: 60 },
    ];
    const sorted = QueryEngine.sortEntities(data, [{ field: "score", direction: "asc" }]);
    expect(sorted[0].__id).toBe("2"); // null first
    expect(sorted[1].__id).toBe("3"); // 60
    expect(sorted[2].__id).toBe("1"); // 80
  });

  it("places null values last in descending sort", () => {
    const data: Entity[] = [
      { __id: "1", name: "Anna", score: 80 },
      { __id: "2", name: "Jan", score: null as unknown as number },
      { __id: "3", name: "Piotr", score: 60 },
    ];
    const sorted = QueryEngine.sortEntities(data, [{ field: "score", direction: "desc" }]);
    expect(sorted[0].__id).toBe("1"); // 80
    expect(sorted[1].__id).toBe("3"); // 60
    expect(sorted[2].__id).toBe("2"); // null last
  });
});

describe("groupEntities with undefined field value", () => {
  it("groups entities including those with undefined keys", () => {
    const data: Entity[] = [
      { __id: "1", name: "Anna", city: "Warszawa" },
      { __id: "2", name: "Jan" },
      { __id: "3", name: "Piotr", city: "Warszawa" },
    ];
    const groups = QueryEngine.groupEntities(data, "city");
    expect(groups).toHaveLength(2);
    const warszawaGroup = groups.find((g) => g.key === "Warszawa");
    const undefinedGroup = groups.find((g) => g.key === undefined);
    expect(warszawaGroup!.count).toBe(2);
    expect(undefinedGroup!.count).toBe(1);
  });
});

describe("empty input edge cases", () => {
  it("sortEntities returns empty array for empty input", () => {
    const result = QueryEngine.sortEntities([], [{ field: "name", direction: "asc" }]);
    expect(result).toEqual([]);
  });

  it("groupEntities returns empty array for empty input", () => {
    const result = QueryEngine.groupEntities([], "name");
    expect(result).toEqual([]);
  });

  it("filterEntities with empty entity list returns empty", () => {
    const filters: Filter[] = [{ field: "name", operator: "=", value: "Anna" }];
    const result = QueryEngine.filterEntities([], filters);
    expect(result).toEqual([]);
  });

  it("paginateEntities with Infinity limit defaults to full length", () => {
    const result = QueryEngine.paginateEntities(users, 0, Infinity);
    expect(result.items).toHaveLength(users.length);
    expect(result.total).toBe(users.length);
    expect(result.limit).toBe(users.length);
    expect(result.hasNext).toBe(false);
  });
});
});
