import type { FieldDefinition } from "../src/core/types/FieldDefinition";
import { Serialization } from "../src/utils/Serialization";

describe("serializeValue", () => {
  it("serializes string", () => {
    const fd: FieldDefinition = { name: "x", type: "string" };
    expect(Serialization.serializeValue("hello", fd)).toBe("hello");
    expect(Serialization.serializeValue(123, fd)).toBe("123");
    expect(Serialization.serializeValue(null, fd)).toBe("");
  });

  it("serializes number", () => {
    const fd: FieldDefinition = { name: "x", type: "number" };
    expect(Serialization.serializeValue(42, fd)).toBe(42);
    expect(Serialization.serializeValue("7", fd)).toBe(7);
    expect(Serialization.serializeValue(NaN, fd)).toBe("");
  });

  it("serializes boolean", () => {
    const fd: FieldDefinition = { name: "x", type: "boolean" };
    expect(Serialization.serializeValue(true, fd)).toBe(true);
    expect(Serialization.serializeValue("true", fd)).toBe(true);
    expect(Serialization.serializeValue("false", fd)).toBe(false);
    expect(Serialization.serializeValue(NaN, fd)).toBe(false);
  });

  it("serializes json", () => {
    const fd: FieldDefinition = { name: "x", type: "json" };
    expect(Serialization.serializeValue({ a: 1 }, fd)).toBe('{"a":1}');
    expect(Serialization.serializeValue("already string", fd)).toBe('"already string"');
  });

  it("serializes date", () => {
    const fd: FieldDefinition = { name: "x", type: "date" };
    const d = new Date("2024-01-15T10:00:00.000Z");
    expect(Serialization.serializeValue(d, fd)).toBe("2024-01-15T10:00:00.000Z");
  });

  it("serializes reference", () => {
    const fd: FieldDefinition = { name: "x", type: "reference" };
    expect(Serialization.serializeValue("user-001", fd)).toBe("user-001");
  });
});

describe("deserializeValue", () => {
  it("deserializes string", () => {
    const fd: FieldDefinition = { name: "x", type: "string" };
    expect(Serialization.deserializeValue("hello", fd)).toBe("hello");
    expect(Serialization.deserializeValue("", fd)).toBeNull();
    expect(Serialization.deserializeValue(null, fd)).toBeNull();
    expect(Serialization.deserializeValue(undefined, fd)).toBeNull();
  });

  it("applies defaultValue when empty", () => {
    const fd: FieldDefinition = { name: "x", type: "string", defaultValue: "default" };
    expect(Serialization.deserializeValue("", fd)).toBe("default");
  });

  it("deserializes number", () => {
    const fd: FieldDefinition = { name: "x", type: "number" };
    expect(Serialization.deserializeValue(42, fd)).toBe(42);
    expect(Serialization.deserializeValue("3.14", fd)).toBe(3.14);
    expect(Serialization.deserializeValue("abc", fd)).toBeNull();
  });

  it("deserializes boolean", () => {
    const fd: FieldDefinition = { name: "x", type: "boolean" };
    expect(Serialization.deserializeValue(true, fd)).toBe(true);
    expect(Serialization.deserializeValue("true", fd)).toBe(true);
    expect(Serialization.deserializeValue("false", fd)).toBe(false);
    expect(Serialization.deserializeValue(NaN, fd)).toBe(false);
  });

  it("deserializes date from ISO string", () => {
    const fd: FieldDefinition = { name: "x", type: "date" };
    const result = Serialization.deserializeValue("2024-01-15T10:00:00.000Z", fd);
    expect(result).toBe("2024-01-15T10:00:00.000Z");
  });

  it("deserializes json", () => {
    const fd: FieldDefinition = { name: "x", type: "json" };
    expect(Serialization.deserializeValue('{"a":1}', fd)).toEqual({ a: 1 });
    expect(Serialization.deserializeValue("invalid json", fd)).toBeNull();
  });

  it("round-trips json string values", () => {
    const fd: FieldDefinition = { name: "x", type: "json" };
    const serialized = Serialization.serializeValue("hello", fd);
    expect(Serialization.deserializeValue(serialized, fd)).toBe("hello");
  });
});

describe("buildHeaders", () => {
  it("prepends system columns", () => {
    const fields: FieldDefinition[] = [
      { name: "name", type: "string" },
      { name: "age", type: "number" },
    ];
    expect(Serialization.buildHeaders(fields)).toEqual(["__id", "__createdAt", "__updatedAt", "name", "age"]);
  });
});

describe("entityToRow / rowToEntity", () => {
  const fields: FieldDefinition[] = [
    { name: "name", type: "string" },
    { name: "age", type: "number" },
    { name: "active", type: "boolean" },
  ];
  const headers = Serialization.buildHeaders(fields);

  it("round-trips an entity", () => {
    const entity = {
      __id: "id-1",
      __createdAt: "2024-01-01T00:00:00.000Z",
      __updatedAt: "2024-01-02T00:00:00.000Z",
      name: "Jan",
      age: 30,
      active: true,
    };

    const row = Serialization.entityToRow(entity, fields, headers);
    expect(row).toEqual(["id-1", "2024-01-01T00:00:00.000Z", "2024-01-02T00:00:00.000Z", "Jan", 30, true]);

    const restored = Serialization.rowToEntity(row, headers, fields);
    expect(restored.__id).toBe("id-1");
    expect(restored.name).toBe("Jan");
    expect(restored.age).toBe(30);
    expect(restored.active).toBe(true);
  });

  it("round-trips an entity with explicit fieldMap", () => {
    const entity = {
      __id: "id-fm",
      __createdAt: "2024-06-01T00:00:00.000Z",
      __updatedAt: "2024-06-02T00:00:00.000Z",
      name: "Piotr",
      age: 40,
      active: false,
    };

    const fieldMap = new Map(fields.map((f) => [f.name, f]));
    const row = Serialization.entityToRow(entity, fields, headers, fieldMap);
    const restored = Serialization.rowToEntity(row, headers, fields, fieldMap);
    expect(restored.__id).toBe("id-fm");
    expect(restored.name).toBe("Piotr");
    expect(restored.age).toBe(40);
    expect(restored.active).toBe(false);
  });

  it("handles missing optional fields", () => {
    const entity = {
      __id: "id-2",
      name: "Anna",
      age: 25,
      active: false,
    };

    const row = Serialization.entityToRow(entity, fields, headers);
    expect(row[1]).toBe(""); // __createdAt missing
    expect(row[2]).toBe(""); // __updatedAt missing

    const restored = Serialization.rowToEntity(row, headers, fields);
    expect(restored.__createdAt).toBeUndefined();
  });

  it("deserializes native Date objects in date fields to ISO strings", () => {
    const dateFd: FieldDefinition = { name: "birthday", type: "date" };
    const nativeDate = new Date("2024-03-15T10:30:00.000Z");
    const result = Serialization.deserializeValue(nativeDate, dateFd);
    expect(result).toBe("2024-03-15T10:30:00.000Z");
  });
});
