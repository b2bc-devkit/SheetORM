import { Uuid } from "../src/utils/Uuid";

describe("Uuid.generate", () => {
  it("returns a string of UUID v4 format", () => {
    const uuid = Uuid.generate();
    expect(uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
  });

  it("generates unique values", () => {
    const uuids = new Set(Array.from({ length: 100 }, () => Uuid.generate()));
    expect(uuids.size).toBe(100);
  });
});
