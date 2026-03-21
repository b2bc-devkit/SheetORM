import { GasEntrypoints } from "../src/index";
import { SheetOrm } from "../src/SheetOrm";
import { Decorators } from "../src/core/Decorators";
import { Record } from "../src/core/Record";
import { Registry } from "../src/core/Registry";
import { IndexStore } from "../src/index/IndexStore";
import { Query } from "../src/query/Query";

describe("SheetOrm", () => {
	it("exposes core classes for npm consumers", () => {
		expect(SheetOrm.Record).toBe(Record);
		expect(SheetOrm.Query).toBe(Query);
		expect(SheetOrm.Decorators).toBe(Decorators);
		expect(SheetOrm.IndexStore).toBe(IndexStore);
		expect(SheetOrm.Registry).toBe(Registry);
	});
});

describe("GasEntrypoints", () => {
	it("exposes the same API for Apps Script consumers", () => {
		expect(GasEntrypoints.Record).toBe(Record);
		expect(GasEntrypoints.Query).toBe(Query);
		expect(GasEntrypoints.Decorators).toBe(Decorators);
		expect(GasEntrypoints.IndexStore).toBe(IndexStore);
		expect(GasEntrypoints.Registry).toBe(Registry);
	});
});
