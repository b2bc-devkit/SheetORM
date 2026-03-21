// SheetORM — GAS entry point
// Only the three callable GAS functions are exposed as globals.
// Internal types, classes and utilities are bundled but not surfaced as GAS menu items.

import { Decorators } from "./core/Decorators.js";
import { Record } from "./core/Record.js";
import { Registry } from "./core/Registry.js";
import { IndexStore } from "./index/IndexStore.js";
import { Query } from "./query/Query.js";
import { RuntimeBenchmark } from "./testing/RuntimeBenchmark.js";
import { RuntimeParity } from "./testing/RuntimeParity.js";

export class GasEntrypoints {
	static readonly Record = Record;
	static readonly Query = Query;
	static readonly Decorators = Decorators;
	static readonly IndexStore = IndexStore;
	static readonly Registry = Registry;

	static runTests(): void {
		RuntimeParity.run();
	}

	static validateTests(): void {
		RuntimeParity.validate();
	}

	static runBenchmark(): void {
		RuntimeBenchmark.run();
	}
}
