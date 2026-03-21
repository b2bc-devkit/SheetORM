// SheetORM — GAS entry point
// Only the three callable GAS functions are exposed as globals.
// Internal types, classes and utilities are bundled but not surfaced as GAS menu items.

import { Decorators } from "./core/Decorators";
import { Record } from "./core/Record";
import { Registry } from "./core/Registry";
import { IndexStore } from "./index/IndexStore";
import { Query } from "./query/Query";
import { RuntimeBenchmark } from "./testing/RuntimeBenchmark";
import { RuntimeParity } from "./testing/RuntimeParity";

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
