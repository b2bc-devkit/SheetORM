// SheetORM — GAS entry point
// Only the three callable GAS functions are exposed as globals.
// Internal types, classes and utilities are bundled but not surfaced as GAS menu items.

import { RuntimeParity } from "./testing/RuntimeParity";
import { RuntimeBenchmark } from "./testing/RuntimeBenchmark";

export class GasEntrypoints {
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
