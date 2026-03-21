import { Decorators } from "./core/Decorators.js";
import { Record } from "./core/Record.js";
import { Registry } from "./core/Registry.js";
import { IndexStore } from "./index/IndexStore.js";
import { Query } from "./query/Query.js";

export class SheetOrm {
	static readonly Record = Record;
	static readonly Query = Query;
	static readonly Decorators = Decorators;
	static readonly IndexStore = IndexStore;
	static readonly Registry = Registry;
}
