import { Decorators } from "./core/Decorators";
import { Record } from "./core/Record";
import { Registry } from "./core/Registry";
import { IndexStore } from "./index/IndexStore";
import { Query } from "./query/Query";

export class SheetOrm {
	static readonly Record = Record;
	static readonly Query = Query;
	static readonly Decorators = Decorators;
	static readonly IndexStore = IndexStore;
	static readonly Registry = Registry;
}
