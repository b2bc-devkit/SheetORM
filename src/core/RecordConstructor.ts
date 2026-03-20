import type { Record } from "./Record";

export interface RecordConstructor<T extends Record = Record> {
  new (): T;
  tableName: string;
  indexTableName: string;
}
