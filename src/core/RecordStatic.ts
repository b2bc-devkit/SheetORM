import type { Entity } from "./types/Entity.js";

export interface RecordStatic {
  new (): Entity;
  tableName: string;
  indexTableName: string;
  name: string;
}
