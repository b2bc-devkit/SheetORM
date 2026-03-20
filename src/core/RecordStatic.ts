import type { Entity } from "./types/Entity";

export interface RecordStatic {
  new (): Entity;
  tableName: string;
  indexTableName: string;
  name: string;
}
