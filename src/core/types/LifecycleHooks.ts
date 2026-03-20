import type { Entity } from "./Entity";

export interface LifecycleHooks<T extends Entity> {
  beforeSave?(entity: Partial<T>, isNew: boolean): Partial<T> | void;
  afterSave?(entity: T, isNew: boolean): void;
  beforeDelete?(id: string): boolean | void;
  afterDelete?(id: string): void;
  onValidate?(entity: Partial<T>): string[] | void;
}
