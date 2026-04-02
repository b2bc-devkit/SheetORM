import type { Entity } from "./Entity.js";

/**
 * Optional lifecycle hooks that can be attached to a SheetRepository.
 *
 * Hooks run synchronously during save / delete operations and can
 * modify entities (beforeSave), validate data (onValidate), or
 * perform side-effects (afterSave, afterDelete).
 *
 * @typeParam T - The entity type managed by the repository.
 */
export interface LifecycleHooks<T extends Entity> {
  /**
   * Called before an entity is written to the sheet.
   * Return a modified partial to alter the data, or void to keep it unchanged.
   */
  beforeSave?(entity: Partial<T>, isNew: boolean): Partial<T> | void;

  /** Called after an entity has been persisted to the sheet. */
  afterSave?(entity: T, isNew: boolean): void;

  /**
   * Called before an entity is deleted.
   * Return false to cancel the deletion.
   */
  beforeDelete?(id: string): boolean | void;

  /** Called after an entity has been removed from the sheet. */
  afterDelete?(id: string): void;

  /**
   * Validate entity data before save.
   * Return an array of error messages to abort the save, or void to pass.
   */
  onValidate?(entity: Partial<T>): string[] | void;
}
