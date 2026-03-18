// SheetORM — Serialization utilities for converting between entity objects and sheet rows

import { FieldDefinition, Entity, SYSTEM_COLUMNS } from '../core/types';

/**
 * Serialize a field value for storage in a Google Sheet cell.
 */
export function serializeValue(value: unknown, fieldDef: FieldDefinition): unknown {
  if (value === null || value === undefined) return '';

  switch (fieldDef.type) {
    case 'string':
    case 'reference':
      return String(value);
    case 'number':
      return typeof value === 'number' ? value : Number(value);
    case 'boolean':
      return typeof value === 'boolean' ? value : value === 'true';
    case 'date':
      if (value instanceof Date) return value.toISOString();
      return String(value);
    case 'json':
      return typeof value === 'string' ? value : JSON.stringify(value);
    default:
      return String(value);
  }
}

/**
 * Deserialize a cell value from a Google Sheet into a typed value.
 */
export function deserializeValue(cellValue: unknown, fieldDef: FieldDefinition): unknown {
  if (cellValue === '' || cellValue === null || cellValue === undefined) {
    return fieldDef.defaultValue ?? null;
  }

  switch (fieldDef.type) {
    case 'string':
    case 'reference':
      return String(cellValue);
    case 'number': {
      const num = Number(cellValue);
      return isNaN(num) ? null : num;
    }
    case 'boolean':
      if (typeof cellValue === 'boolean') return cellValue;
      if (typeof cellValue === 'string') return cellValue.toLowerCase() === 'true';
      return Boolean(cellValue);
    case 'date':
      return String(cellValue);
    case 'json':
      if (typeof cellValue === 'string' && cellValue.length > 0) {
        try {
          return JSON.parse(cellValue);
        } catch {
          return null;
        }
      }
      return cellValue;
    default:
      return cellValue;
  }
}

/**
 * Build the full header row for a table (system columns + field columns).
 */
export function buildHeaders(fields: FieldDefinition[]): string[] {
  return [
    SYSTEM_COLUMNS.ID,
    SYSTEM_COLUMNS.CREATED_AT,
    SYSTEM_COLUMNS.UPDATED_AT,
    ...fields.map((f) => f.name),
  ];
}

/**
 * Convert an entity object into a row array matching the header order.
 */
export function entityToRow(entity: Entity, fields: FieldDefinition[], headers: string[]): unknown[] {
  const row: unknown[] = new Array(headers.length).fill('');

  for (let i = 0; i < headers.length; i++) {
    const col = headers[i];
    if (col === SYSTEM_COLUMNS.ID) {
      row[i] = entity.__id;
    } else if (col === SYSTEM_COLUMNS.CREATED_AT) {
      row[i] = entity.__createdAt ?? '';
    } else if (col === SYSTEM_COLUMNS.UPDATED_AT) {
      row[i] = entity.__updatedAt ?? '';
    } else {
      const fieldDef = fields.find((f) => f.name === col);
      if (fieldDef) {
        row[i] = serializeValue(entity[col], fieldDef);
      } else {
        row[i] = entity[col] ?? '';
      }
    }
  }

  return row;
}

/**
 * Convert a row array back into an entity object.
 */
export function rowToEntity<T extends Entity>(row: unknown[], headers: string[], fields: FieldDefinition[]): T {
  const entity: Record<string, unknown> = {};

  for (let i = 0; i < headers.length; i++) {
    const col = headers[i];
    const cellValue = i < row.length ? row[i] : '';

    if (col === SYSTEM_COLUMNS.ID) {
      entity.__id = String(cellValue);
    } else if (col === SYSTEM_COLUMNS.CREATED_AT) {
      entity.__createdAt = cellValue ? String(cellValue) : undefined;
    } else if (col === SYSTEM_COLUMNS.UPDATED_AT) {
      entity.__updatedAt = cellValue ? String(cellValue) : undefined;
    } else {
      const fieldDef = fields.find((f) => f.name === col);
      if (fieldDef) {
        entity[col] = deserializeValue(cellValue, fieldDef);
      } else {
        entity[col] = cellValue;
      }
    }
  }

  return entity as T;
}
