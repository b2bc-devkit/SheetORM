// SheetORM — Serialization utilities for converting between entity objects and sheet rows

import { FieldDefinition, Entity, SYSTEM_COLUMNS } from "../core/types";

/**
 * Serialize a field value for storage in a Google Sheet cell.
 * When fieldDef.type is undefined, infers type from the JS value at runtime.
 */
export function serializeValue(value: unknown, fieldDef: FieldDefinition): unknown {
  if (value === null || value === undefined) return "";

  if (fieldDef.type) {
    switch (fieldDef.type) {
      case "string":
      case "reference":
        return String(value);
      case "number":
        return typeof value === "number" ? value : Number(value);
      case "boolean":
        return typeof value === "boolean" ? value : value === "true";
      case "date":
        if (value instanceof Date) return value.toISOString();
        return String(value);
      case "json":
        return typeof value === "string" ? value : JSON.stringify(value);
      default:
        return String(value);
    }
  }

  // Auto-infer from JS value (when type is not set)
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value;
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/**
 * Deserialize a cell value from a Google Sheet into a typed value.
 * When fieldDef.type is undefined, returns the value as-is (trusts storage layer).
 */
export function deserializeValue(cellValue: unknown, fieldDef: FieldDefinition): unknown {
  if (cellValue === "" || cellValue === null || cellValue === undefined) {
    return fieldDef.defaultValue ?? null;
  }

  if (fieldDef.type) {
    switch (fieldDef.type) {
      case "string":
      case "reference":
        return String(cellValue);
      case "number": {
        const num = Number(cellValue);
        return isNaN(num) ? null : num;
      }
      case "boolean":
        if (typeof cellValue === "boolean") return cellValue;
        if (typeof cellValue === "string") return cellValue.toLowerCase() === "true";
        return Boolean(cellValue);
      case "date":
        return String(cellValue);
      case "json":
        if (typeof cellValue === "string" && cellValue.length > 0) {
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

  // Auto: trust storage layer to return the correct JS type
  return cellValue;
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
 * Accepts an optional pre-built fieldMap to avoid rebuilding it per call.
 */
export function entityToRow(
  entity: Entity,
  fields: FieldDefinition[],
  headers: string[],
  fieldMap?: Map<string, FieldDefinition>,
): unknown[] {
  const len = headers.length;
  const row: unknown[] = new Array(len);

  // Use provided fieldMap or fall back to linear scan for small field lists
  const useMap = fieldMap !== undefined;

  for (let i = 0; i < len; i++) {
    const col = headers[i];
    if (col === SYSTEM_COLUMNS.ID) {
      row[i] = entity.__id;
    } else if (col === SYSTEM_COLUMNS.CREATED_AT) {
      row[i] = entity.__createdAt ?? "";
    } else if (col === SYSTEM_COLUMNS.UPDATED_AT) {
      row[i] = entity.__updatedAt ?? "";
    } else {
      const fieldDef = useMap ? fieldMap!.get(col) : fields.find((f) => f.name === col);
      if (fieldDef) {
        row[i] = serializeValue(entity[col], fieldDef);
      } else {
        row[i] = entity[col] ?? "";
      }
    }
  }

  return row;
}

/**
 * Convert a row array back into an entity object.
 * Accepts an optional pre-built fieldMap to avoid rebuilding it per call.
 */
export function rowToEntity<T extends Entity>(
  row: unknown[],
  headers: string[],
  fields: FieldDefinition[],
  fieldMap?: Map<string, FieldDefinition>,
): T {
  const entity: Record<string, unknown> = {};

  // Use provided fieldMap or fall back to linear scan for small field lists
  const useMap = fieldMap !== undefined;

  const rowLen = row.length;
  for (let i = 0, len = headers.length; i < len; i++) {
    const col = headers[i];
    const cellValue = i < rowLen ? row[i] : "";

    if (col === SYSTEM_COLUMNS.ID) {
      entity.__id = String(cellValue);
    } else if (col === SYSTEM_COLUMNS.CREATED_AT) {
      entity.__createdAt =
        cellValue instanceof Date
          ? (cellValue as Date).toISOString()
          : cellValue
            ? String(cellValue)
            : undefined;
    } else if (col === SYSTEM_COLUMNS.UPDATED_AT) {
      entity.__updatedAt =
        cellValue instanceof Date
          ? (cellValue as Date).toISOString()
          : cellValue
            ? String(cellValue)
            : undefined;
    } else {
      const fieldDef = useMap ? fieldMap!.get(col) : fields.find((f) => f.name === col);
      if (fieldDef) {
        entity[col] = deserializeValue(cellValue, fieldDef);
      } else {
        entity[col] = cellValue;
      }
    }
  }

  return entity as T;
}
