/**
 * Serialization utilities for converting between entity objects and Google Sheet rows.
 *
 * This module handles the bidirectional mapping:
 *   Entity object ←→ sheet row (unknown[])
 *
 * Each conversion is type-aware: when a FieldDefinition carries an explicit `type`,
 * the value is coerced accordingly. When type is omitted, the serializer infers
 * the best representation from the JavaScript runtime type.
 *
 * @module Serialization
 */

import type { FieldDefinition } from "../core/types/FieldDefinition.js";
import type { Entity } from "../core/types/Entity.js";
import { SystemColumns } from "../core/types/SystemColumns.js";

/**
 * Serialize a single field value for storage in a Google Sheet cell.
 *
 * Conversion rules by FieldType:
 *   - "string" / "reference" → String()
 *   - "number"  → Number(); non-finite values become empty string
 *   - "boolean" → true/false; strings "true"/"1"/"yes" are truthy
 *   - "date"    → ISO 8601 string; invalid Date → empty string
 *   - "json"    → JSON.stringify()
 *
 * When fieldDef.type is undefined, the JS runtime type is inspected
 * (number, boolean, Date, object → JSON, else String).
 *
 * @param value    - The entity field value to serialize.
 * @param fieldDef - Field metadata (type, name, etc.).
 * @returns A cell-safe value (string, number, or boolean).
 */
function serializeValue(value: unknown, fieldDef: FieldDefinition): unknown {
  // Null / undefined → empty cell
  if (value === null || value === undefined) return "";

  // --- Explicit type branch ---
  if (fieldDef.type) {
    switch (fieldDef.type) {
      case "string":
      case "reference":
        return String(value);
      case "number": {
        // Accept numeric values directly; coerce strings to numbers
        if (typeof value === "number") return Number.isFinite(value) ? value : "";
        const num = Number(value);
        return Number.isFinite(num) ? num : "";
      }
      case "boolean": {
        // Accept booleans directly; coerce numbers and strings
        if (typeof value === "boolean") return value;
        if (typeof value === "number") return !isNaN(value) && value !== 0;
        const lower = typeof value === "string" ? value.toLowerCase().trim() : "";
        return lower === "true" || lower === "1" || lower === "yes";
      }
      case "date":
        // Dates are stored as ISO 8601 strings; invalid dates become empty
        if (value instanceof Date) return isNaN(value.getTime()) ? "" : value.toISOString();
        return String(value);
      case "json":
        // Arbitrary structures are JSON-stringified for cell storage
        return JSON.stringify(value);
      default:
        return String(value);
    }
  }

  // --- Auto-infer branch (no explicit type) ---
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value;
  if (value instanceof Date) return isNaN(value.getTime()) ? "" : value.toISOString();
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/**
 * Deserialize a cell value read from a Google Sheet into a typed JavaScript value.
 *
 * Empty / null cells return the field's defaultValue (or null if none is defined).
 * Conversion mirrors serializeValue in reverse:
 *   - "number"   → Number(); non-finite → null
 *   - "boolean"  → true/false from various truthy representations
 *   - "date"     → ISO string (Date objects are converted)
 *   - "json"     → JSON.parse(); malformed JSON → null
 *   - "string" / "reference" → String()
 *
 * When fieldDef.type is undefined, the raw cell value is returned as-is,
 * trusting the Google Sheets storage layer to preserve JavaScript types.
 *
 * @param cellValue - Raw value read from the sheet cell.
 * @param fieldDef  - Field metadata describing the expected type.
 * @returns The deserialized JavaScript value.
 */
function deserializeValue(cellValue: unknown, fieldDef: FieldDefinition): unknown {
  // Empty cell → use default or null
  if (cellValue === "" || cellValue === null || cellValue === undefined) {
    return fieldDef.defaultValue ?? null;
  }

  // --- Explicit type branch ---
  if (fieldDef.type) {
    switch (fieldDef.type) {
      case "string":
      case "reference":
        return String(cellValue);
      case "number": {
        const num = Number(cellValue);
        return Number.isFinite(num) ? num : null;
      }
      case "boolean":
        // Accept native booleans, numbers, and truthy strings
        if (typeof cellValue === "boolean") return cellValue;
        if (typeof cellValue === "number") return !isNaN(cellValue) && cellValue !== 0;
        if (typeof cellValue === "string") {
          const lower = cellValue.toLowerCase().trim();
          return lower === "true" || lower === "1" || lower === "yes";
        }
        return Boolean(cellValue);
      case "date":
        // Normalise GAS Date objects to ISO strings
        if (cellValue instanceof Date) return isNaN(cellValue.getTime()) ? null : cellValue.toISOString();
        return String(cellValue);
      case "json":
        // Parse JSON-stringified cell; return null on malformed input
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
 * Build the complete header row for a SheetORM table.
 *
 * Layout: `[__id, __createdAt, __updatedAt, ...userFields]`.
 * This order is assumed by entityToRow / rowToEntity and must remain stable.
 *
 * @param fields - User-defined field definitions (from decorators).
 * @returns An array of column header strings.
 */
function buildHeaders(fields: FieldDefinition[]): string[] {
  // System columns always occupy the first three positions
  return [SystemColumns.ID, SystemColumns.CREATED_AT, SystemColumns.UPDATED_AT, ...fields.map((f) => f.name)];
}

/**
 * Convert an entity object into a row array matching the header order.
 *
 * The resulting array can be passed directly to ISheetAdapter.appendRow()
 * or updateRow(). System columns are placed first, followed by user
 * fields in the same order as the header row.
 *
 * @param entity   - The entity to serialise.
 * @param fields   - User-defined field definitions.
 * @param headers  - Full header row (system + user columns).
 * @param fieldMap - Optional pre-built name→FieldDefinition map for O(1) lookup.
 *                   When omitted, falls back to linear scan (fine for <20 fields).
 * @returns An unknown[] row array ready to be written to the sheet.
 */
function entityToRow(
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
    // System columns are handled directly (no serialization needed)
    if (col === SystemColumns.ID) {
      row[i] = entity.__id;
    } else if (col === SystemColumns.CREATED_AT) {
      row[i] = entity.__createdAt ?? "";
    } else if (col === SystemColumns.UPDATED_AT) {
      row[i] = entity.__updatedAt ?? "";
    } else {
      // User field: look up the definition and serialize the value
      const fieldDef = useMap ? fieldMap!.get(col) : fields.find((f) => f.name === col);
      if (fieldDef) {
        row[i] = serializeValue(entity[col], fieldDef);
      } else {
        // Unknown column — pass through raw value (defensive, shouldn't happen normally)
        row[i] = entity[col] ?? "";
      }
    }
  }

  return row;
}

/**
 * Convert a sheet row array back into a typed entity object.
 *
 * Iterates over the header columns and deserializes each cell value
 * into the appropriate JS type.
 *
 * @param row      - Raw row data from ISheetAdapter.getAllData().
 * @param headers  - Full header row (system + user columns).
 * @param fields   - User-defined field definitions.
 * @param fieldMap - Optional pre-built name→FieldDefinition map for O(1) lookup.
 * @returns A fully hydrated entity object of type T.
 */
function rowToEntity<T extends Entity>(
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
    // Guard against short rows (missing trailing columns)
    const cellValue = i < rowLen ? row[i] : "";

    if (col === SystemColumns.ID) {
      // __id is always stored as a string
      entity.__id = String(cellValue);
    } else if (col === SystemColumns.CREATED_AT) {
      // GAS may return a Date object; normalise to ISO string
      entity.__createdAt =
        cellValue instanceof Date
          ? (cellValue as Date).toISOString()
          : cellValue !== "" && cellValue !== null && cellValue !== undefined
            ? String(cellValue)
            : undefined;
    } else if (col === SystemColumns.UPDATED_AT) {
      entity.__updatedAt =
        cellValue instanceof Date
          ? (cellValue as Date).toISOString()
          : cellValue !== "" && cellValue !== null && cellValue !== undefined
            ? String(cellValue)
            : undefined;
    } else {
      // User field: deserialize using the matching FieldDefinition
      const fieldDef = useMap ? fieldMap!.get(col) : fields.find((f) => f.name === col);
      if (fieldDef) {
        entity[col] = deserializeValue(cellValue, fieldDef);
      } else {
        // Unknown column — keep raw value (defensive)
        entity[col] = cellValue;
      }
    }
  }

  return entity as T;
}

/**
 * Public serialization utility class.
 *
 * Wraps five static methods that handle the Entity ←→ row conversion:
 * - serializeValue / deserializeValue — single-field level
 * - buildHeaders — construct the header row
 * - entityToRow / rowToEntity — full entity conversion
 */
export class Serialization {
  /** Serialize a single field value for cell storage. */
  static serializeValue = serializeValue;

  /** Deserialize a single cell value back to a JS value. */
  static deserializeValue = deserializeValue;

  /** Build the full header row (system + user columns). */
  static buildHeaders = buildHeaders;

  /** Convert an entity to a sheet row array. */
  static entityToRow = entityToRow;

  /** Convert a sheet row array to an entity object. */
  static rowToEntity = rowToEntity;
}
