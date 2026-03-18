import { FieldDefinition } from '../src/core/types';
import {
  serializeValue,
  deserializeValue,
  buildHeaders,
  entityToRow,
  rowToEntity,
} from '../src/utils/serialization';

describe('serializeValue', () => {
  it('serializes string', () => {
    const fd: FieldDefinition = { name: 'x', type: 'string' };
    expect(serializeValue('hello', fd)).toBe('hello');
    expect(serializeValue(123, fd)).toBe('123');
    expect(serializeValue(null, fd)).toBe('');
  });

  it('serializes number', () => {
    const fd: FieldDefinition = { name: 'x', type: 'number' };
    expect(serializeValue(42, fd)).toBe(42);
    expect(serializeValue('7', fd)).toBe(7);
  });

  it('serializes boolean', () => {
    const fd: FieldDefinition = { name: 'x', type: 'boolean' };
    expect(serializeValue(true, fd)).toBe(true);
    expect(serializeValue('true', fd)).toBe(true);
    expect(serializeValue('false', fd)).toBe(false);
  });

  it('serializes json', () => {
    const fd: FieldDefinition = { name: 'x', type: 'json' };
    expect(serializeValue({ a: 1 }, fd)).toBe('{"a":1}');
    expect(serializeValue('already string', fd)).toBe('already string');
  });

  it('serializes date', () => {
    const fd: FieldDefinition = { name: 'x', type: 'date' };
    const d = new Date('2024-01-15T10:00:00.000Z');
    expect(serializeValue(d, fd)).toBe('2024-01-15T10:00:00.000Z');
  });

  it('serializes reference', () => {
    const fd: FieldDefinition = { name: 'x', type: 'reference' };
    expect(serializeValue('user-001', fd)).toBe('user-001');
  });
});

describe('deserializeValue', () => {
  it('deserializes string', () => {
    const fd: FieldDefinition = { name: 'x', type: 'string' };
    expect(deserializeValue('hello', fd)).toBe('hello');
    expect(deserializeValue('', fd)).toBeNull();
  });

  it('applies defaultValue when empty', () => {
    const fd: FieldDefinition = { name: 'x', type: 'string', defaultValue: 'default' };
    expect(deserializeValue('', fd)).toBe('default');
  });

  it('deserializes number', () => {
    const fd: FieldDefinition = { name: 'x', type: 'number' };
    expect(deserializeValue(42, fd)).toBe(42);
    expect(deserializeValue('3.14', fd)).toBe(3.14);
    expect(deserializeValue('abc', fd)).toBeNull();
  });

  it('deserializes boolean', () => {
    const fd: FieldDefinition = { name: 'x', type: 'boolean' };
    expect(deserializeValue(true, fd)).toBe(true);
    expect(deserializeValue('true', fd)).toBe(true);
    expect(deserializeValue('false', fd)).toBe(false);
  });

  it('deserializes json', () => {
    const fd: FieldDefinition = { name: 'x', type: 'json' };
    expect(deserializeValue('{"a":1}', fd)).toEqual({ a: 1 });
    expect(deserializeValue('invalid json', fd)).toBeNull();
  });
});

describe('buildHeaders', () => {
  it('prepends system columns', () => {
    const fields: FieldDefinition[] = [
      { name: 'name', type: 'string' },
      { name: 'age', type: 'number' },
    ];
    expect(buildHeaders(fields)).toEqual(['__id', '__createdAt', '__updatedAt', 'name', 'age']);
  });
});

describe('entityToRow / rowToEntity', () => {
  const fields: FieldDefinition[] = [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'number' },
    { name: 'active', type: 'boolean' },
  ];
  const headers = buildHeaders(fields);

  it('round-trips an entity', () => {
    const entity = {
      __id: 'id-1',
      __createdAt: '2024-01-01T00:00:00.000Z',
      __updatedAt: '2024-01-02T00:00:00.000Z',
      name: 'Jan',
      age: 30,
      active: true,
    };

    const row = entityToRow(entity, fields, headers);
    expect(row).toEqual(['id-1', '2024-01-01T00:00:00.000Z', '2024-01-02T00:00:00.000Z', 'Jan', 30, true]);

    const restored = rowToEntity(row, headers, fields);
    expect(restored.__id).toBe('id-1');
    expect(restored.name).toBe('Jan');
    expect(restored.age).toBe(30);
    expect(restored.active).toBe(true);
  });

  it('handles missing optional fields', () => {
    const entity = {
      __id: 'id-2',
      name: 'Anna',
      age: 25,
      active: false,
    };

    const row = entityToRow(entity, fields, headers);
    expect(row[1]).toBe(''); // __createdAt missing
    expect(row[2]).toBe(''); // __updatedAt missing

    const restored = rowToEntity(row, headers, fields);
    expect(restored.__createdAt).toBeUndefined();
  });
});
