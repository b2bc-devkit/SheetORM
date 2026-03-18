import { MockSpreadsheetAdapter } from './mocks';
import { IndexStore } from '../src/index/IndexStore';
import { SchemaMigrator } from '../src/schema/SchemaMigrator';
import { TableSchema } from '../src/core/types';

describe('SchemaMigrator', () => {
  let adapter: MockSpreadsheetAdapter;
  let indexStore: IndexStore;
  let migrator: SchemaMigrator;

  const userSchema: TableSchema = {
    tableName: 'Users',
    fields: [
      { name: 'name', type: 'string', required: true },
      { name: 'email', type: 'string', required: true },
      { name: 'age', type: 'number' },
    ],
    indexes: [{ field: 'email', unique: true }],
  };

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
    indexStore = new IndexStore(adapter);
    migrator = new SchemaMigrator(adapter, indexStore);
  });

  it('initializes meta sheet and data sheet', () => {
    migrator.initialize(userSchema);

    expect(adapter.getSheetNames()).toContain('_meta');
    expect(adapter.getSheetNames()).toContain('Users');
  });

  it('sets headers on data sheet', () => {
    migrator.initialize(userSchema);

    const sheet = adapter._getSheet('Users');
    expect(sheet).toBeDefined();
    const headers = sheet!.getHeaders();
    expect(headers).toEqual(['__id', '__createdAt', '__updatedAt', 'name', 'email', 'age']);
  });

  it('creates indexes during initialization', () => {
    migrator.initialize(userSchema);
    expect(indexStore.exists('Users', 'email')).toBe(true);
  });

  it('stores schema in _meta sheet', () => {
    migrator.initialize(userSchema);

    const retrieved = migrator.getSchema('Users');
    expect(retrieved).not.toBeNull();
    expect(retrieved!.tableName).toBe('Users');
    expect(retrieved!.fields).toHaveLength(3);
  });

  it('tableExists returns correct value', () => {
    expect(migrator.tableExists('Users')).toBe(false);
    migrator.initialize(userSchema);
    expect(migrator.tableExists('Users')).toBe(true);
  });

  it('addField adds a column to the schema', () => {
    migrator.initialize(userSchema);
    migrator.addField('Users', { name: 'phone', type: 'string' });

    const schema = migrator.getSchema('Users');
    expect(schema!.fields).toHaveLength(4);
    expect(schema!.fields.map((f) => f.name)).toContain('phone');
  });

  it('addField is idempotent for existing fields', () => {
    migrator.initialize(userSchema);
    migrator.addField('Users', { name: 'email', type: 'string' });

    const schema = migrator.getSchema('Users');
    expect(schema!.fields.filter((f) => f.name === 'email')).toHaveLength(1);
  });

  it('addField throws for unknown table', () => {
    expect(() => {
      migrator.addField('NonExistent', { name: 'x', type: 'string' });
    }).toThrow(/not found/);
  });

  it('removeField removes a field from schema', () => {
    migrator.initialize(userSchema);
    migrator.removeField('Users', 'age');

    const schema = migrator.getSchema('Users');
    expect(schema!.fields.map((f) => f.name)).not.toContain('age');
  });

  it('sync initializes if table does not exist', () => {
    migrator.sync(userSchema);
    expect(migrator.tableExists('Users')).toBe(true);
  });

  it('sync adds missing fields to existing table', () => {
    migrator.initialize(userSchema);

    const updatedSchema: TableSchema = {
      ...userSchema,
      fields: [
        ...userSchema.fields,
        { name: 'phone', type: 'string' },
      ],
    };

    migrator.sync(updatedSchema);
    const schema = migrator.getSchema('Users');
    expect(schema!.fields.map((f) => f.name)).toContain('phone');
  });

  it('sync adds missing indexes', () => {
    migrator.initialize(userSchema);

    const updatedSchema: TableSchema = {
      ...userSchema,
      indexes: [...userSchema.indexes, { field: 'name' }],
    };

    migrator.sync(updatedSchema);
    expect(indexStore.exists('Users', 'name')).toBe(true);
  });
});
