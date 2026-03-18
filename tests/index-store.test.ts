import { MockSpreadsheetAdapter } from './mocks';
import { IndexStore } from '../src/index/IndexStore';
import { MemoryCache } from '../src/utils/cache';

describe('IndexStore', () => {
  let adapter: MockSpreadsheetAdapter;
  let cache: MemoryCache;
  let indexStore: IndexStore;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
    cache = new MemoryCache();
    indexStore = new IndexStore(adapter, cache);
  });

  it('creates an index sheet', () => {
    indexStore.createIndex('Users', 'email', { unique: true });
    expect(adapter.getSheetNames()).toContain('_idx_Users_email');
  });

  it('adds and looks up entries', () => {
    indexStore.createIndex('Users', 'email');
    indexStore.registerIndex('Users', 'email', false);
    indexStore.add('Users', 'email', 'jan@example.com', 'user-001');
    indexStore.add('Users', 'email', 'anna@example.com', 'user-002');

    const ids = indexStore.lookup('Users', 'email', 'jan@example.com');
    expect(ids).toEqual(['user-001']);
  });

  it('enforces unique index', () => {
    indexStore.createIndex('Users', 'email', { unique: true });
    indexStore.registerIndex('Users', 'email', true);
    indexStore.add('Users', 'email', 'jan@example.com', 'user-001');

    expect(() => {
      indexStore.add('Users', 'email', 'jan@example.com', 'user-002');
    }).toThrow(/Unique index violation/);
  });

  it('allows same entity to re-index with same value (unique)', () => {
    indexStore.createIndex('Users', 'email', { unique: true });
    indexStore.registerIndex('Users', 'email', true);
    indexStore.add('Users', 'email', 'jan@example.com', 'user-001');
    // Should not throw
    indexStore.add('Users', 'email', 'jan@example.com', 'user-001');
  });

  it('removes entries', () => {
    indexStore.createIndex('Users', 'email');
    indexStore.registerIndex('Users', 'email', false);
    indexStore.add('Users', 'email', 'jan@example.com', 'user-001');
    indexStore.remove('Users', 'email', 'jan@example.com', 'user-001');

    const ids = indexStore.lookup('Users', 'email', 'jan@example.com');
    expect(ids).toEqual([]);
  });

  it('removes all entries for an entity', () => {
    indexStore.createIndex('Users', 'email');
    indexStore.createIndex('Users', 'name');
    indexStore.registerIndex('Users', 'email', false);
    indexStore.registerIndex('Users', 'name', false);

    indexStore.add('Users', 'email', 'jan@example.com', 'user-001');
    indexStore.add('Users', 'name', 'Jan', 'user-001');

    indexStore.removeAllForEntity('Users', 'user-001');

    expect(indexStore.lookup('Users', 'email', 'jan@example.com')).toEqual([]);
    expect(indexStore.lookup('Users', 'name', 'Jan')).toEqual([]);
  });

  it('updates entries when value changes', () => {
    indexStore.createIndex('Users', 'email');
    indexStore.registerIndex('Users', 'email', false);
    indexStore.add('Users', 'email', 'old@example.com', 'user-001');

    indexStore.updateForEntity(
      'Users',
      'user-001',
      { email: 'old@example.com' },
      { email: 'new@example.com' },
    );

    expect(indexStore.lookup('Users', 'email', 'old@example.com')).toEqual([]);
    expect(indexStore.lookup('Users', 'email', 'new@example.com')).toEqual(['user-001']);
  });

  it('rebuilds index from entity data', () => {
    indexStore.createIndex('Users', 'name');
    indexStore.registerIndex('Users', 'name', false);
    indexStore.add('Users', 'name', 'stale-data', 'user-xxx');

    indexStore.rebuild('Users', 'name', [
      { id: 'user-001', value: 'Jan' },
      { id: 'user-002', value: 'Anna' },
    ]);

    expect(indexStore.lookup('Users', 'name', 'stale-data')).toEqual([]);
    expect(indexStore.lookup('Users', 'name', 'Jan')).toEqual(['user-001']);
    expect(indexStore.lookup('Users', 'name', 'Anna')).toEqual(['user-002']);
  });

  it('drops an index', () => {
    indexStore.createIndex('Users', 'email');
    indexStore.dropIndex('Users', 'email');
    expect(adapter.getSheetNames()).not.toContain('_idx_Users_email');
  });

  it('exists() checks for index sheet', () => {
    expect(indexStore.exists('Users', 'email')).toBe(false);
    indexStore.createIndex('Users', 'email');
    expect(indexStore.exists('Users', 'email')).toBe(true);
  });

  it('getIndexedFields() returns registered fields', () => {
    indexStore.registerIndex('Users', 'email', true);
    indexStore.registerIndex('Users', 'name', false);

    const fields = indexStore.getIndexedFields('Users');
    expect(fields).toHaveLength(2);
    expect(fields.map((f) => f.field).sort()).toEqual(['email', 'name']);
  });
});
