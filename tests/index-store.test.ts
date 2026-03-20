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

  it('creates a combined index sheet', () => {
    indexStore.createCombinedIndex('idx_Users');
    expect(adapter.getSheetNames()).toContain('idx_Users');
  });

  it('adds and looks up entries', () => {
    indexStore.createCombinedIndex('idx_Users');
    indexStore.registerIndex('idx_Users', 'email', false);
    indexStore.addToCombined('idx_Users', 'email', 'jan@example.com', 'user-001');
    indexStore.addToCombined('idx_Users', 'email', 'anna@example.com', 'user-002');

    const ids = indexStore.lookupCombined('idx_Users', 'email', 'jan@example.com');
    expect(ids).toEqual(['user-001']);
  });

  it('enforces unique index', () => {
    indexStore.createCombinedIndex('idx_Users');
    indexStore.registerIndex('idx_Users', 'email', true);
    indexStore.addToCombined('idx_Users', 'email', 'jan@example.com', 'user-001');

    expect(() => {
      indexStore.addToCombined('idx_Users', 'email', 'jan@example.com', 'user-002');
    }).toThrow(/Unique index violation/);
  });

  it('allows same entity to re-index with same value (unique)', () => {
    indexStore.createCombinedIndex('idx_Users');
    indexStore.registerIndex('idx_Users', 'email', true);
    indexStore.addToCombined('idx_Users', 'email', 'jan@example.com', 'user-001');
    // Should not throw
    indexStore.addToCombined('idx_Users', 'email', 'jan@example.com', 'user-001');
  });

  it('removes entries when value is cleared in update', () => {
    indexStore.createCombinedIndex('idx_Users');
    indexStore.registerIndex('idx_Users', 'email', false);
    indexStore.addToCombined('idx_Users', 'email', 'jan@example.com', 'user-001');
    indexStore.updateInCombined(
      'idx_Users',
      'user-001',
      { email: 'jan@example.com' },
      { email: '' },
    );

    const ids = indexStore.lookupCombined('idx_Users', 'email', 'jan@example.com');
    expect(ids).toEqual([]);
  });

  it('removes all entries for an entity', () => {
    indexStore.createCombinedIndex('idx_Users');
    indexStore.registerIndex('idx_Users', 'email', false);
    indexStore.registerIndex('idx_Users', 'name', false);

    indexStore.addToCombined('idx_Users', 'email', 'jan@example.com', 'user-001');
    indexStore.addToCombined('idx_Users', 'name', 'Jan', 'user-001');

    indexStore.removeAllFromCombined('idx_Users', 'user-001');

    expect(indexStore.lookupCombined('idx_Users', 'email', 'jan@example.com')).toEqual([]);
    expect(indexStore.lookupCombined('idx_Users', 'name', 'Jan')).toEqual([]);
  });

  it('updates entries when value changes', () => {
    indexStore.createCombinedIndex('idx_Users');
    indexStore.registerIndex('idx_Users', 'email', false);
    indexStore.addToCombined('idx_Users', 'email', 'old@example.com', 'user-001');

    indexStore.updateInCombined(
      'idx_Users',
      'user-001',
      { email: 'old@example.com' },
      { email: 'new@example.com' },
    );

    expect(indexStore.lookupCombined('idx_Users', 'email', 'old@example.com')).toEqual([]);
    expect(indexStore.lookupCombined('idx_Users', 'email', 'new@example.com')).toEqual(['user-001']);
  });

  it('supports independent lookups per indexed field', () => {
    indexStore.createCombinedIndex('idx_Users');
    indexStore.registerIndex('idx_Users', 'name', false);
    indexStore.registerIndex('idx_Users', 'city', false);

    indexStore.addToCombined('idx_Users', 'name', 'Jan', 'user-001');
    indexStore.addToCombined('idx_Users', 'city', 'Warszawa', 'user-001');

    expect(indexStore.lookupCombined('idx_Users', 'name', 'Jan')).toEqual(['user-001']);
    expect(indexStore.lookupCombined('idx_Users', 'city', 'Warszawa')).toEqual(['user-001']);
    expect(indexStore.lookupCombined('idx_Users', 'name', 'Warszawa')).toEqual([]);
  });

  it('drops a combined index', () => {
    indexStore.createCombinedIndex('idx_Users');
    indexStore.dropCombinedIndex('idx_Users');
    expect(adapter.getSheetNames()).not.toContain('idx_Users');
  });

  it('existsCombined() checks for index sheet', () => {
    expect(indexStore.existsCombined('idx_Users')).toBe(false);
    indexStore.createCombinedIndex('idx_Users');
    expect(indexStore.existsCombined('idx_Users')).toBe(true);
  });

  it('getIndexedFields() returns registered fields', () => {
    indexStore.registerIndex('idx_Users', 'email', true);
    indexStore.registerIndex('idx_Users', 'name', false);

    const fields = indexStore.getIndexedFields('idx_Users');
    expect(fields).toHaveLength(2);
    expect(fields.map((f) => f.field).sort()).toEqual(['email', 'name']);
  });
});
