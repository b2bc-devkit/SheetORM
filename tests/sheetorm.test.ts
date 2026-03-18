import { MockSpreadsheetAdapter } from './mocks';
import { Entity, TableSchema } from '../src/core/types';
import { SheetORM } from '../src/SheetORM';
import { MemoryCache } from '../src/utils/cache';

interface Product extends Entity {
  name: string;
  price: number;
  category: string;
}

const productSchema: TableSchema = {
  tableName: 'Products',
  fields: [
    { name: 'name', type: 'string', required: true },
    { name: 'price', type: 'number', required: true },
    { name: 'category', type: 'string' },
  ],
  indexes: [{ field: 'category' }],
};

describe('SheetORM facade', () => {
  let orm: SheetORM;
  let adapter: MockSpreadsheetAdapter;

  beforeEach(() => {
    adapter = new MockSpreadsheetAdapter();
    orm = new SheetORM({ adapter, cache: new MemoryCache() });
  });

  it('registers a schema and creates sheets', () => {
    orm.register(productSchema);
    expect(adapter.getSheetNames()).toContain('Products');
    expect(adapter.getSheetNames()).toContain('_meta');
  });

  it('getRepository returns a working repo', () => {
    orm.register(productSchema);
    const repo = orm.getRepository<Product>('Products');

    const saved = repo.save({ name: 'Widget', price: 9.99, category: 'tools' } as Partial<Product>);
    expect(saved.__id).toBeDefined();

    const found = repo.findById(saved.__id);
    expect(found).not.toBeNull();
    expect(found!.name).toBe('Widget');
  });

  it('getRepository caches instances', () => {
    orm.register(productSchema);
    const repo1 = orm.getRepository<Product>('Products');
    const repo2 = orm.getRepository<Product>('Products');
    expect(repo1).toBe(repo2);
  });

  it('throws when getting repo for unregistered table', () => {
    expect(() => orm.getRepository('Unknown')).toThrow(/not registered/);
  });

  it('static create() works', () => {
    const instance = SheetORM.create({ adapter });
    expect(instance).toBeInstanceOf(SheetORM);
  });

  it('clearCache() clears the cache', () => {
    orm.register(productSchema);
    const repo = orm.getRepository<Product>('Products');
    repo.save({ name: 'A', price: 1, category: 'x' } as Partial<Product>);
    // Should not throw
    orm.clearCache();
  });

  it('getMigrator() returns the migrator', () => {
    expect(orm.getMigrator()).toBeDefined();
  });

  it('getIndexStore() returns the index store', () => {
    expect(orm.getIndexStore()).toBeDefined();
  });

  it('full workflow: register → save → query → delete', () => {
    orm.register(productSchema);
    const repo = orm.getRepository<Product>('Products');

    repo.save({ name: 'Apple', price: 1.5, category: 'fruit' } as Partial<Product>);
    repo.save({ name: 'Banana', price: 0.8, category: 'fruit' } as Partial<Product>);
    repo.save({ name: 'Hammer', price: 15.0, category: 'tools' } as Partial<Product>);

    // Query
    const fruits = repo.query()
      .where('category', '=', 'fruit')
      .orderBy('price', 'asc')
      .execute();
    expect(fruits).toHaveLength(2);
    expect(fruits[0].name).toBe('Banana');

    // Delete
    repo.delete(fruits[0].__id);
    expect(repo.count()).toBe(2);

    // Paginate
    const page = repo.select(0, 1);
    expect(page.items).toHaveLength(1);
    expect(page.total).toBe(2);
  });
});
