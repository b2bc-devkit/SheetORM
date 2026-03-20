// SheetORM — Benchmark Tests: performance comparison for Record classes
// Test 1: tbl_Cars (1,000 records, @Indexed on all fields) → idx_Cars auto-created
// Test 2: tbl_Workers (1,000 records, no @Indexed) → idx_Workers NOT created

import { MockSpreadsheetAdapter } from "./mocks";
import { Record as OrmRecord } from "../src/core/Record";
import { Indexed, Required, resetDecoratorCaches } from "../src/core/decorators";
import { Query } from "../src/query/Query";
import { Registry } from "../src/core/Registry";

type DataFactory = (i: number) => { [key: string]: unknown };

const RECORD_COUNT = 1_000;

// ─── Model definitions ───────────────────────────────────────────────────────

class Car extends OrmRecord {
  @Indexed()
  make: string;

  @Indexed()
  @Required()
  model: string;

  @Indexed()
  year: number;

  @Indexed()
  color: string;
}

class Worker extends OrmRecord {
  name: string;

  @Required()
  department: string;

  salary: number;
  active: boolean;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function log(msg: string): void {
  process.stdout.write(`  ${msg}\n`);
}

function assertEq<T>(actual: T, expected: T, label: string): void {
  if (actual !== expected) {
    throw new Error(`${label}: expected ${String(expected)}, got ${String(actual)}`);
  }
}

function runAllOperations<T extends OrmRecord>(
  Ctor: { new (): T } & typeof OrmRecord,
  makeData: DataFactory,
  tableName: string,
): void {
  log(`─── ${tableName}: save() × ${RECORD_COUNT} ───`);
  const saved: T[] = [];
  for (let i = 0; i < RECORD_COUNT; i++) {
    const instance = Ctor.create(makeData(i)) as T;
    instance.save();
    saved.push(instance);
  }
  log(`  ✓ Created ${saved.length} records`);

  log(`─── ${tableName}: count() ───`);
  const total = Ctor.count();
  assertEq(total, RECORD_COUNT, "count()");
  log(`  ✓ count() = ${total}`);

  log(`─── ${tableName}: findById() ───`);
  const mid = saved[Math.floor(RECORD_COUNT / 2)];
  const found = Ctor.findById(mid.__id);
  assertEq(found !== null, true, "findById() not null");
  assertEq(found!.__id, mid.__id, "findById() id match");
  log(`  ✓ findById() found record id=${mid.__id.slice(0, 8)}...`);

  log(`─── ${tableName}: find() all ───`);
  const all = Ctor.find();
  assertEq(all.length, RECORD_COUNT, "find() all length");
  log(`  ✓ find() returned ${all.length} records`);

  log(`─── ${tableName}: find() with filter ───`);
  const fields = Object.keys(makeData(0));
  const firstField = fields[0];
  const firstVal = makeData(0)[firstField];
  const filtered = Ctor.find({ where: [{ field: firstField, operator: "=", value: firstVal }] });
  assertEq(filtered.length > 0, true, `find(where ${firstField}=${String(firstVal)}) not empty`);
  log(`  ✓ find(where ${firstField}=${String(firstVal)}) returned ${filtered.length} records`);

  log(`─── ${tableName}: findOne() ───`);
  const one = Ctor.findOne({ where: [{ field: firstField, operator: "=", value: firstVal }] });
  assertEq(one !== null, true, "findOne() not null");
  log(`  ✓ findOne() OK`);

  log(`─── ${tableName}: where() query chain ───`);
  const queryResult = Ctor.where(firstField, "=", firstVal).execute();
  assertEq(queryResult.length > 0, true, "where() result not empty");
  log(`  ✓ where() returned ${queryResult.length} records`);

  log(`─── ${tableName}: query().orderBy() ───`);
  const ordered = Ctor.query().orderBy(firstField, "asc").limit(10).execute();
  assertEq(ordered.length, Math.min(10, RECORD_COUNT), "query().orderBy().limit() length");
  log(`  ✓ query().orderBy().limit(10) returned ${ordered.length} records`);

  log(`─── ${tableName}: select() pagination ───`);
  const page = Ctor.select(0, 10);
  assertEq(page.items.length, 10, "select(0, 10) items length");
  assertEq(page.total, RECORD_COUNT, "select total");
  assertEq(page.hasNext, true, "select hasNext");
  log(`  ✓ select(0, 10) page OK (total=${page.total}, hasNext=${page.hasNext})`);

  log(`─── ${tableName}: groupBy() ───`);
  const groups = Ctor.groupBy(firstField);
  assertEq(groups.length > 0, true, "groupBy() not empty");
  log(`  ✓ groupBy(${firstField}) returned ${groups.length} groups`);

  log(`─── ${tableName}: update (save existing) ───`);
  const toUpdate = Ctor.findById(saved[0].__id)!;
  const updatedField = fields[fields.length - 1];
  (toUpdate as { [key: string]: unknown })[updatedField] = "updated-value";
  toUpdate.save();
  const reloaded = Ctor.findById(saved[0].__id)!;
  assertEq((reloaded as { [key: string]: unknown })[updatedField], "updated-value", "update persisted");
  log(`  ✓ Updated record id=${saved[0].__id.slice(0, 8)}...`);

  log(`─── ${tableName}: set() / get() ───`);
  const inst = new Ctor();
  inst.set(firstField, "test-val");
  assertEq(inst.get(firstField), "test-val", "set/get");
  log(`  ✓ set()/get() OK`);

  log(`─── ${tableName}: delete() single record ───`);
  const toDelete = Ctor.findById(saved[RECORD_COUNT - 1].__id)!;
  const deleted = toDelete.delete();
  assertEq(deleted, true, "delete() true");
  assertEq(Ctor.findById(toDelete.__id), null, "deleted record not found");
  log(`  ✓ delete() removed 1 record, count now ${Ctor.count()}`);

  log(`─── ${tableName}: deleteAll() with filter ───`);
  const before = Ctor.count();
  const delCount = Ctor.deleteAll({ where: [{ field: firstField, operator: "=", value: firstVal }] });
  const after = Ctor.count();
  assertEq(after, before - delCount, "deleteAll() count consistency");
  log(`  ✓ deleteAll() removed ${delCount} records (${before} → ${after})`);

  log(`─── ${tableName}: Query.from() ───`);
  const qResult = Query.from(Ctor as unknown as Parameters<typeof Query.from>[0])
    .limit(5)
    .execute();
  assertEq(qResult.length <= 5, true, "Query.from() limit");
  log(`  ✓ Query.from() returned ${qResult.length} records`);

  log(`─── ${tableName}: toJSON() ───`);
  const jsonCar = Ctor.find({ limit: 1 })[0];
  if (jsonCar) {
    const json = jsonCar.toJSON();
    assertEq(typeof json.__id, "string", "toJSON().__id is string");
    log(`  ✓ toJSON() OK (id=${String(json.__id).slice(0, 8)}...)`);
  }
}

// ─── Benchmark test suite ────────────────────────────────────────────────────

describe("Benchmark: tbl_Cars (1,000 records, @Indexed on all fields)", () => {
  let adapter: MockSpreadsheetAdapter;
  let startTime: number;

  beforeAll(() => {
    adapter = new MockSpreadsheetAdapter();
    Registry.getInstance().configure({ adapter });
    startTime = Date.now();
    log(`\n${"═".repeat(60)}`);
    log(`Starting benchmark: tbl_Cars (${RECORD_COUNT} records, @Indexed on all fields)`);
    log(`${"═".repeat(60)}`);
  });

  afterAll(() => {
    const elapsed = Date.now() - startTime;
    log(`\n${"─".repeat(60)}`);
    log(`tbl_Cars benchmark finished in ${elapsed} ms`);

    const sheetNames = adapter.getSheetNames();
    log(`Sheets created: ${sheetNames.join(", ")}`);

    const indexSheetExists = sheetNames.includes(Car.indexTableName);
    log(`Index sheet "${Car.indexTableName}" exists: ${indexSheetExists}`);
    log(`${"─".repeat(60)}`);

    // Store timing for cross-suite comparison (global)
    (globalThis as { [key: string]: unknown }).__carsBenchmarkMs = elapsed;
  });

  afterEach(() => {
    Registry.reset();
    resetDecoratorCaches();
  });

  it(`creates index sheet "${Car.indexTableName}" when @Indexed fields exist`, () => {
    // Trigger table creation by saving one record
    Car.create({ make: "Ford", model: "Mustang", year: 2020, color: "red" }).save();

    const sheetNames = adapter.getSheetNames();
    log(`  Sheet names after first save: ${sheetNames.join(", ")}`);

    expect(sheetNames).toContain(Car.tableName);
    expect(sheetNames).toContain(Car.indexTableName);
  });

  it(`runs all operations on ${RECORD_COUNT} records`, () => {
    // Re-configure adapter for a fresh run
    adapter = new MockSpreadsheetAdapter();
    Registry.getInstance().configure({ adapter });

    const carData = (i: number): { [key: string]: unknown } => ({
      make: ["Toyota", "Honda", "BMW", "Ford", "VW"][i % 5],
      model: `Model-${i}`,
      year: 2015 + (i % 10),
      color: ["red", "blue", "white", "black", "silver"][i % 5],
    });

    runAllOperations(Car, carData, Car.tableName);

    const sheetNames = adapter.getSheetNames();
    expect(sheetNames).toContain(Car.tableName);
    expect(sheetNames).toContain(Car.indexTableName);
    log(`  ✓ Index sheet "${Car.indexTableName}" is present`);
  });
});

describe("Benchmark: tbl_Workers (1,000 records, no @Indexed)", () => {
  let adapter: MockSpreadsheetAdapter;
  let startTime: number;

  beforeAll(() => {
    adapter = new MockSpreadsheetAdapter();
    Registry.getInstance().configure({ adapter });
    startTime = Date.now();
    log(`\n${"═".repeat(60)}`);
    log(`Starting benchmark: tbl_Workers (${RECORD_COUNT} records, no @Indexed)`);
    log(`${"═".repeat(60)}`);
  });

  afterAll(() => {
    const elapsed = Date.now() - startTime;
    log(`\n${"─".repeat(60)}`);
    log(`tbl_Workers benchmark finished in ${elapsed} ms`);

    const sheetNames = adapter.getSheetNames();
    log(`Sheets created: ${sheetNames.join(", ")}`);

    const indexSheetExists = sheetNames.includes(Worker.indexTableName);
    log(`Index sheet "${Worker.indexTableName}" does NOT exist: ${!indexSheetExists}`);
    log(`${"─".repeat(60)}`);

    // Timing comparison
    const carsMs = (globalThis as { [key: string]: unknown }).__carsBenchmarkMs as number | undefined;
    if (carsMs !== undefined) {
      const diff = elapsed - carsMs;
      const faster = diff > 0 ? "tbl_Cars" : "tbl_Workers";
      log(`\n${"═".repeat(60)}`);
      log(`BENCHMARK SUMMARY`);
      log(`${"═".repeat(60)}`);
      log(`tbl_Cars  (with @Indexed):    ${carsMs} ms`);
      log(`tbl_Workers (no @Indexed):   ${elapsed} ms`);
      log(`Difference:                  ${Math.abs(diff)} ms`);
      log(`Faster suite: ${faster} (by ${Math.abs(diff)} ms)`);
      log(
        `Note: in mock environment @Indexed adds write overhead (index sheet writes).`,
      );
      log(
        `      In real Google Sheets, @Indexed enables faster lookups (fewer API reads).`,
      );
      log(`${"═".repeat(60)}`);
    }
  });

  afterEach(() => {
    Registry.reset();
    resetDecoratorCaches();
  });

  it(`does NOT create index sheet "${Worker.indexTableName}" when no @Indexed fields`, () => {
    // Trigger table creation by saving one record
    Worker.create({ name: "Alice", department: "Engineering", salary: 5000, active: true }).save();

    const sheetNames = adapter.getSheetNames();
    log(`  Sheet names after first save: ${sheetNames.join(", ")}`);

    expect(sheetNames).toContain(Worker.tableName);
    expect(sheetNames).not.toContain(Worker.indexTableName);
  });

  it(`runs all operations on ${RECORD_COUNT} records`, () => {
    // Re-configure adapter for a fresh run
    adapter = new MockSpreadsheetAdapter();
    Registry.getInstance().configure({ adapter });

    const workerData = (i: number): { [key: string]: unknown } => ({
      name: `Worker-${i}`,
      department: ["Engineering", "Marketing", "HR", "Finance", "Sales"][i % 5],
      salary: 3000 + (i % 10) * 500,
      active: i % 3 !== 0,
    });

    runAllOperations(Worker, workerData, Worker.tableName);

    const sheetNames = adapter.getSheetNames();
    expect(sheetNames).toContain(Worker.tableName);
    expect(sheetNames).not.toContain(Worker.indexTableName);
    log(`  ✓ Index sheet "${Worker.indexTableName}" is absent (as expected)`);
  });
});
