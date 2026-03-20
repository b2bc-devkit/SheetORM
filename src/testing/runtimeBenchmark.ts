// SheetORM — Runtime Benchmark: GAS performance comparison for Record classes
// Mirrors tests/benchmark.test.ts but runs against real Google Sheets API.
//
// Functions exposed to GAS:
//   runBenchmark()  — full benchmark for Cars + Workers (100 records each)

import { GoogleSpreadsheetAdapter } from "../storage/GoogleSheetsAdapter";
import { Registry } from "../core/Registry";
import { Record as BaseRecord } from "../core/Record";
import { Indexed, Required, resetDecoratorCaches } from "../core/decorators";
import { Query } from "../query/Query";

// ─── Constants ───────────────────────────────────────────────────────────────

const RECORD_COUNT = 100;

// ─── GAS logger helper ───────────────────────────────────────────────────────

function gasLog(msg: string): void {
  if (typeof Logger !== "undefined" && typeof Logger.log === "function") {
    Logger.log(msg);
  }
}

// ─── Model definitions (created fresh each run to avoid class cache issues) ──

function createCarClass(suffix: string) {
  Registry.reset();
  resetDecoratorCaches();

  class Car extends BaseRecord {
    static override get tableName() {
      return `tbl_Cars_${suffix}`;
    }

    static override get indexTableName() {
      return `idx_Cars_${suffix}`;
    }

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

  return Car;
}

function createWorkerClass(suffix: string) {
  Registry.reset();
  resetDecoratorCaches();

  class Worker extends BaseRecord {
    static override get tableName() {
      return `tbl_Workers_${suffix}`;
    }

    static override get indexTableName() {
      return `idx_Workers_${suffix}`;
    }

    name: string;

    @Required()
    department: string;

    salary: number;
    active: boolean;
  }

  return Worker;
}

// ─── Assertion helpers ───────────────────────────────────────────────────────

function gasAssertEq<T>(actual: T, expected: T, label: string): void {
  if (actual !== expected) {
    throw new Error(`${label}: expected ${String(expected)}, got ${String(actual)}`);
  }
}

function gasAssertTrue(condition: boolean, label: string): void {
  if (!condition) {
    throw new Error(`Assertion failed: ${label}`);
  }
}

// ─── Core benchmark operations ───────────────────────────────────────────────

interface BenchResult {
  tableName: string;
  indexTableName: string;
  durationMs: number;
  recordCount: number;
  indexSheetCreated: boolean;
  passed: number;
  failed: number;
  errors: string[];
}

function runBenchmarkFor<T extends BaseRecord>(
  Ctor: { new (): T } & typeof BaseRecord,
  makeData: (i: number) => { [key: string]: unknown },
  log: (msg: string) => void,
): BenchResult {
  const tableName = Ctor.tableName;
  const indexTableName = Ctor.indexTableName;
  const errors: string[] = [];
  let passed = 0;
  let failed = 0;

  function step(label: string, fn: () => void): void {
    log(`[SheetORM] ─── ${label} ───`);
    try {
      fn();
      passed++;
      log(`[SheetORM]   ✓ ${label} passed`);
    } catch (e) {
      failed++;
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`${label}: ${msg}`);
      log(`[SheetORM]   ✗ ${label} FAILED: ${msg}`);
    }
  }

  const startMs = Date.now();

  log(`[SheetORM] ════════════════════════════════════════════════════`);
  log(`[SheetORM] Benchmark: ${tableName} (${RECORD_COUNT} records)`);
  log(`[SheetORM] ════════════════════════════════════════════════════`);

  const saved: T[] = [];

  step(`save() × ${RECORD_COUNT}`, () => {
    for (let i = 0; i < RECORD_COUNT; i++) {
      const inst = Ctor.create(makeData(i)) as T;
      inst.save();
      saved.push(inst);
    }
    gasAssertEq(saved.length, RECORD_COUNT, "saved.length");
    log(`[SheetORM]   Created ${saved.length} records in ${tableName}`);
  });

  step("count()", () => {
    const c = Ctor.count();
    gasAssertEq(c, RECORD_COUNT, "count()");
    log(`[SheetORM]   count() = ${c}`);
  });

  step("findById()", () => {
    if (saved.length === 0) return;
    const mid = saved[Math.floor(RECORD_COUNT / 2)];
    const found = Ctor.findById(mid.__id);
    gasAssertTrue(found !== null, "findById not null");
    gasAssertEq(found!.__id, mid.__id, "findById id");
    log(`[SheetORM]   findById() returned id=${found!.__id.slice(0, 8)}...`);
  });

  step("find() all", () => {
    const all = Ctor.find();
    gasAssertEq(all.length, RECORD_COUNT, "find() all length");
    log(`[SheetORM]   find() returned ${all.length} records`);
  });

  const firstField = Object.keys(makeData(0))[0];
  const firstVal = makeData(0)[firstField];

  step("find() with filter", () => {
    const filtered = Ctor.find({ where: [{ field: firstField, operator: "=", value: firstVal }] });
    gasAssertTrue(filtered.length > 0, "find(where) not empty");
    log(`[SheetORM]   find(where ${firstField}=${String(firstVal)}) returned ${filtered.length} records`);
  });

  step("findOne()", () => {
    const one = Ctor.findOne({ where: [{ field: firstField, operator: "=", value: firstVal }] });
    gasAssertTrue(one !== null, "findOne not null");
  });

  step("where() query chain", () => {
    const results = Ctor.where(firstField, "=", firstVal).execute();
    gasAssertTrue(results.length > 0, "where() not empty");
    log(`[SheetORM]   where() returned ${results.length} records`);
  });

  step("query().orderBy().limit()", () => {
    const results = Ctor.query().orderBy(firstField, "asc").limit(10).execute();
    gasAssertEq(results.length, Math.min(10, RECORD_COUNT), "query().limit(10)");
    log(`[SheetORM]   query().orderBy().limit(10) returned ${results.length} records`);
  });

  step("select() pagination", () => {
    const page = Ctor.select(0, 10);
    gasAssertEq(page.items.length, 10, "select items length");
    gasAssertEq(page.total, RECORD_COUNT, "select total");
    gasAssertTrue(page.hasNext, "select hasNext");
    log(`[SheetORM]   select(0,10) total=${page.total}, hasNext=${page.hasNext}`);
  });

  step("groupBy()", () => {
    const groups = Ctor.groupBy(firstField);
    gasAssertTrue(groups.length > 0, "groupBy not empty");
    log(`[SheetORM]   groupBy(${firstField}) returned ${groups.length} groups`);
  });

  step("update via save()", () => {
    if (saved.length === 0) return;
    const toUpdate = Ctor.findById(saved[0].__id)!;
    const fields = Object.keys(makeData(0));
    const lastField = fields[fields.length - 1];
    (toUpdate as { [k: string]: unknown })[lastField] = "runtime-updated";
    toUpdate.save();
    const reloaded = Ctor.findById(saved[0].__id)!;
    gasAssertEq(
      (reloaded as { [k: string]: unknown })[lastField] as string,
      "runtime-updated",
      "update persisted",
    );
    log(`[SheetORM]   update persisted for id=${saved[0].__id.slice(0, 8)}...`);
  });

  step("delete() single record", () => {
    if (saved.length < 2) return;
    const toDelete = Ctor.findById(saved[saved.length - 1].__id)!;
    const ok = toDelete.delete();
    gasAssertEq(ok, true, "delete() returned true");
    gasAssertTrue(Ctor.findById(toDelete.__id) === null, "deleted record not found");
    log(`[SheetORM]   delete() removed record, count now ${Ctor.count()}`);
  });

  step("deleteAll() with filter", () => {
    const before = Ctor.count();
    const delCount = Ctor.deleteAll({
      where: [{ field: firstField, operator: "=", value: firstVal }],
    });
    const after = Ctor.count();
    gasAssertEq(after, before - delCount, "deleteAll count consistency");
    log(`[SheetORM]   deleteAll() removed ${delCount} records (${before} → ${after})`);
  });

  step("Query.from() class ref", () => {
    const results = Query.from(Ctor as unknown as Parameters<typeof Query.from>[0])
      .limit(5)
      .execute();
    gasAssertTrue(results.length <= 5, "Query.from() limit");
    log(`[SheetORM]   Query.from() returned ${results.length} records`);
  });

  step("toJSON()", () => {
    const items = Ctor.find({ limit: 1 });
    if (items.length > 0) {
      const json = items[0].toJSON();
      gasAssertEq(typeof json.__id, "string", "toJSON().__id is string");
    }
  });

  const durationMs = Date.now() - startMs;

  // Check index sheet
  const adapter = Registry.getInstance().getIndexStore();
  void adapter;
  const indexSheetCreated = Registry.getInstance().getIndexStore().existsCombined(indexTableName);

  log(`[SheetORM] ────────────────────────────────────────────────────`);
  log(`[SheetORM] ${tableName} finished in ${durationMs} ms`);
  log(`[SheetORM] Index sheet "${indexTableName}" exists: ${indexSheetCreated}`);
  log(`[SheetORM] Passed: ${passed}, Failed: ${failed}`);
  log(`[SheetORM] ────────────────────────────────────────────────────`);

  return {
    tableName,
    indexTableName,
    durationMs,
    recordCount: RECORD_COUNT,
    indexSheetCreated,
    passed,
    failed,
    errors,
  };
}

// ─── Public GAS function ─────────────────────────────────────────────────────

export function runBenchmark(): string {
  const log = gasLog;
  const runId = String(Date.now());

  if (typeof SpreadsheetApp === "undefined") {
    throw new Error("runBenchmark() must be run in Google Apps Script runtime.");
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const adapter = new GoogleSpreadsheetAdapter(spreadsheet);

  log(`[SheetORM] ════════════════════════════════════════════════════`);
  log(`[SheetORM] SheetORM Benchmark (run ID: ${runId})`);
  log(`[SheetORM] Records per table: ${RECORD_COUNT}`);
  log(`[SheetORM] ════════════════════════════════════════════════════`);

  // ── Cars benchmark (with @Indexed on all fields) ──────────────────────────
  const CarClass = createCarClass(runId);
  Registry.getInstance().configure({ adapter });

  const carData = (i: number): { [key: string]: unknown } => ({
    make: ["Toyota", "Honda", "BMW", "Ford", "VW"][i % 5],
    model: `Model-${i}`,
    year: 2015 + (i % 10),
    color: ["red", "blue", "white", "black", "silver"][i % 5],
  });

  const carsResult = runBenchmarkFor(CarClass, carData, log);

  // ── Workers benchmark (no @Indexed) ──────────────────────────────────────
  const WorkerClass = createWorkerClass(runId);
  Registry.getInstance().configure({ adapter });

  const workerData = (i: number): { [key: string]: unknown } => ({
    name: `Worker-${i}`,
    department: ["Engineering", "Marketing", "HR", "Finance", "Sales"][i % 5],
    salary: 3000 + (i % 10) * 500,
    active: i % 3 !== 0,
  });

  const workersResult = runBenchmarkFor(WorkerClass, workerData, log);

  // ── Summary ───────────────────────────────────────────────────────────────
  const diff = workersResult.durationMs - carsResult.durationMs;
  const fasterSuite = diff > 0 ? carsResult.tableName : workersResult.tableName;
  const slowerSuite = diff > 0 ? workersResult.tableName : carsResult.tableName;

  log(`[SheetORM] ════════════════════════════════════════════════════`);
  log(`[SheetORM] BENCHMARK SUMMARY`);
  log(`[SheetORM] ════════════════════════════════════════════════════`);
  log(`[SheetORM] ${carsResult.tableName} (with @Indexed): ${carsResult.durationMs} ms`);
  log(`[SheetORM] ${workersResult.tableName} (no @Indexed): ${workersResult.durationMs} ms`);
  log(`[SheetORM] Difference: ${Math.abs(diff)} ms`);
  log(`[SheetORM] Faster: ${fasterSuite} (by ${Math.abs(diff)} ms)`);
  log(`[SheetORM] Slower: ${slowerSuite}`);
  log(`[SheetORM] Note: @Indexed adds write overhead in both mock and real GAS (index sheet writes).`);
  log(
    `[SheetORM]       In real Google Sheets, @Indexed enables faster point-lookups (fewer API reads per query).`,
  );
  log(`[SheetORM]       The time saved by @Indexed in real GAS grows with dataset size.`);
  log(`[SheetORM] ════════════════════════════════════════════════════`);

  const totalPassed = carsResult.passed + workersResult.passed;
  const totalFailed = carsResult.failed + workersResult.failed;
  const allErrors = [...carsResult.errors, ...workersResult.errors];

  log(`[SheetORM] Total: ${totalPassed} operations passed, ${totalFailed} failed`);

  if (allErrors.length > 0) {
    log(`[SheetORM] Errors:`);
    for (const err of allErrors) {
      log(`[SheetORM]   ✗ ${err}`);
    }
  }

  const report = {
    runId,
    recordCount: RECORD_COUNT,
    cars: {
      tableName: carsResult.tableName,
      indexTableName: carsResult.indexTableName,
      durationMs: carsResult.durationMs,
      indexSheetCreated: carsResult.indexSheetCreated,
      passed: carsResult.passed,
      failed: carsResult.failed,
    },
    workers: {
      tableName: workersResult.tableName,
      indexTableName: workersResult.indexTableName,
      durationMs: workersResult.durationMs,
      indexSheetCreated: workersResult.indexSheetCreated,
      passed: workersResult.passed,
      failed: workersResult.failed,
    },
    summary: {
      fasterSuite,
      slowerSuite,
      differenceMs: Math.abs(diff),
    },
    errors: allErrors,
    spreadsheetUrl: spreadsheet.getUrl(),
  };

  log(`[SheetORM] ${JSON.stringify(report)}`);
  return JSON.stringify(report);
}
