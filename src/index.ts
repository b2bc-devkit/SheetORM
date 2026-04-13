/**
 * @module index
 *
 * Google Apps Script (GAS) entry point for the SheetORM library.
 *
 * This file is the root module consumed by the Vite/Webpack build that
 * produces the `Code.js` bundle deployed to GAS.  It exposes:
 *
 * - **GasEntrypoints** — a single exported class whose static members
 *   are the public API surface visible inside the GAS environment.
 *   GAS functions (`demoCreate`, `runTestsStageOne`, etc.) are mapped
 *   to `GasEntrypoints.*` methods.
 *
 * - **DemoCar** — an internal demo model (not exported) used by the
 *   CRUD demo methods to showcase SheetORM's capabilities in the
 *   GAS script editor / execution log.
 *
 * Internal types, classes, and utilities are bundled but **not** surfaced
 * as GAS menu items.
 */

// ─── Imports ──────────────────────────────────────────────────────────────────

import { Decorators } from "./core/Decorators.js";
import { Record } from "./core/Record.js";
import { Registry } from "./core/Registry.js";
import { IndexStore } from "./index/IndexStore.js";
import { Query } from "./query/Query.js";
import { GoogleSpreadsheetAdapter } from "./storage/GoogleSpreadsheetAdapter.js";
import { RuntimeBenchmark } from "./testing/RuntimeBenchmark.js";
import { RuntimeParity } from "./testing/RuntimeParity.js";
import { SheetOrmLogger } from "./utils/SheetOrmLogger.js";

// ─── Demo model (internal, not exported) ──────────────────────────────────────

/** Destructure decorator functions for concise property annotations. */
const { Indexed, Required, Field } = Decorators;

/**
 * DemoCar — a sample entity class used by the `demoCreate`, `demoRead`,
 * `demoUpdate`, and `demoDelete` methods below.  Demonstrates decorators
 * (`@Indexed`, `@Required`, `@Field`) and the ActiveRecord save/delete API.
 *
 * Not exported — exists only for in-GAS demonstration purposes.
 */
class DemoCar extends Record {
  /** Make / brand, indexed for fast lookup (e.g. "Toyota"). */
  @Indexed()
  make: string = "";

  /** Model name, required — save will fail if empty (e.g. "Corolla"). */
  @Required()
  model: string = "";

  /** Model year, stored as a number in the sheet. */
  @Field({ type: "number" })
  year: number = 0;

  /** Body colour — plain string field, no special annotation. */
  color: string = "";

  /** Retail price in USD, stored as a number. */
  @Field({ type: "number" })
  price: number = 0;
}

// ─── GasEntrypoints ───────────────────────────────────────────────────────────

/**
 * Single public export of this module.
 *
 * Every static member becomes a top-level symbol in the GAS runtime once
 * the Vite build maps them via `globalThis.*` assignments in `Code.js`.
 *
 * **Core API classes** — consumers use these to define models and queries:
 * - `Record` — ActiveRecord base class
 * - `Query`  — fluent query builder
 * - `Decorators` — `@Field`, `@Indexed`, `@Required`
 * - `IndexStore` — secondary-index manager with n-gram search
 * - `Registry`   — singleton class registry
 * - `SheetOrmLogger` — verbose logging toggle
 *
 * **Test / benchmark runners** — callable from the GAS editor:
 * - `runTestsStageOne()` … `runTestsStageFour()` — staged parity tests
 * - `validateTests()` — validate parity results
 * - `runBenchmark()` — performance benchmark
 *
 * **Utility functions**:
 * - `removeAllSheets()` — deletes every sheet in the active spreadsheet
 *
 * **CRUD demos**:
 * - `demoCreate()`, `demoRead()`, `demoUpdate()`, `demoDelete()`
 */
export class GasEntrypoints {
  /** ActiveRecord base class. */
  static readonly Record = Record;
  /** Fluent query builder. */
  static readonly Query = Query;
  /** Property decorators (`@Field`, `@Indexed`, `@Required`). */
  static readonly Decorators = Decorators;
  /** Secondary-index manager with n-gram text search. */
  static readonly IndexStore = IndexStore;
  /** Singleton entity-class registry. */
  static readonly Registry = Registry;
  /** Verbose logger — set `SheetOrmLogger.verbose = true` for API-call traces. */
  static readonly SheetOrmLogger = SheetOrmLogger;

  // ─── Parity test runners ────────────────────────────────────────────────

  /** Run stage-one parity tests (basic CRUD + query). */
  static runTestsStageOne(): void {
    RuntimeParity.runStageOne();
  }

  /** Run stage-two parity tests (serialization, uuid). */
  static runTestsStageTwo(): void {
    RuntimeParity.runStageTwo();
  }

  /** Run stage-three parity tests (record). */
  static runTestsStageThree(): void {
    RuntimeParity.runStageThree();
  }

  /** Run stage-four parity tests (sheet-repository). */
  static runTestsStageFour(): void {
    RuntimeParity.runStageFour();
  }

  /** Validate all parity test results collected across stages. */
  static validateTests(): void {
    RuntimeParity.validate();
  }

  /** Execute the performance benchmark suite. */
  static runBenchmark(): void {
    RuntimeBenchmark.run();
  }

  /** Delete every sheet in the active spreadsheet (destructive — use with caution). */
  static removeAllSheets(): void {
    new GoogleSpreadsheetAdapter().removeAllSheets();
  }

  // ─── CRUD demos ─────────────────────────────────────────────────────────

  /**
   * demoCreate — saves 5 DemoCar records to the "DemoCar" sheet.
   *
   * Run this function first (from the GAS editor's function selector)
   * to populate the table.  Each car is created via `new DemoCar()`,
   * fields are assigned, and `car.save()` persists it to the sheet.
   * Console output mirrors the code for educational purposes.
   */
  static demoCreate(): void {
    console.log("[demoCreate — START]");

    // ── car1 ──────────────────────────────────────────────────────────────
    console.log("\n▶ const car1 = new DemoCar();");
    console.log('  car1.make = "Toyota";  car1.model = "Corolla";');
    console.log('  car1.year = 2022;      car1.color = "White";   car1.price = 28000;');
    console.log("  car1.save();");
    const car1 = new DemoCar();
    car1.make = "Toyota";
    car1.model = "Corolla";
    car1.year = 2022;
    car1.color = "White";
    car1.price = 28_000;
    car1.save();
    console.log(`  → [${car1.__id}]  ${car1.make} ${car1.model}  ${car1.color}  $${car1.price}`);

    // ── car2 ──────────────────────────────────────────────────────────────
    console.log("\n▶ const car2 = new DemoCar();");
    console.log('  car2.make = "BMW";     car2.model = "X5";');
    console.log('  car2.year = 2023;      car2.color = "Black";   car2.price = 85000;');
    console.log("  car2.save();");
    const car2 = new DemoCar();
    car2.make = "BMW";
    car2.model = "X5";
    car2.year = 2023;
    car2.color = "Black";
    car2.price = 85_000;
    car2.save();
    console.log(`  → [${car2.__id}]  ${car2.make} ${car2.model}  ${car2.color}  $${car2.price}`);

    // ── car3 ──────────────────────────────────────────────────────────────
    console.log("\n▶ const car3 = new DemoCar();");
    console.log('  car3.make = "Honda";   car3.model = "Civic";');
    console.log('  car3.year = 2024;      car3.color = "Red";     car3.price = 32000;');
    console.log("  car3.save();");
    const car3 = new DemoCar();
    car3.make = "Honda";
    car3.model = "Civic";
    car3.year = 2024;
    car3.color = "Red";
    car3.price = 32_000;
    car3.save();
    console.log(`  → [${car3.__id}]  ${car3.make} ${car3.model}  ${car3.color}  $${car3.price}`);

    // ── car4 ──────────────────────────────────────────────────────────────
    console.log("\n▶ const car4 = new DemoCar();");
    console.log('  car4.make = "Ford";    car4.model = "Mustang";');
    console.log('  car4.year = 2023;      car4.color = "Blue";    car4.price = 57000;');
    console.log("  car4.save();");
    const car4 = new DemoCar();
    car4.make = "Ford";
    car4.model = "Mustang";
    car4.year = 2023;
    car4.color = "Blue";
    car4.price = 57_000;
    car4.save();
    console.log(`  → [${car4.__id}]  ${car4.make} ${car4.model}  ${car4.color}  $${car4.price}`);

    // ── car5 ──────────────────────────────────────────────────────────────
    console.log("\n▶ const car5 = new DemoCar();");
    console.log('  car5.make = "Toyota";  car5.model = "Camry";');
    console.log('  car5.year = 2021;      car5.color = "Silver";  car5.price = 34000;');
    console.log("  car5.save();");
    const car5 = new DemoCar();
    car5.make = "Toyota";
    car5.model = "Camry";
    car5.year = 2021;
    car5.color = "Silver";
    car5.price = 34_000;
    car5.save();
    console.log(`  → [${car5.__id}]  ${car5.make} ${car5.model}  ${car5.color}  $${car5.price}`);

    console.log("\n▶ DemoCar.count();");
    console.log(`  → ${DemoCar.count()} rows in sheet`);
    console.log("[demoCreate — END]");
  }

  /**
   * demoRead — demonstrates querying the DemoCar table.
   *
   * Shows five query styles:
   * 1. `DemoCar.find()` — fetch all records.
   * 2. `DemoCar.where(...)` — filter by make ("Toyota").
   * 3. `Query.from(DemoCar).where(...).orderBy(...)` — recent cars sorted by price desc.
   * 4. `Query.from(DemoCar).where(...)` — cheap cars (price < 35 000).
   * 5. `DemoCar.count()` — total row count.
   */
  static demoRead(): void {
    console.log("[demoRead — START]");

    // 1. All records
    console.log("\n▶ [1]  DemoCar.find()");
    const all = DemoCar.find();
    console.log(`  → ${all.length} records:`);
    for (const c of all) {
      console.log(`  • ${c.make} ${c.model} ${c.year}  ${c.color}  $${c.price}`);
    }

    // 2. Filter: only Toyotas
    console.log('\n▶ [2]  DemoCar.where("make", "=", "Toyota").execute()');
    const toyotas = DemoCar.where("make", "=", "Toyota").execute();
    console.log(`  → ${toyotas.length} records:`);
    for (const c of toyotas) {
      console.log(`  • ${c.model} (${c.year})`);
    }

    // 3. Cars from 2023 and newer, sorted by price descending
    console.log('\n▶ [3]  Query.from(DemoCar).where("year", ">=", 2023).orderBy("price", "desc").execute()');
    const recent = Query.from(DemoCar).where("year", ">=", 2023).orderBy("price", "desc").execute();
    console.log(`  → ${recent.length} records:`);
    for (const c of recent) {
      console.log(`  • ${c.make} ${c.model}  $${c.price}`);
    }

    // 4. Cheap cars (price < 35 000)
    console.log('\n▶ [4]  Query.from(DemoCar).where("price", "<", 35000).execute()');
    const cheap = Query.from(DemoCar).where("price", "<", 35_000).execute();
    console.log(`  → ${cheap.length} records:`);
    for (const c of cheap) {
      console.log(`  • ${c.make} ${c.model}  $${c.price}`);
    }

    console.log("\n▶ [5]  DemoCar.count()");
    console.log(`  → ${DemoCar.count()} rows total`);
    console.log("[demoRead — END]");
  }

  /**
   * demoUpdate — finds each Toyota and raises its price by 5 %.
   *
   * Demonstrates three update patterns:
   * 1. Filter Toyotas → mutate price & colour → save each.
   * 2. Filter BMWs → bump year, discount price, change colour → save each.
   * 3. Fetch all → append "(updated)" to model name → save each.
   */
  static demoUpdate(): void {
    console.log("[demoUpdate — START]");

    // ── Step 1: raise Toyota prices by 5 % and update colour ─────────────
    console.log('\n▶ [1] const toyotas = DemoCar.where("make", "=", "Toyota").execute();');
    console.log("  for (const car of toyotas) {");
    console.log("    car.price = Math.round(car.price * 1.05);");
    console.log('    car.color = car.color === "White" ? "Pearl White" : car.color;');
    console.log("    car.save();");
    console.log("  }");
    const toyotas = DemoCar.where("make", "=", "Toyota").execute();

    if (toyotas.length === 0) {
      console.log("⚠  No records found — run demoCreate first.");
      return;
    }

    console.log(`  → ${toyotas.length} Toyota(s) found:`);
    for (const car of toyotas) {
      const oldPrice = car.price;
      const oldColor = car.color;
      car.price = Math.round(car.price * 1.05);
      car.color = car.color === "White" ? "Pearl White" : car.color;
      car.save();
      console.log(
        `  ✎ [${car.__id}]  ${car.make} ${car.model}` +
          `  color: ${oldColor} → ${car.color}` +
          `  price: $${oldPrice} → $${car.price}`,
      );
    }

    // ── Step 2: give every BMW a model-year bump and a discount ───────────
    console.log('\n▶ [2] const bmws = DemoCar.where("make", "=", "BMW").execute();');
    console.log("  for (const car of bmws) {");
    console.log("    car.year += 1;");
    console.log("    car.price = Math.round(car.price * 0.92);");
    console.log('    car.color = "Midnight Blue";');
    console.log("    car.save();");
    console.log("  }");
    const bmws = DemoCar.where("make", "=", "BMW").execute();
    console.log(`  → ${bmws.length} BMW(s) found:`);
    for (const car of bmws) {
      const oldYear = car.year;
      const oldPrice = car.price;
      const oldColor = car.color;
      car.year += 1;
      car.price = Math.round(car.price * 0.92);
      car.color = "Midnight Blue";
      car.save();
      console.log(
        `  ✎ [${car.__id}]  ${car.make} ${car.model}` +
          `  year: ${oldYear} → ${car.year}` +
          `  color: ${oldColor} → ${car.color}` +
          `  price: $${oldPrice} → $${car.price}`,
      );
    }

    // ── Step 3: append "(updated)" suffix to every car's model ──────────
    console.log("\n▶ [3] const all = DemoCar.find();");
    console.log("  for (const car of all) {");
    console.log('    car.model = car.model.replace(/ \\(updated\\)$/, "") + " (updated)";');
    console.log("    car.save();");
    console.log("  }");
    const all = DemoCar.find();
    console.log(`  → ${all.length} total cars:`);
    for (const car of all) {
      car.model = car.model.replace(/ \(updated\)$/, "") + " (updated)";
      car.save();
      console.log(`  ✎ [${car.__id}]  ${car.make}  ${car.model}  $${car.price}  ${car.color}`);
    }

    console.log("\n▶ DemoCar.count()");
    console.log(`  → ${DemoCar.count()} rows total`);
    console.log("[demoUpdate — END]");
  }

  /**
   * demoDelete — removes all Honda and Ford records from the sheet.
   *
   * Uses `Query.from(DemoCar).where(...).or(...)` to select multiple
   * makes, then iterates and calls `car.delete()` on each.
   * Before/after row counts are printed for verification.
   */
  static demoDelete(): void {
    console.log("[demoDelete — START]");

    console.log("\n▶ const cars = Query.from(DemoCar)");
    console.log('    .where("make", "=", "Honda")');
    console.log('    .or("make", "=", "Ford")');
    console.log("    .execute();");
    console.log("  for (const car of cars) { car.delete(); }");
    const cars = Query.from(DemoCar).where("make", "=", "Honda").or("make", "=", "Ford").execute();

    if (cars.length === 0) {
      console.log("⚠  No Honda / Ford records found — run demoCreate first.");
      return;
    }

    console.log(`  → ${cars.length} records found`);

    console.log("\n▶ DemoCar.count();");
    const before = DemoCar.count();
    console.log(`  → ${before} rows before delete`);

    for (const car of cars) {
      car.delete();
      console.log(`  ✗ Deleted [${car.__id}]  ${car.make} ${car.model}`);
    }

    console.log("\n▶ DemoCar.count();");
    console.log(`  → ${DemoCar.count()} rows after delete`);
    console.log("[demoDelete — END]");
  }
}
