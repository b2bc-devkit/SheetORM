// SheetORM — GAS entry point
// Only the three callable GAS functions are exposed as globals.
// Internal types, classes and utilities are bundled but not surfaced as GAS menu items.

import { Decorators } from "./core/Decorators.js";
import { Record } from "./core/Record.js";
import { Registry } from "./core/Registry.js";
import { IndexStore } from "./index/IndexStore.js";
import { Query } from "./query/Query.js";
import { RuntimeBenchmark } from "./testing/RuntimeBenchmark.js";
import { RuntimeParity } from "./testing/RuntimeParity.js";

// ─── Demo model (internal, not exported) ──────────────────────────────────────

const { Indexed, Required, Field } = Decorators;

class DemoCar extends Record {
  @Indexed()
  make: string = "";

  @Required()
  model: string = "";

  @Field({ type: "number" })
  year: number = 0;

  color: string = "";

  @Field({ type: "number" })
  price: number = 0;
}

// ─── GasEntrypoints ───────────────────────────────────────────────────────────

export class GasEntrypoints {
  static readonly Record = Record;
  static readonly Query = Query;
  static readonly Decorators = Decorators;
  static readonly IndexStore = IndexStore;
  static readonly Registry = Registry;

  static runTestsStageOne(): void {
    RuntimeParity.runStageOne();
  }

  static runTestsStageTwo(): void {
    RuntimeParity.runStageTwo();
  }

  static validateTests(): void {
    RuntimeParity.validate();
  }

  static runBenchmark(): void {
    RuntimeBenchmark.run();
  }

  // ─── CRUD demos ─────────────────────────────────────────────────────────

  /**
   * demoCreate — saves 5 DemoCar records to the sheet.
   * Run this first to populate the table.
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
   * demoRead — queries the DemoCar table in several ways.
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
