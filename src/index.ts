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

  static runTests(): void {
    RuntimeParity.run();
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
    console.log("╔══════════════════════════════════╗");
    console.log("║         demoCreate — START       ║");
    console.log("╚══════════════════════════════════╝");

    const seeds: Array<{ make: string; model: string; year: number; color: string; price: number }> = [
      { make: "Toyota", model: "Corolla", year: 2022, color: "White", price: 28_000 },
      { make: "BMW", model: "X5", year: 2023, color: "Black", price: 85_000 },
      { make: "Honda", model: "Civic", year: 2024, color: "Red", price: 32_000 },
      { make: "Ford", model: "Mustang", year: 2023, color: "Blue", price: 57_000 },
      { make: "Toyota", model: "Camry", year: 2021, color: "Silver", price: 34_000 },
    ];

    const saved = DemoCar.saveAll(seeds);

    for (const car of saved) {
      console.log(
        `  ✓ Saved  [${car.__id}]  ${car.make} ${car.model} (${car.year})  ${car.color}  $${car.price}`,
      );
    }

    console.log(`\nTotal rows in sheet: ${DemoCar.count()}`);
    console.log("══════════════════════════════════");
  }

  /**
   * demoRead — queries the DemoCar table in several ways.
   */
  static demoRead(): void {
    console.log("╔══════════════════════════════════╗");
    console.log("║          demoRead — START        ║");
    console.log("╚══════════════════════════════════╝");

    // 1. All records
    const all = DemoCar.find();
    console.log(`\n[1] All cars (${all.length} total):`);
    for (const c of all) {
      console.log(`  • ${c.make} ${c.model} ${c.year}  ${c.color}  $${c.price}`);
    }

    // 2. Filter: only Toyotas
    const toyotas = DemoCar.where("make", "=", "Toyota").execute();
    console.log(`\n[2] Toyotas (${toyotas.length}):`);
    for (const c of toyotas) {
      console.log(`  • ${c.model} (${c.year})`);
    }

    // 3. Cars from 2023 and newer, sorted by price descending
    const recent = Query.from(DemoCar).where("year", ">=", 2023).orderBy("price", "desc").execute();
    console.log(`\n[3] Cars from 2023+ sorted by price desc (${recent.length}):`);
    for (const c of recent) {
      console.log(`  • ${c.make} ${c.model}  $${c.price}`);
    }

    // 4. Count & cheap cars (price < 35 000)
    const cheap = Query.from(DemoCar).where("price", "<", 35_000).execute();
    console.log(`\n[4] Cheap cars <$35 000 (${cheap.length}):`);
    for (const c of cheap) {
      console.log(`  • ${c.make} ${c.model}  $${c.price}`);
    }

    console.log("\nTotal rows in sheet: " + DemoCar.count());
    console.log("══════════════════════════════════");
  }

  /**
   * demoUpdate — finds each Toyota and raises its price by 5 %.
   */
  static demoUpdate(): void {
    console.log("╔══════════════════════════════════╗");
    console.log("║        demoUpdate — START        ║");
    console.log("╚══════════════════════════════════╝");

    const toyotas = DemoCar.where("make", "=", "Toyota").execute();

    if (toyotas.length === 0) {
      console.log("⚠  No Toyota records found — run demoCreate first.");
      return;
    }

    for (const car of toyotas) {
      const oldPrice = car.price;
      const oldColor = car.color;
      car.price = Math.round(car.price * 1.05);
      car.color = car.color === "White" ? "Pearl White" : car.color;
      car.save();
      console.log(
        `  ✎ Updated [${car.__id}]  ${car.make} ${car.model}` +
          `\n      color: ${oldColor} → ${car.color}` +
          `\n      price: $${oldPrice} → $${car.price}`,
      );
    }

    console.log("\nTotal rows in sheet: " + DemoCar.count());
    console.log("══════════════════════════════════");
  }

  /**
   * demoDelete — removes all Honda and Ford records from the sheet.
   */
  static demoDelete(): void {
    console.log("╔══════════════════════════════════╗");
    console.log("║        demoDelete — START        ║");
    console.log("╚══════════════════════════════════╝");

    const toDelete = Query.from(DemoCar).where("make", "=", "Honda").or("make", "=", "Ford").execute();

    if (toDelete.length === 0) {
      console.log("⚠  No Honda / Ford records found — run demoCreate first.");
      return;
    }

    const before = DemoCar.count();
    console.log(`\nRows before delete: ${before}`);

    for (const car of toDelete) {
      car.delete();
      console.log(`  ✗ Deleted [${car.__id}]  ${car.make} ${car.model}`);
    }

    console.log(`\nRows after delete:  ${DemoCar.count()}`);
    console.log("══════════════════════════════════");
  }
}
