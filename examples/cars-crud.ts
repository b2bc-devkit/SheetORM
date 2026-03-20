// Example: Cars CRUD with SheetORM ActiveRecord API
// Copy this into your GAS project alongside the SheetORM bundle.
//
// This file is NOT compiled into the library — it serves as a reference.
// In GAS, import from the bundled SheetORM globals directly.

import { Record } from "../src/core/Record";
import { Decorators } from "../src/core/Decorators";
import { Query } from "../src/query/Query";

const { Indexed, Required } = Decorators;

// ─── Define models by extending Record ───────────────

class Car extends Record {
  @Indexed()
  make: string;

  @Required()
  model: string;

  year: number;
  color: string;
}

// ─── Usage — everything is fully automatic ───────────

export function helloWorld() {
  // Create and save — table is auto-created on first use
  const car = new Car();
  car.make = "Toyota";
  car.model = "Corolla";
  car.year = 2024;
  car.color = "blue";
  car.save(); // auto-creates 'Cars' sheet, persists entity

  // Static create + save
  const car2 = Car.create({ make: "Honda", model: "Civic", year: 2023 });
  car2.save();

  // Fluent set + save
  const car3 = new Car();
  car3.set("make", "BMW").set("model", "X5").set("year", 2024).save();

  // Static queries — return typed Car[]
  const toyotas = Car.where("make", "=", "Toyota").execute();
  console.log("Toyotas:", toyotas.length);

  // Find by ID
  const found = Car.findById(car.__id);
  console.log("Found:", found?.model);

  // Query.from() — works with class ref (typed) or string
  const recent = Query.from(Car).where("year", ">=", 2023).orderBy("year", "desc").limit(10).execute();
  console.log("Recent cars:", recent.length);

  // Update
  car.color = "red";
  car.save();

  // Delete
  car2.delete();
  car3.delete();

  // Count
  console.log("Total cars:", Car.count());
}
