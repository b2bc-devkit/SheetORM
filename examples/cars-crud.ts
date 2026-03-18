// Example: Cars CRUD with SheetORM ActiveRecord API
// Copy this into your GAS project alongside the SheetORM bundle.
//
// This file is NOT compiled into the library — it serves as a reference.
// In GAS, import from the bundled SheetORM globals directly.

import { Record, QueryBuilder } from "../src/index";
import type { FieldDefinition, IndexDefinition } from "../src/index";

// ─── Define models by extending Record ───────────────

class Car extends Record {
  static tableName = "Cars";
  static fields: FieldDefinition[] = [
    { name: "make", type: "string", required: true },
    { name: "model", type: "string", required: true },
    { name: "year", type: "number" },
    { name: "color", type: "string" },
  ];
  static indexes: IndexDefinition[] = [{ field: "make" }];

  declare make: string;
  declare model: string;
  declare year: number;
  declare color: string;
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

  // Fluent set + save
  const car2 = new Car();
  car2.set("make", "Honda").set("model", "Civic").set("year", 2023).save();

  // Static queries — return typed Car[]
  const toyotas = Car.where("make", "=", "Toyota").execute();
  console.log("Toyotas:", toyotas.length);

  // Find by ID
  const found = Car.findById(car.__id);
  console.log("Found:", found?.model);

  // QueryBuilder.from() — works with class ref (typed) or string
  const recent = QueryBuilder.from(Car).where("year", ">=", 2023).orderBy("year", "desc").limit(10).execute();
  console.log("Recent cars:", recent.length);

  // Update
  car.color = "red";
  car.save();

  // Delete
  car2.delete();

  // Count
  console.log("Total cars:", Car.count());
}
