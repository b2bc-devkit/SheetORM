/**
 * Jest configuration for SheetORM.
 *
 * Uses @swc/jest to transpile TypeScript on the fly — it does not depend on the
 * TypeScript compiler API, so it works with TypeScript 7 (native, no JS API).
 * Test type-checking is covered separately by `tsc -p tsconfig.test.json`.
 *
 * @type {import('jest').Config}
 */
module.exports = {
  // Run tests in Node.js (no DOM needed).
  testEnvironment: "node",
  testEnvironmentOptions: {
    // Prevent Jest from clearing global state between test files; some tests
    // share Registry state intentionally.
    globalsCleanup: "off",
  },
  // All test files live under tests/.
  roots: ["<rootDir>/tests"],
  transform: {
    // Transform .ts and .tsx files via @swc/jest.
    "^.+\\.tsx?$": [
      "@swc/jest",
      {
        sourceMaps: "inline",
        jsc: {
          target: "es2022",
          parser: {
            syntax: "typescript",
            tsx: false,
            decorators: true,
          },
          transform: {
            // Matches tsconfig "experimentalDecorators": true.
            legacyDecorator: true,
          },
        },
        module: {
          // CommonJS is required by Jest's default module system.
          type: "commonjs",
        },
      },
    ],
  },
  // Only files matching *.test.ts are treated as tests.
  testMatch: ["**/*.test.ts"],
  // Strip .js extensions from imports so TypeScript source paths resolve
  // correctly under CommonJS (TypeScript emits .js extensions in imports).
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
  },
};
