/**
 * Jest configuration for SheetORM.
 *
 * Uses ts-jest to compile TypeScript on the fly with the test-specific
 * tsconfig (tsconfig.test.json) which emits CommonJS for Jest compatibility.
 *
 * @type {import('ts-jest').JestConfigWithTsJest}
 */
module.exports = {
  // ts-jest preset provides TypeScript transform and source-map support.
  preset: "ts-jest",
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
    // Transform .ts and .tsx files via ts-jest using the test-specific config.
    "^.+\\.tsx?$": [
      "ts-jest",
      {
        tsconfig: "tsconfig.test.json",
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
