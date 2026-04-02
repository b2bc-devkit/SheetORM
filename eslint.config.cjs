/**
 * ESLint flat configuration for SheetORM.
 *
 * Uses @typescript-eslint for TypeScript-aware linting, the google-apps-script
 * plugin to recognise GAS global variables (SpreadsheetApp, Logger, etc.),
 * and eslint-config-prettier to disable rules that conflict with Prettier.
 */

/* eslint-disable @typescript-eslint/no-require-imports */
const tsEslintPlugin = require("@typescript-eslint/eslint-plugin");
const tsParser = require("@typescript-eslint/parser");
const googleAppsScript = require("eslint-plugin-googleappsscript");
const prettierConfig = require("eslint-config-prettier");

// Extract all Google Apps Script global names (SpreadsheetApp, Logger, etc.)
// and mark them as read-only so ESLint doesn't flag them as undefined.
const gasGlobals = Object.fromEntries(
  Object.keys(googleAppsScript.environments.googleappsscript.globals).map((name) => [name, "readonly"]),
);

// Jest global functions available in test files without explicit imports.
const jestGlobals = {
  describe: "readonly",
  it: "readonly",
  test: "readonly",
  expect: "readonly",
  jest: "readonly",
  beforeAll: "readonly",
  beforeEach: "readonly",
  afterAll: "readonly",
  afterEach: "readonly",
};

module.exports = [
  // Ignore generated output directories and compiled JS files.
  {
    ignores: ["build/**", "dist/**", "node_modules/**", "coverage/**", "**/*.js"],
  },
  // Apply recommended TypeScript-ESLint rules to all files.
  ...tsEslintPlugin.configs["flat/recommended"],
  {
    // TypeScript source files: use the TS parser and inject GAS globals.
    files: ["**/*.ts"],
    languageOptions: {
      parser: tsParser,
      ecmaVersion: "latest",
      sourceType: "module",
      globals: gasGlobals,
    },
  },
  // Disable ESLint formatting rules that conflict with Prettier.
  prettierConfig,
  {
    // Test files: inject Jest globals (describe, it, expect, etc.).
    files: ["tests/**/*.ts"],
    languageOptions: {
      globals: jestGlobals,
    },
  },
];
