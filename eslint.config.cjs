/* eslint-disable @typescript-eslint/no-require-imports */
const tsEslintPlugin = require("@typescript-eslint/eslint-plugin");
const tsParser = require("@typescript-eslint/parser");
const googleAppsScript = require("eslint-plugin-googleappsscript");
const prettierConfig = require("eslint-config-prettier");

const gasGlobals = Object.fromEntries(
  Object.keys(googleAppsScript.environments.googleappsscript.globals).map((name) => [name, "readonly"]),
);

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
  {
    ignores: ["build/**", "dist/**", "node_modules/**", "coverage/**", "**/*.js"],
  },
  ...tsEslintPlugin.configs["flat/recommended"],
  {
    files: ["**/*.ts"],
    languageOptions: {
      parser: tsParser,
      ecmaVersion: "latest",
      sourceType: "module",
      globals: gasGlobals,
    },
  },
  prettierConfig,
  {
    files: ["tests/**/*.ts"],
    languageOptions: {
      globals: jestGlobals,
    },
  },
];
