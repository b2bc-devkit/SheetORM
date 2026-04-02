/**
 * Build script for the npm package.
 *
 * Pipeline:
 *   1. Remove previous npm build output (dist/npm/).
 *   2. Compile TypeScript → ES modules + declarations in dist/npm/
 *      (via tsconfig.npm.json).
 *
 * The output is published to npm as defined by the "files" field in
 * package.json, which includes only dist/npm/**\/*.
 *
 * Usage: `node scripts/build-npm.mjs`  (also available as `npm run build:npm`)
 */

import { execSync } from "child_process";
import fs from "fs";

/** Execute a shell command with inherited stdio for real-time output. */
const run = (cmd) => execSync(cmd, { stdio: "inherit" });

/** Output directory for the npm package (mirrors src/ directory structure). */
const outDir = "dist/npm";

// 1. Clean previous build output
if (fs.existsSync(outDir)) {
  fs.rmSync(outDir, { recursive: true });
}

// 2. Compile TypeScript → ESM + .d.ts files
run("tsc -p tsconfig.npm.json");
