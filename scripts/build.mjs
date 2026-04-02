/**
 * Build script for the Google Apps Script (GAS) bundle.
 *
 * Pipeline:
 *   1. Remove previous tsc output (build/).
 *   2. Compile TypeScript → intermediate JS in build/ (via tsconfig.gas.json).
 *   3. Bundle with Vite/Rollup → dist/Code.js (single IIFE).
 *   4. Copy dist/Code.js to the project root (clasp expects Code.js here).
 *   5. Clean up the dist/ directory.
 *
 * Usage: `node scripts/build.mjs`  (also available as `npm run build`)
 */

import { execSync } from "child_process";
import fs from "fs";

/** Execute a shell command with inherited stdio for real-time output. */
const run = (cmd) => execSync(cmd, { stdio: "inherit" });

// 1. Clean previous tsc output
if (fs.existsSync("build")) fs.rmSync("build", { recursive: true });

// 2. Compile TypeScript → build/
run("tsc -p tsconfig.gas.json");

// 3. Bundle with Vite → dist/Code.js
run("vite build");

// 4. Move bundle to project root (required by clasp)
fs.copyFileSync("dist/Code.js", "Code.js");

// 5. Clean up intermediate dist/ directory
fs.rmSync("dist", { recursive: true });
