/**
 * Publish script for the Google Apps Script (GAS) library.
 *
 * Pipeline:
 *   1. Run lint, tests, and GAS build (npm run lint → test → build).
 *   2. Push the bundled Code.js to Google Apps Script via clasp.
 *   3. Create a new version with a descriptive message.
 *   4. (Optional) Deploy to an existing deployment ID if GAS_DEPLOYMENT_ID is set.
 *
 * Environment variables:
 *   GAS_VERSION_MESSAGE  – version description (default: "SheetORM Apps Script release")
 *   GAS_DEPLOYMENT_ID    – existing deployment to update (optional; validated for safety)
 *
 * Usage: `node scripts/publish-gas.mjs`
 */

import { execSync, execFileSync } from "child_process";

/** Execute a shell command with inherited stdio for real-time output. */
const run = (cmd) => execSync(cmd, { stdio: "inherit" });

// Resolve the clasp binary name (Windows uses .cmd wrapper).
const claspBin = process.platform === "win32" ? "clasp.cmd" : "clasp";
/** Execute clasp with an array of arguments (avoids shell injection). */
const runClasp = (args) => execFileSync(claspBin, args, { stdio: "inherit" });

// Version message shown in the GAS editor's version history.
const versionMessage = process.env.GAS_VERSION_MESSAGE || "SheetORM Apps Script release";
// Optional: update an existing deployment instead of creating a new one.
const deploymentId = process.env.GAS_DEPLOYMENT_ID;

// 1. Quality gates: lint, test, build
run("npm run lint");
run("npm test");
run("npm run build");

// 2. Push bundle to GAS project (overwrites remote Code.js)
runClasp(["push", "-f"]);

// 3. Create a new immutable version
runClasp(["version", versionMessage]);

// 4. Optionally update an existing deployment
if (deploymentId) {
  // Validate deployment ID to prevent command injection.
  if (!/^[\w-]+$/.test(deploymentId)) {
    throw new Error("Invalid GAS_DEPLOYMENT_ID; only letters, digits, underscore, and dash are allowed.");
  }
  runClasp(["deploy", "--deploymentId", deploymentId, "--description", versionMessage]);
}
