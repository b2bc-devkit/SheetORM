/**
 * Publish script for the npm package.
 *
 * Pipeline:
 *   1. Run lint, tests, and npm build (npm run lint → test → build:npm).
 *   2. Publish to the npm registry with public access.
 *
 * Environment variables:
 *   NPM_TAG – optional dist-tag for the release (e.g. "beta", "next").
 *             If not set, npm defaults to the "latest" tag.
 *             Validated to prevent command injection.
 *
 * Prerequisites:
 *   - Authenticate with `npm login` before running this script.
 *   - Bump the version in package.json before publishing.
 *
 * Usage: `node scripts/publish-npm.mjs`
 */

import { execSync, execFileSync } from "child_process";

/** Execute a shell command with inherited stdio for real-time output. */
const run = (cmd) => execSync(cmd, { stdio: "inherit" });

// Optional: publish under a custom dist-tag (e.g. "beta").
const npmTag = process.env.NPM_TAG;
const publishArgs = ["publish", "--access", "public"];

if (npmTag) {
  const normalizedTag = npmTag.trim();
  // Validate tag to prevent command injection via environment variables.
  if (!/^[A-Za-z0-9._-]+$/.test(normalizedTag)) {
    throw new Error(`Invalid NPM_TAG value: ${npmTag}`);
  }
  publishArgs.push("--tag", normalizedTag);
}

// 1. Quality gates: lint, test, build
run("npm run lint");
run("npm test");
run("npm run build:npm");

// 2. Publish to npm (uses execFileSync to avoid shell injection)
const npmBin = process.platform === "win32" ? "npm.cmd" : "npm";
execFileSync(npmBin, publishArgs, { stdio: "inherit" });
