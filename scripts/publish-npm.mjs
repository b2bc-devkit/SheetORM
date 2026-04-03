/**
 * Interactive publish script for the npm package.
 *
 * Pipeline:
 *   1. Interactive menu: choose access (public / restricted) and dist-tag.
 *   2. Temporarily patch package.json with selected options.
 *   3. Run lint, tests, and npm build (npm run lint → test → build:npm).
 *   4. Publish to the npm registry.
 *   5. Restore original package.json regardless of success or failure.
 *
 * Prerequisites:
 *   - Authenticate with `npm login` before running this script.
 *   - Bump the version in package.json before publishing.
 *
 * Usage: `node scripts/publish-npm.mjs`
 */

import { execSync } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { createInterface } from "readline";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const pkgPath = resolve(__dirname, "..", "package.json");

/** Execute a shell command with inherited stdio for real-time output. */
const run = (cmd) => execSync(cmd, { stdio: "inherit" });

// ─── Interactive menu helpers ────────────────────────────────────────

const CYAN = "\x1b[36m";
const GREEN = "\x1b[32m";
const YELLOW = "\x1b[33m";
const DIM = "\x1b[2m";
const BOLD = "\x1b[1m";
const RESET = "\x1b[0m";

/**
 * Show an interactive single-select menu in the terminal.
 * Navigate with ↑/↓ arrows, confirm with Enter.
 * Returns the index of the selected option.
 */
function selectMenu(title, options, defaultIndex = 0) {
  return new Promise((resolvePromise) => {
    let selected = defaultIndex;

    const render = () => {
      // Move cursor up to re-draw (skip on first render)
      if (render.drawn) {
        process.stdout.write(`\x1b[${options.length + 1}A`);
      }
      process.stdout.write(`${BOLD}${CYAN}? ${title}${RESET}\n`);
      for (let i = 0; i < options.length; i++) {
        const marker = i === selected ? `${GREEN}>` : " ";
        const label = i === selected ? `${GREEN}${options[i].label}${RESET}` : ` ${options[i].label}`;
        const desc = options[i].description ? ` ${DIM}${options[i].description}${RESET}` : "";
        process.stdout.write(`  ${marker} ${label}${desc}\n`);
      }
      render.drawn = true;
    };

    render();

    const rl = createInterface({ input: process.stdin });
    process.stdin.setRawMode(true);
    process.stdin.resume();

    const onKeypress = (chunk) => {
      const key = chunk.toString();
      if (key === "\x1b[A") {
        // Up arrow
        selected = (selected - 1 + options.length) % options.length;
        render();
      } else if (key === "\x1b[B") {
        // Down arrow
        selected = (selected + 1) % options.length;
        render();
      } else if (key === "\r" || key === "\n") {
        // Enter
        process.stdin.setRawMode(false);
        process.stdin.removeListener("data", onKeypress);
        rl.close();
        process.stdout.write(`  ${DIM}-> ${options[selected].label}${RESET}\n\n`);
        resolvePromise(selected);
      } else if (key === "\x03") {
        // Ctrl-C
        process.stdin.setRawMode(false);
        process.stdout.write("\n");
        process.exit(1);
      }
    };

    process.stdin.on("data", onKeypress);
  });
}

// ─── Menu definitions ────────────────────────────────────────────────

const ACCESS_OPTIONS = [
  {
    label: "public",
    value: "public",
    description: "– visible to everyone",
  },
  {
    label: "restricted (private)",
    value: "restricted",
    description: "– you / org members only (requires npm Pro)",
  },
];

const TAG_OPTIONS = [
  {
    label: "latest",
    value: "latest",
    description: "– default production tag",
  },
  {
    label: "beta",
    value: "beta",
    description: "– pre-release, not installed by default",
  },
  {
    label: "next",
    value: "next",
    description: "– upcoming version",
  },
  {
    label: "canary",
    value: "canary",
    description: "– experimental build",
  },
];

const SCOPE_OPTIONS = [
  {
    label: "sheetorm",
    value: "sheetorm",
    description: "– unscoped (current name)",
  },
  {
    label: "@b2bc-devkit/sheetorm",
    value: "@b2bc-devkit/sheetorm",
    description: "– b2bc-devkit org scope",
  },
  {
    label: "@b2bc/sheetorm",
    value: "@b2bc/sheetorm",
    description: "– b2bc user scope",
  },
];

// ─── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n${BOLD}${CYAN}+--------------------------------------+${RESET}`);
  console.log(`${BOLD}${CYAN}|   SheetORM  -  npm publish wizard    |${RESET}`);
  console.log(`${BOLD}${CYAN}+--------------------------------------+${RESET}\n`);

  // Read current package.json
  const originalPkgContent = readFileSync(pkgPath, "utf-8");
  const pkg = JSON.parse(originalPkgContent);

  console.log(`  ${DIM}Current version: ${RESET}${YELLOW}${pkg.version}${RESET}`);
  console.log(`  ${DIM}Current name:    ${RESET}${YELLOW}${pkg.name}${RESET}\n`);

  // 1. Choose package name / scope
  const scopeIdx = await selectMenu(
    "Package name (scope):",
    SCOPE_OPTIONS,
    SCOPE_OPTIONS.findIndex((o) => o.value === pkg.name),
  );
  const chosenName = SCOPE_OPTIONS[scopeIdx].value;

  // 2. Choose access level
  const accessIdx = await selectMenu("Access level:", ACCESS_OPTIONS, 0);
  const chosenAccess = ACCESS_OPTIONS[accessIdx].value;

  // Warn if restricted without scope
  if (chosenAccess === "restricted" && !chosenName.startsWith("@")) {
    console.log(`${YELLOW}⚠  Restricted packages require a scoped name (e.g. @user/pkg).${RESET}`);
    console.log(`${YELLOW}   Automatically switching to @b2bc-devkit/sheetorm.${RESET}\n`);
  }
  const finalName =
    chosenAccess === "restricted" && !chosenName.startsWith("@") ? "@b2bc-devkit/sheetorm" : chosenName;

  // 3. Choose dist-tag
  const tagIdx = await selectMenu("Dist-tag:", TAG_OPTIONS, 0);
  const chosenTag = TAG_OPTIONS[tagIdx].value;

  // ── Summary ──
  console.log(`${BOLD}${CYAN}── Summary ──${RESET}`);
  console.log(`  Name:    ${GREEN}${finalName}${RESET}`);
  console.log(`  Access:  ${GREEN}${chosenAccess}${RESET}`);
  console.log(`  Tag:     ${GREEN}${chosenTag}${RESET}`);
  console.log(`  Version: ${GREEN}${pkg.version}${RESET}\n`);

  // Confirm
  const confirmIdx = await selectMenu("Publish?", [
    { label: "Yes", value: true },
    { label: "No, cancel", value: false },
  ]);
  if (confirmIdx === 1) {
    console.log(`${YELLOW}Cancelled.${RESET}`);
    process.exit(0);
  }

  // ── Temporarily patch package.json ──
  pkg.name = finalName;
  pkg.publishConfig = { access: chosenAccess };
  writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n", "utf-8");

  const restore = () => {
    writeFileSync(pkgPath, originalPkgContent, "utf-8");
    console.log(`\n${DIM}✔ package.json restored to original.${RESET}`);
  };

  try {
    // ── Check npm auth ──
    console.log(`\n${BOLD}${CYAN}[1/5]${RESET} Checking npm login...`);
    try {
      const whoami = execSync("npm whoami", { encoding: "utf-8" }).trim();
      console.log(`  ${DIM}Logged in as:${RESET} ${GREEN}${whoami}${RESET}`);
    } catch {
      console.log(`  ${YELLOW}Not logged in. Opening npm login...${RESET}\n`);
      run("npm login");
    }

    // ── Quality gates ──
    console.log(`\n${BOLD}${CYAN}[2/5]${RESET} Lint...`);
    run("npm run lint");

    console.log(`\n${BOLD}${CYAN}[3/5]${RESET} Tests...`);
    run("npm test");

    console.log(`\n${BOLD}${CYAN}[4/5]${RESET} Build...`);
    run("npm run build:npm");

    // ── Publish ──
    console.log(`\n${BOLD}${CYAN}[5/5]${RESET} Publishing...`);
    const publishArgs = ["npm", "publish", "--access", chosenAccess];
    if (chosenTag !== "latest") {
      publishArgs.push("--tag", chosenTag);
    }
    try {
      run(publishArgs.join(" "));
    } catch (pubErr) {
      const msg = pubErr.stderr?.toString() ?? pubErr.message ?? "";
      if (/E402|402 Payment Required|sign up for private/i.test(msg)) {
        console.error(`\n${YELLOW}Error: npm returned 402 Payment Required.${RESET}`);
        console.error(`${YELLOW}Private (restricted) scoped packages require a paid npm plan.${RESET}`);
        console.error(`${DIM}Options:${RESET}`);
        console.error(
          `${DIM}  1. Upgrade to npm Pro / Org paid plan at https://www.npmjs.com/settings/billing${RESET}`,
        );
        console.error(`${DIM}  2. Re-run this wizard and choose "public" access instead.${RESET}`);
        throw new Error("Publish failed: paid plan required for restricted packages.");
      }
      if (/ENEEDAUTH|npm adduser|You need to authorize/i.test(msg)) {
        console.error(`\n${YELLOW}Error: not logged in to npm.${RESET}`);
        console.error(`${DIM}Run "npm login" and try again.${RESET}`);
        throw new Error("Publish failed: npm authentication required.");
      }
      if (/EPUBLISHCONFLICT|cannot publish over|previously published/i.test(msg)) {
        console.error(`\n${YELLOW}Error: version ${pkg.version} already exists on the registry.${RESET}`);
        console.error(`${DIM}Bump the version in package.json and try again.${RESET}`);
        throw new Error(`Publish failed: ${finalName}@${pkg.version} already exists.`);
      }
      // Unknown publish error – re-throw with original message
      throw pubErr;
    }

    console.log(`\n${GREEN}${BOLD}✔ Published ${finalName}@${pkg.version} [${chosenTag}]${RESET}`);
  } finally {
    // Always restore package.json, even on error
    restore();
  }
}

main().catch((err) => {
  console.error(`\n${YELLOW}Error: ${err.message}${RESET}`);
  process.exit(1);
});
