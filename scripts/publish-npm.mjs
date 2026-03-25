import { execSync, execFileSync } from "child_process";

const run = (cmd) => execSync(cmd, { stdio: "inherit" });

const npmTag = process.env.NPM_TAG;
const publishArgs = ["publish", "--access", "public"];

if (npmTag) {
  const normalizedTag = npmTag.trim();
  if (!/^[A-Za-z0-9._-]+$/.test(normalizedTag)) {
    throw new Error(`Invalid NPM_TAG value: ${npmTag}`);
  }
  publishArgs.push("--tag", normalizedTag);
}

run("npm run lint");
run("npm test");
run("npm run build:npm");
const npmBin = process.platform === "win32" ? "npm.cmd" : "npm";
execFileSync(npmBin, publishArgs, { stdio: "inherit" });
