import { execSync, execFileSync } from "child_process";

const run = (cmd) => execSync(cmd, { stdio: "inherit" });
const claspBin = process.platform === "win32" ? "clasp.cmd" : "clasp";
const runClasp = (args) => execFileSync(claspBin, args, { stdio: "inherit" });

const versionMessage = process.env.GAS_VERSION_MESSAGE || "SheetORM Apps Script release";
const deploymentId = process.env.GAS_DEPLOYMENT_ID;

run("npm run lint");
run("npm test");
run("npm run build");
runClasp(["push", "-f"]);
runClasp(["version", versionMessage]);

if (deploymentId) {
  if (!/^[\w-]+$/.test(deploymentId)) {
    throw new Error("Invalid GAS_DEPLOYMENT_ID; only letters, digits, underscore, and dash are allowed.");
  }
  runClasp(["deploy", "--deploymentId", deploymentId, "--description", versionMessage]);
}
