import { execSync } from "child_process";

const run = (cmd) => execSync(cmd, { stdio: "inherit" });

const versionMessage = process.env.GAS_VERSION_MESSAGE || "SheetORM Apps Script release";
const deploymentId = process.env.GAS_DEPLOYMENT_ID;
const descriptionArg = JSON.stringify(versionMessage);

run("npm run lint");
run("npm test");
run("npm run build");
run("clasp push -f");
run(`clasp version ${descriptionArg}`);

if (deploymentId) {
	run(`clasp deploy --deploymentId ${deploymentId} --description ${descriptionArg}`);
}
