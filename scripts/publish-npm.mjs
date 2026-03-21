import { execSync } from "child_process";

const run = (cmd) => execSync(cmd, { stdio: "inherit" });

const npmTag = process.env.NPM_TAG;
const publishArgs = ["npm", "publish", "--access", "public"];

if (npmTag) {
	publishArgs.push("--tag", npmTag);
}

run("npm run lint");
run("npm test");
run("npm run build:npm");
run(publishArgs.join(" "));
