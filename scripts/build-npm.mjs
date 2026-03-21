import { execSync } from "child_process";
import fs from "fs";

const run = (cmd) => execSync(cmd, { stdio: "inherit" });

const outDir = "dist/npm";

if (fs.existsSync(outDir)) {
	fs.rmSync(outDir, { recursive: true });
}

run("tsc -p tsconfig.npm.json");
