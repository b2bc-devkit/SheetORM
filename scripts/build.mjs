import { execSync } from "child_process";
import fs from "fs";

const run = (cmd) => execSync(cmd, { stdio: "inherit" });

// 1. Clean previous tsc output
if (fs.existsSync("build")) fs.rmSync("build", { recursive: true });

// 2. Compile TypeScript → build/
run("tsc -p tsconfig.gas.json");

// 3. Bundle with Vite → dist/Code.js
run("vite build");

// 4. Move bundle to project root (required by clasp)
fs.copyFileSync("dist/Code.js", "Code.js");
fs.rmSync("dist", { recursive: true });
