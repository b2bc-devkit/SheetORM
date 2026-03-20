import { defineConfig } from "vite";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const REAL_ENTRY = path.resolve(__dirname, "build/src/index.js");
const VIRTUAL_ID = "\0gas-entry";

/**
 * Detect named exports from the compiled ES module entry file
 * by scanning `export { Name, ... } from "..."` statements.
 */
function detectExportNames(entryPath) {
  const src = fs.readFileSync(entryPath, "utf8");
  const names = new Set();
  const re = /export\s*\{([^}]+)\}/g;
  let m;
  while ((m = re.exec(src)) !== null) {
    for (const token of m[1].split(",")) {
      const name = token
        .trim()
        .split(/\s+as\s+/)
        .pop()
        .trim();
      if (name) names.add(name);
    }
  }
  return [...names];
}

/**
 * Vite/Rollup plugin that replicates gas-webpack-plugin behaviour for GAS:
 *
 * 1. Creates a virtual entry that imports all exports from the real entry
 *    and assigns them to `globalThis` (GAS global scope) at runtime.
 * 2. Prepends top-level `function Name() {}` stubs so that GAS recognises
 *    all exported names at parse time (shown in the script editor Run menu).
 */
function gasPlugin() {
  let exportNames = [];

  return {
    name: "gas-plugin",

    resolveId(source) {
      if (source === "gas-entry") return VIRTUAL_ID;
      return null;
    },

    load(id) {
      if (id === VIRTUAL_ID) {
        exportNames = detectExportNames(REAL_ENTRY);
        const imports = exportNames.join(", ");
        const entryUrl = REAL_ENTRY.replace(/\\/g, "/");
        const assignments = exportNames.map((n) => `__g.${n} = ${n};`).join("\n");
        return [
          `import { ${imports} } from "${entryUrl}";`,
          `var __g = typeof globalThis !== "undefined" ? globalThis : this;`,
          assignments,
        ].join("\n");
      }
    },

    generateBundle(_options, bundle) {
      for (const chunk of Object.values(bundle)) {
        if (chunk.type === "chunk" && exportNames.length > 0) {
          const stubs = exportNames.map((name) => `function ${name}() {}`).join("\n");
          // Wrap the entire Rollup output in an outer IIFE so that Rollup-generated
          // module helpers (__defNormalProp, __publicField, etc.) are kept out of the
          // GAS global scope — otherwise GAS shows them in the script editor Run menu.
          chunk.code = stubs + "\n!function(){\n" + chunk.code + "\n}();";
        }
      }
    },
  };
}

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [gasPlugin()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // GAS V8 runtime supports ES2020+ natively — no need for Babel down-level transforms
    target: "es2020",
    rollupOptions: {
      input: "gas-entry",
      output: {
        format: "iife",
        entryFileNames: "Code.js",
        exports: "none",
        inlineDynamicImports: true,
      },
    },
    minify: "terser",
    terserOptions: {
      compress: {
        drop_console: false,
        // keep the top-level stub functions even though they look unused
        unused: false,
      },
      mangle: false,
      format: {
        comments: false,
      },
    },
    sourcemap: false,
  },
});
