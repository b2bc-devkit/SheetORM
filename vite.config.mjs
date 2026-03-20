import { defineConfig } from "vite";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const REAL_ENTRY = path.resolve(__dirname, "build/src/index.js");
const VIRTUAL_ID = "\0gas-entry";
const GAS_CLASS_ENTRYPOINTS = {
  GasEntrypoints: ["runTests", "validateTests", "runBenchmark"],
};

/**
 * Detect named exports from the compiled ES module entry file
 * by scanning both:
 * - `export { Name, ... }` statements
 * - direct named exports (`export class Name`, `export function Name`, ...)
 */
function detectExportNames(entryPath) {
  const src = fs.readFileSync(entryPath, "utf8");
  const names = new Set();

  // export { Foo, Bar as Baz }
  const reNamedList = /export\s*\{([^}]+)\}/g;
  // export class Foo / export function Foo / export const Foo ...
  const reDirect = /export\s+(?:class|function|const|let|var)\s+([A-Za-z_$][\w$]*)/g;

  let m;
  while ((m = reNamedList.exec(src)) !== null) {
    for (const token of m[1].split(",")) {
      const name = token
        .trim()
        .split(/\s+as\s+/)
        .pop()
        .trim();
      if (name) names.add(name);
    }
  }

  while ((m = reDirect.exec(src)) !== null) {
    const name = m[1]?.trim();
    if (name) names.add(name);
  }

  return [...names];
}

function resolveGasBindings(exportNames) {
  const bindings = [];

  for (const exportName of exportNames) {
    const methods = GAS_CLASS_ENTRYPOINTS[exportName];
    if (methods) {
      for (const methodName of methods) {
        bindings.push({ globalName: methodName, exportName, methodName });
      }
      continue;
    }

    bindings.push({ globalName: exportName, exportName, methodName: null });
  }

  return bindings;
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
  let gasBindings = [];

  return {
    name: "gas-plugin",

    resolveId(source) {
      if (source === "gas-entry") return VIRTUAL_ID;
      return null;
    },

    load(id) {
      if (id === VIRTUAL_ID) {
        exportNames = detectExportNames(REAL_ENTRY);
        gasBindings = resolveGasBindings(exportNames);
        const imports = [...new Set(gasBindings.map((binding) => binding.exportName))].join(", ");
        const entryUrl = REAL_ENTRY.replace(/\\/g, "/");
        const assignments = gasBindings
          .map((binding) =>
            binding.methodName
              ? `__g.${binding.globalName} = function(){ return ${binding.exportName}.${binding.methodName}(); };`
              : `__g.${binding.globalName} = ${binding.exportName};`,
          )
          .join("\n");
        return [
          `import { ${imports} } from "${entryUrl}";`,
          `var __g = typeof globalThis !== "undefined" ? globalThis : this;`,
          assignments,
        ].join("\n");
      }
    },

    generateBundle(_options, bundle) {
      for (const chunk of Object.values(bundle)) {
        if (chunk.type === "chunk" && gasBindings.length > 0) {
          const stubs = gasBindings.map((binding) => `function ${binding.globalName}() {}`).join("\n");
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
