import { defineConfig } from "vite";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const REAL_ENTRY = path.resolve(__dirname, "build/src/index.js");
const VIRTUAL_ID = "\0gas-entry";

/**
 * Vite/Rollup plugin that replicates gas-webpack-plugin behaviour for GAS:
 *
 * 1. Creates a virtual entry that imports all exports from the real entry
 *    and assigns them to `globalThis` (GAS global scope) at runtime.
 * 2. Prepends top-level `function Name() {}` stubs so that GAS recognises
 *    all exported names at parse time (shown in the script editor Run menu).
 */
function gasPlugin() {
  const CLASS_ENTRYPOINTS = {
    GasEntrypoints: ["runTests", "validateTests", "runBenchmark"],
  };

  function detectExportNames(entryPath) {
    const src = fs.readFileSync(entryPath, "utf8");
    const names = new Set();

    for (const [, list] of src.matchAll(/export\s*\{([^}]+)\}/g)) {
      for (const token of list.split(",")) {
        const name = token
          .trim()
          .split(/\s+as\s+/)
          .pop()
          .trim();
        if (name) names.add(name);
      }
    }
    for (const [, name] of src.matchAll(/export\s+(?:class|function|const|let|var)\s+([A-Za-z_$][\w$]*)/g)) {
      if (name) names.add(name.trim());
    }

    return [...names];
  }

  function resolveBindings(exportNames) {
    return exportNames.flatMap((name) => {
      const methods = CLASS_ENTRYPOINTS[name];
      return methods
        ? methods.map((m) => ({ globalName: m, exportName: name, methodName: m }))
        : [{ globalName: name, exportName: name, methodName: null }];
    });
  }

  let gasBindings = [];

  return {
    name: "gas-plugin",

    resolveId(source) {
      if (source === "gas-entry") return VIRTUAL_ID;
      return null;
    },

    load(id) {
      if (id === VIRTUAL_ID) {
        const exportNames = detectExportNames(REAL_ENTRY);
        gasBindings = resolveBindings(exportNames);
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
    // TypeScript emits ESNext; no syntax down-levelling needed for the GAS V8 runtime
    target: "esnext",
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
