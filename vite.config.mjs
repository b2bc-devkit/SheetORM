/**
 * Vite configuration for the Google Apps Script (GAS) bundle.
 *
 * This config compiles the SheetORM library into a single IIFE script (Code.js)
 * that runs in the GAS V8 runtime.  A custom Rollup plugin (gasPlugin) handles
 * two GAS-specific requirements:
 *
 * 1. Top-level function stubs — GAS discovers runnable functions by scanning for
 *    top-level `function Name() {}` declarations at parse time.  The plugin
 *    prepends these stubs so every entry point appears in the Run menu.
 *
 * 2. globalThis assignment — at module-evaluation time the plugin replaces the
 *    stubs with real implementations by assigning exports to `globalThis`.
 */

import { defineConfig } from "vite";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

// Resolve __dirname equivalent for ESM modules.
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/** Rollup virtual module ID for the synthetic GAS entry point. */
const VIRTUAL_ID = "\0gas-entry";

/**
 * Locate the compiled entry file produced by `tsc -p tsconfig.gas.json`.
 * Checks two candidate paths because tsc output structure can vary.
 */
function resolveRealEntry() {
  const candidates = [
    path.resolve(__dirname, "build/index.js"),
    path.resolve(__dirname, "build/src/index.js"),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }

  // Fall back to the first candidate so the build fails with a clear error.
  return candidates[0];
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
  /**
   * Map of exported class names → array of static method names that should
   * be exposed as individual top-level GAS functions.
   *
   * For example, GasEntrypoints.demoCreate becomes a global `demoCreate()`.
   */
  const CLASS_ENTRYPOINTS = {
    GasEntrypoints: [
      "runTestsStageOne",
      "runTestsStageTwo",
      "runTestsStageThree",
      "validateTests",
      "runBenchmark",
      "removeAllSheets",
      "demoCreate",
      "demoRead",
      "demoUpdate",
      "demoDelete",
    ],
  };

  /**
   * Parse compiled JS entry file to discover all exported names.
   * Handles both `export { A, B }` and `export class/function/const X` forms.
   */
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

  /**
   * Convert export names into binding descriptors.  Class entry points are
   * expanded into per-method bindings; plain exports become direct bindings.
   */
  function resolveBindings(exportNames) {
    return exportNames.flatMap((name) => {
      const methods = CLASS_ENTRYPOINTS[name];
      return methods
        ? methods.map((m) => ({ globalName: m, exportName: name, methodName: m }))
        : [{ globalName: name, exportName: name, methodName: null }];
    });
  }

  /** Accumulated bindings resolved during the load phase. */
  let gasBindings = [];

  return {
    name: "gas-plugin",

    /** Intercept the virtual "gas-entry" module ID for Rollup resolution. */
    resolveId(source) {
      if (source === "gas-entry") return VIRTUAL_ID;
      return null;
    },

    /**
     * Generate the virtual entry module that:
     * - Imports all detected exports from the real compiled entry.
     * - Assigns each binding to globalThis so GAS can invoke it at runtime.
     */
    load(id) {
      if (id === VIRTUAL_ID) {
        const realEntry = resolveRealEntry();
        const exportNames = detectExportNames(realEntry);
        gasBindings = resolveBindings(exportNames);
        // Build the import statement pulling all needed exports.
        const imports = [...new Set(gasBindings.map((binding) => binding.exportName))].join(", ");
        // Normalise Windows backslashes to forward slashes for the import path.
        const entryUrl = realEntry.replace(/\\/g, "/");
        // Generate globalThis assignments: plain exports are assigned directly,
        // class methods are wrapped in a function that delegates to the class.
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

    /**
     * Post-process the final bundle:
     * - Prepend top-level `function Name() {}` stubs for GAS parse-time discovery.
     * - Wrap the Rollup output in an IIFE so internal helpers (__defNormalProp,
     *   __publicField, etc.) stay out of the GAS global scope.
     */
    generateBundle(_options, bundle) {
      for (const chunk of Object.values(bundle)) {
        if (chunk.type === "chunk" && gasBindings.length > 0) {
          // Generate one stub per binding (e.g. `function demoCreate() {}`).
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

// ── Vite build configuration ───────────────────────────────────────────
// https://vitejs.dev/config/
export default defineConfig({
  plugins: [gasPlugin()],
  build: {
    // Output directory for the bundled GAS script.
    outDir: "dist",
    // Remove previous build artifacts before each build.
    emptyOutDir: true,
    // GAS V8 doesn't support class fields (ES2022); es2021 is the highest
    // compatible target.
    target: "es2021",
    rollupOptions: {
      // Use the virtual "gas-entry" module as the bundle entry point.
      input: "gas-entry",
      output: {
        // IIFE format: single self-executing file with no import/export statements.
        format: "iife",
        // Output filename matching the GAS project structure.
        entryFileNames: "Code.js",
        // No module exports — everything is assigned to globalThis.
        exports: "none",
        // Merge all dynamic imports into a single chunk (GAS has no module loader).
        inlineDynamicImports: true,
      },
    },
    // Use Terser for minification (more configurable than esbuild for GAS needs).
    minify: "terser",
    terserOptions: {
      compress: {
        // Keep Logger.log() calls for runtime diagnostics.
        drop_console: false,
        // Preserve top-level GAS stub functions even though they appear unused.
        unused: false,
      },
      // Do not mangle variable names — aids debugging in the GAS editor.
      mangle: false,
      format: {
        // Strip comments from the output to reduce bundle size.
        comments: false,
      },
    },
    // No source maps needed in the GAS runtime environment.
    sourcemap: false,
  },
});
