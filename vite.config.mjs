import { defineConfig } from "vite";
import gasClassEntrypoints from "@b2bc-devkit/gas-class-entrypoints";

export default defineConfig({
  plugins: [
    gasClassEntrypoints({
      classEntrypoints: {
        GasEntrypoints: [
          "runTestsStageOne",
          "runTestsStageTwo",
          "runTestsStageThree",
          "runTestsStageFour",
          "validateTests",
          "runBenchmark",
          "removeAllSheets",
          "demoCreate",
          "demoRead",
          "demoUpdate",
          "demoDelete",
        ],
      },
    }),
  ],
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
