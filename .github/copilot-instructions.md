# Copilot instructions for SheetORM

Follow these repository structure rules for every change:

- Export exactly one public class, interface, type, or enum per file.
- Do not create barrel files or multi-export utility modules.
- Do not export loose top-level functions; place public behavior on classes as static methods.
- Keep filenames aligned with the exported artifact name, for example `Record.ts`, `QueryOptions.ts`, or
  `SystemColumns.ts`.
- Prefer small, explicit imports from the exact file that owns the symbol instead of aggregated imports.
- When Google Apps Script entrypoints are needed, add static methods to `GasEntrypoints` in `src/index.ts` and
  keep the file's single public export as that class.
- Preserve existing runtime behavior, tests, and build output while enforcing the structure above.
