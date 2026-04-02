/**
 * Lightweight, globally togglable logger for SheetORM internals.
 *
 * When {@link verbose} is `true`, every log call is forwarded to either
 * the Google Apps Script `Logger` (when running inside GAS) or `console.log`
 * (when running in Node / Jest / browser).
 *
 * Usage from a GAS script:
 * ```ts
 * SheetORM.SheetOrmLogger.verbose = true;
 * GasEntrypoints.runBenchmark();
 * ```
 *
 * All SheetORM subsystems (GoogleSheetAdapter, SheetRepository, IndexStore,
 * MemoryCache) emit structured log lines through this class, e.g.:
 *   `[Cache] HIT "tbl_Cars"`, `[Adapter] getAllData "idx_Cars" → 42 rows`.
 */
export class SheetOrmLogger {
  /**
   * Master switch for verbose logging.
   * Set to `true` before any ORM operation to capture detailed traces.
   */
  static verbose: boolean = false;

  /**
   * Emit a log message if verbose mode is enabled.
   *
   * @param msg - Human-readable message (typically prefixed with a subsystem tag).
   */
  static log(msg: string): void {
    // Short-circuit when verbose logging is off (zero overhead in production)
    if (!SheetOrmLogger.verbose) return;

    // Use GAS Logger when available, fall back to console.log in test / Node environments
    if (typeof Logger !== "undefined" && typeof Logger.log === "function") {
      Logger.log(msg);
    } else {
      console.log(msg);
    }
  }
}
