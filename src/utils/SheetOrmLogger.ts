// SheetORM — Global verbose logger with on/off switch
// Set SheetOrmLogger.verbose = true from GAS code before calling runBenchmark() or any other
// entry point to see detailed API-call traces in the Google Apps Script execution log.

/**
 * Lightweight logger for SheetORM internals.
 *
 * Usage from a GAS script:
 *   SheetORM.SheetOrmLogger.verbose = true;
 *   GasEntrypoints.runBenchmark();
 */
export class SheetOrmLogger {
  static verbose: boolean = false;

  static log(msg: string): void {
    if (!SheetOrmLogger.verbose) return;
    // Use GAS Logger when available, fall back to console.log in test / Node environments
    if (typeof Logger !== "undefined" && typeof Logger.log === "function") {
      Logger.log(msg);
    } else {
      console.log(msg);
    }
  }
}
