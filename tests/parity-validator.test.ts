import fs from "node:fs";
import path from "node:path";

import { PARITY_CASE_IDS, PARITY_SUITES, toParityCaseId } from "../src/testing/parityCatalog";
import { RUNTIME_PARITY_CASE_IDS, validateSheetOrmRuntimeParity } from "../src/testing/runtimeParity";

function extractJestCaseIdsFromFile(filePath: string, fileName: string): string[] {
  const content = fs.readFileSync(filePath, "utf8");
  const re = /\bit\(\s*['"`]([^'"`]+)['"`]/g;
  const ids: string[] = [];

  let match: RegExpExecArray | null;
  while ((match = re.exec(content)) !== null) {
    ids.push(toParityCaseId(fileName, match[1]));
  }

  return ids;
}

describe("Jest/runtime parity validator", () => {
  it("parity catalog matches actual jest test cases", () => {
    const testsDir = path.resolve(__dirname);
    const suiteFiles = PARITY_SUITES.map((suite) => suite.file).sort();

    const discoveredIds = suiteFiles.flatMap((fileName) =>
      extractJestCaseIdsFromFile(path.join(testsDir, fileName), fileName),
    );

    expect(new Set(PARITY_CASE_IDS)).toEqual(new Set(discoveredIds));
    expect(PARITY_CASE_IDS.length).toBe(discoveredIds.length);
  });

  it("runtime parity handlers match parity catalog 1:1", () => {
    expect(new Set(RUNTIME_PARITY_CASE_IDS)).toEqual(new Set(PARITY_CASE_IDS));
    expect(RUNTIME_PARITY_CASE_IDS.length).toBe(PARITY_CASE_IDS.length);
  });

  it("runtime parity validation function reports no drift", () => {
    expect(() => validateSheetOrmRuntimeParity()).not.toThrow();
  });
});
