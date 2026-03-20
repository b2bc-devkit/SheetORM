import fs from "node:fs";
import path from "node:path";

import { ParityCatalog } from "../src/testing/ParityCatalog";
import { RuntimeParity } from "../src/testing/RuntimeParity";

function extractJestCaseIdsFromFile(filePath: string, fileName: string): string[] {
  const content = fs.readFileSync(filePath, "utf8");
  const re = /\bit\(\s*['"`]([^'"`]+)['"`]/g;
  const ids: string[] = [];

  let match: RegExpExecArray | null;
  while ((match = re.exec(content)) !== null) {
    ids.push(ParityCatalog.toCaseId(fileName, match[1]));
  }

  return ids;
}

describe("Jest/runtime parity validator", () => {
  it("parity catalog matches actual jest test cases", () => {
    const testsDir = path.resolve(__dirname);
    const suiteFiles = ParityCatalog.SUITES.map((suite) => suite.file).sort();

    const discoveredIds = suiteFiles.flatMap((fileName) =>
      extractJestCaseIdsFromFile(path.join(testsDir, fileName), fileName),
    );

    expect(new Set(ParityCatalog.CASE_IDS)).toEqual(new Set(discoveredIds));
    expect(ParityCatalog.CASE_IDS.length).toBe(discoveredIds.length);
  });

  it("runtime parity handlers match parity catalog 1:1", () => {
    expect(new Set(RuntimeParity.CASE_IDS)).toEqual(new Set(ParityCatalog.CASE_IDS));
    expect(RuntimeParity.CASE_IDS.length).toBe(ParityCatalog.CASE_IDS.length);
  });

  it("runtime parity validation function reports no drift", () => {
    expect(() => RuntimeParity.validate()).not.toThrow();
  });
});
