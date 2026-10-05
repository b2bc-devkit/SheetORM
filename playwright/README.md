# GAS Testing via Playwright MCP

Runbook for executing and measuring the SheetORM test suite inside the live Google Apps Script editor using the `mcp-playwright` browser tools.

## Targets

| Resource | URL |
|---|---|
| Apps Script project | `https://script.google.com/home/projects/1PLKuBbnYETjDlxxeE0DIifAZztex_hU2JzMH51EzvRleQJMUTOI_Xfae/edit` |
| Test spreadsheet | `https://docs.google.com/spreadsheets/d/1t5YjRRM1dyIpurFiVRfTaH2VMB1xIPb1CbDjK-J6Pz4/edit` |

The suite writes into whichever spreadsheet is bound by the test classes (the one above). `removeAllSheets()` is destructive — it wipes every sheet in that spreadsheet.

## Deploy first

The editor runs `Code.js` — always rebuild and push before testing:

```bash
# tsc is NOT on PATH for execSync — prepend node_modules/.bin
PATH="$PWD/node_modules/.bin:$PATH" node scripts/build.mjs
npx clasp push --force     # "Script is already up to date" = bundle identical, nothing to do
```

## Driving the editor

1. `browser_navigate` to the project URL (log output is in Polish: "Uruchom", "Ukończono wykonywanie").
2. **The function picker is a custom ARIA listbox, NOT a `<select>`** — `browser_select_option` fails with "Element is not a <select> element". Instead:
   - `browser_click` on `listbox "Wybierz funkcję do uruchomienia"`
   - find the fresh `option` ref in the returned snapshot (`grep 'option "runTestsStage' <snapshot-file>`)
   - `browser_click` that option
3. `browser_click` `button "Uruchom wybraną funkcję"` to run.
4. Wait, then `browser_snapshot` and grep the output file for `totalDurationMs` / `Done —` / `FAIL`. The final line is a JSON blob: `{"status":"ok","total":N,"passed":N,"failed":0,"timing":{"totalDurationMs":...}}`. `"Ukończono wykonywanie"` = execution finished.
5. Snapshot refs (`f123e…`) are **regenerated on every snapshot** — never reuse refs across snapshots; always grep the freshest `.playwright-mcp/page-*.yml`.

The execution log panel auto-opens on run. If the log looks empty right after a run, re-snapshot — fast functions like `validateTests` may finish before the panel renders.

## Function catalogue

| Function | Contents | Expected |
|---|---|---|
| `runTestsStageOne` | cache, index-store, query, query-engine | **161/161**, ~15–21 s |
| `runTestsStageTwo` | serialization, uuid | **52/52**, ~0.7–3.8 s |
| `runTestsStageThree` | record (CRUD + hooks, heaviest) | **81/81**, ~114–168 s |
| `runTestsStageFour` | sheet-repository | **35/35**, ~12–41 s |
| `validateTests` | Jest↔GAS handler parity, no Sheets calls | clean log, ~1 s |
| `runBenchmark` | 1000-record Cars(@Indexed) vs Workers suites + 100-iteration search race | **30/30 ops**, ~75 s; JSON with `indexedSearchMs`, `fullScanMs`, `ratio` |
| `removeAllSheets` | deletes ALL sheets | destructive — test env only |
| `demoCreate/Read/Update/Delete` | CRUD demos | smoke |

## Baselines (pre-refactor) vs current

| Stage | Baseline | Latest measured | Δ |
|---|---|---|---|
| Stage 1 | 132 s | ~15.6 s | ~8.5× |
| Stage 2 | ~9.3 s | ~1.2 s | ~7.8× |
| Stage 3 | **timeout >360 s** (died at 78/81) | ~114.2 s | >3.2× + completes |
| Stage 4 | 169.8 s | ~13.0 s | ~13× |
| Benchmark ratio | 1.6× indexed/full-scan | 7.8–10.2× | index planner live |

Raw baseline logs live in `playwright/baselines/` (per-test timings + the timed-out Stage 3 run).

## Reading per-test timings

Log lines look like `PASS [n/N] test name (X ms)`. Extract with:

```bash
grep -oE 'PASS \[[0-9]+/[0-9]+\] .+ \([0-9]+ ms\)' <snapshot> | sed -E 's/PASS \[([0-9]+)\/[0-9]+\] (.*) \(([0-9]+) ms\)/\1|\3|\2/'
```

## GAS timing caveats

- ±20–40% run-to-run variance is normal (quota windows, network). Compare trends, not single runs.
- Quota stalls show as multi-second jumps between consecutive log timestamps; the engine retries internally — a PASS with high ms is still a pass.
- `runTestsStageThree` must finish under the 6-minute GAS execution cap; it now lands ~114–168 s.
