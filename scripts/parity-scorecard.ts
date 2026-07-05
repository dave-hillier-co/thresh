/**
 * Parity scorecard: accounts for every Orleans test declared in the parity
 * suite (packages/parity). Static mode (default) parses `orleansTest*`
 * declarations; `--run` also executes the parity vitest project and joins in
 * pass/fail results. `--json <path>` writes the raw data for CI artifacts —
 * output is regenerable and never committed.
 */
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";

const ROOT = join(import.meta.dirname, "..");
const PARITY_SRC = join(ROOT, "packages", "parity", "src");

type Status = "ported" | "gap" | "excluded";

interface Declaration {
  id: string;
  status: Status;
  suite: string; // folder under packages/parity/src, mirroring an Orleans test project
  file: string;
  gapTag?: string;
  reason?: string;
  result?: "pass" | "fail" | "skip";
}

function* walk(dir: string): Generator<string> {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) yield* walk(path);
    else if (entry.name.endsWith(".test.ts")) yield path;
  }
}

const DECLARATION_RE =
  /orleansTest(?:\.(gap|excluded))?\s*(?:\(|\.each\([^)]*\)\()\s*(?:"([^"]+)"|'([^']+)')\s*(?:,\s*(?:"([^"]+)"|'([^']+)'))?/g;

function parseFile(path: string): Declaration[] {
  const source = readFileSync(path, "utf8");
  const file = relative(ROOT, path);
  const suite = relative(PARITY_SRC, path).split("/")[0] ?? "?";
  const declarations: Declaration[] = [];
  for (const match of source.matchAll(DECLARATION_RE)) {
    const kind = match[1];
    const firstArg = match[2] ?? match[3] ?? "";
    const secondArg = match[4] ?? match[5];
    if (kind === "gap") {
      declarations.push({ id: secondArg ?? "?", status: "gap", suite, file, gapTag: firstArg });
    } else if (kind === "excluded") {
      declarations.push({
        id: secondArg ?? "?",
        status: "excluded",
        suite,
        file,
        reason: firstArg,
      });
    } else {
      declarations.push({ id: firstArg, status: "ported", suite, file });
    }
  }
  return declarations;
}

interface VitestResult {
  testResults?: Array<{
    assertionResults?: Array<{ fullName?: string; title?: string; status?: string }>;
  }>;
}

function joinRunResults(declarations: Declaration[]): void {
  let stdout: string;
  try {
    stdout = execFileSync(
      "pnpm",
      ["vitest", "run", "--project", "parity", "--reporter=json", "--silent"],
      { cwd: ROOT, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
    );
  } catch (error) {
    // vitest exits non-zero when tests fail; the JSON report is still on stdout.
    stdout = (error as { stdout?: string }).stdout ?? "";
  }
  const jsonStart = stdout.indexOf('{"numTotalTestSuites"');
  if (jsonStart < 0) throw new Error("parity-scorecard: no JSON report found in vitest output");
  const report = JSON.parse(stdout.slice(jsonStart)) as VitestResult;
  const byTitle = new Map<string, string>();
  for (const suite of report.testResults ?? []) {
    for (const test of suite.assertionResults ?? []) {
      const title = test.title ?? "";
      if (test.status !== undefined) byTitle.set(title, test.status);
    }
  }
  for (const declaration of declarations) {
    const title =
      declaration.status === "gap" ? `[${declaration.gapTag}] ${declaration.id}` : declaration.id;
    // A [Theory] ported via orleansTest.each registers one case per title
    // ("id :: case N"); the upstream test passes only if every case passes.
    const statuses = [byTitle.get(title)].filter((s): s is string => s !== undefined);
    for (const [caseTitle, status] of byTitle) {
      if (caseTitle.startsWith(`${title} :: `)) statuses.push(status);
    }
    if (statuses.length === 0) continue;
    if (statuses.every((s) => s === "passed")) declaration.result = "pass";
    else if (statuses.some((s) => s === "failed")) declaration.result = "fail";
    else declaration.result = "skip";
  }
}

function pad(value: string | number, width: number): string {
  return String(value).padStart(width);
}

function main(): void {
  const args = process.argv.slice(2);
  const withRun = args.includes("--run");
  const jsonIndex = args.indexOf("--json");
  const jsonPath = jsonIndex >= 0 ? args[jsonIndex + 1] : undefined;

  const declarations = [...walk(PARITY_SRC)].flatMap(parseFile);
  if (withRun) joinRunResults(declarations);

  const suites = [...new Set(declarations.map((d) => d.suite))].sort();
  console.log("Orleans parity scorecard");
  console.log(
    `  pin: ${(JSON.parse(readFileSync(join(ROOT, "packages/parity/package.json"), "utf8")) as { orleans?: { tag?: string } }).orleans?.tag ?? "?"}`,
  );
  console.log("");
  console.log("  suite               ported    gap  excluded" + (withRun ? "   pass   fail" : ""));
  for (const suite of suites) {
    const of = declarations.filter((d) => d.suite === suite);
    const counts = {
      ported: of.filter((d) => d.status === "ported").length,
      gap: of.filter((d) => d.status === "gap").length,
      excluded: of.filter((d) => d.status === "excluded").length,
      pass: of.filter((d) => d.result === "pass").length,
      fail: of.filter((d) => d.result === "fail").length,
    };
    let line = `  ${suite.padEnd(18)}${pad(counts.ported, 8)}${pad(counts.gap, 7)}${pad(counts.excluded, 10)}`;
    if (withRun) line += `${pad(counts.pass, 7)}${pad(counts.fail, 7)}`;
    console.log(line);
  }
  const total = {
    ported: declarations.filter((d) => d.status === "ported").length,
    gap: declarations.filter((d) => d.status === "gap").length,
    excluded: declarations.filter((d) => d.status === "excluded").length,
  };
  console.log(
    `  ${"total".padEnd(18)}${pad(total.ported, 8)}${pad(total.gap, 7)}${pad(total.excluded, 10)}` +
      (withRun
        ? `${pad(
            declarations.filter((d) => d.result === "pass").length,
            7,
          )}${pad(declarations.filter((d) => d.result === "fail").length, 7)}`
        : ""),
  );

  const gapTags = [...new Set(declarations.filter((d) => d.gapTag).map((d) => d.gapTag!))].sort();
  if (gapTags.length > 0) {
    console.log("");
    console.log("  gap backlog (skipped tests per missing feature):");
    for (const tag of gapTags) {
      const count = declarations.filter((d) => d.gapTag === tag).length;
      console.log(`    ${tag.padEnd(30)}${pad(count, 5)}`);
    }
  }

  const failed = declarations.filter((d) => d.result === "fail");
  if (failed.length > 0) {
    console.log("");
    console.log("  failing:");
    for (const declaration of failed) console.log(`    ${declaration.id} (${declaration.file})`);
    process.exitCode = 1;
  }

  if (jsonPath !== undefined) {
    writeFileSync(jsonPath, JSON.stringify({ declarations }, null, 2));
    console.log(`\n  json written to ${jsonPath}`);
  }
}

main();
