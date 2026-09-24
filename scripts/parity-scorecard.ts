/**
 * Parity scorecard: accounts for every Orleans test declared in the parity
 * suite (packages/parity). Static mode (default) parses `orleansTest*`
 * declarations; `--run` also executes the parity vitest project and joins in
 * pass/fail results. `--json <path>` writes the raw data for CI artifacts —
 * output is regenerable and never committed.
 *
 * Declarations are read from the TypeScript AST rather than matched by pattern.
 * The suite states most of them through a named constant or a template literal —
 * `const NS = "..."; orleansTest.excluded(REASON, \`${NS}.GetOwnerTest\`)` — so a
 * parser that insists on an inline literal sees roughly a quarter of the suite
 * and reports the rest in no column at all: not ported, not gap, not excluded.
 * Where a regex scan would silently undercount, this one resolves those
 * expressions through the file's own string constants.
 *
 * An id that cannot be resolved statically is still counted (the declaration
 * exists whatever it is called) and marked `<unresolved: ...>`, so `reconcile`
 * below fails loudly rather than the declaration quietly vanishing.
 */
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import * as ts from "typescript";

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

/** One file's declarations, plus every `orleansTest*` call site the parser saw. */
interface ParsedFile {
  declarations: Declaration[];
  /** Call sites that are not one of the three accounted-for forms. */
  unaccounted: string[];
}

function* walk(dir: string): Generator<string> {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) yield* walk(path);
    else if (entry.name.endsWith(".test.ts")) yield path;
  }
}

/**
 * Which declaration form a call is, by its callee. `undefined` means the call is
 * not a declaration at all — including the inner half of the curried
 * `orleansTest.each(cases)(id, body)`, which `declarationOf` handles as one call
 * so a `[Theory]` is one upstream test rather than two.
 */
function declarationKind(callee: ts.Expression): "test" | "gap" | "excluded" | "each" | undefined {
  if (ts.isIdentifier(callee) && callee.text === "orleansTest") return "test";
  if (
    ts.isCallExpression(callee) &&
    ts.isPropertyAccessExpression(callee.expression) &&
    ts.isIdentifier(callee.expression.expression) &&
    callee.expression.expression.text === "orleansTest" &&
    callee.expression.name.text === "each"
  ) {
    return "each";
  }
  if (
    ts.isPropertyAccessExpression(callee) &&
    ts.isIdentifier(callee.expression) &&
    callee.expression.text === "orleansTest"
  ) {
    const member = callee.name.text;
    return member === "gap" || member === "excluded" ? member : undefined;
  }
  return undefined;
}

/**
 * True for a `orleansTest.<member>` property access this parser does not know:
 * a form that declares tests (`orleansTest.only`, say) would be counted by
 * nothing, so it is reported instead of ignored.
 */
function isUnknownDeclarationMember(callee: ts.Expression): boolean {
  return (
    ts.isPropertyAccessExpression(callee) &&
    ts.isIdentifier(callee.expression) &&
    callee.expression.text === "orleansTest" &&
    callee.name.text !== "gap" &&
    callee.name.text !== "excluded" &&
    callee.name.text !== "each"
  );
}

/** `const NAME = "..."` (however wrapped), for resolving ids written as constants. */
type StringConsts = Map<string, ts.Expression>;

function unwrap(expr: ts.Expression): ts.Expression {
  let current = expr;
  while (
    ts.isParenthesizedExpression(current) ||
    ts.isAsExpression(current) ||
    ts.isSatisfiesExpression(current) ||
    ts.isTypeAssertionExpression(current) ||
    ts.isNonNullExpression(current)
  ) {
    current = current.expression;
  }
  return current;
}

/**
 * True for an expression that *may* resolve to a string (see `resolveString`):
 * a literal, a template literal, a concatenation, or a name bound to one of
 * those. Anything else is left alone rather than followed.
 */
function isStringy(expr: ts.Expression): boolean {
  const current = unwrap(expr);
  return (
    ts.isStringLiteral(current) ||
    ts.isNoSubstitutionTemplateLiteral(current) ||
    ts.isTemplateExpression(current) ||
    isConcat(current) ||
    ts.isIdentifier(current)
  );
}

/** A `+` chain, as the suite's long exclusion reasons are written. */
function isConcat(expr: ts.Expression): expr is ts.BinaryExpression {
  return ts.isBinaryExpression(expr) && expr.operatorToken.kind === ts.SyntaxKind.PlusToken;
}

function collectStringConsts(source: ts.SourceFile): StringConsts {
  const consts: StringConsts = new Map();
  const visit = (node: ts.Node): void => {
    if (
      ts.isVariableDeclaration(node) &&
      ts.isIdentifier(node.name) &&
      node.initializer !== undefined &&
      isStringy(node.initializer)
    ) {
      consts.set(node.name.text, node.initializer);
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  return consts;
}

/**
 * The string an expression evaluates to, when it can be known without running
 * the file: a literal, a template literal whose substitutions resolve, a `+`
 * concatenation of those, or a constant naming one of them. Anything else — a
 * value read in a loop, an import, a function call — is `undefined`, so the
 * caller reports the argument as unresolved rather than inventing one. `seen`
 * breaks a constant that (illegally) references itself.
 */
function resolveString(
  expr: ts.Expression,
  consts: StringConsts,
  seen: ReadonlySet<string> = new Set(),
): string | undefined {
  const current = unwrap(expr);
  if (ts.isStringLiteral(current) || ts.isNoSubstitutionTemplateLiteral(current)) {
    return current.text;
  }
  if (ts.isTemplateExpression(current)) {
    let out = current.head.text;
    for (const span of current.templateSpans) {
      const value = resolveString(span.expression, consts, seen);
      if (value === undefined) return undefined;
      out += value + span.literal.text;
    }
    return out;
  }
  if (isConcat(current)) {
    const left = resolveString(current.left, consts, seen);
    const right = resolveString(current.right, consts, seen);
    return left === undefined || right === undefined ? undefined : left + right;
  }
  if (ts.isIdentifier(current)) {
    const initializer = consts.get(current.text);
    if (initializer === undefined || seen.has(current.text)) return undefined;
    return resolveString(initializer, consts, new Set([...seen, current.text]));
  }
  return undefined;
}

/** The declaration a call site makes, or `undefined` for a call that makes none. */
function declarationOf(
  call: ts.CallExpression,
  consts: StringConsts,
  suite: string,
  file: string,
): Declaration | undefined {
  const kind = declarationKind(call.expression);
  if (kind === undefined) return undefined;
  // `orleansTest(id, body)` is the id; `gap(tag, id)` and `excluded(reason, id)`
  // name something else first, so the id is the second argument there.
  const idExpr = kind === "test" || kind === "each" ? call.arguments[0] : call.arguments[1];
  const labelExpr = kind === "test" || kind === "each" ? undefined : call.arguments[0];
  // A declaration this parser cannot read is still a declaration: keep it under a
  // marker so it lands in a column and the run-mode join reports it as unmatched.
  const id = resolveOrMark(idExpr, consts);
  const label = labelExpr === undefined ? undefined : resolveOrMark(labelExpr, consts);

  if (kind === "gap") return { id, status: "gap", suite, file, gapTag: label ?? "?" };
  if (kind === "excluded") {
    return {
      id,
      status: "excluded",
      suite,
      file,
      ...(label !== undefined ? { reason: label } : {}),
    };
  }
  return { id, status: "ported", suite, file };
}

/** A declaration kept under a marker because its id could not be resolved statically. */
const UNRESOLVED_PREFIX = "<unresolved: ";

function isUnresolvedId(id: string): boolean {
  return id.startsWith(UNRESOLVED_PREFIX);
}

/** Resolve an argument to its string, or mark it so its unreadability is visible. */
function resolveOrMark(expr: ts.Expression | undefined, consts: StringConsts): string {
  if (expr === undefined) return `${UNRESOLVED_PREFIX}missing argument>`;
  return resolveString(expr, consts) ?? `${UNRESOLVED_PREFIX}${expr.getText()}>`;
}

function parseFile(path: string): ParsedFile {
  const source = readFileSync(path, "utf8");
  const file = relative(ROOT, path);
  const suite = relative(PARITY_SRC, path).split("/")[0] ?? "?";
  const ast = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const consts = collectStringConsts(ast);
  const declarations: Declaration[] = [];
  const unaccounted: string[] = [];

  const visit = (node: ts.Node): void => {
    if (ts.isCallExpression(node)) {
      const declaration = declarationOf(node, consts, suite, file);
      if (declaration !== undefined) declarations.push(declaration);
      else if (isUnknownDeclarationMember(node.expression)) {
        const { line } = ast.getLineAndCharacterOfPosition(node.expression.getStart());
        unaccounted.push(`${node.expression.getText()} (${file}:${line + 1})`);
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(ast);
  return { declarations, unaccounted };
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

/**
 * Failures that must stop the scorecard exiting 0. The headline numbers are
 * load-bearing (EPICS.md, todo.md, the dated reviews), so a run that cannot
 * account for every declaration it found — or every ported test it claims — has
 * to say so rather than print a plausible undercount.
 */
function reconcile(
  declarations: Declaration[],
  unaccounted: readonly string[],
  withRun: boolean,
): boolean {
  let ok = true;
  if (unaccounted.length > 0) {
    ok = false;
    console.log("");
    console.log(
      `  RECONCILIATION FAILED: ${declarations.length} declarations accounted for, ` +
        `but ${declarations.length + unaccounted.length} call sites found`,
    );
    console.log("    not accounted for:");
    for (const site of unaccounted) console.log(`      ${site}`);
  }
  if (!withRun) return ok;

  const ported = declarations.filter((d) => d.status === "ported");
  const pass = ported.filter((d) => d.result === "pass");
  const fail = ported.filter((d) => d.result === "fail");
  const unresolved = ported.filter((d) => isUnresolvedId(d.id));
  const missing = ported.filter((d) => d.result === undefined && !isUnresolvedId(d.id));
  const skipped = ported.filter((d) => d.result === "skip");
  // A ported test the scorecard cannot name, or cannot find a vitest result for, is not evidence of
  // anything: it drops out of the pass column while the run still exits 0. Every way a ported
  // declaration can fail to reconcile is named here instead.
  if (pass.length + fail.length !== ported.length) {
    ok = false;
    console.log("");
    console.log(
      `  RECONCILIATION FAILED: ${ported.length} ported tests, ` +
        `${pass.length} passed + ${fail.length} failed`,
    );
    if (unresolved.length > 0) {
      console.log(
        `    id not statically resolvable (${unresolved.length}) — declared through a runtime value, so no vitest title can be matched to it:`,
      );
      for (const declaration of unresolved)
        console.log(`      ${declaration.id} (${declaration.file})`);
    }
    if (missing.length > 0) {
      console.log(`    no vitest result (${missing.length}):`);
      for (const declaration of missing)
        console.log(`      ${declaration.id} (${declaration.file})`);
    }
    if (skipped.length > 0) {
      console.log(`    skipped (${skipped.length}):`);
      for (const declaration of skipped)
        console.log(`      ${declaration.id} (${declaration.file})`);
    }
  }
  return ok;
}

function pad(value: string | number, width: number): string {
  return String(value).padStart(width);
}

function main(): void {
  const args = process.argv.slice(2);
  const withRun = args.includes("--run");
  const jsonIndex = args.indexOf("--json");
  const jsonPath = jsonIndex >= 0 ? args[jsonIndex + 1] : undefined;

  const parsed = [...walk(PARITY_SRC)].map(parseFile);
  const declarations = parsed.flatMap((p) => p.declarations);
  const unaccounted = parsed.flatMap((p) => p.unaccounted);
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
  }

  if (!reconcile(declarations, unaccounted, withRun) || failed.length > 0) {
    process.exitCode = 1;
  }

  if (jsonPath !== undefined) {
    writeFileSync(jsonPath, JSON.stringify({ declarations }, null, 2));
    console.log(`\n  json written to ${jsonPath}`);
  }
}

main();
