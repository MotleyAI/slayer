// Malloy runner for the semantic-layer probe suite: run every probe in ../probes.yaml, check against its expectation.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import YAML from 'yaml';
import {SingleConnectionRuntime} from '@malloydata/malloy';
import {DuckDBConnection} from '@malloydata/db-duckdb';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, '..');
const PASS = 'PASS', KNOWN = 'KNOWN-BUG', FIXED = 'FIXED', FAIL = 'FAIL';
const STATUSES = [PASS, KNOWN, FIXED, FAIL];
const DATE_RE = /^\d{4}-\d{2}-\d{2}([ T]00:00:00(\.0+)?Z?)?$/;
const NUM_RE = /^-?\d+(\.\d+)?([eE][-+]?\d+)?$/;

function parseArgs(argv) {
  const args = {row: null, id: null, verbose: false};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--row') args.row = argv[++i];
    else if (argv[i] === '--id') args.id = argv[++i];
    else if (argv[i] === '--verbose') args.verbose = true;
    else if (argv[i] === '--help' || argv[i] === '-h') {
      console.log('usage: node run_malloy.mjs [--row Q4] [--id substring] [--verbose]');
      process.exit(0);
    } else throw new Error(`unknown argument ${argv[i]}`);
  }
  return args;
}

function sqlStatements(text) {
  return text.replace(/--[^\n]*/g, '').split(';').map(s => s.trim()).filter(Boolean);
}

// Normalise a cell so Malloy, SLayer and DuckDB spellings of one value compare equal.
function canon(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'bigint') return Number(v);
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (typeof v === 'string') {
    if (DATE_RE.test(v)) return v.slice(0, 10);
    if (NUM_RE.test(v)) return Number(v);
  }
  return v;
}

function cellEq(a, b, tol) {
  a = canon(a);
  b = canon(b);
  if (a === null || b === null) return a === null && b === null;
  if (typeof a === 'number' && typeof b === 'number') {
    return Math.abs(a - b) <= Math.max(tol, tol * Math.max(Math.abs(a), Math.abs(b)));
  }
  return a === b;
}

function sortKey(row, nkeys) {
  const part = nkeys ? row.slice(0, nkeys) : row;
  return JSON.stringify(part.map(c => {
    const x = canon(c);
    return [x === null, String(x)];
  }));
}

function rowsMatch(got, exp, cmp, nkeys) {
  if (got.length !== exp.length) return [false, `${got.length} rows, expected ${exp.length}`];
  if (!cmp.ordered) {
    const by = r => sortKey(r, nkeys);
    got = [...got].sort((x, y) => (by(x) < by(y) ? -1 : by(x) > by(y) ? 1 : 0));
    exp = [...exp].sort((x, y) => (by(x) < by(y) ? -1 : by(x) > by(y) ? 1 : 0));
  }
  for (let i = 0; i < got.length; i++) {
    const g = got[i], e = exp[i];
    if (g.length !== e.length || !g.every((c, j) => cellEq(c, e[j], cmp.tolerance))) {
      return [false, `row ${JSON.stringify(g.map(canon))} != expected ${JSON.stringify(e.map(canon))}`];
    }
  }
  return [true, `${exp.length} rows match`];
}

function hasRow(got, want, tol) {
  return got.some(g => g.length === want.length && g.every((c, j) => cellEq(c, want[j], tol)));
}

// One output row per child of the nested field, with the child's fields prefixed '<nest>.'.
function flatten(rows, nest) {
  const out = [];
  for (const r of rows) {
    const parent = Object.fromEntries(Object.entries(r).filter(([k]) => k !== nest));
    for (const child of r[nest] ?? []) {
      out.push({...parent, ...Object.fromEntries(Object.entries(child).map(([k, v]) => [`${nest}.${k}`, v]))});
    }
  }
  return out;
}

async function runProbe(runtime, model, block) {
  const q = runtime.loadQuery(`${model}\n${block.query}`);
  const problems = await q.validate();
  const errors = problems.filter(p => p.severity === 'error');
  if (errors.length) return {compileErrors: errors};
  const prepared = await q.getPreparedQuery();
  const warnings = (prepared.problems ?? []).filter(p => p.severity === 'warn');
  const sql = await q.getSQL();
  try {
    const result = await q.run();
    let rows = result.data.toObject();
    if (block.flatten) rows = flatten(rows, block.flatten);
    return {rows, warnings, sql};
  } catch (e) {
    return {dbError: e.message, warnings, sql};
  }
}

function errorText(out) {
  if (out.compileErrors) return out.compileErrors.map(p => `[${p.code}] ${p.message}`).join('; ');
  if (out.dbError) return out.dbError;
  return '';
}

function firstLine(s) {
  return s.trim().split('\n')[0].slice(0, 160);
}

function evaluate(p, out, truth) {
  const block = p.malloy;
  const expect = block.expect ?? 'match';
  const ex = typeof expect === 'string' ? {} : expect;
  const cmp = {tolerance: 1e-6, ordered: false, columns_exact: false, keys: [], values: [], ...(p.compare ?? {})};
  const keys = block.keys ?? cmp.keys;
  const values = block.values ?? cmp.values;
  const err = errorText(out);

  if (ex.compile_error) {
    if (!out.compileErrors) return [FAIL, `expected compile error ${ex.compile_error}, got ${err ? 'db error ' + firstLine(err) : 'a result'}`];
    const hit = out.compileErrors.some(e => e.code === ex.compile_error || e.message.includes(ex.compile_error));
    return hit ? [PASS, `compile error as expected: ${firstLine(err)}`] : [FAIL, `wrong compile error: ${firstLine(err)}`];
  }
  if (ex.db_error) {
    if (!out.dbError) return [FAIL, `expected db error ${ex.db_error}, got ${err ? firstLine(err) : 'a result'}`];
    return out.dbError.includes(ex.db_error) ? [PASS, `db error as expected: ${firstLine(err)}`] : [FAIL, `wrong db error: ${firstLine(err)}`];
  }
  if (ex.known_bug && ex.buggy_error) {
    if (err.includes(ex.buggy_error)) return [KNOWN, `${ex.known_bug} (${firstLine(err)})`];
    if (out.dbError) return [FAIL, `different db error: ${firstLine(err)}`];
    return [FIXED, `no longer fails with '${ex.buggy_error}': ${err ? firstLine(err) : 'returns rows'}; update the expectation`];
  }
  if (err) return [FAIL, `unexpected error: ${firstLine(err)}`];

  const cols = [...keys, ...values];
  const missing = cols.filter(c => out.rows.length && !(c in out.rows[0]));
  if (missing.length) return [FAIL, `columns ${JSON.stringify(missing)} not in ${JSON.stringify(Object.keys(out.rows[0]))}`];
  if (cmp.columns_exact && out.rows.length && JSON.stringify(Object.keys(out.rows[0])) !== JSON.stringify(cols)) {
    return [FAIL, `columns ${JSON.stringify(Object.keys(out.rows[0]))} != ${JSON.stringify(cols)}`];
  }
  const got = out.rows.map(r => cols.map(c => r[c]));
  const [ok, why] = p.truth_sql ? rowsMatch(got, truth.truth, cmp, keys.length) : [true, 'no truth_sql'];
  const codes = out.warnings.map(w => w.code);

  if (ex.known_bug) {
    if (ok) return [FIXED, `now matches truth (${why}); update the expectation`];
    let sig = (ex.buggy_rows ?? []).every(b => hasRow(got, b, cmp.tolerance));
    if (ex.buggy_row_count !== undefined) sig = sig && got.length === ex.buggy_row_count;
    return sig ? [KNOWN, `${ex.known_bug} (${why})`] : [FAIL, `neither truth nor the known buggy value: ${why}`];
  }
  if (!ok) return [FAIL, `${why}; warnings=${JSON.stringify(codes)}`];
  if (p.contrast_sql && rowsMatch(got, truth.contrast, cmp, keys.length)[0]) {
    return [FAIL, 'result also equals contrast_sql; the probe shows nothing'];
  }
  if (ex.warning && !codes.includes(ex.warning)) return [FAIL, `${why}, but warning ${ex.warning} missing (got ${JSON.stringify(codes)})`];
  return [PASS, ex.warning ? `${why} + warning ${ex.warning}` : why];
}

function rowOrder(r) {
  return [r[0], Number(r.replace(/\D/g, '') || 0)];
}

function printSummary(results) {
  const rows = [...new Set(results.map(([p]) => p.row))].sort((a, b) => {
    const [x, y] = [rowOrder(a), rowOrder(b)];
    return x[0] === y[0] ? x[1] - y[1] : x[0] < y[0] ? -1 : 1;
  });
  const line = (label, counts, tail = '') => console.log(label.padEnd(6) + counts.map(c => String(c).padStart(11)).join('') + tail);
  console.log('\nSummary (Malloy)');
  line('row', STATUSES);
  for (const r of rows) line(r, STATUSES.map(s => results.filter(([p, st]) => p.row === r && st === s).length));
  line('total', STATUSES.map(s => results.filter(([, st]) => st === s).length), `   (${results.length} probes)`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const doc = YAML.parse(fs.readFileSync(path.join(ROOT, 'probes.yaml'), 'utf8'));
  const probes = doc.probes.filter(p => p.malloy && (!args.row || p.row === args.row) && (!args.id || p.id.includes(args.id)));
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'malloy-probes-'));
  const connection = new DuckDBConnection('duckdb', path.join(tmp, 'probe.duckdb'));
  const results = [];
  try {
    for (const stmt of sqlStatements(fs.readFileSync(path.join(ROOT, 'dataset.sql'), 'utf8'))) {
      await connection.runSQL(stmt);
    }
    const runtime = new SingleConnectionRuntime({connection});
    const model = fs.readFileSync(path.join(HERE, 'model.malloy'), 'utf8');
    for (const p of probes) {
      const truth = {};
      for (const kind of ['truth', 'contrast']) {
        const sql = p[`${kind}_sql`];
        if (sql) truth[kind] = (await connection.runSQL(sql)).rows.map(r => Object.values(r));
      }
      const out = await runProbe(runtime, model, p.malloy);
      const [status, detail] = evaluate(p, out, truth);
      results.push([p, status]);
      console.log(`${status.padEnd(9)} ${p.id.padEnd(22)} ${p.row.padEnd(4)} ${p.title.slice(0, 60).padEnd(60)}  ${detail}`);
      if (args.verbose) {
        if (out.sql) console.log(`    sql: ${out.sql.replace(/\n/g, '\n         ')}`);
        if (out.rows) console.log(`    rows: ${JSON.stringify(out.rows)}`);
        if (out.warnings?.length) console.log(`    warnings: ${JSON.stringify(out.warnings.map(w => w.code))}`);
        if (errorText(out)) console.log(`    error: ${errorText(out)}`);
        if (truth.truth) console.log(`    truth: ${JSON.stringify(truth.truth)}`);
      }
    }
  } finally {
    await connection.close();
    fs.rmSync(tmp, {recursive: true, force: true});
  }
  printSummary(results);
  return results.some(([, s]) => s === FAIL) ? 1 : 0;
}

process.exitCode = await main();
