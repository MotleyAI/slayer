## Context

See proposal.md — Why. Today `tests/_doc_examples.py` both extracts queries from docs (fenced code
blocks only) and runs them on the demo; the README's only query sits in the alt text of
`docs/images/sql-vs-slayer.png` (`README.md:47`). `publish.yml` / `publish-test.yml` set up the Poetry
env (`./.github/actions/poetry-env`, `install-args: --no-interaction`, so the dev group and `mcp` are
present), run `poetry build`, then upload. No `slayer.*` package changes; `system.arc42.md` §3 #12
(Pydantic, never dataclasses) binds the new scripts.

## Goals / Non-Goals

**Goals:**
- One extractor feeds both the per-PR doc-example check and the release gate.
- The gate tests only the freshly installed wheel, never the checkout.

**Non-Goals:**
- A daily scheduled probe, a macOS leg, and opening a Linear issue on failure (covered by the docs check
  and `latest-deps.yml`).
- A PR-time job that builds and gates the wheel.
- The `alt` attribute of HTML `<img>` tags (the README's only one, line 2, carries no query).
- A Python-version matrix for the fresh install.

## Decisions

1. **The query is read from README alt text with the shared doc-example extractor, which therefore also
   checks image alt text on every PR.** Alternative: a gate-only alt-text parser — two extractors to
   keep in agreement by hand, and the headline query stays unchecked on PRs. (user)
2. **The gate exercises both the installed CLI and `slayer mcp --demo` over stdio.** Alternative: CLI
   only — the README quickstart command (`slayer mcp --demo`) would never run from the wheel. (user)
3. **Both `publish.yml` and `publish-test.yml` run the gate through one composite action,
   `.github/actions/wheel-gate`.** Alternative: `publish.yml` only. (user)
4. **No PR-time wheel-gate job.** The commands are already unit-tested against the checkout; the gate
   fails closed in `publish.yml`, so a broken gate only delays a release, never ships a bad wheel.
   Alternative: a path-gated `ci.yml` job (Codex finding 1). (user)
5. **Fresh install via `uv tool install`, the README's documented install path, into an isolated
   `UV_TOOL_DIR` / `UV_TOOL_BIN_DIR` under `$RUNNER_TEMP`, with the job's Python (3.12), no lockfile.**
   (default)
6. **The wheel is resolved to exactly one absolute `dist/*.whl` path and installed as the PEP 508 direct
   reference `motley-slayer[all] @ file://<abs path>`**, so neither shell globbing nor extra-suffix
   syntax can mangle it. (codex)
7. **The gate controller runs as `poetry run python -m scripts.release_gate --slayer <bin> --wheel
   <path>` from the repo root**, so `from scripts import doc_examples` resolves the way the existing
   `from scripts import …` tests do; only child processes get a temporary cwd. Tests invoke it the same
   way. (codex)
8. **The controller runs under the Poetry env's Python** because the MCP leg drives stdio with the `mcp`
   client; the `slayer` under test is only the fresh install's binary. (default)
9. **Child processes get a clean environment**: a minimal `PATH` without the Poetry venv, no
   `VIRTUAL_ENV` / `PYTHONPATH`, a temporary `HOME` and cwd, per-leg temporary storage, and
   `SLAYER_USAGE_REPORTING=off`, `DO_NOT_TRACK=1` so CI sends no usage events. (default)
10. **Provenance is checked first**: the expected version is read from the wheel's own metadata (not the
    checkout's `pyproject.toml`) and `slayer --version` must print exactly `slayer <version>`; the
    interpreter from the binary's shebang, run in the clean env, must import `slayer` from outside the
    checkout and have no `sys.path` entry inside it. The checkout root is a `--checkout` option
    defaulting to the repo root, so tests can run the passing path against the editable dev install.
    (codex)
11. **Every checked README query runs, and zero queries fails the gate**, so deleting the image cannot
    silently disable it. Rejected (codex finding 8) narrowing to the single headline query: the
    per-PR doc check already requires every unmarked README query to run on the demo, so a new README
    query cannot newly block a release. (default)
12. **MCP responses are validated as payloads**: each tool call must succeed with exactly one text
    content item that parses as JSON — a non-empty row list for `query`, an object whose `joins_to`
    includes `customers` and `stores` for `inspect`. Server start-up and every call have timeouts. (codex)
13. **Extraction moves to `scripts/doc_examples.py`, which depends on Pydantic but never imports
    `slayer`**; `tests/_doc_examples.py` keeps the demo checks and imports extraction from it.
    Rejected (codex finding 6) switching to dataclasses, which the repo forbids. (codex)
14. **Image alt text is matched with balanced brackets after `![`**, covering inline and reference-style
    images, multi-line alt text and escaped brackets; an ordinary `[link](…)` is not an image and is not
    scanned. Alt text obeys the code-block rules: illustrative marker, `source_model` guard. (codex)
15. **Failures print one `release gate: <check>: <reason>` line each and exit non-zero.** (default)
16. **`CONTRIBUTING.md` § Publishing gains one sentence on the gate; nothing under `docs/`**, since the
    release process is not user-facing. (default)

## Risks / Trade-offs

- [The `[all]` install pulls a large dependency set and can be slow or hit a transient index error] →
  generous step timeout; a failure blocks the release, which is re-run after the fix or retry.
- [A gate bug first surfaces during a real release] → it fails closed (no upload); the TestPyPI dry run
  exercises the same action whenever dispatched.
- [MCP cold start on a fresh install takes about a minute] → the start-up timeout allows for it.
- [Alt-text scanning could pick up a prose `source_model` mention in an image] → the same marker and
  `not-a-query` escape hatches as code blocks apply.
