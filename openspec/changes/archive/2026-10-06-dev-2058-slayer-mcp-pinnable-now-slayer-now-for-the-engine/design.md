## Context

`SlayerQueryEngine(clock=datetime.now)` is the source of "now" for every execution: both bundle-building
paths (`_prepare_pipeline`, the refresh path) pass `now=self._clock()` explicitly. About 15 surfaces
build engines without a clock (`slayer serve` builds two: REST and the embedded MCP server). Two
low-level fallbacks also default to `datetime.now()` — `build_resolved_source_bundle(now=None)` and
`ResolvedSourceBundle.now`'s default factory — but no engine execution reaches them.

## Goals / Non-Goals

**Goals:** one canonical default clock for engines; startup validation with no side effects before it.

**Non-Goals:** a `--now` CLI flag; new parameters on `create_mcp_server` / `create_app`; changing
the low-level bundle fallbacks; SQL-side `now()` / `current_date()` (database clock); cache policy
(relative tokens already bake into SQL, so the cache key changes with the pinned reading as before).

## Decisions

1. **`host_clock()` in `slayer/core/time_points.py`** — the one interpreter of `SLAYER_NOW`. Returns a
   `Callable[[], datetime]`: `lambda: <pinned>` or `datetime.now`. Parsing: strip; blank → unpinned;
   `datetime.fromisoformat` (date-only → midnight); `tzinfo is not None` or a parse failure →
   `SlayerError` naming `SLAYER_NOW` and the value. `core` already reads env (`core/timing.py`).
2. **Engine default:** `clock: Optional[Callable[[], datetime]] = None`;
   `self._clock = clock if clock is not None else host_clock()` — `is not None`, never `or` (a falsey
   callable must still win). Read once at construction, so a running server's pin is fixed.
   *Alternative rejected:* reading `SLAYER_NOW` in `create_mcp_server` / `create_app` only — every
   engine-building surface would have to repeat it, and new surfaces would silently miss it.
3. **Low-level fallbacks stay plain `datetime.now()`** (Codex review): IR/builder defaults stay free of
   ambient configuration (engine §3.3 — the binder is a function of its bundle); every execution
   still reads the pin because the engine passes its reading down.
4. **No side effects before validation:**
   - `slayer.cli.main()` calls `host_clock()` once before dispatching any command (validation only),
     so `--demo` on `serve` / `mcp` / `flight-serve` / `pg-serve` and every other command fail first.
     A `SlayerError` there exits non-zero with the message on stderr (stdout stays protocol-clean
     for `slayer mcp`).
   - `create_mcp_server` and `create_app` construct their engine first, before help-memory seeding and
     `ingest_on_startup`, for library callers that bypass the CLI.
5. **Probe runner:** `run_slayer.main()` sets `SLAYER_NOW = NOW.isoformat()` beside the existing
   `SLAYER_EMBEDDING_MODEL` override; the explicit `clock=lambda: NOW` arguments go.

Architecture: no new import arrows (`engine → core`, `surfaces → core` exist); system §3.7 and engine
§3.3 hold — the variable is read at engine construction, never during resolution or binding.

## Risks / Trade-offs

- [A stray `SLAYER_NOW` in a user's shell pins every engine, including library use] → documented in
  `docs/concepts/time.md` and `docs/interfaces/cli.md`; an explicit `clock=` still overrides it.
- [Test-suite leakage: a test setting `SLAYER_NOW` must not leak into others] → tests use
  `monkeypatch.setenv` / `delenv` only.
- [A future production path builds a bundle without `now` and skips the pin] → out of scope; the
  clean fix (make `now` required, delete the fallbacks) is a separate refactor.
