## 1. Tests (pr-tests stage — each must fail before implementation)

- [x] 1.1 `host_clock` parsing: naive datetime pins; date-only → midnight; unset / empty / whitespace → host clock; `Z` and `+02:00` rejected and unparseable rejected, each with a `SlayerError` naming `SLAYER_NOW` and the value — verify via `poetry run pytest` on the new test file (monkeypatch env only)
- [x] 1.2 Engine scenarios on in-process SQLite: pinned `last 3 months` rows; explicit `clock=` wins (also with an invalid `SLAYER_NOW`, and with a falsey callable clock); read at construction (change env after build; a second engine sees the new value); `whole_periods_only` default upper bound equals the explicit-clock result
- [x] 1.3 MCP: `create_mcp_server` under `SLAYER_NOW` → `server.call_tool("query", …)` with `'last 3 months'` returns the pinned-window rows; invalid value raises before the seeding and ingestion seams are reached (assert via monkeypatched `seed_help_memories` / `ingest_all_datasources_idempotent`)
- [x] 1.4 REST: `create_app` under `SLAYER_NOW` → `POST /query` returns the pinned-window rows and the embedded MCP server's engine carries the same pin; invalid value raises before seeding / ingestion
- [x] 1.5 CLI: invalid `SLAYER_NOW` with `slayer mcp --demo` and `slayer serve --demo` exits non-zero naming `SLAYER_NOW` on stderr, and the demo seam (`ensure_demo_datasource`) is never called

## 2. Implementation

- [x] 2.1 Add `host_clock()` to `slayer/core/time_points.py` per design Decision 1 — verify 1.1 passes
- [x] 2.2 `SlayerQueryEngine.__init__`: `clock: Optional[...] = None`, `clock if clock is not None else host_clock()` — verify 1.2 passes
- [x] 2.3 `create_mcp_server` / `create_app`: build the engine before seeding and ingestion — verify 1.3 and 1.4 pass
- [x] 2.4 `slayer.cli.main()`: validate via `host_clock()` before dispatch; `SlayerError` → stderr + exit 1 — verify 1.5 passes
- [x] 2.5 `query` tool docstring (`slayer/mcp/server.py`, "the host clock") gains "(pinned by `SLAYER_NOW` when set)" — verify `tests/test_time_points_mcp_docs.py` still passes
- [x] 2.6 `examples/comparisons/slayer/run_slayer.py`: set `SLAYER_NOW` from `NOW` in `main()`, drop both `clock=lambda: NOW` — verify `poetry run python examples/comparisons/slayer/run_slayer.py` shows no new FAIL vs. the pre-change run (record both summaries)

## 3. Docs

- [x] 3.1 One sentence each: `docs/concepts/time.md` ("now" paragraph), `docs/interfaces/cli.md` (environment paragraph), `docs/reference/mcp.md` — verify `grep -rn SLAYER_NOW docs/` hits all three

## 4. Gates

- [x] 4.1 `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors), `poetry run la-arch-check` — all green
