## 1. Tests first (pr-tests stage; all must fail before implementation)

- [x] 1.1 `tests/conftest.py`: autouse session fixture sets `SLAYER_TELEMETRY=off`; a `telemetry_env` fixture re-enables it with a tmp config dir (`XDG_CONFIG_HOME` / platform override), an injected clock, and a local HTTP capture server via `SLAYER_TELEMETRY_ENDPOINT`. Verify: the existing suite still passes with the autouse fixture.
- [x] 1.2 `tests/test_telemetry_payload.py` (principle 17): schema-policy walk — every payload leaf is a `Literal`/enum, bool, bucket label, date, generated UUID or `int`; sentinel fuzz (hostile Unicode, 10 kB strings) through model / column / datasource / memory names, SQL, filters, custom aggregations, MCP args, CLI args, error messages, `clientInfo`, paths — no sentinel or derivative in the serialized report; unknown exception → `other`; unknown MCP client → `other`; dates only. Verify: fails today (no `slayer.telemetry`).
- [x] 1.3 `tests/test_telemetry_scope.py`: library use (engine, `create_app`, `create_mcp_server`, client) records nothing, writes no file, prints nothing, makes no request; CLI use records. Verify: fails today.
- [x] 1.4 `tests/test_telemetry_controls.py`: full precedence matrix (`DO_NOT_TRACK`, `SLAYER_TELEMETRY`, `CI`, editable via a fake `direct_url.json`, unwritable dir, persisted setting, default); `slayer telemetry status|enable|disable|show` per spec; notice once on stderr, re-shown on schema-major bump, none when off. Verify: fails today.
- [x] 1.5 `tests/test_telemetry_counting.py`: CLI leaf tokens, parse errors / `--help` not counted; MCP tool calls counted with ok / error token, `clientInfo` mapping, tool schemas byte-identical with telemetry on vs off; REST route template counted, `/mcp` mount counted as MCP only; Flight validate+fetch counted once; PG multi-statement and repeated prepared execution counted per execution, probes under their own token. Verify: fails today.
- [x] 1.6 `tests/test_telemetry_features.py`: each structural flag; built-in transform / aggregation names counted by name, user-defined → `custom`; counted per attempt; dialect counts; datasource / model-count buckets; extras detected from metadata without importing (assert module not in `sys.modules`); no storage call or connection made by telemetry (spy). Verify: fails today.
- [x] 1.7 `tests/test_telemetry_delivery.py`: spool written on normal exit, SIGINT, SIGTERM for stdio `slayer mcp` (handler chained), server shutdown, MCP EOF / broken pipe (subprocess tests); concurrent writers; fork resets counters; 24 h gate (idle → nothing, due → exactly one merged send, future `last_sent` → due); two simultaneous senders → one report; 500 / timeout → spool kept, `last_sent` unchanged; crash after claim recovered; size cap; non-regular files ignored; file mode 0600; `disable` racing a send; request shape (`slayer_usage`, distinct_id, uuid, `$geoip_disable`, `$process_person_profile`); install-ID rotation after 13 months. Verify: fails today.
- [x] 1.8 `tests/test_telemetry_isolation.py` (telemetry.arc42 P1): endpoint refused / timeout / 500, corrupt spool, unwritable files → command output and exit code identical to telemetry-off; telemetry never writes stdout; a stalled send delays exit by ≤ ~1 s and only when in flight. Verify: fails today.

## 2. Architecture (each exact edit shown to the user and approved before it lands)

- [ ] 2.1 `architecture/model/slayer.c4`: node `telemetry` in `python` (metadata `package 'slayer.telemetry'`, `arc42 'architecture/telemetry.arc42.md'`, `specs ['telemetry']`), child `features`; edges `surfaces -> telemetry`, `protocols -> telemetry`, `telemetry.features -> core` (+ any measured extractor edge). Verify: `poetry run la-arch-check` green after 3.x.
- [ ] 2.2 `architecture/system.arc42.md` §3 principle 17 (approved wording in design.md D2); new `architecture/telemetry.arc42.md` (§1–§4, one principle: Fail-silent). Verify: `la-arch-check` arc42 checks green.
- [ ] 2.3 Regenerate the landscape diagram: `poetry run la-arch-diagrams`. Verify: `diagrams-fresh` green; `npx -y likec4@1.47.0 validate architecture` green.

## 3. Telemetry package

- [x] 3.1 `slayer/telemetry/` settings: config-dir resolution, `telemetry.json` model, precedence + deciding rule, editable detection, unwritable → off, install ID + 13-month rotation. Verify: 1.4, 1.7 rotation.
- [x] 3.2 Payload model + fixed token tables (error classes, MCP clients, CLI leaves, buckets, extras list). Verify: 1.2.
- [x] 3.3 Recorder (no-op until `start()`), fork reset, merge semantics. Verify: 1.3, 1.7 fork.
- [x] 3.4 Spool (random names, temp + rename, 0600, cap, regular files only) and lease / claim / commit / stale-claim recovery. Verify: 1.7.
- [x] 3.5 Sender (stdlib `urllib`, PostHog EU request shape, endpoint override, daemon thread, ≤ ~1 s exit join only when in flight, 24 h gate). Verify: 1.7, 1.8.
- [x] 3.6 Notice (stderr, once per schema major). Verify: 1.4.
- [x] 3.7 `slayer/telemetry/features/`: structural flags + built-in names from `slayer.core` sets. Verify: 1.6.

## 4. Hooks

- [x] 4.1 `slayer/cli.py`: `telemetry.start()` in `main`, a `set_defaults` telemetry token on every leaf parser, `slayer telemetry status|enable|disable|show`, `try/finally` flush around each server run, SIGTERM handler for stdio `mcp`. Verify: 1.4, 1.5 CLI, 1.7 lifecycle.
- [x] 4.2 `slayer/mcp/server.py`: one `call_tool` interception + `clientInfo` capture; features before execution in `query`. Verify: 1.5 MCP, 1.6.
- [x] 4.3 `slayer/api/server.py`: route-template middleware excluding `/mcp`; features before execution. Verify: 1.5 REST.
- [x] 4.4 `slayer/flight/handlers.py` `_execute_full` and `slayer/pg_facade/connection.py` `_run_query`: count + features. Verify: 1.5 Flight / PG.

## 5. Docs and release

- [ ] 5.1 `docs/reference/telemetry.md`: every field, controls, notice text, destination (IP received at transport, discarded, no GeoIP), privacy notice (Motley as controller, purpose, PostHog EU processor, 13-month raw retention, ID rotation, erasure by install ID + contact address); nav entry in `zensical.toml`. Verify: page renders in nav.
- [ ] 5.2 README telemetry section (one paragraph + opt-out); CLI reference for `slayer telemetry`; release-notes item. Verify: grep.

## 6. Gates

- [ ] 6.1 Full non-integration suite `poetry run pytest -m "not integration" -n auto`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`. Verify: all green.
- [ ] 6.2 **Release-blocking:** PostHog EU project created with "Discard client IP data", DPA signed, project API key (`phc_…`) received from the user and set as the shipped default; a live smoke send verified in PostHog. Implementation and tests proceed without it via `SLAYER_TELEMETRY_ENDPOINT`.
