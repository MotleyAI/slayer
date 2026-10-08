## Context

See proposal.md for motivation; the comparable-tools matrix and the decision log (D1–D13) live on the Linear issue DEV-2035. SLayer's entry points all pass through `slayer.cli:main` (the Docker image runs `slayer serve`). Flight validates a query with a `LIMIT 0` run in `get_flight_info` and executes it again in `_execute_full`; PG executes every statement through `_run_query`; FastMCP routes every tool call through `FastMCP.call_tool` and exposes `clientInfo` on the session's `client_params`; the built-in aggregation and transform name sets live in `slayer/core/enums.py` and `slayer/core/formula.py`.

## Goals / Non-Goals

**Goals:** the specs under `specs/telemetry/`; zero new dependencies; no telemetry code path able to change user-visible behaviour.

**Non-Goals:** library-use telemetry; error messages or stack traces; a Motley-hosted proxy; publishing aggregates (later, not v1); consent prompts.

## Decisions

1. **Architecture — a precise LikeC4 node `telemetry`** inside `python` (metadata: `package 'slayer.telemetry'`, `arc42 'architecture/telemetry.arc42.md'`, `specs ['telemetry']`) with one child `features`. Edges: `surfaces -> telemetry`, `protocols -> telemetry`, `telemetry.features -> core`, plus any other edge `la-arch-check` measures for the extractor; the rest of `telemetry` imports no SLayer node. Why: protocols (Flight/PG) and surfaces both record, so telemetry cannot live inside `surfaces`; the child proves only the extractor reads queries. The exact `.c4` diff is shown for approval before it lands.
2. **arc42** (wording approved): `system.arc42.md` §3 gains principle 17 ("Telemetry carries no user content … Library use never reports.", enforced by `tests/test_telemetry_payload.py`); new `telemetry.arc42.md` with one principle, **Fail-silent** (enforced by `tests/test_telemetry_isolation.py`). The node file's §1/§2/§4 prose is shown for approval before it lands.
3. **Activation by construction.** A module-level recorder is a no-op until `slayer.cli.main` calls `telemetry.start()`; every hook calls the recorder unconditionally. Library use therefore never records without any per-site check.
4. **Vocabulary at the type level.** The payload is a Pydantic model whose every leaf is a `Literal`/enum, bool, bucket label, date, generated UUID or `int`. Tokens come from fixed tables (CLI leaf tokens via `set_defaults`, MCP tool names from the registered tool set, REST route templates from the matched `APIRoute`, exception-class→token table, MCP client list, built-in name sets); a lookup miss maps to `other`/`custom`. A schema-policy test walks the model to enforce this; sentinel fuzzing tests the hooks.
5. **Hook points.** CLI: `main` (start + leaf token; `argparse` errors exit before start). MCP: override/wrap `FastMCP.call_tool` once — tool schemas untouched — and read `client_params.clientInfo` on first call per session. REST: a middleware reading `request.scope["route"]` after the handler; the `/mcp` mount excluded. Flight: `_execute_full`. PG: `_run_query`. Features: on the normalized `SlayerQuery` immediately before engine execution in each query surface.
6. **Storage and sending.** Config dir via the platform convention (`$XDG_CONFIG_HOME/slayer`, `~/Library/Application Support/slayer`, `%APPDATA%\slayer`), never `SLAYER_STORAGE`. `telemetry.json` holds setting, install ID + creation date, notice schema, last-send time. Spool files: random names, temp-write + rename, mode 0600, capped. A lease file with expiry serialises senders; claim by rename, delete + record `last_sent` only after 2xx; stale claims recovered by age. Send: stdlib `urllib` POST in a daemon thread; `atexit` joins it for ≤ ~1 s only if in flight.
7. **Lifecycle.** Flush counters to the spool in `try/finally` around each server run function, `atexit` as fallback; a chaining SIGTERM handler only for stdio `slayer mcp` that raises `SystemExit`; `os.register_at_fork` resets the recorder in children.
8. **Own-usage exclusion.** Editable installs detected from PEP 610 `direct_url.json`; an autouse session fixture in `tests/conftest.py` sets `SLAYER_TELEMETRY=off`, and telemetry tests re-enable it with a local endpoint.

## Risks / Trade-offs

- [Default-on telemetry draws criticism] → strict vocabulary, stderr notice, `DO_NOT_TRACK`, CI auto-off, `show` for inspection, docs page with every field.
- [ePrivacy Art. 5(3) exposure for EU users with an on-by-default stored ID] → accepted for now (D1); revisit if Motley establishes in the EU.
- [Backend frozen into releases — PostHog EU URL ships in every version] → `SLAYER_TELEMETRY_ENDPOINT` override; old versions keep posting to PostHog.
- [PostHog sees client IPs at transport] → project discards IPs, GeoIP disabled per event; documented exactly.
- [Short-lived stdio MCP sessions killed with SIGKILL lose counts] → accepted.
- [FastMCP internals change across `mcp` <2 versions] → interception tested against the pinned version; tool-schema identity test.

## Migration Plan

Additive. Release notes announce telemetry, the notice, and the opt-outs. Rollback: ship `SLAYER_TELEMETRY` default `off` in a patch release.
