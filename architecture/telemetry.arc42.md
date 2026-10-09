# telemetry — usage telemetry

## 1. Purpose & context

`slayer/telemetry` records anonymous usage counters in processes started by the
`slayer` CLI, spools them per process, and sends them to PostHog EU at most once
per 24 h. Behaviour is specified in `openspec/specs/telemetry/`.

## 2. Building blocks

Child `features` turns the queries and datasources a surface has already loaded
into vocabulary tokens, and is the only part that imports other SLayer nodes
(`core`, `sql`). The rest (settings, payload vocabulary, recorder, spool, sender)
imports no SLayer node.

## 3. Principles

1. **Fail-silent**: no telemetry path raises into, writes to stdout of, or blocks
   the host process beyond the one bounded send wait.
   [enforced: test:tests/test_telemetry_isolation.py]
2. **Telemetry carries no user content**: every value a usage report sends is
   drawn from a SLayer-defined vocabulary (versions, enums, booleans, buckets,
   counts keyed by built-in names); anything else is reported as `other`.
   Library use never reports.
   [enforced: test:tests/test_telemetry_payload.py]
3. **No user-derived values**: no value supplied by a user, an MCP client or a
   database leaves the process or reaches a telemetry file in any form: verbatim,
   truncated, case-folded, hashed, encoded, or as its length. Such a value may
   only pick a token from a fixed table; a miss is reported as `other` / `custom`.
   The one exception is an MCP client's self-reported major version, bounded to
   0–999.
   [enforced: test:tests/test_telemetry_payload.py]
4. **Errors by class only**: a failure is reduced to a token by its exception
   class hierarchy; telemetry never reads an exception's message, arguments,
   traceback or `repr`.
   [enforced: test:tests/test_telemetry_isolation.py]
5. **Sizes are bucketed**: a count of user-owned things (models, datasources) is
   reported only as a bucket, never as the exact number.
   [enforced: test:tests/test_telemetry_features.py]

## 4. Rationale

Telemetry runs inside every CLI process, so its own defects must never reach the
user; principles 2–5 keep user content, and anything derived from it, from
leaving the machine.
