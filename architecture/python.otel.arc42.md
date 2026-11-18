# otel — OpenTelemetry

## 1. Purpose & context

`slayer/otel` turns the probes that SLayer's code marks into OpenTelemetry
traces, metrics and logs, and exports them over OTLP to the operator's own
backend. It is opt-in and ships as the `otel` extra. Behaviour is specified in
`openspec/specs/otel/`.

## 2. Building blocks

No view of its own: the node is one leaf in the `landscape` view
([system.arc42.md](system.arc42.md)). `surfaces` and `protocols` import it to open
one request per call; it imports only the probe API in `core`.

## 3. Principles

1. **Off unless asked**: OTel runs only in a process the `slayer` command started
   with it switched on, or for a library caller that instruments explicitly;
   nothing else turns it on.
   [enforced: test:tests/test_otel_guards.py]
2. **Fail-silent**: no OTel path raises into, writes to stdout of, or delays the
   host process beyond one bounded exit wait.
   [enforced: test:tests/test_otel_guards.py]
   [enforced: test:tests/test_otel_isolation.py]
3. **No content without the opt-in**: without content capture, no SQL text, value,
   exception message or traceback leaves the process; an error leaves by class only.
   [enforced: test:tests/test_otel_privacy.py]
   [enforced: test:tests/test_otel_logs.py]
4. **Metrics never carry content**: every metric attribute value comes from a
   bounded vocabulary, with or without content capture.
   [enforced: test:tests/test_otel_privacy.py]
5. **Observing changes nothing**: results, generated SQL, errors and warnings —
   including where a warning points — are the same with OTel on or off.
   [enforced: test:tests/test_otel_isolation.py]
   [enforced: test:tests/test_probes_decorator.py]
6. **One way in**: other nodes reach OTel only through `core.probes`; only
   `surfaces` and `protocols` import this node, and it imports nothing but
   `core.probes`.
   [enforced: arch_check:model-truth]

## 4. Rationale

Probes placed in SLayer's own code keep every observed point visible to
reviewers and to the import law, where patches applied from outside would break
silently on a rename (`gh-483-opentelemetry-instrumentation`). Principles 1–4
follow usage reporting's: OTel runs inside the host process, so its defects
and the data it sends must never reach further than the operator asked.
