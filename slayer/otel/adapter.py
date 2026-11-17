"""core.stages listener: each stage becomes an OTel span, plus duration and count metrics.

The one place where names gain the ``slayer.`` namespace. OTel standard keys (semantic
conventions) pass through unchanged. Sensitive standard keys leave only on the operator's
opt-in: ``db.query.text`` with ``SLAYER_OTEL_CAPTURE=sql``, exception text with ``exceptions``.
The listener reads stages and never writes to them, except to copy a query's datasource up
from its preparation stage.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import metrics, trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from slayer import __version__
from slayer.core import stages

PREFIX = "slayer."

STANDARD_KEYS = frozenset({
    "error.type",
    "db.system.name",
    "db.namespace",
    "db.query.text",
    "db.response.returned_rows",
    "server.address",
    "server.port",
    "mcp.method.name",
    "gen_ai.operation.name",
    "gen_ai.tool.name",
})
_SENSITIVE = {"db.query.text": "sql"}

# SLayer datasource types with an OTel db.system.name value; every other type is "other_sql".
DB_SYSTEMS: dict[str, str] = {
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "sqlite": "sqlite",
    "clickhouse": "clickhouse",
    "trino": "trino",
    "oracle": "oracle.db",
    "redshift": "aws.redshift",
    "sqlserver": "microsoft.sql_server",
    "mssql": "microsoft.sql_server",
    "tsql": "microsoft.sql_server",
}

# Histogram bucket boundaries, in seconds. Without them the SDK uses millisecond-sized
# defaults (0 to 10000), and every SLayer duration falls into the first bucket.
DB_BUCKETS = (0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0)  # OTel database conventions
QUERY_BUCKETS = (*DB_BUCKETS, 30.0, 60.0, 120.0)  # warehouse queries can take minutes
INGEST_BUCKETS = (0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0)
_DURATIONS = {
    "request": QUERY_BUCKETS,
    "query": QUERY_BUCKETS,
    "query.prepare": QUERY_BUCKETS,
    "ingest": INGEST_BUCKETS,
}
_LABEL_KEYS = frozenset({
    "surface",
    "operation",
    "datasource.name",
    "datasource.type",
    "error.type",
})
_DB_LABEL_KEYS = frozenset({
    "db.system.name",
    "db.namespace",
    "server.address",
    "server.port",
    "datasource.name",
    "error.type",
})
_SERVER_SURFACES = frozenset({"flight", "pg"})
_INGEST_CHANGES = ("added", "updated", "skipped")

Attributes = Mapping[str, stages.AttributeValue]


class OtelStageListener:
    """Mirrors each stage as a span and records the metrics derived from stage ends."""

    def __init__(
        self,
        *,
        tracer_provider: trace.TracerProvider | None = None,
        meter_provider: metrics.MeterProvider | None = None,
        capture: frozenset[str] = frozenset(),
    ) -> None:
        self.capture = capture
        self._tracer = trace.get_tracer("slayer", __version__, tracer_provider=tracer_provider)
        meter = metrics.get_meter("slayer", __version__, meter_provider=meter_provider)
        self._durations = {
            name: meter.create_histogram(
                f"{PREFIX}{name}.duration", unit="s", explicit_bucket_boundaries_advisory=buckets,
            )
            for name, buckets in _DURATIONS.items()
        }
        self._db_duration = meter.create_histogram(
            "db.client.operation.duration", unit="s", explicit_bucket_boundaries_advisory=DB_BUCKETS,
        )
        self._retries = meter.create_counter(f"{PREFIX}db.retries", unit="{retry}")
        self._cache = meter.create_counter(f"{PREFIX}cache.lookups", unit="{lookup}")
        self._models = meter.create_counter(f"{PREFIX}ingest.models", unit="{model}")
        self._drift = meter.create_counter(f"{PREFIX}ingest.drift.entries", unit="{entry}")
        self._open: dict[int, tuple[trace.Span, Any]] = {}

    def on_start(self, stage: stages.Stage) -> None:
        attributes = {**stage.attributes, **_derived(stage, error=None)}
        span = self._tracer.start_span(
            PREFIX + stage.name, kind=_kind(stage), attributes=self._exported(attributes), start_time=stage.start_ns,
        )
        self._open[id(stage)] = (span, otel_context.attach(trace.set_span_in_context(span)))

    def on_end(self, stage: stages.Stage, *, error: BaseException | None) -> None:
        opened = self._open.pop(id(stage), None)
        if opened is None:
            return
        span, token = opened
        if error is not None:
            span.set_status(Status(StatusCode.ERROR))
            if "exceptions" in self.capture:
                span.record_exception(error)
        _lift_datasource(stage)
        attributes = {**stage.attributes, **_derived(stage, error=error)}
        span.set_attributes(self._exported(attributes))
        for event in stage.events:
            span.add_event(PREFIX + event.name, attributes=self._exported(event.attributes), timestamp=event.time_ns)
        span.end(end_time=stage.start_ns + int(stage.duration_s * 1e9))
        otel_context.detach(token)
        self._record_metrics(stage, attributes=attributes)

    def _allowed(self, key: str) -> bool:
        """A sensitive key leaves only with its ``SLAYER_OTEL_CAPTURE`` value; every other key leaves."""
        needed = _SENSITIVE.get(key)
        return needed is None or needed in self.capture

    def _exported(self, attributes: Attributes) -> dict[str, stages.AttributeValue]:
        return {exported_name(key): value for key, value in attributes.items() if self._allowed(key)}

    def _record_metrics(self, stage: stages.Stage, *, attributes: Attributes) -> None:
        labels = _labels(attributes, keys=_LABEL_KEYS)
        if stage.name == "db.execute":
            self._db_duration.record(stage.duration_s, attributes=_labels(attributes, keys=_DB_LABEL_KEYS))
        duration = self._durations.get(stage.name)
        if duration is not None:
            duration.record(stage.duration_s, attributes=labels)
        for event in stage.events:
            if event.name == "db.retry":
                self._retries.add(1, attributes=labels)
        if stage.name == "query.cache.get" and "cache.hit" in attributes:
            self._cache.add(1, attributes={**labels, exported_name("cache.hit"): attributes["cache.hit"]})
        if stage.name == "ingest":
            for change in _INGEST_CHANGES:
                count = attributes.get(f"ingest.models.{change}")
                if isinstance(count, int) and count:
                    self._models.add(count, attributes={**labels, exported_name("ingest.change"): change})
        if stage.name == "ingest.drift":
            count = attributes.get("ingest.drift.entries")
            if isinstance(count, int) and count:
                self._drift.add(count, attributes=labels)


def exported_name(key: str) -> str:
    """``slayer.`` + key, except OTel standard keys, which keep their name."""
    return key if key in STANDARD_KEYS else PREFIX + key


def _labels(attributes: Attributes, *, keys: frozenset[str]) -> dict[str, stages.AttributeValue]:
    return {exported_name(key): value for key, value in attributes.items() if key in keys}


def _derived(stage: stages.Stage, *, error: BaseException | None) -> dict[str, stages.AttributeValue]:
    """Export-only attributes: ``error.type``, and the standard ``db.system.name`` on database stages."""
    derived: dict[str, stages.AttributeValue] = {}
    if error is not None:
        derived["error.type"] = type(error).__qualname__
    datasource_type = stage.attributes.get("datasource.type")
    if stage.name.startswith("db.") and datasource_type:
        derived["db.system.name"] = DB_SYSTEMS.get(str(datasource_type), "other_sql")
    return derived


def _lift_datasource(stage: stages.Stage) -> None:
    """A query's datasource is known only after preparation: copy it onto the enclosing query.

    This is the one write to a stage, so that ``slayer.query`` spans and metrics carry the datasource.
    """
    parent = stage.parent
    if stage.name == "query.prepare" and parent is not None and parent.name == "query":
        parent.set({key: value for key, value in stage.attributes.items() if key.startswith("datasource.")})


def _kind(stage: stages.Stage) -> SpanKind:
    if stage.name.startswith("db."):
        return SpanKind.CLIENT
    if stage.name == "request" and stage.attributes.get("surface") in _SERVER_SURFACES:
        return SpanKind.SERVER
    return SpanKind.INTERNAL
