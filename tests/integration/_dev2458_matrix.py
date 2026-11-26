"""Discrete and approximate percentiles and DISTINCT aggregates on each server dialect (spec:
aggregations/distinct-and-percentiles).

``pq`` amounts by ``grp``: a 10, 20, 30, 40, 60, NULL | b 5, 5, 7, 9 | c all NULL | NULL 50, 70. ``pq_items`` (one to
many from ``pq``): p1 over pq 1 (twice), 2, 7, 8 | p2 over pq 3, 4, 5, 7. Expected values come from the Python oracles.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Sequence
from decimal import Decimal
from typing import Any

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

Row = tuple[Any, ...]

#: (id, grp, grp2, amount, price, label, sold_on)
PQ: tuple[Row, ...] = (
    (1, "a", "x", 10, "10.00", "pear", "2024-01-05"),
    (2, "a", "x", 20, "20.00", "apple", "2024-01-02"),
    (3, "a", "y", 30, "30.00", "fig", "2024-01-09"),
    (4, "a", "y", 40, "40.00", "kiwi", "2024-01-01"),
    (5, "a", "x", 60, "60.00", "date", "2024-01-07"),
    (6, "a", "y", None, None, None, None),
    (7, "b", "x", 5, "5.00", "b1", "2024-02-01"),
    (8, "b", "x", 5, "5.00", "b1", "2024-02-01"),
    (9, "b", "y", 7, "7.00", "b2", "2024-02-02"),
    (10, "b", "y", 9, "9.00", "b3", "2024-02-03"),
    (11, "c", "x", None, None, None, None),
    (12, "c", "y", None, None, None, None),
    (13, None, "x", 50, "50.00", "n1", "2024-03-01"),
    (14, None, "y", 70, "70.00", "n2", "2024-03-02"),
)
#: (id, pq_id, product)
ITEMS: tuple[Row, ...] = (
    (1, 1, "p1"), (2, 1, "p1"), (3, 2, "p1"), (4, 7, "p1"), (5, 8, "p1"),
    (6, 3, "p2"), (7, 4, "p2"), (8, 5, "p2"), (9, 7, "p2"),
)
_COLUMNS = ("id", "grp", "grp2", "amount", "price", "label", "sold_on")

#: dialect → (integer, text, decimal, date) column types.
_TYPES: dict[str, tuple[str, str, str, str]] = {
    "postgres": ("INTEGER", "VARCHAR(16)", "DECIMAL(10,2)", "DATE"),
    "mysql": ("INTEGER", "VARCHAR(16)", "DECIMAL(10,2)", "DATE"),
    "tsql": ("INT", "VARCHAR(16)", "DECIMAL(10,2)", "DATE"),
    "trino": ("INTEGER", "VARCHAR", "DECIMAL(10,2)", "DATE"),
    "bigquery": ("INT64", "STRING", "NUMERIC", "DATE"),
    "snowflake": ("NUMBER(38,0)", "VARCHAR", "NUMBER(10,2)", "DATE"),
    "clickhouse": ("Nullable(Int32)", "Nullable(String)", "Nullable(Decimal(10,2))", "Nullable(Date)"),
}
_SUFFIX = {"clickhouse": " ENGINE = MergeTree ORDER BY id"}

#: The ClickHouse group past the 8192-row reservoir of ``quantile``.
BIG_ROWS = 10_000
BIG_VALUES: tuple[float, ...] = tuple(float((i * 7919) % 10_007) for i in range(BIG_ROWS))


# --------------------------------------------------------------------------- #
# Oracles
# --------------------------------------------------------------------------- #
def disc(values: Iterable[Any], p: float) -> Any:
    """PERCENTILE_DISC: the smallest non-NULL value whose cumulative distribution is at least ``p``."""
    ordered = sorted(v for v in values if v is not None)
    if not ordered:
        return None
    rank = max(1, math.ceil(Decimal(str(p)) * len(ordered)))
    return ordered[rank - 1]


def cont(values: Iterable[Any], p: float) -> float | None:
    """PERCENTILE_CONT (R-7) over the non-NULL values."""
    ordered = sorted(float(v) for v in values if v is not None)
    if not ordered:
        return None
    h = (len(ordered) - 1) * p
    low = math.floor(h)
    return ordered[low] + (h - low) * (ordered[min(low + 1, len(ordered) - 1)] - ordered[low])


def _column(name: str, rows: Sequence[Row] = PQ) -> list[Any]:
    i = _COLUMNS.index(name)
    return [r[i] for r in rows]


def _by(rows: Sequence[Row], *keys: str) -> dict[Any, list[Row]]:
    out: dict[Any, list[Row]] = {}
    for r in rows:
        key = tuple(r[_COLUMNS.index(k)] for k in keys)
        out.setdefault(key[0] if len(keys) == 1 else key, []).append(r)
    return out


def _associated(product: str) -> list[Row]:
    """The ``pq`` rows associated with ``product``, each once."""
    ids = {pq_id for _, pq_id, p in ITEMS if p == product}
    return [r for r in PQ if r[0] in ids]


def _distinct_sum(values: Iterable[Any]) -> float | None:
    present = {v for v in values if v is not None}
    return float(sum(present)) if present else None


def _distinct_avg(values: Iterable[Any]) -> float | None:
    present = {v for v in values if v is not None}
    return sum(present) / len(present) if present else None


# --------------------------------------------------------------------------- #
# Seeds
# --------------------------------------------------------------------------- #
def _literal(value: Any, *, kind: str, dialect: str) -> str:
    if value is None:
        return "NULL"
    if kind == "text":
        return "'" + str(value).replace("'", "''") + "'"
    if kind == "date":
        return f"DATE '{value}'" if dialect in ("trino", "bigquery") else f"'{value}'"
    if kind == "decimal" and dialect == "bigquery":
        return f"NUMERIC '{value}'"
    return str(value)


def statements(*, dialect: str, prefix: str = "") -> list[str]:
    """CREATE + INSERT of ``pq`` and ``pq_items`` on a server ``dialect``; ``prefix`` qualifies table names."""
    integer, text, decimal, day = _TYPES[dialect]
    key = "Int32" if dialect == "clickhouse" else integer
    suffix = _SUFFIX.get(dialect, "")
    kinds = ("int", "text", "text", "int", "decimal", "text", "date")
    pq_rows = ", ".join("(" + ", ".join(_literal(v, kind=k, dialect=dialect) for v, k in zip(r, kinds)) + ")"
                        for r in PQ)
    item_rows = ", ".join(f"({i}, {pq_id}, '{product}')" for i, pq_id, product in ITEMS)
    item_text = "String" if dialect == "clickhouse" else text
    return [
        f"CREATE TABLE {prefix}pq (id {key}, grp {text}, grp2 {text}, amount {integer}, price {decimal}, "
        f"label {text}, sold_on {day}){suffix}",
        f"INSERT INTO {prefix}pq (id, grp, grp2, amount, price, label, sold_on) VALUES {pq_rows}",
        f"CREATE TABLE {prefix}pq_items (id {key}, pq_id {key}, product {item_text}){suffix}",
        f"INSERT INTO {prefix}pq_items (id, pq_id, product) VALUES {item_rows}",
    ]


def clickhouse_statements() -> list[str]:
    """``pq_big``: one group of ``BIG_ROWS`` values; ``pq_nulls``: nullable and non-nullable numbers and dates."""
    return [
        "CREATE TABLE pq_big (id UInt32, g Int32, x Float64) ENGINE = MergeTree ORDER BY id",
        f"INSERT INTO pq_big SELECT number, 1, toFloat64((number * 7919) % 10007) FROM numbers({BIG_ROWS})",
        "CREATE TABLE pq_nulls (id Int32, n Nullable(Float64), m Float64, d Nullable(Date), dd Date, "
        "t Nullable(DateTime64(6)), tt DateTime64(6)) ENGINE = MergeTree ORDER BY id",
        "INSERT INTO pq_nulls VALUES (1, NULL, 1.5, NULL, '2024-01-01', NULL, '2024-01-01 10:00:00'), "
        "(2, NULL, 2.5, NULL, '2024-01-02', NULL, '2024-01-02 10:00:00')",
    ]


def _fields(specs: Sequence[tuple[str, DataType]], *, explicit_sql: bool) -> list[DatasetField]:
    return [DatasetField.model_validate({"name": name, "datatype": datatype, "primary_key": name == "id",
                                         **({"expression": name} if explicit_sql else {})}) for name, datatype in specs]


def datasets(*, data_source: str, explicit_sql: bool = False, prefix: str = "") -> list[SlayerDataset]:
    pq = SlayerDataset(
        name="pq", sql_table=f"{prefix}pq", data_source=data_source,
        fields=_fields([("id", DataType.INTEGER), ("grp", DataType.STRING), ("grp2", DataType.STRING),
                        ("amount", DataType.INTEGER), ("price", DataType.DECIMAL), ("label", DataType.STRING),
                        ("sold_on", DataType.DATE)], explicit_sql=explicit_sql),
        relationships=[DatasetRelationship(to="pq_items", from_columns=["id"], to_columns=["pq_id"],
                                           cardinality=RelationshipCardinality.ONE_TO_MANY)],
    )
    items = SlayerDataset(
        name="pq_items", sql_table=f"{prefix}pq_items", data_source=data_source,
        fields=_fields([("id", DataType.INTEGER), ("pq_id", DataType.INTEGER), ("product", DataType.STRING)],
                       explicit_sql=explicit_sql),
    )
    return [pq, items]


def clickhouse_datasets(*, data_source: str) -> list[SlayerDataset]:
    big = SlayerDataset(name="pq_big", sql_table="pq_big", data_source=data_source, fields=_fields(
        [("id", DataType.INTEGER), ("g", DataType.INTEGER), ("x", DataType.FLOAT)], explicit_sql=False))
    nulls = SlayerDataset(name="pq_nulls", sql_table="pq_nulls", data_source=data_source, fields=_fields(
        [("id", DataType.INTEGER), ("n", DataType.FLOAT), ("m", DataType.FLOAT), ("d", DataType.DATE),
         ("dd", DataType.DATE), ("t", DataType.DATETIME), ("tt", DataType.DATETIME)], explicit_sql=False))
    return [big, nulls]


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #
def _norm(value: Any) -> Any:
    """Numbers as floats, dates as ``YYYY-MM-DD``."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return value.isoformat()[:10]


async def _run(engine: SlayerQueryEngine, *, metrics: dict[str, str], dimensions: Sequence[str] = (),
               filters: Sequence[str] = (), population: str = "pq") -> SlayerResponse:
    return await engine.execute(SlayerQuery.model_validate({
        "population": population, "dimensions": list(dimensions), "filter": list(filters),
        "metrics": [QueryMetric(metric=text, alias=alias) for alias, text in metrics.items()],
    }))


def _compare(resp: SlayerResponse, *, keys: Sequence[str], expected: dict[Any, dict[str, Any]],
             approx: Callable[[str], float] | None = None) -> None:
    failures: list[str] = []
    seen: set[Any] = set()
    for row in resp.data:
        key = tuple(row[k] for k in keys)
        key = key[0] if len(keys) == 1 else key
        if key not in expected:
            continue
        seen.add(key)
        for alias, want in expected[key].items():
            got = _norm(row[alias])
            want = _norm(want)
            tolerance = approx(alias) if approx else 1e-9
            ok = got == want if got is None or want is None or isinstance(want, str) else math.isclose(
                got, want, abs_tol=tolerance)
            if not ok:
                failures.append(f"{key!r} {alias}: got {row[alias]!r}, want {want!r}")
    failures += [f"{k!r}: no row" for k in expected if k not in seen]
    assert not failures, "\n".join(failures) + f"\nSQL:\n{resp.sql}"


#: alias → (Mode-B text, oracle over a group's rows)
_DISC: dict[str, tuple[str, Callable[[Sequence[Row]], Any]]] = {
    "d0": ("percentile_disc(pq.amount, 0)", lambda rows: disc(_column("amount", rows), 0)),
    "d25": ("PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY pq.amount)", lambda rows: disc(_column("amount", rows), 0.25)),
    "d50": ("percentile_disc(pq.amount, p=0.5)", lambda rows: disc(_column("amount", rows), 0.5)),
    "d75": ("percentile_disc(pq.amount, 0.75)", lambda rows: disc(_column("amount", rows), 0.75)),
    "d100": ("percentile_disc(pq.amount, 1)", lambda rows: disc(_column("amount", rows), 1)),
    "fx": ("percentile_disc(pq.amount, 0.5) FILTER (WHERE pq.grp2 = 'x')",
           lambda rows: disc(_column("amount", [r for r in rows if r[2] == "x"]), 0.5)),
}


def _expected(groups: dict[Any, list[Row]], checks: dict[str, tuple[str, Callable[[Sequence[Row]], Any]]]
              ) -> dict[Any, dict[str, Any]]:
    return {key: {alias: oracle(rows) for alias, (_, oracle) in checks.items()} for key, rows in groups.items()}


def _texts(checks: dict[str, tuple[str, Any]]) -> dict[str, str]:
    return {alias: text for alias, (text, _) in checks.items()}


async def check_discrete(engine: SlayerQueryEngine) -> None:
    """Grouped (a NULL key included) and ungrouped: NULLs, duplicates, p 0 and 1, several in one query, FILTER."""
    grouped = await _run(engine, metrics=_texts(_DISC), dimensions=["pq.grp"])
    _compare(grouped, keys=["pq.grp"], expected=_expected(_by(PQ, "grp"), _DISC))
    ungrouped = await _run(engine, metrics=_texts(_DISC))
    _compare(ungrouped, keys=[], expected={(): {alias: oracle(PQ) for alias, (_, oracle) in _DISC.items()}})


async def check_several_keys(engine: SlayerQueryEngine) -> None:
    checks = {k: _DISC[k] for k in ("d25", "d50")}
    resp = await _run(engine, metrics=_texts(checks), dimensions=["pq.grp", "pq.grp2"])
    _compare(resp, keys=["pq.grp", "pq.grp2"], expected=_expected(_by(PQ, "grp", "grp2"), checks))


async def check_text_and_date(engine: SlayerQueryEngine) -> None:
    checks: dict[str, tuple[str, Callable[[Sequence[Row]], Any]]] = {
        "t": ("percentile_disc(pq.label, 0.5)", lambda rows: disc(_column("label", rows), 0.5)),
        "dt": ("percentile_disc(pq.sold_on, 0.5)", lambda rows: disc(_column("sold_on", rows), 0.5)),
        "t1": ("percentile_disc(pq.label, 1)", lambda rows: disc(_column("label", rows), 1)),
    }
    resp = await _run(engine, metrics=_texts(checks), dimensions=["pq.grp"])
    _compare(resp, keys=["pq.grp"], expected=_expected(_by(PQ, "grp"), checks))


async def check_empty(engine: SlayerQueryEngine) -> None:
    """Over no rows every new aggregation and the exact percentiles are NULL."""
    metrics = {"d": "percentile_disc(pq.amount, 0.5)", "dt": "percentile_disc(pq.sold_on, 0.5)",
               "c": "percentile(pq.amount, 0.5)", "md": "median(pq.amount)", "ap": "percentile_approx(pq.amount, 0.5)",
               "sd": "sum_distinct(pq.amount)", "ad": "avg_distinct(pq.amount)"}
    resp = await _run(engine, metrics=metrics, filters=["pq.id < 0"])
    _compare(resp, keys=[], expected={(): dict.fromkeys(metrics)})


_DISTINCT: dict[str, tuple[str, Callable[[Sequence[Row]], Any]]] = {
    "sd": ("sum_distinct(pq.amount)", lambda rows: _distinct_sum(_column("amount", rows))),
    "sql_sd": ("SUM(DISTINCT pq.amount)", lambda rows: _distinct_sum(_column("amount", rows))),
    "ad": ("avg_distinct(pq.amount)", lambda rows: _distinct_avg(_column("amount", rows))),
    "mn": ("MIN(DISTINCT pq.amount)", lambda rows: min((v for v in _column("amount", rows) if v is not None),
                                                       default=None)),
    "mx": ("max(distinct pq.amount)", lambda rows: max((v for v in _column("amount", rows) if v is not None),
                                                       default=None)),
    "fsd": ("SUM(DISTINCT pq.amount) FILTER (WHERE pq.grp2 = 'x')",
            lambda rows: _distinct_sum(_column("amount", [r for r in rows if r[2] == "x"]))),
}


async def check_distinct(engine: SlayerQueryEngine) -> None:
    resp = await _run(engine, metrics=_texts(_DISTINCT), dimensions=["pq.grp"])
    _compare(resp, keys=["pq.grp"], expected=_expected(_by(PQ, "grp"), _DISTINCT))


async def check_association(engine: SlayerQueryEngine) -> None:
    """By ``pq_items.product`` each ``pq`` row counts once per product, never once per item."""
    checks = {"d": _DISC["d50"], "sd": _DISTINCT["sd"]}
    resp = await _run(engine, metrics=_texts(checks), dimensions=["pq_items.product"])
    groups = {product: _associated(product) for product in ("p1", "p2")}
    _compare(resp, keys=["pq_items.product"], expected=_expected(groups, checks))


async def check_approx_tolerance(engine: SlayerQueryEngine) -> None:
    """Within 1 of 30 over group ``a`` (10, 20, 30, 40, 60, NULL)."""
    metrics = {"ap": "APPROX_PERCENTILE(pq.amount, 0.5)", "pa": "percentile_approx(pq.amount, p=0.5)"}
    resp = await _run(engine, metrics=metrics, filters=["pq.grp = 'a'"])
    _compare(resp, keys=[], expected={(): {"ap": 30, "pa": 30}}, approx=lambda _: 1.0)


async def check_approx_exact(engine: SlayerQueryEngine) -> None:
    """Where the database has no approximate percentile, ``percentile_approx`` is ``percentile``."""
    metrics = {"ap": "percentile_approx(pq.amount, 0.9)", "pc": "percentile(pq.amount, 0.9)"}
    resp = await _run(engine, metrics=metrics, dimensions=["pq.grp"])
    expected = {key: {"ap": cont(_column("amount", rows), 0.9), "pc": cont(_column("amount", rows), 0.9)}
                for key, rows in _by(PQ, "grp").items()}
    _compare(resp, keys=["pq.grp"], expected=expected)


async def check_approx_types(engine: SlayerQueryEngine) -> None:
    """``percentile_approx`` over INTEGER and DECIMAL gives the float ``percentile`` gives."""
    metrics = {"ai": "percentile_approx(pq.amount, 0.5)", "ci": "percentile(pq.amount, 0.5)",
               "ad": "percentile_approx(pq.price, 0.5)", "cd": "percentile(pq.price, 0.5)"}
    resp = await _run(engine, metrics=metrics, filters=["pq.grp = 'a'"])
    (row,) = resp.data
    for approx, exact in (("ai", "ci"), ("ad", "cd")):
        assert isinstance(row[approx], float), (approx, row[approx], resp.sql)
        assert type(row[approx]) is type(row[exact]), (approx, row, resp.sql)
        assert row[approx] == pytest.approx(30, abs=1)


async def check_clickhouse_large_group(engine: SlayerQueryEngine) -> None:
    """Past ``quantile``'s 8192-row reservoir, ``percentile`` and ``median`` are still the interpolated value."""
    metrics = {"p90": "percentile(pq_big.x, 0.9)", "md": "median(pq_big.x)", "p10": "percentile(pq_big.x, p=0.1)"}
    resp = await _run(engine, metrics=metrics, dimensions=["pq_big.g"], population="pq_big")
    expected = {1: {"p90": cont(BIG_VALUES, 0.9), "md": cont(BIG_VALUES, 0.5), "p10": cont(BIG_VALUES, 0.1)}}
    _compare(resp, keys=["pq_big.g"], expected=expected)


async def check_clickhouse_null_groups(engine: SlayerQueryEngine) -> None:
    """All-NULL (nullable) and empty (non-nullable) inputs — numeric, date, timestamp — are NULL, never NaN or a
    default."""
    all_null = {"p": "percentile(pq_nulls.n, 0.5)", "md": "median(pq_nulls.n)",
                "d": "percentile_disc(pq_nulls.n, 0.5)", "dd": "percentile_disc(pq_nulls.d, 0.5)",
                "dt": "percentile_disc(pq_nulls.t, 0.5)"}
    resp = await _run(engine, metrics=all_null, population="pq_nulls")
    _compare(resp, keys=[], expected={(): dict.fromkeys(all_null)})
    empty = {"p": "percentile(pq_nulls.m, 0.5)", "md": "median(pq_nulls.m)", "d": "percentile_disc(pq_nulls.m, 0.5)",
             "dd": "percentile_disc(pq_nulls.dd, 0.5)", "pn": "percentile(pq_nulls.n, 0.5)",
             "dn": "percentile_disc(pq_nulls.d, 0.5)", "dtt": "percentile_disc(pq_nulls.tt, 0.5)",
             "dtn": "percentile_disc(pq_nulls.t, 0.5)"}
    resp = await _run(engine, metrics=empty, population="pq_nulls", filters=["pq_nulls.id < 0"])
    _compare(resp, keys=[], expected={(): dict.fromkeys(empty)})


#: Every check every server dialect runs.
COMMON_CHECKS: tuple[Callable[[SlayerQueryEngine], Any], ...] = (
    check_discrete, check_several_keys, check_text_and_date, check_empty, check_distinct, check_association,
    check_approx_tolerance,
)
