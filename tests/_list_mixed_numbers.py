"""An INT list beside a DOUBLE list, per server dialect (spec: queries/list-functions › Mixed numeric types).

``mixed``: id 1 has scores ``[2, 3]`` / prices ``[2.0]``; id 2 has ``[5]`` / ``[7.5]``.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

_ROWS: tuple[tuple[int, tuple[int, ...], tuple[float, ...]], ...] = ((1, (2, 3), (2.0,)), (2, (5,), (7.5,)))
#: dialect → (id, INT list, DOUBLE list) column types.
_TYPES: dict[str, tuple[str, str, str]] = {
    "postgres": ("INTEGER", "INTEGER[]", "DOUBLE PRECISION[]"),
    "trino": ("INTEGER", "ARRAY(INTEGER)", "ARRAY(DOUBLE)"),
    "clickhouse": ("Int32", "Array(Int32)", "Array(Float64)"),
    "bigquery": ("INT64", "ARRAY<INT64>", "ARRAY<FLOAT64>"),
    "snowflake": ("NUMBER(38,0)", "ARRAY", "ARRAY"),
}
_SUFFIX = {"clickhouse": " ENGINE = MergeTree ORDER BY tuple()"}
#: Each filter keeps id 1 only.
MIXED_FILTERS = ("contains(scores, 2.0)", "contains_any(scores, prices)", "contains_any(prices, scores)")


def _list(values: tuple[float, ...], *, dialect: str, sql_type: str, double: bool) -> str:
    items = ", ".join(repr(v) for v in values)
    if dialect == "postgres":
        return f"ARRAY[{items}]::{sql_type}"
    if dialect == "trino":
        return f"CAST(ARRAY[{items}] AS {sql_type})"
    if dialect == "bigquery":
        return f"{sql_type}[{items}]"
    if dialect == "snowflake":  # REAL elements, so VARIANT compares FIXED with REAL
        return "ARRAY_CONSTRUCT(" + ", ".join(f"{v}::FLOAT" if double else str(v) for v in values) + ")"
    return f"[{items}]"


def mixed_numbers_statements(*, dialect: str, prefix: str = "") -> list[str]:
    """CREATE + INSERT of ``mixed`` on a server ``dialect``."""
    id_type, int_list, double_list = _TYPES[dialect]
    name = f"{prefix}mixed"
    rows = [(str(rid), _list(scores, dialect=dialect, sql_type=int_list, double=False),
             _list(prices, dialect=dialect, sql_type=double_list, double=True)) for rid, scores, prices in _ROWS]
    if dialect == "snowflake":  # ARRAY_CONSTRUCT is not allowed in VALUES
        insert = f"INSERT INTO {name} " + " UNION ALL ".join("SELECT " + ", ".join(r) for r in rows)
    else:
        insert = f"INSERT INTO {name} VALUES " + ", ".join("(" + ", ".join(r) + ")" for r in rows)
    return [f"CREATE TABLE {name} (id {id_type}, scores {int_list}, prices {double_list}){_SUFFIX.get(dialect, '')}",
            insert]


def mixed_numbers_model(*, data_source: str, explicit_sql: bool = False) -> SlayerModel:
    columns = [Column(name="id", type=DataType.INT, primary_key=True),
               Column(name="scores", type=DataType.ARRAY, element_type=DataType.INT),
               Column(name="prices", type=DataType.ARRAY, element_type=DataType.DOUBLE)]
    if explicit_sql:
        columns = [c.model_copy(update={"sql": c.name}) for c in columns]
    return SlayerModel(name="mixed", sql_table="mixed", data_source=data_source, columns=columns)


async def check_mixed_numbers(engine: SlayerQueryEngine) -> None:
    """Every ``MIXED_FILTERS`` entry keeps id 1 alone."""
    failures: list[str] = []
    for flt in MIXED_FILTERS:
        resp = await engine.execute(SlayerQuery.model_validate(
            {"source_model": "mixed", "dimensions": ["id"], "filters": [flt]}))
        kept = {int(r["mixed.id"]) for r in resp.data}
        if kept != {1}:
            failures.append(f"{flt}: kept {sorted(kept)}\nSQL:\n{resp.sql}")
    assert not failures, "\n\n".join(failures)
