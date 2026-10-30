"""Stored ``agg(distinct)`` is upgraded; text that is not old grammar is left alone (spec: models/stored-upgrade)."""

from __future__ import annotations

import logging
import warnings
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory
from slayer.sql import engine_factory
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._stored_upgrade_fixtures import BACKENDS, DS, memory_v2, measure_total, raw_store, run

_COLUMNS = [
    {"name": "order_id", "type": "INT", "primary_key": True},
    {"name": "customer_id", "type": "INT"},
    {"name": "status", "type": "TEXT"},
    {"name": "amount", "type": "DOUBLE"},
    {"name": "distinct", "type": "DOUBLE"},
]
#: sum of the ``distinct`` column.
DISTINCT_TOTAL = 8.0


def _model_doc(*, version: int, measures: list[dict], name: str = "orders", **extra: Any) -> dict:
    return {"version": version, "name": name, "sql_table": "orders", "data_source": DS, "columns": _COLUMNS,
            "measures": measures, **extra}


def _formulas(doc: dict) -> list[str]:
    return [m.formula for m in SlayerModel.model_validate(doc).measures]


def _query(*, version: int, **fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"version": version, "source_model": "orders", **fields})


class TestVersions:
    def test_versions_bumped(self) -> None:
        assert CURRENT_VERSIONS["SlayerModel"] == 18
        assert CURRENT_VERSIONS["SlayerQuery"] == 9
        assert CURRENT_VERSIONS["DatasourceConfig"] == 3
        assert CURRENT_VERSIONS["Memory"] == 7


#: (stored v17 text, the text it loads as).
DISTINCT_CASES = [
    pytest.param("sum(distinct)", 'sum("distinct")', id="sole-argument"),
    pytest.param("percentile(distinct, 0.5)", 'percentile("distinct", 0.5)', id="first-of-several"),
    pytest.param("weighted_avg(distinct , amount)", 'weighted_avg("distinct" , amount)', id="space-before-comma"),
    pytest.param("count(DISTINCT customer_id)", "count(DISTINCT customer_id)", id="modifier-untouched"),
    pytest.param("sum(distinct) / count(DISTINCT customer_id)", 'sum("distinct") / count(DISTINCT customer_id)',
                 id="beside-the-modifier"),
    pytest.param("sum('distinct')", "sum('distinct')", id="inside-a-string"),
]


class TestDistinctRewrite:
    @pytest.mark.parametrize(("stored", "loaded"), DISTINCT_CASES)
    def test_model_measure(self, stored: str, loaded: str) -> None:
        assert _formulas(_model_doc(version=17, measures=[{"name": "x", "formula": stored}])) == [loaded]

    @pytest.mark.parametrize(("stored", "loaded"), DISTINCT_CASES)
    def test_query_fields(self, stored: str, loaded: str) -> None:
        query = _query(version=8, measures=[stored], filters=[f"{stored} > 1"])
        assert [m.formula for m in query.measures or []] == [loaded]
        assert query.filters == [f"{loaded} > 1"]

    def test_saved_query_stage(self) -> None:
        doc = {"version": 17, "name": "rev_q", "data_source": DS,
               "source_queries": [{"version": 8, "source_model": "orders",
                                   "measures": [{"formula": "sum(distinct)", "name": "d"}]}]}
        model = SlayerModel.model_validate(doc)
        stage = (model.source_queries or [])[0]
        assert [m.formula for m in stage.measures or []] == ['sum("distinct")']

    def test_memory_query(self) -> None:
        query = {"version": 8, "source_model": "orders", "measures": ["sum(distinct)"]}
        mem = Memory.model_validate(memory_v2(memory_id="m", version=6, query=query))
        assert mem.query is not None
        assert [m.formula for m in mem.query.measures or []] == ['sum("distinct")']

    def test_a_current_document_is_not_rewritten(self) -> None:
        assert _formulas(_model_doc(version=18, measures=[{"name": "x", "formula": "sum(distinct)"}])) == [
            "sum(distinct)"]

    def test_a_client_query_is_not_rewritten(self) -> None:
        query = SlayerQuery.model_validate({"source_model": "orders", "measures": ["sum(distinct)"]})
        assert [m.formula for m in query.measures or []] == ["sum(distinct)"]


@pytest.fixture
def distinct_ds(tmp_path: Path) -> Generator[DatasourceConfig]:
    duckdb = pytest.importorskip("duckdb")
    db = str(tmp_path / "shop.duckdb")
    con = duckdb.connect(db)
    try:
        con.execute('CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, status TEXT, amount DOUBLE, '
                    '"distinct" DOUBLE)')
        con.execute("INSERT INTO orders VALUES (1, 1, 'ok', 10, 1.5), (2, 1, 'new', 20, 2.5), (3, 2, 'ok', 30, 4.0)")
    finally:
        con.close()
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


class TestDistinctResults:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_stored_measure_sums_the_column(self, tmp_path: Path, distinct_ds: DatasourceConfig,
                                                  backend: str) -> None:
        doc = _model_doc(version=17, measures=[{"name": "total", "formula": "sum(distinct)"}])
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=distinct_ds,
                                  models=[doc])
        model = await storage.get_model("orders", data_source=DS)
        assert model is not None
        assert [m.formula for m in model.measures] == ['sum("distinct")']
        resp = await run(storage, {"source_model": "orders", "measures": ["total"]})
        assert measure_total(resp, measure="total") == DISTINCT_TOTAL

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_memory_query_sums_the_column(self, tmp_path: Path, distinct_ds: DatasourceConfig,
                                                backend: str) -> None:
        query = {"version": 8, "source_model": "orders", "measures": [{"formula": "sum(distinct)", "name": "d"}]}
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=distinct_ds,
                                  models=[_model_doc(version=17, measures=[])],
                                  memories=[memory_v2(memory_id="m_d", version=6, query=query)])
        mem = await storage.get_memory("m_d")
        assert mem.query is not None
        assert measure_total(await run(storage, mem.query), measure="d") == DISTINCT_TOTAL


@contextmanager
def _messages(caplog: pytest.LogCaptureFixture) -> Generator[list[str]]:
    """Every warning raised or logged inside the block, as text."""
    out: list[str] = []
    with caplog.at_level(logging.WARNING), warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield out
    out += [str(w.message) for w in caught] + [r.getMessage() for r in caplog.records]


def _warned(messages: list[str], *, document: str, field: tuple[str, ...]) -> bool:
    """Some message names ``document`` and one of the spellings of ``field``."""
    return any(document in m and any(f in m for f in field) for m in messages)


#: (measure name, stored text the old grammar could not have held).
NEW_ONLY = [
    ("m_cast_op", "sum(x::int)"),
    ("m_concat", "sum(a || b)"),
    ("m_cast", "sum(CAST(x AS INT))"),
    ("m_filter", "sum(x) FILTER (WHERE y > 1)"),
    ("m_doubled_quote", "sum(x) FILTER (WHERE name = 'O''Brien')"),
    ("m_quoted_name", 'sum("order total"::int)'),
    ("m_backslash", r"count(*) FILTER (WHERE path = 'C:\temp')"),
]
BETWEEN = "x BETWEEN 1 AND 2"


class TestOldGrammarGate:
    def test_text_already_in_the_current_grammar_is_left_alone(self, caplog: pytest.LogCaptureFixture) -> None:
        doc = _model_doc(version=15, name="gate_probe", measures=[{"name": n, "formula": t} for n, t in NEW_ONLY],
                         filters=[BETWEEN])
        with _messages(caplog) as messages:
            model = SlayerModel.model_validate(doc)
        assert [m.formula for m in model.measures] == [t for _, t in NEW_ONLY]
        assert model.filters == [BETWEEN]
        missing = [n for n, t in NEW_ONLY if not _warned(messages, document="gate_probe", field=(n, t))]
        assert missing == [], messages

    def test_genuine_old_text_is_still_upgraded(self, caplog: pytest.LogCaptureFixture) -> None:
        doc = _model_doc(version=15, name="gate_probe", measures=[
            {"name": "m_cast", "formula": "sum(CAST(x AS INT))"},
            {"name": "m_old", "formula": 'sum(iif(status == "paid", amount, 0))'},
        ])
        with _messages(caplog) as messages:
            formulas = _formulas(doc)
        assert formulas == ["sum(CAST(x AS INT))", "sum(iif(status == 'paid', amount, 0))"]
        assert not _warned(messages, document="gate_probe", field=("m_old", 'status == "paid"')), messages

    def test_the_warning_names_each_field(self, caplog: pytest.LogCaptureFixture) -> None:
        with _messages(caplog) as messages:
            _query(version=6, name="gate_q", filters=[BETWEEN, "y > 0", BETWEEN])
        assert [m for m in messages if "filters[0]" in m], messages
        assert [m for m in messages if "filters[2]" in m], messages

    @pytest.mark.parametrize("pad", [" ", "\n  "], ids=["space", "newline"])
    def test_old_text_with_leading_whitespace_is_upgraded(self, pad: str) -> None:
        query = _query(version=6, filters=[f'{pad}status == "paid"'])
        assert query.filters == [f"{pad}status == 'paid'"]

    def test_saved_query(self, caplog: pytest.LogCaptureFixture) -> None:
        with _messages(caplog) as messages:
            query = _query(version=6, name="gate_q", filters=[BETWEEN, 'status == "paid"'],
                           dimensions=[{"expression": "status | region", "name": "tag"}])
        assert query.filters == [BETWEEN, "status == 'paid'"]
        assert [getattr(d, "expression") for d in query.dimensions or []] == ["status || region"]
        assert _warned(messages, document="gate_q", field=(BETWEEN, "filters")), messages

    def test_memory(self, caplog: pytest.LogCaptureFixture) -> None:
        query = {"version": 6, "source_model": "orders", "filters": [BETWEEN],
                 "measures": [{"formula": "sum(a || b)", "name": "ab"}]}
        with _messages(caplog) as messages:
            mem = Memory.model_validate(memory_v2(memory_id="m_gate", version=4, query=query))
        assert mem.query is not None
        assert mem.query.filters == [BETWEEN]
        assert [m.formula for m in mem.query.measures or []] == ["sum(a || b)"]
        assert _warned(messages, document="m_gate", field=(BETWEEN, "filters")), messages
        assert _warned(messages, document="m_gate", field=("sum(a || b)", "ab")), messages
