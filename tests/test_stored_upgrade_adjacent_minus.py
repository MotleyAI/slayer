"""Stored pre-grammar text with adjacent minus signs keeps its meaning (spec: models/stored-upgrade)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.syntax import parse_expr
from slayer.sql import engine_factory
from slayer.storage.sql_string_migration import sql_quote_strings
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    customers_v10,
    measure_total,
    orders_v10,
    query_backed_v10,
    raw_store,
    run,
    seed_shop,
)

#: (stored legacy text, the text it loads as).
CASES = [
    pytest.param("sum(amount) --sum(refund)", "sum(amount) - -sum(refund)", id="spaced-before"),
    pytest.param("sum(a--b)", "sum(a- -b)", id="touching"),
    pytest.param("sum(a- -b)", "sum(a- -b)", id="already-spaced"),
    pytest.param("sum(a - - b)", "sum(a - - b)", id="already-spaced-wide"),
    pytest.param("sum(a---b)", "sum(a- - -b)", id="three"),
    pytest.param("sum(a) + (sum(b)\n--sum(c))", "sum(a) + (sum(b)\n- -sum(c))", id="multi-line"),
    pytest.param("sum(a) -\n-sum(b)", "sum(a) -\n-sum(b)", id="split-across-lines"),
    pytest.param('note == "a--b"', "note == 'a--b'", id="inside-string"),
    pytest.param("sum(a)  # x--y", "sum(a)  -- x--y", id="inside-python-comment"),
    pytest.param("sum(a--b)  # net", "sum(a- -b)  -- net", id="before-python-comment"),
    pytest.param('concat(status, "--") --x', "concat(status, '--') - -x", id="string-and-minus"),
    pytest.param("sum(a) + (sum(b)-# c\n sum(c))", "sum(a) + (sum(b)- -- c\n sum(c))", id="python-comment-after-minus"),
    pytest.param('concat("a" # note\n "b")', "concat('ab')", id="python-comment-inside-a-string-run"),
    pytest.param("(sum(a) # x\u2028y\n + sum(b)) # end", "(sum(a) -- x\u2028y\n + sum(b)) -- end", id="unicode-line-separator"),
]


@pytest.mark.parametrize(("stored", "loaded"), CASES)
def test_token_rewrite(stored: str, loaded: str) -> None:
    assert sql_quote_strings(stored) == loaded


@pytest.mark.parametrize(("stored", "loaded"), CASES)
def test_loaded_text_parses(stored: str, loaded: str) -> None:
    parse_expr(sql_quote_strings(stored))


@pytest.mark.parametrize(("stored", "loaded"), CASES)
def test_model_measure_formula(stored: str, loaded: str) -> None:
    doc = {**orders_v10(version=15), "measures": [{"name": "x", "formula": stored}]}
    assert SlayerModel.model_validate(doc).measures[0].formula == loaded


@pytest.mark.parametrize(("stored", "loaded"), CASES)
def test_query_filter(stored: str, loaded: str) -> None:
    query = SlayerQuery.model_validate({"version": 6, "source_model": "orders", "filters": [stored]})
    assert query.filters == [loaded]


def test_current_version_text_is_never_rewritten() -> None:
    doc = {**orders_v10(version=16), "measures": [{"name": "x", "formula": "sum(a) --note"}]}
    assert SlayerModel.model_validate(doc).measures[0].formula == "sum(a) --note"


@pytest.fixture
def shop_ds(tmp_path):
    db = str(tmp_path / "shop.duckdb")
    seed_shop(db, dialect="duckdb")
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_stored_double_negation_returns_its_old_value(tmp_path, shop_ds, backend: str) -> None:
    stage: dict[str, Any] = {"source_model": "orders",
                             "measures": [{"formula": "sum(amount) --sum(amount)", "name": "rev"}]}
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                              models=[orders_v10(), customers_v10(), query_backed_v10(name="rev_q", stages=[stage])])
    total = measure_total(await run(storage, {"source_model": "orders", "measures": [{"formula": "sum(amount)",
                                                                                     "name": "t"}]}), measure="t")
    resp = await run(storage, {"source_model": "rev_q", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == 2 * total
