"""Stored Mode-B text is upgraded to the SQL string and identifier rules (spec: models/stored-upgrade)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import UnknownReferenceError
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory
from slayer.database import engine_factory
from slayer.storage.document_keys import dataset_in, keys_for
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    customers_v10,
    measure_total,
    memory_v2,
    orders_v10,
    query_backed_v10,
    raw_store,
    rev_query,
    run,
    seed_shop,
    stored_bytes,
)

#: (stored legacy text, the text it loads as).
TOKEN_CASES = [
    pytest.param('status == "paid"', "status = 'paid'", id="double-quoted"),
    pytest.param('name == "O\'Brien"', "name = 'O''Brien'", id="embedded-apostrophe"),
    pytest.param(r"name == 'O\'Brien'", "name = 'O''Brien'", id="escaped-apostrophe"),
    pytest.param('note == "tab\\there"', "note = 'tab\there'", id="control-escape"),
    pytest.param(r"path == 'C:\\temp'", r"path = 'C:\temp'", id="escaped-backslash"),
    pytest.param(r'path == r"C:\temp"', r"path = 'C:\temp'", id="raw-string"),
    pytest.param("note == 'say \"hi\"'", "note = 'say \"hi\"'", id="embedded-double-quote"),
    pytest.param('city == "café"', "city = 'café'", id="unicode"),
    pytest.param(r'city == "caf\u00e9"', "city = 'café'", id="unicode-escape"),
    pytest.param('note == "a | b"', "note = 'a | b'", id="pipe-inside-string"),
    pytest.param("note == \"a\" 'b'", "note = 'ab'", id="adjacent-literals"),
    pytest.param('note == """x"""', "note = 'x'", id="triple-quoted"),
    pytest.param("status | region", "status || region", id="pipe"),
    pytest.param("concat(status | region, \"-\")", "concat(status || region, '-')", id="pipe-and-string"),
    pytest.param("status == 'paid'", "status = 'paid'", id="already-sql"),
    pytest.param("sum(amount", "sum(amount", id="untokenisable"),
    pytest.param('status == "paid', 'status == "paid', id="unterminated"),
]


def _model_doc(*, formula: str, version: int = 15, **extra: Any) -> dict:
    doc = {**orders_v10(version=version), "measures": [{"name": "x", "formula": formula}], **extra}
    return dataset_in(doc, keys_for(entity="SlayerModel", version=version))


def _model_formula(text: str) -> str:
    return SlayerDataset.model_validate(_model_doc(formula=text)).metrics[0].expression


def _query(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"version": 6, "source_model": "orders", **fields})


class TestVersions:
    def test_versions_bumped(self) -> None:
        assert CURRENT_VERSIONS["SlayerModel"] >= 21
        assert CURRENT_VERSIONS["SlayerQuery"] >= 13
        assert CURRENT_VERSIONS["Memory"] >= 9


class TestTokenRewrite:
    @pytest.mark.parametrize(("stored", "loaded"), TOKEN_CASES)
    def test_model_measure_formula(self, stored: str, loaded: str) -> None:
        assert _model_formula(stored) == loaded

    @pytest.mark.parametrize(("stored", "loaded"), TOKEN_CASES)
    def test_query_filter(self, stored: str, loaded: str) -> None:
        assert _query(filters=[stored]).filter == [loaded]

    def test_current_version_document_is_never_rewritten(self) -> None:
        text = 'sum(iif(status == "paid", amount, 0))'
        doc = _model_doc(formula=text, version=CURRENT_VERSIONS["SlayerModel"])
        assert SlayerDataset.model_validate(doc).metrics[0].expression == text
        query = SlayerQuery.model_validate(
            {"version": CURRENT_VERSIONS["SlayerQuery"], "population": "orders", "filter": ['status == "paid"']})
        assert query.filter == ['status == "paid"']

    def test_post_string_rule_document_keeps_identifiers_and_drops_legacy_spellings(self) -> None:
        doc = _model_doc(formula='sum(iif(status == "paid", amount, 0))', version=16)
        assert SlayerDataset.model_validate(doc).metrics[0].expression == 'sum(iif(status = "paid", amount, 0))'


class TestEverySurface:
    def test_query_fields(self) -> None:
        query = _query(
            measures=['sum(iif(region == "EU", amount, 0))', {"formula": 'count_distinct(status | "x")', "name": "c"}],
            dimensions=['concat(status, "!")', {"expression": 'status | "-"', "name": "tag"}],
            filters=['status == "paid"'],
            order=[{"column": 'sum(iif(status == "paid", amount, 0))', "direction": "desc"}],
        )
        assert [m.metric for m in query.metrics or []] == [
            "sum(iif(region = 'EU', amount, 0))", "count_distinct(status || 'x')",
        ]
        assert [getattr(d, "expression") for d in query.dimensions or []] == ["concat(status, '!')", "status || '-'"]
        assert query.filter == ["status = 'paid'"]
        assert query.order is not None
        assert query.order[0].raw_expression == "sum(iif(status = 'paid', amount, 0))"

    def test_inline_extension_measures(self) -> None:
        query = _query(source_model={"source_name": "orders", "measures": [
            {"name": "paid", "formula": 'sum(iif(status == "paid", amount, 0))'}]}, measures=["paid"])
        assert query.population == "orders"
        (extension,) = query.extensions or []
        assert (extension.metrics or [])[0].expression == "sum(iif(status = 'paid', amount, 0))"

    def test_model_source_queries(self) -> None:
        doc = query_backed_v10(name="paid_rev", version=15, stages=[rev_query(filters=['status == "ok"'])])
        model = SlayerDataset.model_validate(doc)
        stage = (model.source_queries or [])[0]
        assert stage.filter == ["status = 'ok'"]

    def test_memory_query(self) -> None:
        mem = Memory.model_validate(memory_v2(memory_id="m", version=4, query=rev_query(filters=['status == "ok"'])))
        assert mem.query is not None
        assert mem.query.filter == ["status = 'ok'"]

    def test_mode_a_text_is_untouched(self) -> None:
        doc = _model_doc(formula="sum(amount)")
        doc["columns"] = [*doc["columns"], {"name": "total2", "sql": '"Order Total" * 2', "type": "DOUBLE"}]
        doc["filters"] = ['status = "x"']
        model = SlayerDataset.model_validate(doc)
        assert next(c for c in model.fields if c.name == "total2").sql == '"Order Total" * 2'
        assert model.filters == ['status = "x"']

    def test_unversioned_client_query_is_untouched(self) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "filter": ['status = "paid"']})
        assert query.filter == ['status = "paid"']


@pytest.fixture
def shop_ds(tmp_path):
    db = str(tmp_path / "shop.duckdb")
    seed_shop(db, dialect="duckdb")
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def _store(*, tmp_path, ds, backend: str, stage: dict, memories: list[dict] | None = None):
    qb = query_backed_v10(name="rev_q", stages=[stage])
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds,
                           models=[orders_v10(), customers_v10(), qb], memories=memories or [])


class TestStoredResultsUnchanged:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_double_quoted_filter_and_measure(self, tmp_path, shop_ds, backend: str) -> None:
        stage = {"source_model": "orders", "filters": ['status == "ok"'],
                 "measures": [{"formula": 'sum(iif(status == "ok", amount, 0))', "name": "rev"}]}
        storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend=backend, stage=stage)
        model = await storage.get_dataset("rev_q", data_source=DS)
        assert model is not None
        [loaded] = model.source_queries or []
        assert isinstance(loaded, SlayerQuery)
        assert loaded.filter == ["status = 'ok'"]
        assert loaded.metrics is not None
        assert loaded.metrics[0].metric == "sum(iif(status = 'ok', amount, 0))"
        resp = await run(storage, {"population": "rev_q", "metrics": ["sum(rev)"]})
        assert measure_total(resp, measure="rev_sum") == 80.0

    async def test_rewrite_precedes_the_legacy_time_literal_repair(self, tmp_path, shop_ds) -> None:
        stage = rev_query(filters=['ordered_at >= "2024-01-01T00:00:00Z"'])
        storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", stage=stage)
        model = await storage.get_dataset("rev_q", data_source=DS)
        assert model is not None
        stage = (model.source_queries or [])[0]
        assert stage.filter == ["ordered_at >= '2024-01-01T00:00:00'"]
        resp = await run(storage, {"population": "rev_q", "metrics": ["sum(rev)"]})
        assert measure_total(resp, measure="rev_sum") == 200.0

    async def test_pipe_concatenation_returns_the_same_values(self, tmp_path, shop_ds) -> None:
        stage = {"source_model": "orders", "dimensions": [{"expression": 'status | "#"', "name": "tag"}],
                 "measures": [{"formula": "sum(amount)", "name": "rev"}]}
        storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", stage=stage)
        resp = await run(storage, {"population": "rev_q", "dimensions": ["tag"], "metrics": ["sum(rev)"]})
        assert {r["rev_q.tag"]: float(r["rev_q.rev_sum"]) for r in resp.data} == {
            "ok#": 80.0, "new#": 70.0, "2024/01/01#": 60.0,
        }

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_memory_query(self, tmp_path, shop_ds, backend: str) -> None:
        query = rev_query(version=4, filters=['status == "ok"'])
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(), customers_v10()],
                                  memories=[memory_v2(memory_id="m_q", query=query)])
        mem = await storage.get_memory("m_q")
        assert mem.query is not None
        assert mem.query.filter == ["status = 'ok'"]
        assert measure_total(await run(storage, mem.query), measure="rev") == 80.0

    async def test_resave_writes_the_new_spelling(self, tmp_path, shop_ds) -> None:
        stage = rev_query(filters=['status == "ok"'])
        storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", stage=stage)
        model = await storage.get_dataset("rev_q", data_source=DS)
        assert model is not None
        await storage.save_dataset(model)
        assert '"ok"' not in stored_bytes(backend="yaml", base=str(tmp_path / "store"), name="rev_q")

    async def test_unversioned_client_query_fails_with_the_hint(self, tmp_path, shop_ds) -> None:
        storage = await _store(tmp_path=tmp_path, ds=shop_ds, backend="yaml", stage=rev_query())
        with pytest.raises(UnknownReferenceError) as info:
            await run(storage, {"population": "orders", "metrics": ["sum(amount)"], "filter": ['status = "ok"']})
        assert "single quote" in str(info.value).lower(), str(info.value)
