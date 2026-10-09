"""Narrowed access: a model reading a tagged model its own tags do not cover warns on save and in validation."""

from __future__ import annotations

import json
import re
import warnings
from collections.abc import AsyncGenerator

import pytest

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import SlayerWarning
from slayer.engine.query_engine import SlayerQueryEngine
from tests._model_access_fixtures import ACCESS_PARAMS, DS, AccessStore, access_store, mcp_text, must_get

NARROWED = {
    "hr_summary", "hr_rollup", "fin_hop", "fin_ordered", "fin_report",
    "pay", "pay_col", "pay_colfilter", "pay_filter", "pay_measure", "pay_agg",
}
NEVER_NARROWED = {"pub", "pub_link", "dangling"}


@pytest.fixture(params=ACCESS_PARAMS)
async def store(request) -> AsyncGenerator[AccessStore]:
    backend, dialect = request.param
    async with access_store(backend=backend, dialect=dialect) as s:
        yield s


@pytest.fixture
async def engine(store: AccessStore) -> AsyncGenerator[SlayerQueryEngine]:
    engine = SlayerQueryEngine(storage=store.storage)
    try:
        yield engine
    finally:
        engine.close()


def query_backed(name: str, *, source: str, formula: str, access_tags: list[str] | None = None) -> SlayerModel:
    stage = SlayerQuery.model_validate({"source_model": source, "measures": [{"formula": formula, "name": "v"}]})
    return SlayerModel(name=name, source_queries=[stage], access_tags=access_tags or [])


def mentions(text: str, name: str) -> bool:
    return re.search(rf"(?<![\w]){re.escape(name)}(?![\w])", text) is not None


async def narrowed_payloads(save) -> list[str]:
    """JSON of every ``narrowed_access`` warning payload ``save`` emits."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await save
    payloads = [p for w in caught if isinstance(p := getattr(w.message, "payload", None), SlayerWarning)]
    return [p.model_dump_json() for p in payloads if p.kind == "narrowed_access"]


class TestSaveWarning:
    async def test_public_reader_of_tagged_model_warns_and_saves(self, store: AccessStore, engine: SlayerQueryEngine) -> None:
        payloads = await narrowed_payloads(engine.save_model(query_backed("pay_report", source="hr", formula="sum(salary)")))
        assert len(payloads) == 1
        assert mentions(payloads[0], "hr")
        await must_get(store.storage, "pay_report")

    async def test_table_backed_reader_warns(self, store: AccessStore, engine: SlayerQueryEngine) -> None:
        pay = await must_get(store.storage, "pay_col")
        payloads = await narrowed_payloads(engine.save_model(pay.model_copy(update={"description": "edited"})))
        assert any(mentions(p, "hr") for p in payloads)

    async def test_covering_tags_do_not_warn(self, engine: SlayerQueryEngine) -> None:
        model = query_backed("hr_private", source="hr", formula="sum(salary)", access_tags=["hr"])
        assert await narrowed_payloads(engine.save_model(model)) == []

    async def test_tags_beyond_the_read_model_warn(self, engine: SlayerQueryEngine) -> None:
        model = query_backed("hr_wide", source="hr", formula="sum(salary)", access_tags=["hr", "fin"])
        payloads = await narrowed_payloads(engine.save_model(model))
        assert len(payloads) == 1
        assert mentions(payloads[0], "hr")

    async def test_transitive_reader_warns(self, engine: SlayerQueryEngine) -> None:
        payloads = await narrowed_payloads(engine.save_model(query_backed("rollup2", source="hr_summary", formula="sum(total)")))
        assert len(payloads) == 1
        assert mentions(payloads[0], "hr")

    async def test_reader_of_untagged_model_does_not_warn(self, engine: SlayerQueryEngine) -> None:
        assert await narrowed_payloads(engine.save_model(query_backed("pub_count", source="pub", formula="count(*)"))) == []

    async def test_mcp_create_surfaces_the_warning(self, store: AccessStore) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            text = await mcp_text(store.storage, "create_model", name="pay_report",
                                  query={"source_model": "hr", "measures": [{"formula": "sum(salary)", "name": "v"}]})
        assert "narrowed" in text.lower()
        assert mentions(text, "hr")


class TestRetagWarning:
    async def test_retagging_warns_per_newly_narrowed_reader(self, store: AccessStore, engine: SlayerQueryEngine) -> None:
        await engine.save_model(query_backed("pub_count", source="pub", formula="count(*)"))
        await engine.save_model(query_backed("pub_sum", source="pub", formula="count(*)"))
        pub = await must_get(store.storage, "pub")
        payloads = await narrowed_payloads(engine.save_model(pub.model_copy(update={"access_tags": ["ops"]})))
        assert len(payloads) == 2
        assert any(mentions(p, "pub_count") for p in payloads)
        assert any(mentions(p, "pub_sum") for p in payloads)

    async def test_already_narrowed_readers_do_not_warn_again(self, store: AccessStore, engine: SlayerQueryEngine) -> None:
        hr = await must_get(store.storage, "hr")
        payloads = await narrowed_payloads(engine.save_model(hr.model_copy(update={"description": "roster"})))
        assert payloads == []


class TestValidateModels:
    async def test_lists_every_narrowed_model(self, engine: SlayerQueryEngine) -> None:
        entries = await engine.validate_models(data_source=DS)
        findings = [d for d in (json.dumps(e.model_dump(mode="json")) for e in entries) if "narrowed" in d]
        for name in NARROWED:
            assert any(mentions(f, name) for f in findings), name
        for name in NEVER_NARROWED:
            assert not any(mentions(f, name) for f in findings), name

    async def test_finding_names_the_narrowing_models(self, engine: SlayerQueryEngine) -> None:
        entries = await engine.validate_models(data_source=DS)
        findings = [d for d in (json.dumps(e.model_dump(mode="json")) for e in entries) if "narrowed" in d]
        assert any(mentions(f, "hr_summary") and mentions(f, "hr") for f in findings)
        assert any(mentions(f, "fin_report") and mentions(f, "fin") for f in findings)
