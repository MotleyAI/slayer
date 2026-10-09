"""Narrowed-access findings in validation, apply and CLI output; the reads walk's parameter defaults and computed dimensions."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator

import pytest

from slayer.cli import _format_validate_models_output
from slayer.core.models import Aggregation, AggregationParam
from slayer.core.query import SlayerQuery
from slayer.engine.param_binding import default_reference_sites
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import NarrowedAccessFinding
from slayer.memories.resolver import extract_entities_from_query
from tests._model_access_fixtures import DS, AccessStore, access_store, mcp_text


@pytest.fixture
async def store() -> AsyncGenerator[AccessStore]:
    async with access_store(backend="yaml", dialect="sqlite") as s:
        yield s


@pytest.fixture
async def engine(store: AccessStore) -> AsyncGenerator[SlayerQueryEngine]:
    engine = SlayerQueryEngine(storage=store.storage)
    try:
        yield engine
    finally:
        engine.close()


def test_default_reference_sites() -> None:
    agg = Aggregation(name="weighted", formula="SUM({value} * {weight})", params=[
        AggregationParam(name="weight", sql="hr.salary * fx.rate"), AggregationParam(name="floor", sql="0"),
    ])
    assert default_reference_sites(agg) == [(("hr",), "salary"), (("fx",), "rate")]


def test_cli_renders_a_narrowed_finding() -> None:
    finding = NarrowedAccessFinding(model_name="pay", data_source=DS, narrowed_by=[f"{DS}.hr"])
    assert _format_validate_models_output([finding]) == f"NARROWED ACCESS: pay (datasource: {DS}) reads {DS}.hr"


async def test_apply_skips_findings_and_reports_none_as_residual(engine: SlayerQueryEngine) -> None:
    entries = await engine.validate_models(data_source=DS)
    findings = [e for e in entries if isinstance(e, NarrowedAccessFinding)]
    assert findings
    result = await engine.apply_drift_deletes(findings)
    assert result.applied == []
    assert result.errors == []
    assert not any(isinstance(e, NarrowedAccessFinding) for e in result.residual)


async def test_mcp_edit_reports_newly_narrowed_readers(store: AccessStore, engine: SlayerQueryEngine) -> None:
    await engine.create_model_from_query(
        query={"source_model": "pub", "measures": [{"formula": "count(*)", "name": "n"}]}, name="pub_count",
    )
    reply = json.loads(await mcp_text(store.storage, "edit_model", model_name="pub", access_tags=["ops"]))
    assert any("narrowed" in w and "pub_count" in w for w in reply["warnings"])


async def test_order_on_a_measure_alias_is_not_an_entity(store: AccessStore) -> None:
    query = SlayerQuery.model_validate({
        "source_model": "fin", "measures": [{"formula": "sum(amount)", "name": "spend"}],
        "order": [{"column": "spend", "direction": "desc"}, {"column": "staff_id", "direction": "asc"}],
    })
    resolution = await extract_entities_from_query(query, storage=store.storage)
    assert set(resolution.canonical_forms) == {f"{DS}.fin", f"{DS}.fin.amount", f"{DS}.fin.staff_id"}


async def test_computed_dimension_entities(store: AccessStore) -> None:
    query = SlayerQuery.model_validate({
        "source_model": "fin", "dimensions": [{"expression": "amount * 2", "name": "dbl"}],
    })
    resolution = await extract_entities_from_query(query, storage=store.storage)
    assert f"{DS}.fin.amount" in resolution.canonical_forms
