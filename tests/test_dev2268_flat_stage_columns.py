"""A joined stage's flattened columns are read by their flat names; a dotted spelling is refused with the flat name."""

from __future__ import annotations

from typing import Any

import pytest

import slayer.core.scope as scope_module
from slayer.core.enums import DataType
from slayer.core.errors import (
    IllegalScopeReferenceError,
    UnknownReferenceError,
    UnrelatedDatasetError,
    UnresolvableDimensionJoinError,
)
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from tests._dev2102_fixtures import DIALECTS, DS, customers, ext, join, orders, q, rows, shop, with_models

_N = {"formula": "count(*)", "name": "n"}
_S = {"name": "s", "source_model": "orders", "dimensions": ["customers.name"], "measures": [_N]}
_ISSUE_PAIRS = [["id", "n"]]
_REGION_PAIRS = [["region_id", "id"]]


def _joined(root: str, ref: str, *, stage: dict | None = None, pairs: list[list[str]] | None = None,
            leading: str = "id") -> list[SlayerQuery | dict]:
    """Stage ``s``, then a query rooted at ``root`` that joins ``s`` and selects ``ref``."""
    return [q(**(stage or _S)), q(source_model=root, dimensions=[leading, ref],
                                  extensions=[ext(root, joins=[join("s", pairs or _ISSUE_PAIRS)])])]


async def _refused(payload: Any, *, models: list[SlayerModel] | None = None,
                   exc: type[Exception] = UnknownReferenceError) -> Any:
    async with shop("sqlite", models) as engine:
        with pytest.raises(exc) as ei:
            await engine.execute(payload)
    return ei.value


def _hint(err: Exception) -> str:
    return (getattr(err, "suggestion", None) or "") + (getattr(err, "suggested_path", None) or "")


# --------------------------------------------------------------------------- #
# The flat spelling executes.
# --------------------------------------------------------------------------- #
_KEYED = {**_S, "dimensions": ["customer_id", "customers.name"]}
_BY_REGION = {**_S, "dimensions": ["customers.region_id", "customers.name"]}
_BY_CUSTOMER = [(11, "Ann"), (12, "Bob"), (13, "Eve"), (14, "Bob")]
_CUSTOMER_PAIRS = [["id", "customer_id"]]


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize(("payload", "keys", "expected"), [
    pytest.param(_joined("addresses", "s.customers__name", stage=_KEYED, pairs=[["customer_id", "customer_id"]]),
                 ("addresses.id", "addresses.s.customers__name"), _BY_CUSTOMER, id="stored-addresses"),
    pytest.param(_joined("regions", "s.customers__name", stage=_BY_REGION, pairs=[["id", "customers__region_id"]]),
                 ("regions.id", "regions.s.customers__name"), [(1, "Ann"), (2, "Bob"), (3, "Eve")],
                 id="stored-regions"),
    pytest.param(_joined("customers", "s.customers__name", stage=_KEYED, pairs=_CUSTOMER_PAIRS),
                 ("customers.id", "customers.s.customers__name"),
                 [(1, "Ann"), (2, "Bob"), (3, "Cid"), (4, "Dee"), (5, "Eve")], id="stored-customers"),
    pytest.param([q(name="s", source_model="orders", dimensions=["customer_id"]),
                  q(source_model="addresses", dimensions=["id", "s.customers.name"], extensions=[
                      ext("addresses", joins=[join("s", [["customer_id", "customer_id"]])]),
                      ext("s", joins=[join("customers", [["customer_id", "id"]])])])],
                 ("addresses.id", "addresses.s.customers.name"), _BY_CUSTOMER, id="genuine-join-through-a-stage"),
])
async def test_a_joined_stage_column_executes(dialect: str, payload: list[SlayerQuery | dict], keys: tuple[str, str],
                                              expected: list[tuple]) -> None:
    async with shop(dialect) as engine:
        resp = await engine.execute(payload)
    assert rows(resp, *keys) == expected


# --------------------------------------------------------------------------- #
# The dotted spelling is refused with the flat name.
# --------------------------------------------------------------------------- #
async def test_a_stored_roots_dotted_spelling_suggests_the_flat_column() -> None:
    err = await _refused(_joined("addresses", "s.customers.name"))
    assert "'s'" in err.scope_summary
    assert "customers__name" in err.scope_summary
    assert "s.customers__name" in err.suggestion
    assert "__slayer_" not in str(err)


async def test_a_routable_target_does_not_override_the_flat_column() -> None:
    models = with_models(customers(ModelJoin(target_model="regions", join_pairs=_REGION_PAIRS)))
    err = await _refused(_joined("regions", "s.customers.name"), models=models)
    assert "s.customers__name" in err.suggestion
    assert "'customers.name'" not in str(err)


async def test_a_hop_back_to_the_root_is_not_a_circular_join() -> None:
    err = await _refused(_joined("customers", "s.customers.name", stage=_KEYED, pairs=_CUSTOMER_PAIRS))
    assert "s.customers__name" in err.suggestion


def _authored(name: str, flat: str, *joins: ModelJoin) -> SlayerModel:
    return SlayerModel(name=name, data_source=DS, sql_table="customers", joins=list(joins), columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name=flat, sql="name", type=DataType.TEXT)])


def _root_joining(target: str, ref: str) -> SlayerQuery:
    return q(source_model="addresses", dimensions=["id", ref],
             extensions=[ext("addresses", joins=[join(target, [["customer_id", "id"]])])])


async def test_the_longest_flat_name_wins() -> None:
    m1 = _authored("m1", "a__b__c", ModelJoin(target_model="m2", name="a", join_pairs=[["id", "id"]]))
    err = await _refused(_root_joining("m1", "m1.a.b.c"), models=with_models(m1, _authored("m2", "b__c")))
    assert "'m1'" in err.scope_summary
    assert "m1.a__b__c" in err.suggestion
    assert "m1.a.b__c" not in err.suggestion


async def test_an_authored_models_literal_flat_column_is_suggested() -> None:
    err = await _refused(_root_joining("m", "m.a.b"), models=with_models(_authored("m", "a__b")))
    assert "'m'" in err.scope_summary
    assert "m.a__b" in err.suggestion


def _hr(*names: str) -> list[SlayerModel]:
    """``customers → regions`` under each named edge, so ``customers__regions__name`` respells each."""
    return with_models(customers(*(ModelJoin(target_model="regions", name=n, join_pairs=_REGION_PAIRS)
                                   for n in names)))


async def test_a_stale_respelling_suggests_the_canonical_column(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[Any] = []
    monkeypatch.setattr(scope_module, "record_stale_spelling", lambda **kw: recorded.append(kw))
    stage = {**_S, "dimensions": ["customers.hr.name"]}
    err = await _refused(_joined("addresses", "s.customers.regions.name", stage=stage), models=_hr("hr"))
    assert "s.customers__hr__name" in err.suggestion
    assert recorded == []


async def test_an_ambiguous_respelling_gives_no_hint() -> None:
    stage = {**_S, "dimensions": ["customers.hr.name", "customers.hr2.name"]}
    err = await _refused(_joined("addresses", "s.customers.regions.name", stage=stage), models=_hr("hr", "hr2"),
                         exc=UnresolvableDimensionJoinError)
    assert err.reference == "s.customers.regions.name"
    assert "customers__" not in _hint(err)


@pytest.mark.parametrize(("ref", "hint"), [
    ('s."customers"."name"', 's."customers__name"'),
    ('s.customers."name"', 's."customers__name"'),
    ("s.CUSTOMERS.NAME", "s.customers__name"),
], ids=["quoted", "leaf-quoted", "case-folded"])
async def test_the_flat_hint_follows_quoting(ref: str, hint: str) -> None:
    err = await _refused(_joined("addresses", ref))
    assert hint in err.suggestion


@pytest.mark.parametrize(("ref", "reference"), [
    ('s."Customers".name', "s.Customers.name"),
    ('s.customers."Name"', "s.customers.Name"),
], ids=["hop", "leaf"])
async def test_a_quoted_segment_in_the_wrong_case_gives_no_hint(ref: str, reference: str) -> None:
    err = await _refused(_joined("addresses", ref), exc=UnresolvableDimensionJoinError)
    assert err.reference == reference
    assert "customers__name" not in _hint(err)


async def test_a_routing_short_form_is_not_pre_empted_by_a_flat_column() -> None:
    flat = orders().model_copy(update={"columns": [
        *orders().columns, Column(name="regions__name", sql="status", type=DataType.TEXT)]})
    models = with_models(flat, customers(ModelJoin(target_model="regions", join_pairs=_REGION_PAIRS)))
    async with shop("sqlite", models) as engine:
        resp = await engine.execute(q(source_model="orders", dimensions=["id", "regions.name"]))
    assert rows(resp, "orders.id", "orders.customers.regions.name") == [
        (1, "North"), (2, "North"), (3, "South"), (4, None), (5, None), (6, "East"), (7, None)]


async def test_a_downstream_stages_dotted_spelling_names_the_flat_column() -> None:
    err = await _refused([q(**_S), q(source_model="s", dimensions=["s.customers.name"])],
                         exc=IllegalScopeReferenceError)
    assert "customers__name" in err.reason


async def test_a_downstream_dotted_spelling_shaped_like_a_list_element_names_the_flat_column() -> None:
    valued = customers().model_copy(update={"columns": [
        *customers().columns, Column(name="value", sql="credit", type=DataType.DOUBLE)]})
    stage = {**_S, "dimensions": ["customers.value"]}
    err = await _refused([q(**stage), q(source_model="s", dimensions=["s.customers.value"])],
                         models=with_models(valued), exc=IllegalScopeReferenceError)
    assert "customers__value" in err.reason


async def test_an_unrelated_short_form_suggests_the_populations_flat_column() -> None:
    err = await _refused([q(**_S), q(source_model="s", dimensions=["customers.name"])], exc=UnrelatedDatasetError)
    assert "customers__name" in err.suggestion
    assert err.suggestion.index("customers__name") < err.suggestion.index("max(")


# --------------------------------------------------------------------------- #
# Reference errors name models as the user spells them.
# --------------------------------------------------------------------------- #
async def test_an_unknown_field_of_a_joined_stage_names_the_stage() -> None:
    err = await _refused(_joined("addresses", "s.nope"))
    assert "'s'" in err.scope_summary or "'s'" in (err.suggestion or "")
    assert "customers__name" in str(err)
    assert "__slayer_" not in str(err)


async def test_an_unknown_field_of_a_stage_source_names_the_stage() -> None:
    err = await _refused([q(**_S), q(source_model="s", dimensions=["nope"])])
    assert "'s'" in str(err)
    assert "__slayer_" not in str(err)


@pytest.mark.parametrize("payload", [
    _joined("addresses", "s.customers.nope"),
    [q(**_S), q(source_model="addresses", dimensions=["id"], measures=[{"formula": "count(s.customers.*)", "name": "c"}],
                extensions=[ext("addresses", joins=[join("s", _ISSUE_PAIRS)])])],
], ids=["column", "star"])
async def test_a_broken_chain_through_a_joined_stage_names_its_columns(payload: list[SlayerQuery | dict]) -> None:
    err = await _refused(payload, exc=UnresolvableDimensionJoinError)
    msg = str(err)
    assert "'s'" in msg
    assert "customers__name" in msg
    assert "__slayer_" not in msg
