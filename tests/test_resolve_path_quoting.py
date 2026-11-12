"""``resolve_path`` / ``QueryScope.resolve`` honour per-hop quoting as the binder does: a quoted hop matches exactly."""

from __future__ import annotations

import pytest

from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.extensions import query_scopes, resolve_path

DS = "ds"


def _models() -> dict[str, SlayerModel]:
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source=DS,
        columns=[Column(name="order_id", primary_key=True), Column(name="customer_id")],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]], cardinality="many_to_one")],
    )
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source=DS,
        columns=[Column(name="id", primary_key=True), Column(name="region")],
        joins=[ModelJoin(target_model="Regions", join_pairs=[["region", "name"]], cardinality="many_to_one")],
    )
    regions = SlayerModel(
        name="Regions", sql_table="regions", data_source=DS,
        columns=[Column(name="name", primary_key=True), Column(name="opened_on", type="DATE")],
    )
    return {m.name: m for m in (orders, customers, regions)}


def _targets(resolved) -> list[str] | None:
    return None if resolved is None else [edge.target_model for edge in resolved[1]]


@pytest.mark.parametrize(("hops", "quoted", "expected"), [
    pytest.param(["CUSTOMERS"], (), ["customers"], id="unquoted-folds-case"),
    pytest.param(["CUSTOMERS"], (True,), None, id="quoted-wrong-case"),
    pytest.param(["customers"], (True,), ["customers"], id="quoted-exact"),
    pytest.param(["ORDERS", "customers"], (), ["customers"], id="unquoted-root-qualifier-dropped"),
    pytest.param(["ORDERS", "customers"], (True, False), None, id="quoted-root-qualifier-wrong-case-kept"),
    pytest.param(["customers", "REGIONS"], (False, True), None, id="quoted-second-hop-wrong-case"),
    pytest.param(["customers", "Regions"], (False, True), ["customers", "Regions"], id="quoted-second-hop-exact"),
    pytest.param(["customers", "regions"], (False, True), None, id="quoted-second-hop-lower-case"),
    pytest.param(["Regions"], (True,), ["customers", "Regions"], id="quoted-short-form-exact"),
])
def test_resolve_path_quoting(hops: list[str], quoted: tuple[bool, ...], expected: list[str] | None) -> None:
    models = _models()
    resolved = resolve_path(hops, root=models["orders"], models_by_name=models, quoted=quoted)
    assert _targets(resolved) == expected


@pytest.mark.parametrize(("hops", "expected"), [
    (["CUSTOMERS"], ["customers"]),
    (["ORDERS", "customers"], ["customers"]),
    (["customers", "regions"], ["customers", "Regions"]),
    (["Regions"], ["customers", "Regions"]),
])
def test_resolve_path_without_quoting_is_unchanged(hops: list[str], expected: list[str]) -> None:
    models = _models()
    assert _targets(resolve_path(hops, root=models["orders"], models_by_name=models)) == expected


@pytest.mark.parametrize(("hops", "quoted", "expected"), [
    pytest.param(["customers", "REGIONS"], (False, True), None, id="quoted-wrong-case"),
    pytest.param(["customers", "Regions"], (False, True), ["customers", "Regions"], id="quoted-exact"),
    pytest.param(["customers", "REGIONS"], (), ["customers", "Regions"], id="unquoted"),
])
def test_query_scope_resolve_quoting(hops: list[str], quoted: tuple[bool, ...], expected: list[str] | None) -> None:
    stage = SlayerQuery.model_validate({"source_model": "orders", "measures": ["count(*)"]})
    (scope,) = query_scopes([stage], models_by_ds={DS: _models()}, data_source=DS)[0]
    assert _targets(scope.resolve(hops, quoted=quoted)) == expected
