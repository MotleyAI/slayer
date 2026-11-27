"""``resolve_path`` / ``resolve_anchored_path`` honour per-hop quoting: a quoted segment matches its exact spelling, else
its Ossie normalized form."""

from __future__ import annotations

from typing import Optional, Sequence

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousReferenceError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.extensions import resolve_anchored_path, resolve_path

DS = "ds"


def _dataset(name: str, *, fields: Sequence[str], relationships: Sequence[DatasetRelationship] = ()) -> SlayerDataset:
    return SlayerDataset(
        name=name, sql_table=name.lower(), data_source=DS,
        fields=[DatasetField(name=f, expression=f, datatype=DataType.STRING, primary_key=i == 0)
                for i, f in enumerate(fields)],
        relationships=list(relationships),
    )


def _rel(to: str, *, src: str, dst: str, name: Optional[str] = None) -> DatasetRelationship:
    return DatasetRelationship(to=to, from_columns=[src], to_columns=[dst], name=name)


def _chain() -> dict[str, SlayerDataset]:
    """orders → customers → Regions; customers → Offices over the edge named ``Branch``."""
    datasets = [
        _dataset("orders", fields=["order_id", "customer_id"], relationships=[_rel("customers", src="customer_id", dst="id")]),
        _dataset("customers", fields=["id", "region", "office_id"], relationships=[
            _rel("Regions", src="region", dst="name"), _rel("Offices", src="office_id", dst="id", name="Branch")]),
        _dataset("Regions", fields=["name", "opened_on"]),
        _dataset("Offices", fields=["id"]),
    ]
    return {d.name: d for d in datasets}


def _colliding() -> dict[str, SlayerDataset]:
    """``orders`` relates to two datasets whose names differ only in case."""
    datasets = [
        _dataset("orders", fields=["order_id", "region", "area"], relationships=[
            _rel("Regions", src="region", dst="name"), _rel("regions", src="area", dst="name")]),
        _dataset("Regions", fields=["name"]),
        _dataset("regions", fields=["name"]),
    ]
    return {d.name: d for d in datasets}


def _targets(resolved) -> Optional[list[str]]:
    return None if resolved is None else [edge.to for edge in resolved[1]]


PATH_CASES = [
    pytest.param(_chain, ["customers"], (True,), ["customers"], id="quoted-hop-exact"),
    pytest.param(_chain, ["Customers"], (True,), None, id="quoted-hop-wrong-case"),
    pytest.param(_chain, ["orders", "customers"], (True, False), ["customers"], id="quoted-root-qualifier-exact"),
    pytest.param(_chain, ["Orders", "customers"], (True, False), None, id="quoted-root-qualifier-wrong-case"),
    pytest.param(_chain, ["customers", "Branch"], (False, True), ["customers", "Offices"], id="quoted-edge-name-exact"),
    pytest.param(_chain, ["customers", "branch"], (False, True), None, id="quoted-edge-name-wrong-case"),
    pytest.param(_chain, ["Regions"], (True,), ["customers", "Regions"], id="quoted-short-form-exact"),
    pytest.param(_chain, ["REGIONS"], (True,), None, id="quoted-short-form-wrong-case"),
    pytest.param(_chain, ["customers", "Regions"], (False, True), ["customers", "Regions"], id="mixed-exact"),
    pytest.param(_chain, ["customers", "regions"], (False, True), None, id="mixed-wrong-case"),
    pytest.param(_chain, ["CUSTOMERS", "Regions"], (False, True), ["customers", "Regions"], id="mixed-unquoted-folds"),
    pytest.param(_colliding, ["Regions"], (True,), ["Regions"], id="colliding-quoted-upper"),
    pytest.param(_colliding, ["regions"], (True,), ["regions"], id="colliding-quoted-lower"),
    pytest.param(_colliding, ["rEGIONS"], (True,), None, id="colliding-quoted-neither"),
]


@pytest.mark.parametrize(("universe", "hops", "quoted", "expected"), PATH_CASES)
def test_resolve_path_quoting(universe, hops, quoted, expected) -> None:
    datasets = universe()
    resolved = resolve_path(hops, root=datasets["orders"], datasets_by_name=datasets, quoted=quoted)
    assert _targets(resolved) == expected


@pytest.mark.parametrize(("universe", "hops", "expected"), [
    pytest.param(_chain, ["CUSTOMERS"], ["customers"], id="hop-folds"),
    pytest.param(_chain, ["ORDERS", "customers"], ["customers"], id="root-qualifier-dropped"),
    pytest.param(_chain, ["customers", "BRANCH"], ["customers", "Offices"], id="edge-name-folds"),
    pytest.param(_chain, ["customers", "regions"], ["customers", "Regions"], id="second-hop-folds"),
    pytest.param(_chain, ["Regions"], ["customers", "Regions"], id="short-form"),
    pytest.param(_chain, ["REGIONS"], None, id="short-form-exact-only"),
    pytest.param(_colliding, ["Regions"], ["Regions"], id="colliding-exact"),
])
def test_resolve_path_without_quoting_is_unchanged(universe, hops, expected) -> None:
    datasets = universe()
    assert _targets(resolve_path(hops, root=datasets["orders"], datasets_by_name=datasets)) == expected
    assert _targets(resolve_path(hops, root=datasets["orders"], datasets_by_name=datasets, quoted=())) == expected


def test_unquoted_case_collision_stays_ambiguous() -> None:
    datasets = _colliding()
    with pytest.raises(AmbiguousReferenceError):
        resolve_path(["REGIONS"], root=datasets["orders"], datasets_by_name=datasets, quoted=(False,))


ANCHORED_CASES = [
    pytest.param(["orders", "customers"], (True, False), ["customers"], id="quoted-reader-exact"),
    pytest.param(["Orders", "customers"], (True, False), None, id="quoted-reader-wrong-case"),
    pytest.param(["ORDERS", "customers"], (False, False), ["customers"], id="unquoted-reader-folds"),
    pytest.param(["orders", "customers", "Regions"], (False, False, True), ["customers", "Regions"],
                 id="quoted-last-hop-exact"),
    pytest.param(["orders", "customers", "regions"], (False, False, True), None, id="quoted-last-hop-wrong-case"),
    pytest.param(["orders", "customers", "Branch"], (False, False, True), ["customers", "Offices"],
                 id="quoted-edge-name-exact"),
    pytest.param(["orders", "customers", "branch"], (False, False, True), None, id="quoted-edge-name-wrong-case"),
]


@pytest.mark.parametrize(("hops", "quoted", "expected"), ANCHORED_CASES)
def test_resolve_anchored_path_quoting(hops, quoted, expected) -> None:
    datasets = _chain()
    resolved = resolve_anchored_path(hops, reader=datasets["orders"], datasets_by_name=datasets, quoted=quoted)
    assert _targets(resolved) == expected


@pytest.mark.parametrize(("hops", "expected"), [
    pytest.param(["ORDERS", "customers"], ["customers"], id="reader-folds"),
    pytest.param(["orders", "customers", "regions"], ["customers", "Regions"], id="hop-folds"),
    pytest.param(["orders", "customers", "branch"], ["customers", "Offices"], id="edge-name-folds"),
])
def test_resolve_anchored_path_without_quoting_is_unchanged(hops, expected) -> None:
    datasets = _chain()
    assert _targets(resolve_anchored_path(hops, reader=datasets["orders"], datasets_by_name=datasets)) == expected
