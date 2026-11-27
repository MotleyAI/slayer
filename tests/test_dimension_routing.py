"""Unit tests for the pure route resolver
``slayer/engine/population/dimension_routing.py``: ``short_form_route_or_none`` (executable
hop-token path for a uniquely-routable target, else ``None``, never raises),
``route_dotted_target`` (same but raises ``UnresolvableDimensionRelationshipError`` with a
route-aware ``suggested_path`` on ambiguous/unreachable), and ``_safe_hops`` (the
oriented provably-to-one addressable hops off a dataset, ``relationship_search.addressable_hops``
with ``HopFilter.TO_ONE``, as ``(token, target_dataset)`` pairs).
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import UnresolvableDimensionRelationshipError
from slayer.core.relationship_search import HopFilter, addressable_hops
from slayer.core.relationship_walker import canonical_token
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.population.dimension_routing import (
    route_dotted_target,
    short_form_route_or_none,
)


def _pk() -> DatasetField:
    return DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True)


def _d(name: str) -> DatasetField:
    return DatasetField(name=name, expression=name, datatype=DataType.FLOAT)


def _t(name: str) -> DatasetField:
    return DatasetField(name=name, expression=name, datatype=DataType.STRING)


def _dataset(
    name: str, cols: list[DatasetField], relationships: list[DatasetRelationship] | None = None
) -> SlayerDataset:
    return SlayerDataset(
        name=name, sql_table=name, data_source="ds", fields=cols, relationships=relationships or []
    )


def _by_name(*datasets: SlayerDataset) -> dict[str, SlayerDataset]:
    return {m.name: m for m in datasets}


def _relationship(target: str, pairs: list[list[str]], **kw) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[p[0] for p in pairs], to_columns=[p[1] for p in pairs], **kw)


def _safe_hops(*, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]) -> list[tuple[str, str]]:
    return [
        (canonical_token(e), e.to)
        for e in addressable_hops(dataset=dataset, datasets_by_name=datasets_by_name, hop_filter=HopFilter.TO_ONE)
    ]


# short_form_route_or_none / route_dotted_target

def _linear() -> dict[str, SlayerDataset]:
    """A → B → C → D, every hop onto the target PK (all fan-out-free)."""
    d = _dataset("D", [_pk(), _t("name")])
    c = _dataset("C", [_pk(), _d("d_id")], [_relationship("D", [["d_id", "id"]])])
    b = _dataset("B", [_pk(), _d("c_id")], [_relationship("C", [["c_id", "id"]])])
    a = _dataset("A", [_pk(), _d("b_id")], [_relationship("B", [["b_id", "id"]])])
    return _by_name(a, b, c, d)


class TestUniqueRoute:
    def test_single_route_returns_full_hop_path(self) -> None:
        models = _linear()
        route = short_form_route_or_none(
            root=models["A"], target_dataset="D", datasets_by_name=models
        )
        assert route == ["B", "C", "D"]

    def test_route_dotted_target_returns_route_no_raise(self) -> None:
        models = _linear()
        route = route_dotted_target(
            root=models["A"], target_dataset="D", leaf="name", datasets_by_name=models
        )
        assert route == ["B", "C", "D"]

    def test_route_through_reverse_declared_hop(self) -> None:
        """B–C declared on C (reverse from A's side): still one route,
        same dataset-name tokens."""
        d = _dataset("D", [_pk(), _t("name")])
        c = _dataset(
            "C", [_pk(), _d("b_id"), _d("d_id")],
            [_relationship("B", [["b_id", "id"]]), _relationship("D", [["d_id", "id"]])],
        )
        b = _dataset("B", [_pk()])
        a = _dataset("A", [_pk(), _d("b_id")], [_relationship("B", [["b_id", "id"]])])
        models = _by_name(a, b, c, d)
        route = short_form_route_or_none(
            root=models["A"], target_dataset="D", datasets_by_name=models
        )
        assert route == ["B", "C", "D"]


class TestFanoutTieBreak:
    def _two_routes(self, *, second_safe: bool) -> dict[str, SlayerDataset]:
        """A reaches D via a safe branch Bs and a second branch Cs.

        ``second_safe`` toggles whether Cs → D lands on the PK (safe) or on the
        non-unique ``grp`` field (fan-out).
        """
        d = _dataset("D", [_pk(), _t("grp"), _t("name")])
        bs = _dataset("Bs", [_pk(), _d("d_id")], [_relationship("D", [["d_id", "id"]])])
        cs_pair = [["d_id", "id"]] if second_safe else [["d_grp", "grp"]]
        cs_cols = [_pk(), _d("d_id")] if second_safe else [_pk(), _t("d_grp")]
        cs = _dataset("Cs", cs_cols, [_relationship("D", cs_pair)])
        a = _dataset(
            "A",
            [_pk(), _d("b_id"), _d("c_id")],
            [_relationship("Bs", [["b_id", "id"]]), _relationship("Cs", [["c_id", "id"]])],
        )
        return _by_name(a, bs, cs, d)

    def test_exactly_one_fanout_free_route_resolves(self) -> None:
        models = self._two_routes(second_safe=False)
        route = short_form_route_or_none(
            root=models["A"], target_dataset="D", datasets_by_name=models
        )
        assert route == ["Bs", "D"]

    def test_two_fanout_free_routes_are_ambiguous(self) -> None:
        models = self._two_routes(second_safe=True)
        assert (
            short_form_route_or_none(
                root=models["A"], target_dataset="D", datasets_by_name=models
            )
            is None
        )
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(
                root=models["A"], target_dataset="D", leaf="name", datasets_by_name=models
            )
        assert ei.value.suggested_path == "Bs.D.name"

    def test_zero_fanout_free_routes_are_ambiguous_with_suggestion(self) -> None:
        d = _dataset("D", [_pk(), _t("grp"), _t("name")])
        c1 = _dataset("C1", [_pk(), _t("d_grp")], [_relationship("D", [["d_grp", "grp"]])])
        c2 = _dataset("C2", [_pk(), _t("d_grp")], [_relationship("D", [["d_grp", "grp"]])])
        a = _dataset(
            "A",
            [_pk(), _d("c1_id"), _d("c2_id")],
            [_relationship("C1", [["c1_id", "id"]]), _relationship("C2", [["c2_id", "id"]])],
        )
        models = _by_name(a, c1, c2, d)
        assert (
            short_form_route_or_none(
                root=models["A"], target_dataset="D", datasets_by_name=models
            )
            is None
        )
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(
                root=models["A"], target_dataset="D", leaf="name", datasets_by_name=models
            )
        assert ei.value.suggested_path == "C1.D.name"

    def test_suggestion_prefers_fanout_free_over_shorter_unsafe(self) -> None:
        """Two fan-out-free 3-hop routes are ambiguous; the suggestion is the shortest
        fan-out-free path even though a shorter (2-hop) fan-out route exists."""
        d = _dataset("D", [_pk(), _t("grp"), _t("name")])
        u = _dataset("U", [_pk(), _t("d_grp")], [_relationship("D", [["d_grp", "grp"]])])
        x = _dataset("X", [_pk(), _d("d_id")], [_relationship("D", [["d_id", "id"]])])
        s1 = _dataset("S1", [_pk(), _d("x_id")], [_relationship("X", [["x_id", "id"]])])
        s2 = _dataset("S2", [_pk(), _d("x_id")], [_relationship("X", [["x_id", "id"]])])
        a = _dataset(
            "A", [_pk(), _d("u_id"), _d("s1_id"), _d("s2_id")],
            [_relationship("U", [["u_id", "id"]]), _relationship("S1", [["s1_id", "id"]]),
             _relationship("S2", [["s2_id", "id"]])],
        )
        models = _by_name(a, u, x, s1, s2, d)
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(
                root=models["A"], target_dataset="D", leaf="name", datasets_by_name=models
            )
        assert ei.value.suggested_path == "S1.X.D.name"

    def test_lone_executable_fanout_route_with_inexecutable_route_rejects(self) -> None:
        """An unnamed-parallel route counts as distinct routes (spec), so a lone
        executable fan-out route alongside it is ambiguous — rejected, but the
        suggestion is the executable path (never the inexecutable parallel one)."""
        d = _dataset("D", [_pk(), _t("grp"), _t("name")])
        a = _dataset("A", [_pk(), _t("d_grp")], [_relationship("D", [["d_grp", "grp"]])])
        b = _dataset(
            "B", [_pk(), _d("d_id"), _d("d_id2")],
            [_relationship("D", [["d_id", "id"]]), _relationship("D", [["d_id2", "id"]])],
        )
        root = _dataset(
            "Root", [_pk(), _d("a_id"), _d("b_id")],
            [_relationship("A", [["a_id", "id"]]), _relationship("B", [["b_id", "id"]])],
        )
        models = _by_name(root, a, b, d)
        assert short_form_route_or_none(
            root=models["Root"], target_dataset="D", datasets_by_name=models
        ) is None
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(
                root=models["Root"], target_dataset="D", leaf="name", datasets_by_name=models
            )
        assert ei.value.suggested_path == "A.D.name"


class TestUnreachable:
    def test_unreachable_target_returns_none(self) -> None:
        a = _dataset("A", [_pk()])
        d = _dataset("D", [_pk(), _t("name")])
        models = _by_name(a, d)
        assert (
            short_form_route_or_none(
                root=models["A"], target_dataset="D", datasets_by_name=models
            )
            is None
        )

    def test_unreachable_route_dotted_raises_without_suggestion(self) -> None:
        a = _dataset("A", [_pk()])
        d = _dataset("D", [_pk(), _t("name")])
        models = _by_name(a, d)
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            route_dotted_target(
                root=models["A"], target_dataset="D", leaf="name", datasets_by_name=models
            )
        assert ei.value.suggested_path is None


# _safe_hops — the oriented fan-out-free executable hop set

class TestSafeHops:
    def _star(self) -> dict[str, SlayerDataset]:
        """A's edges: B (target PK), D (declared m:1) safe; C (non-unique target)
        and E (declared 1:m) unsafe forward."""
        b = _dataset("B", [_pk()])
        c = _dataset("C", [_pk(), _t("grp")])
        d = _dataset("D", [_pk(), _t("x")])
        e = _dataset("E", [_pk(), _t("y")])
        a = _dataset(
            "A",
            [_pk(), _d("b_id"), _t("c_grp"), _t("d_x"), _t("e_y")],
            [
                _relationship("B", [["b_id", "id"]]),
                _relationship("C", [["c_grp", "grp"]]),
                _relationship("D", [["d_x", "x"]], cardinality=RelationshipCardinality.MANY_TO_ONE),
                _relationship("E", [["e_y", "y"]], cardinality=RelationshipCardinality.ONE_TO_MANY),
            ],
        )
        return _by_name(a, b, c, d, e)

    def test_includes_to_one_excludes_fanout_and_undetermined(self) -> None:
        models = self._star()
        hops = _safe_hops(dataset=models["A"], datasets_by_name=models)
        assert set(hops) == {("B", "B"), ("D", "D")}

    def test_reverse_orientation_of_declared_one_to_many_is_safe(self) -> None:
        """A→E is declared 1:m, so oriented FROM E it is m:1 — safe reverse hop."""
        models = self._star()
        hops = _safe_hops(dataset=models["E"], datasets_by_name=models)
        assert ("A", "A") in hops

    def test_reverse_orientation_onto_non_unique_columns_is_unsafe(self) -> None:
        """A→B lands on B's PK forward; reversed it lands on A's non-unique
        ``b_id`` with no declared cardinality — unsafe, so B has no safe hops."""
        models = self._star()
        assert _safe_hops(dataset=models["B"], datasets_by_name=models) == []

    def test_parallel_named_edges_keep_tokens_unnamed_dropped(self) -> None:
        """Named parallel edges: one entry per uniquely-named safe edge, keyed by
        the edge-name token; the same pair unnamed is inexecutable — no entry."""
        leaf = _dataset("Leaf", [_pk(), _t("grp"), _t("x")])
        named = _dataset(
            "Mid",
            [_pk(), _d("leaf_id"), _t("leaf_grp")],
            [
                _relationship("Leaf", [["leaf_id", "id"]], name="by_id"),
                _relationship("Leaf", [["leaf_grp", "grp"]], name="by_grp"),
            ],
        )
        models = _by_name(named, leaf)
        hops = _safe_hops(dataset=models["Mid"], datasets_by_name=models)
        assert hops == [("by_id", "Leaf")]  # by_grp fans out; bare "Leaf" ambiguous

        unnamed = _dataset(
            "Mid",
            [_pk(), _d("leaf_id"), _t("leaf_grp")],
            [
                _relationship("Leaf", [["leaf_id", "id"]]),
                _relationship("Leaf", [["leaf_grp", "grp"]]),
            ],
        )
        models = _by_name(unnamed, leaf)
        assert _safe_hops(dataset=models["Mid"], datasets_by_name=models) == []

    def test_edge_name_shadowing_a_model_makes_it_unreachable(self) -> None:
        """An edge named ``Leaf`` (pointing at ``Other``) captures the ``Leaf`` token
        by name-priority, so the bare edge to the real ``Leaf`` dataset is
        unaddressable and drops — even though both hops are PK-safe."""
        leaf = _dataset("Leaf", [_pk(), _t("x")])
        other = _dataset("Other", [_pk(), _t("y")])
        mid = _dataset(
            "Mid",
            [_pk(), _d("leaf_id"), _d("other_id")],
            [
                _relationship("Leaf", [["leaf_id", "id"]]),
                _relationship("Other", [["other_id", "id"]], name="Leaf"),
            ],
        )
        models = _by_name(mid, leaf, other)
        targets = {t for _tok, t in _safe_hops(dataset=models["Mid"], datasets_by_name=models)}
        assert targets == {"Other"}

    def test_duplicate_name_across_neighbors_is_unaddressable(self) -> None:
        """A name shared by two edges to different neighbors is each edge's canonical
        token and resolves to both, so neither hop is addressable."""
        leaf = _dataset("Leaf", [_pk(), _t("x")])
        other = _dataset("Other", [_pk(), _t("y")])
        mid = _dataset(
            "Mid",
            [_pk(), _d("leaf_id"), _d("other_id")],
            [
                _relationship("Leaf", [["leaf_id", "id"]], name="dup"),
                _relationship("Other", [["other_id", "id"]], name="dup"),
            ],
        )
        models = _by_name(mid, leaf, other)
        targets = {t for _tok, t in _safe_hops(dataset=models["Mid"], datasets_by_name=models)}
        assert targets == set()
