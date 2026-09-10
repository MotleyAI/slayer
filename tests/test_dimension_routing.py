"""DEV-1856 — unit tests for the pure route resolver
``slayer/engine/dimension_routing.py``: ``short_form_route_or_none`` (executable
hop-token path for a uniquely-routable target, else ``None``, never raises),
``route_dotted_target`` (same but raises ``UnresolvableDimensionJoinError`` with a
route-aware ``suggested_path`` on ambiguous/unreachable), and ``_safe_hops`` (oriented
provably-to-one executable hops off a model as ``(token, target_model)`` pairs).
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import UnresolvableDimensionJoinError
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.dimension_routing import (
    _safe_hops,
    route_dotted_target,
    short_form_route_or_none,
)


def _pk() -> Column:
    return Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)


def _d(name: str) -> Column:
    return Column(name=name, sql=name, type=DataType.DOUBLE)


def _t(name: str) -> Column:
    return Column(name=name, sql=name, type=DataType.TEXT)


def _model(
    name: str, cols: list[Column], joins: list[ModelJoin] | None = None
) -> SlayerModel:
    return SlayerModel(
        name=name, sql_table=name, data_source="ds", columns=cols, joins=joins or []
    )


def _by_name(*models: SlayerModel) -> dict[str, SlayerModel]:
    return {m.name: m for m in models}


def _join(target: str, pairs: list[list[str]], **kw) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=pairs, **kw)


# short_form_route_or_none / route_dotted_target

def _linear() -> dict[str, SlayerModel]:
    """A → B → C → D, every hop onto the target PK (all fan-out-free)."""
    d = _model("D", [_pk(), _t("name")])
    c = _model("C", [_pk(), _d("d_id")], [_join("D", [["d_id", "id"]])])
    b = _model("B", [_pk(), _d("c_id")], [_join("C", [["c_id", "id"]])])
    a = _model("A", [_pk(), _d("b_id")], [_join("B", [["b_id", "id"]])])
    return _by_name(a, b, c, d)


class TestUniqueRoute:
    def test_single_route_returns_full_hop_path(self) -> None:
        models = _linear()
        route = short_form_route_or_none(
            root=models["A"], target_model="D", models_by_name=models
        )
        assert route == ["B", "C", "D"]

    def test_route_dotted_target_returns_route_no_raise(self) -> None:
        models = _linear()
        route = route_dotted_target(
            root=models["A"], target_model="D", leaf="name", models_by_name=models
        )
        assert route == ["B", "C", "D"]

    def test_route_through_reverse_declared_hop(self) -> None:
        """B–C declared on C (reverse from A's side, DEV-1853): still one route,
        same model-name tokens."""
        d = _model("D", [_pk(), _t("name")])
        c = _model(
            "C", [_pk(), _d("b_id"), _d("d_id")],
            [_join("B", [["b_id", "id"]]), _join("D", [["d_id", "id"]])],
        )
        b = _model("B", [_pk()])
        a = _model("A", [_pk(), _d("b_id")], [_join("B", [["b_id", "id"]])])
        models = _by_name(a, b, c, d)
        route = short_form_route_or_none(
            root=models["A"], target_model="D", models_by_name=models
        )
        assert route == ["B", "C", "D"]


class TestFanoutTieBreak:
    def _two_routes(self, *, second_safe: bool) -> dict[str, SlayerModel]:
        """A reaches D via a safe branch Bs and a second branch Cs.

        ``second_safe`` toggles whether Cs → D lands on the PK (safe) or on the
        non-unique ``grp`` column (fan-out).
        """
        d = _model("D", [_pk(), _t("grp"), _t("name")])
        bs = _model("Bs", [_pk(), _d("d_id")], [_join("D", [["d_id", "id"]])])
        cs_pair = [["d_id", "id"]] if second_safe else [["d_grp", "grp"]]
        cs_cols = [_pk(), _d("d_id")] if second_safe else [_pk(), _t("d_grp")]
        cs = _model("Cs", cs_cols, [_join("D", cs_pair)])
        a = _model(
            "A",
            [_pk(), _d("b_id"), _d("c_id")],
            [_join("Bs", [["b_id", "id"]]), _join("Cs", [["c_id", "id"]])],
        )
        return _by_name(a, bs, cs, d)

    def test_exactly_one_fanout_free_route_resolves(self) -> None:
        models = self._two_routes(second_safe=False)
        route = short_form_route_or_none(
            root=models["A"], target_model="D", models_by_name=models
        )
        assert route == ["Bs", "D"]

    def test_two_fanout_free_routes_are_ambiguous(self) -> None:
        models = self._two_routes(second_safe=True)
        assert (
            short_form_route_or_none(
                root=models["A"], target_model="D", models_by_name=models
            )
            is None
        )
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            route_dotted_target(
                root=models["A"], target_model="D", leaf="name", models_by_name=models
            )
        assert ei.value.suggested_path == "Bs.D.name"

    def test_zero_fanout_free_routes_are_ambiguous_with_suggestion(self) -> None:
        d = _model("D", [_pk(), _t("grp"), _t("name")])
        c1 = _model("C1", [_pk(), _t("d_grp")], [_join("D", [["d_grp", "grp"]])])
        c2 = _model("C2", [_pk(), _t("d_grp")], [_join("D", [["d_grp", "grp"]])])
        a = _model(
            "A",
            [_pk(), _d("c1_id"), _d("c2_id")],
            [_join("C1", [["c1_id", "id"]]), _join("C2", [["c2_id", "id"]])],
        )
        models = _by_name(a, c1, c2, d)
        assert (
            short_form_route_or_none(
                root=models["A"], target_model="D", models_by_name=models
            )
            is None
        )
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            route_dotted_target(
                root=models["A"], target_model="D", leaf="name", models_by_name=models
            )
        assert ei.value.suggested_path == "C1.D.name"

    def test_suggestion_prefers_fanout_free_over_shorter_unsafe(self) -> None:
        """Two fan-out-free 3-hop routes are ambiguous; the suggestion is the shortest
        fan-out-free path even though a shorter (2-hop) fan-out route exists."""
        d = _model("D", [_pk(), _t("grp"), _t("name")])
        u = _model("U", [_pk(), _t("d_grp")], [_join("D", [["d_grp", "grp"]])])
        x = _model("X", [_pk(), _d("d_id")], [_join("D", [["d_id", "id"]])])
        s1 = _model("S1", [_pk(), _d("x_id")], [_join("X", [["x_id", "id"]])])
        s2 = _model("S2", [_pk(), _d("x_id")], [_join("X", [["x_id", "id"]])])
        a = _model(
            "A", [_pk(), _d("u_id"), _d("s1_id"), _d("s2_id")],
            [_join("U", [["u_id", "id"]]), _join("S1", [["s1_id", "id"]]),
             _join("S2", [["s2_id", "id"]])],
        )
        models = _by_name(a, u, x, s1, s2, d)
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            route_dotted_target(
                root=models["A"], target_model="D", leaf="name", models_by_name=models
            )
        assert ei.value.suggested_path == "S1.X.D.name"

    def test_lone_executable_fanout_route_with_inexecutable_route_rejects(self) -> None:
        """An unnamed-parallel route counts as distinct routes (spec), so a lone
        executable fan-out route alongside it is ambiguous — rejected, but the
        suggestion is the executable path (never the inexecutable parallel one)."""
        d = _model("D", [_pk(), _t("grp"), _t("name")])
        a = _model("A", [_pk(), _t("d_grp")], [_join("D", [["d_grp", "grp"]])])
        b = _model(
            "B", [_pk(), _d("d_id"), _d("d_id2")],
            [_join("D", [["d_id", "id"]]), _join("D", [["d_id2", "id"]])],
        )
        root = _model(
            "Root", [_pk(), _d("a_id"), _d("b_id")],
            [_join("A", [["a_id", "id"]]), _join("B", [["b_id", "id"]])],
        )
        models = _by_name(root, a, b, d)
        assert short_form_route_or_none(
            root=models["Root"], target_model="D", models_by_name=models
        ) is None
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            route_dotted_target(
                root=models["Root"], target_model="D", leaf="name", models_by_name=models
            )
        assert ei.value.suggested_path == "A.D.name"


class TestUnreachable:
    def test_unreachable_target_returns_none(self) -> None:
        a = _model("A", [_pk()])
        d = _model("D", [_pk(), _t("name")])
        models = _by_name(a, d)
        assert (
            short_form_route_or_none(
                root=models["A"], target_model="D", models_by_name=models
            )
            is None
        )

    def test_unreachable_route_dotted_raises_without_suggestion(self) -> None:
        a = _model("A", [_pk()])
        d = _model("D", [_pk(), _t("name")])
        models = _by_name(a, d)
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            route_dotted_target(
                root=models["A"], target_model="D", leaf="name", models_by_name=models
            )
        assert ei.value.suggested_path is None


# _safe_hops — the oriented fan-out-free executable hop set

class TestSafeHops:
    def _star(self) -> dict[str, SlayerModel]:
        """A's edges: B (target PK), D (declared m:1) safe; C (non-unique target)
        and E (declared 1:m) unsafe forward."""
        b = _model("B", [_pk()])
        c = _model("C", [_pk(), _t("grp")])
        d = _model("D", [_pk(), _t("x")])
        e = _model("E", [_pk(), _t("y")])
        a = _model(
            "A",
            [_pk(), _d("b_id"), _t("c_grp"), _t("d_x"), _t("e_y")],
            [
                _join("B", [["b_id", "id"]]),
                _join("C", [["c_grp", "grp"]]),
                _join("D", [["d_x", "x"]], cardinality=JoinCardinality.MANY_TO_ONE),
                _join("E", [["e_y", "y"]], cardinality=JoinCardinality.ONE_TO_MANY),
            ],
        )
        return _by_name(a, b, c, d, e)

    def test_includes_to_one_excludes_fanout_and_undetermined(self) -> None:
        models = self._star()
        hops = _safe_hops(model=models["A"], models_by_name=models)
        assert set(hops) == {("B", "B"), ("D", "D")}

    def test_reverse_orientation_of_declared_one_to_many_is_safe(self) -> None:
        """A→E is declared 1:m, so oriented FROM E it is m:1 — safe reverse hop."""
        models = self._star()
        hops = _safe_hops(model=models["E"], models_by_name=models)
        assert ("A", "A") in hops

    def test_reverse_orientation_onto_non_unique_columns_is_unsafe(self) -> None:
        """A→B lands on B's PK forward; reversed it lands on A's non-unique
        ``b_id`` with no declared cardinality — unsafe, so B has no safe hops."""
        models = self._star()
        assert _safe_hops(model=models["B"], models_by_name=models) == []

    def test_parallel_named_edges_keep_tokens_unnamed_dropped(self) -> None:
        """Named parallel edges: one entry per uniquely-named safe edge, keyed by
        the edge-name token; the same pair unnamed is inexecutable — no entry."""
        leaf = _model("Leaf", [_pk(), _t("grp"), _t("x")])
        named = _model(
            "Mid",
            [_pk(), _d("leaf_id"), _t("leaf_grp")],
            [
                _join("Leaf", [["leaf_id", "id"]], name="by_id"),
                _join("Leaf", [["leaf_grp", "grp"]], name="by_grp"),
            ],
        )
        models = _by_name(named, leaf)
        hops = _safe_hops(model=models["Mid"], models_by_name=models)
        assert hops == [("by_id", "Leaf")]  # by_grp fans out; bare "Leaf" ambiguous

        unnamed = _model(
            "Mid",
            [_pk(), _d("leaf_id"), _t("leaf_grp")],
            [
                _join("Leaf", [["leaf_id", "id"]]),
                _join("Leaf", [["leaf_grp", "grp"]]),
            ],
        )
        models = _by_name(unnamed, leaf)
        assert _safe_hops(model=models["Mid"], models_by_name=models) == []

    def test_edge_name_shadowing_a_model_makes_it_unreachable(self) -> None:
        """An edge named ``Leaf`` (pointing at ``Other``) captures the ``Leaf`` token
        by name-priority, so the bare edge to the real ``Leaf`` model is
        unaddressable and drops — even though both hops are PK-safe."""
        leaf = _model("Leaf", [_pk(), _t("x")])
        other = _model("Other", [_pk(), _t("y")])
        mid = _model(
            "Mid",
            [_pk(), _d("leaf_id"), _d("other_id")],
            [
                _join("Leaf", [["leaf_id", "id"]]),
                _join("Other", [["other_id", "id"]], name="Leaf"),
            ],
        )
        models = _by_name(mid, leaf, other)
        targets = {t for _tok, t in _safe_hops(model=models["Mid"], models_by_name=models)}
        assert targets == {"Other"}

    def test_duplicate_name_across_neighbors_leaves_model_tokens(self) -> None:
        """A name shared by two edges to DIFFERENT neighbors is a dead (ambiguous)
        token, but each neighbor's bare model-name token still resolves — both
        PK-safe hops survive under their model names."""
        leaf = _model("Leaf", [_pk(), _t("x")])
        other = _model("Other", [_pk(), _t("y")])
        mid = _model(
            "Mid",
            [_pk(), _d("leaf_id"), _d("other_id")],
            [
                _join("Leaf", [["leaf_id", "id"]], name="dup"),
                _join("Other", [["other_id", "id"]], name="dup"),
            ],
        )
        models = _by_name(mid, leaf, other)
        targets = {t for _tok, t in _safe_hops(model=models["Mid"], models_by_name=models)}
        assert targets == {"Leaf", "Other"}
