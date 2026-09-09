"""DEV-1856 — unit tests for the pure route resolver ``slayer/engine/dimension_routing.py``
(TDD; the module does not exist yet). Contract under test:

* ``short_form_route_or_none(*, root, target_model, models_by_name) -> list[str] | None``
  — the hop path (model names, excl. ``root``, incl. target) for a uniquely-routable
  target, else ``None``; never raises.
* ``route_dotted_target(*, root, target_model, leaf, models_by_name) -> list[str]``
  — same, but raises ``UnresolvableDimensionJoinError`` (route-aware ``suggested_path``
  including ``leaf``) on ambiguous / unreachable targets.
* ``_to_one_adjacency(models) -> dict[str, set[str]]`` — provably-to-one edges only.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import UnresolvableDimensionJoinError
from slayer.core.models import Column, ModelJoin, SlayerModel

# Written in the implementation stage; skip (not error) while absent so the suite still collects.
dimension_routing = pytest.importorskip("slayer.engine.dimension_routing")
short_form_route_or_none = dimension_routing.short_form_route_or_none
route_dotted_target = dimension_routing.route_dotted_target
_to_one_adjacency = dimension_routing._to_one_adjacency


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


# _to_one_adjacency — the fan-out-free subgraph

class TestToOneAdjacency:
    def test_includes_to_one_excludes_fanout_and_undetermined(self) -> None:
        """A's edges: B (target PK), D (declared m:1) kept; C (non-unique target)
        and E (declared 1:m) dropped."""
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
        adj = _to_one_adjacency([a, b, c, d, e])
        assert adj["A"] == {"B", "D"}
