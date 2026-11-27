"""``localize_stages``: minted identities, respelled query-written references, typed displays."""

from __future__ import annotations

from slayer.core.query import DatasetExtension
from slayer.core.scope import StageDisplay
from slayer.engine.stage_ordering import localize_stages

from tests._dev1966_fixtures import ext_m, m, query

USER_X = "__slayer_stage_x"


def _x():
    return query(name="x", population="orders", dimensions=["orders.status"], metrics=[m("sum(orders.amount)", "a")])


class TestUserScope:
    def test_named_stage_gets_an_identity_and_the_root_keeps_its_name(self) -> None:
        stages, displays = localize_stages([_x(), query(name="r", population="x", metrics=[m("sum(x.a)", "t")])])
        assert [q.name for q in stages] == [USER_X, "r"]
        assert displays == {USER_X: StageDisplay(name="x")}

    def test_source_string_is_respelled_and_its_prefix_stripped(self) -> None:
        root = query(population="x", dimensions=["x.status"], metrics=[m("sum(x.a)", "t")])
        stages, _ = localize_stages([_x(), root])
        assert stages[-1].population == USER_X
        assert [d.name for d in stages[-1].dimensions or []] == ["status"]

    def test_extension_base_is_respelled(self) -> None:
        root = query(population="x", extensions=[{"dataset": "x", "metrics": [ext_m("sum(a * 2)", "t")]}],
                     metrics=["x.t"])
        stages, _ = localize_stages([_x(), root])
        assert stages[-1].population == USER_X
        (spec,) = stages[-1].extensions or []
        assert isinstance(spec, DatasetExtension)
        assert spec.dataset == USER_X

    def test_extension_join_target_is_respelled(self) -> None:
        root = query(population="customers", extensions=[{"dataset": "customers", "relationships": [
            {"to": "x", "from_columns": ["id"], "to_columns": ["customer_id"]}]}], metrics=[m("sum(x.a)", "t")])
        stages, _ = localize_stages([_x(), root])
        (spec,) = stages[-1].extensions or []
        assert isinstance(spec, DatasetExtension)
        assert [(j.to, j.name) for j in spec.relationships or []] == [(USER_X, None)]

    def test_a_model_name_that_is_not_a_stage_is_untouched(self) -> None:
        root = query(population="orders", extensions=[{"dataset": "orders", "relationships": [
            {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}]}],
            metrics=[m("sum(orders.amount)", "t")])
        stages, displays = localize_stages([_x(), root])
        (spec,) = stages[-1].extensions or []
        assert isinstance(spec, DatasetExtension)
        assert spec.dataset == "orders"
        assert [j.to for j in spec.relationships or []] == ["customers"]
        assert list(displays) == [USER_X]


class TestModelScope:
    def test_private_identities_and_the_final_stage_named_after_the_model(self) -> None:
        stages, displays = localize_stages(
            [_x(), query(population="x", metrics=[m("sum(x.a)", "t")])], dataset="cust_rev")
        assert [q.name for q in stages] == ["__slayer_qb__cust_rev__x", "cust_rev"]
        assert stages[-1].population == "__slayer_qb__cust_rev__x"
        assert displays == {
            "__slayer_qb__cust_rev__x": StageDisplay(name="x", dataset="cust_rev"),
            "cust_rev": StageDisplay(name="cust_rev", dataset="cust_rev"),
        }

    def test_labels_name_the_stage_and_its_model(self) -> None:
        _, displays = localize_stages(
            [_x(), query(population="x", metrics=[m("sum(x.a)", "t")])], dataset="cust_rev")
        assert displays["__slayer_qb__cust_rev__x"].label == "stage 'x' of dataset 'cust_rev'"
        assert displays["cust_rev"].label == "dataset 'cust_rev'"
