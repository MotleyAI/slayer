"""``localize_stages``: minted identities, respelled query-written references, typed displays."""

from __future__ import annotations

from slayer.core.query import ModelExtension
from slayer.core.scope import StageDisplay
from slayer.engine.stage_ordering import localize_stages

from tests._dev1966_fixtures import m, query

USER_X = "__slayer_stage_x"


def _x():
    return query(name="x", source_model="orders", dimensions=["status"], measures=[m("sum(amount)", "a")])


class TestUserScope:
    def test_named_stage_gets_an_identity_and_the_root_keeps_its_name(self) -> None:
        stages, displays = localize_stages([_x(), query(name="r", source_model="x", measures=[m("sum(a)", "t")])])
        assert [q.name for q in stages] == [USER_X, "r"]
        assert displays == {USER_X: StageDisplay(name="x")}

    def test_source_string_is_respelled_and_its_prefix_stripped(self) -> None:
        root = query(source_model="x", dimensions=["x.status"], measures=[m("sum(a)", "t")])
        stages, _ = localize_stages([_x(), root])
        assert stages[-1].source_model == USER_X
        assert [d.name for d in stages[-1].dimensions or []] == ["status"]

    def test_extension_base_is_respelled(self) -> None:
        root = query(source_model="x", extensions=[{"source_name": "x", "measures": [m("sum(a * 2)", "t")]}],
                     measures=["t"])
        stages, _ = localize_stages([_x(), root])
        assert stages[-1].source_model == USER_X
        (spec,) = stages[-1].extensions or []
        assert isinstance(spec, ModelExtension)
        assert spec.source_name == USER_X

    def test_extension_join_target_is_respelled(self) -> None:
        root = query(source_model="customers", extensions=[{"source_name": "customers", "joins": [
            {"target_model": "x", "join_pairs": [["id", "customer_id"]]}]}], measures=[m("sum(x.a)", "t")])
        stages, _ = localize_stages([_x(), root])
        (spec,) = stages[-1].extensions or []
        assert isinstance(spec, ModelExtension)
        assert [(j.target_model, j.name) for j in spec.joins or []] == [(USER_X, None)]

    def test_a_model_name_that_is_not_a_stage_is_untouched(self) -> None:
        root = query(source_model="orders", extensions=[{"source_name": "orders", "joins": [
            {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}]}],
            measures=[m("sum(amount)", "t")])
        stages, displays = localize_stages([_x(), root])
        (spec,) = stages[-1].extensions or []
        assert isinstance(spec, ModelExtension)
        assert spec.source_name == "orders"
        assert [j.target_model for j in spec.joins or []] == ["customers"]
        assert list(displays) == [USER_X]


class TestModelScope:
    def test_private_identities_and_the_final_stage_named_after_the_model(self) -> None:
        stages, displays = localize_stages(
            [_x(), query(source_model="x", measures=[m("sum(a)", "t")])], model="cust_rev")
        assert [q.name for q in stages] == ["__slayer_qb__cust_rev__x", "cust_rev"]
        assert stages[-1].source_model == "__slayer_qb__cust_rev__x"
        assert displays == {
            "__slayer_qb__cust_rev__x": StageDisplay(name="x", model="cust_rev"),
            "cust_rev": StageDisplay(name="cust_rev", model="cust_rev"),
        }

    def test_labels_name_the_stage_and_its_model(self) -> None:
        _, displays = localize_stages(
            [_x(), query(source_model="x", measures=[m("sum(a)", "t")])], model="cust_rev")
        assert displays["__slayer_qb__cust_rev__x"].label == "stage 'x' of model 'cust_rev'"
        assert displays["cust_rev"].label == "model 'cust_rev'"
