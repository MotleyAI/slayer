"""Unit tests for NamedQuery and SlayerQuery variable enumeration."""

import pytest

from slayer.core.models import NamedQuery
from slayer.core.query import SlayerQuery


def _stage(name=None, source_model="orders", filters=None, variables=None, **kwargs):
    """Helper to build a SlayerQuery with sensible defaults."""
    return SlayerQuery(
        name=name,
        source_model=source_model,
        filters=filters,
        variables=variables,
        **kwargs,
    )


class TestSlayerQueryVariableEnumeration:
    def test_referenced_variables_extracts_from_filters(self) -> None:
        q = _stage(filters=["amount > {min_amount}", "status = '{status}'"])
        assert q.referenced_variables() == {"min_amount", "status"}

    def test_referenced_variables_skips_escaped_braces(self) -> None:
        q = _stage(filters=["literal {{ and }}", "real {var}"])
        assert q.referenced_variables() == {"var"}

    def test_referenced_variables_skips_invalid_names(self) -> None:
        # Invalid names (with spaces, hyphens, etc.) are not enumerated even
        # though they will fail at substitution time.
        q = _stage(filters=["{bad name}", "{good_one}"])
        assert q.referenced_variables() == {"good_one"}

    def test_referenced_variables_dedupes_across_filters(self) -> None:
        q = _stage(filters=["a > {x}", "b < {x}"])
        assert q.referenced_variables() == {"x"}

    def test_referenced_variables_empty_when_no_filters(self) -> None:
        q = _stage(filters=None)
        assert q.referenced_variables() == set()

    def test_unsupplied_variables_subtracts_self_variables(self) -> None:
        q = _stage(filters=["a > {x}", "b < {y}"], variables={"x": 1})
        assert q.unsupplied_variables() == {"y"}

    def test_unsupplied_variables_subtracts_extra(self) -> None:
        q = _stage(filters=["a > {x}", "b < {y}"])
        assert q.unsupplied_variables(extra={"x": 1}) == {"y"}

    def test_unsupplied_variables_combines_self_and_extra(self) -> None:
        q = _stage(filters=["{a}", "{b}", "{c}"], variables={"a": 1})
        assert q.unsupplied_variables(extra={"b": 2}) == {"c"}


class TestNamedQueryValidation:
    def test_rejects_empty_stages(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            NamedQuery(name="empty", stages=[])

    def test_rejects_unnamed_intermediate_stage(self) -> None:
        with pytest.raises(ValueError, match="must have a `name`"):
            NamedQuery(
                name="bad",
                stages=[
                    _stage(),  # no name
                    _stage(source_model="orders"),
                ],
            )

    def test_allows_unnamed_final_stage(self) -> None:
        nq = NamedQuery(
            name="ok",
            stages=[
                _stage(name="inner", source_model="orders"),
                _stage(source_model="inner"),  # final, may be anonymous
            ],
        )
        assert len(nq.stages) == 2

    def test_rejects_duplicate_stage_names(self) -> None:
        with pytest.raises(ValueError, match="duplicate stage name"):
            NamedQuery(
                name="bad",
                stages=[
                    _stage(name="dup", source_model="orders"),
                    _stage(name="dup", source_model="dup"),
                    _stage(source_model="dup"),
                ],
            )

    def test_rejects_double_underscore_in_name(self) -> None:
        with pytest.raises(ValueError, match="must contain only|reserved|double underscore"):
            NamedQuery(name="bad__name", stages=[_stage()])

    def test_rejects_dot_in_name(self) -> None:
        with pytest.raises(ValueError, match="must contain only|reserved"):
            NamedQuery(name="bad.name", stages=[_stage()])

    def test_accepts_valid_simple_name(self) -> None:
        nq = NamedQuery(name="my_query", stages=[_stage()])
        assert nq.name == "my_query"

    def test_default_variables_is_empty_dict(self) -> None:
        nq = NamedQuery(name="q", stages=[_stage()])
        assert nq.variables == {}


class TestNamedQueryVariableEnumeration:
    def test_referenced_variables_unions_all_stages(self) -> None:
        nq = NamedQuery(
            name="q",
            stages=[
                _stage(name="a", filters=["x > {a_var}"]),
                _stage(source_model="a", filters=["y < {b_var}"]),
            ],
        )
        assert nq.referenced_variables() == {"a_var", "b_var"}

    def test_unsupplied_variables_falls_back_to_top_level(self) -> None:
        nq = NamedQuery(
            name="q",
            variables={"shared": 1},
            stages=[
                _stage(name="a", filters=["x > {shared}"]),
                _stage(source_model="a", filters=["y < {shared}"]),
            ],
        )
        # Top-level satisfies both stages → nothing unsupplied
        assert nq.unsupplied_variables() == set()

    def test_unsupplied_variables_per_stage_satisfies_just_that_stage(self) -> None:
        nq = NamedQuery(
            name="q",
            stages=[
                _stage(name="a", filters=["{x}"], variables={"x": 1}),
                _stage(source_model="a", filters=["{x}"]),  # x not satisfied here
            ],
        )
        # Stage 'a' has x supplied, but the second stage references x without
        # supplying it; top-level also doesn't supply it.
        assert nq.unsupplied_variables() == {"x"}

    def test_unsupplied_variables_combines_top_level_and_stage(self) -> None:
        nq = NamedQuery(
            name="q",
            variables={"a": 1},
            stages=[
                _stage(name="s", filters=["{a}", "{b}"], variables={"b": 2}),
                _stage(source_model="s", filters=["{a}", "{c}"]),
            ],
        )
        # 'a' satisfied by top-level for both stages.
        # 'b' satisfied by stage 's' itself (only stage that references it).
        # 'c' not satisfied anywhere.
        assert nq.unsupplied_variables() == {"c"}


class TestNamedQueryRoundTrip:
    """Pydantic-level round-trip: dict → model → dict preserves the data."""

    def test_round_trip_preserves_all_fields(self) -> None:
        original = NamedQuery(
            name="my_query",
            description="Some description",
            variables={"top": 0.25},
            stages=[
                _stage(name="inner", filters=["amount > {min}"]),
                _stage(source_model="inner", filters=["{top}"]),
            ],
        )
        data = original.model_dump(mode="json", exclude_none=True)
        loaded = NamedQuery.model_validate(data)
        assert loaded.name == original.name
        assert loaded.description == original.description
        assert loaded.variables == original.variables
        assert len(loaded.stages) == 2
        assert loaded.stages[0].name == "inner"

    def test_default_version_is_1(self) -> None:
        nq = NamedQuery(name="q", stages=[_stage()])
        assert nq.version == 1
