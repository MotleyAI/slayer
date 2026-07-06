"""Unit tests for the session-policy data model (DEV-1578).

``ColumnFilterRule`` / ``SessionPolicy`` are the immutable, agent-invisible
forced-filter configuration. These tests pin validation, immutability
(``frozen`` + tuple fields so list contents can't be mutated after init),
``extra="forbid"``, the empty-list / blank-column rejections, and the
scalar-vs-list value shape that drives ``=`` vs ``IN`` at the SQL layer.
"""

import pytest
from pydantic import ValidationError

from slayer.core.policy import ColumnFilterRule, SessionPolicy


# -- ColumnFilterRule --------------------------------------------------------


def test_scalar_value_rule():
    rule = ColumnFilterRule(column="organization_uuid", value="7ef3")
    assert rule.kind == "column"
    assert rule.column == "organization_uuid"
    assert rule.value == "7ef3"
    assert rule.on_unapplicable == "block"
    assert rule.name is None


def test_list_value_coerced_to_tuple():
    rule = ColumnFilterRule(column="org", value=["a", "b"])
    assert rule.value == ("a", "b")
    assert isinstance(rule.value, tuple)


def test_numeric_and_bool_scalar_values():
    assert ColumnFilterRule(column="tenant_id", value=42).value == 42
    assert ColumnFilterRule(column="ratio", value=3.5).value == pytest.approx(3.5)
    assert ColumnFilterRule(column="is_active", value=True).value is True


def test_empty_list_value_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRule(column="org", value=[])


def test_empty_tuple_value_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRule(column="org", value=())


def test_blank_column_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRule(column="", value="x")


def test_whitespace_column_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRule(column="   ", value="x")


def test_on_unapplicable_pass_allowed():
    rule = ColumnFilterRule(column="org", value="x", on_unapplicable="pass")
    assert rule.on_unapplicable == "pass"


def test_on_unapplicable_invalid_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRule(column="org", value="x", on_unapplicable="skip")


def test_name_stored_for_diagnostics():
    rule = ColumnFilterRule(name="tenant", column="org", value="x")
    assert rule.name == "tenant"


def test_rule_extra_forbidden():
    with pytest.raises(ValidationError):
        ColumnFilterRule(column="org", value="x", op="=")


def test_rule_is_frozen():
    rule = ColumnFilterRule(column="org", value="x")
    with pytest.raises(ValidationError):
        rule.column = "other"


def test_rule_kind_literal_enforced():
    with pytest.raises(ValidationError):
        ColumnFilterRule(kind="join", column="org", value="x")


# -- SessionPolicy -----------------------------------------------------------


def test_empty_policy_defaults():
    policy = SessionPolicy()
    assert policy.version == 1
    assert policy.data_filters == ()
    assert isinstance(policy.data_filters, tuple)


def test_policy_unknown_version_rejected():
    """Unknown schema versions fail closed rather than running through the v1
    rewrite path."""
    with pytest.raises(ValidationError):
        SessionPolicy(version=2)


def test_policy_data_filters_list_coerced_to_tuple():
    policy = SessionPolicy(
        data_filters=[ColumnFilterRule(column="org", value="x")]
    )
    assert isinstance(policy.data_filters, tuple)
    assert len(policy.data_filters) == 1
    assert policy.data_filters[0].column == "org"


def test_policy_accepts_dict_rules():
    policy = SessionPolicy(data_filters=[{"column": "org", "value": ["a", "b"]}])
    assert policy.data_filters[0].value == ("a", "b")


def test_policy_extra_forbidden():
    with pytest.raises(ValidationError):
        SessionPolicy(data_filters=[], join_filters=[])


def test_policy_is_frozen():
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    with pytest.raises(ValidationError):
        policy.data_filters = ()


def test_policy_data_filters_tuple_has_no_append():
    """Immutability: the container is a tuple, so contents can't be mutated."""
    policy = SessionPolicy(data_filters=[ColumnFilterRule(column="org", value="x")])
    assert not hasattr(policy.data_filters, "append")
