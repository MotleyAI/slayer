"""Unit tests for the session-policy data model (DEV-1578).

``ColumnFilterRule`` / ``SessionPolicy`` are the immutable, agent-invisible
forced-filter configuration. These tests pin validation, immutability
(``frozen`` + tuple fields so list contents can't be mutated after init),
``extra="forbid"``, the empty-list / blank-column rejections, and the
scalar-vs-list value shape that drives ``=`` vs ``IN`` at the SQL layer.
"""

import pytest
from pydantic import ValidationError

from slayer.core.policy import (
    ColumnFilterRule,
    JoinFilterRule,
    JoinHop,
    SessionPolicy,
)


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


# -- JoinHop (DEV-1627) ------------------------------------------------------


def _hop(**kw):
    base = dict(
        from_table="orders",
        from_column="customer_id",
        to_table="customers",
        to_column="id",
    )
    base.update(kw)
    return JoinHop(**base)


def test_join_hop_construction():
    hop = _hop()
    assert hop.from_table == "orders"
    assert hop.from_column == "customer_id"
    assert hop.to_table == "customers"
    assert hop.to_column == "id"


def test_join_hop_is_frozen():
    hop = _hop()
    with pytest.raises(ValidationError):
        hop.from_table = "other"


def test_join_hop_extra_forbidden():
    with pytest.raises(ValidationError):
        JoinHop(
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            join_type="inner",
        )


@pytest.mark.parametrize(
    "field", ["from_table", "from_column", "to_table", "to_column"]
)
def test_join_hop_blank_field_rejected(field):
    with pytest.raises(ValidationError):
        _hop(**{field: "   "})


# -- JoinFilterRule (DEV-1627) -----------------------------------------------


def _join_rule(**kw):
    base = dict(
        target_table="orders",
        join_path=[_hop()],
        column="organization_uuid",
        value="7ef3",
    )
    base.update(kw)
    return JoinFilterRule(**base)


def test_join_rule_construction():
    rule = _join_rule()
    assert rule.kind == "join"
    assert rule.target_table == "orders"
    assert rule.column == "organization_uuid"
    assert rule.value == "7ef3"
    assert rule.name is None
    assert len(rule.join_path) == 1
    assert isinstance(rule.join_path, tuple)


def test_join_rule_is_frozen():
    rule = _join_rule()
    with pytest.raises(ValidationError):
        rule.target_table = "other"


def test_join_rule_extra_forbidden():
    with pytest.raises(ValidationError):
        _join_rule(on_unapplicable="block")  # join rules have no such field


def test_join_rule_kind_literal_enforced():
    with pytest.raises(ValidationError):
        JoinFilterRule(
            kind="column",
            target_table="orders",
            join_path=[_hop()],
            column="org",
            value="x",
        )


def test_join_rule_join_path_list_coerced_to_tuple():
    rule = _join_rule(join_path=[_hop()])
    assert isinstance(rule.join_path, tuple)


def test_join_rule_empty_join_path_rejected():
    with pytest.raises(ValidationError):
        _join_rule(join_path=[])


def test_join_rule_first_hop_must_start_at_target():
    """The first hop's from_table must equal target_table."""
    with pytest.raises(ValidationError):
        _join_rule(
            target_table="orders",
            join_path=[_hop(from_table="line_items")],
        )


def test_join_rule_hops_must_chain():
    """Each hop's from_table must equal the previous hop's to_table."""
    with pytest.raises(ValidationError):
        _join_rule(
            target_table="line_items",
            join_path=[
                _hop(from_table="line_items", to_table="orders"),
                # broken chain: starts at "customers", not "orders"
                _hop(from_table="customers", to_table="regions"),
            ],
        )


def test_join_rule_multihop_valid_chain():
    rule = _join_rule(
        target_table="line_items",
        join_path=[
            _hop(
                from_table="line_items",
                from_column="order_id",
                to_table="orders",
                to_column="id",
            ),
            _hop(
                from_table="orders",
                from_column="customer_id",
                to_table="customers",
                to_column="id",
            ),
        ],
        column="organization_uuid",
        value="7ef3",
    )
    assert len(rule.join_path) == 2
    assert rule.join_path[1].to_table == "customers"


def test_join_rule_list_value_coerced_to_tuple():
    rule = _join_rule(value=["a", "b"])
    assert rule.value == ("a", "b")


def test_join_rule_empty_list_value_rejected():
    with pytest.raises(ValidationError):
        _join_rule(value=[])


def test_join_rule_empty_tuple_value_rejected():
    with pytest.raises(ValidationError):
        _join_rule(value=())


@pytest.mark.parametrize("field", ["target_table", "column"])
def test_join_rule_blank_field_rejected(field):
    with pytest.raises(ValidationError):
        _join_rule(**{field: "  "})


def test_join_rule_schema_qualified_tables_allowed():
    """Optional schema/catalog qualification (Codex finding #1)."""
    rule = _join_rule(
        target_table="public.orders",
        join_path=[
            _hop(from_table="public.orders", to_table="public.customers")
        ],
    )
    assert rule.target_table == "public.orders"
    assert rule.join_path[0].to_table == "public.customers"


# -- SessionPolicy discriminated union (DEV-1627) ----------------------------


_BLOCK_BACKSTOP = ColumnFilterRule(column="organization_uuid", value="7ef3")


def test_policy_accepts_join_rule_instance():
    policy = SessionPolicy(data_filters=[_BLOCK_BACKSTOP, _join_rule()])
    assert any(isinstance(r, JoinFilterRule) for r in policy.data_filters)


def test_policy_mixed_column_and_join_rules():
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="organization_uuid", value="7ef3"),
            _join_rule(),
        ]
    )
    assert isinstance(policy.data_filters[0], ColumnFilterRule)
    assert isinstance(policy.data_filters[1], JoinFilterRule)


def test_policy_join_rule_dict_with_kind():
    policy = SessionPolicy(
        data_filters=[
            _BLOCK_BACKSTOP,
            {
                "kind": "join",
                "target_table": "orders",
                "join_path": [
                    {
                        "from_table": "orders",
                        "from_column": "customer_id",
                        "to_table": "customers",
                        "to_column": "id",
                    }
                ],
                "column": "organization_uuid",
                "value": "7ef3",
            },
        ]
    )
    assert any(isinstance(r, JoinFilterRule) for r in policy.data_filters)


def test_policy_join_rule_dict_without_kind_inferred():
    """A dict carrying join fields but no explicit kind resolves to a join
    rule (kind inference keeps the discriminated union working alongside the
    kind-less column-rule dict shape)."""
    policy = SessionPolicy(
        data_filters=[
            _BLOCK_BACKSTOP,
            {
                "target_table": "orders",
                "join_path": [
                    {
                        "from_table": "orders",
                        "from_column": "customer_id",
                        "to_table": "customers",
                        "to_column": "id",
                    }
                ],
                "column": "organization_uuid",
                "value": "7ef3",
            },
        ]
    )
    assert any(isinstance(r, JoinFilterRule) for r in policy.data_filters)


def test_policy_column_rule_dict_without_kind_still_inferred():
    """Backward-compat: a kind-less column dict still resolves to a column
    rule (pins the existing DEV-1578 dict shape under the new union)."""
    policy = SessionPolicy(data_filters=[{"column": "org", "value": "x"}])
    assert isinstance(policy.data_filters[0], ColumnFilterRule)


# -- mandatory block backstop (DEV-1627, decision 5) -------------------------


def test_join_only_policy_rejected_no_backstop():
    """A policy with a join rule but no block column rule fails closed at
    construction — an untargeted table could otherwise leak unfiltered."""
    with pytest.raises(ValidationError):
        SessionPolicy(data_filters=[_join_rule()])


def test_join_plus_pass_only_column_rule_rejected():
    """A pass-only column rule is not a valid backstop: it leaves an absent-
    column table unfiltered, so it does not satisfy the requirement."""
    with pytest.raises(ValidationError):
        SessionPolicy(
            data_filters=[
                ColumnFilterRule(
                    column="organization_uuid", value="x", on_unapplicable="pass"
                ),
                _join_rule(),
            ]
        )


def test_join_plus_block_column_rule_accepted():
    policy = SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="organization_uuid", value="x"),  # block default
            _join_rule(),
        ]
    )
    assert any(isinstance(r, JoinFilterRule) for r in policy.data_filters)
    assert any(
        isinstance(r, ColumnFilterRule) and r.on_unapplicable == "block"
        for r in policy.data_filters
    )


def test_column_only_policy_needs_no_backstop():
    """The backstop requirement fires only when a join rule is present: a pure
    column-only policy is accepted with or without a block rule."""
    assert SessionPolicy(
        data_filters=[
            ColumnFilterRule(column="org", value="x", on_unapplicable="pass")
        ]
    )
    assert SessionPolicy(
        data_filters=[ColumnFilterRule(column="org", value="x")]
    )
