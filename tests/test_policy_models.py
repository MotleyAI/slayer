"""Unit tests for the session-policy data model: validation, immutability, the
explicit ``kind`` discriminator, and the structural ``JoinFilterRuleset`` invariants.
"""

import pytest
from pydantic import ValidationError

import slayer.core.policy as policy_mod
from slayer.core.policy import (
    ColumnFilterRuleset,
    FilterRuleset,  # noqa: F401 — exported union
    JoinFilterRule,
    JoinFilterRuleset,
    JoinHop,  # internal parse product — imported only to pin strings-only input
    SessionPolicy,
)


# -- ColumnFilterRuleset -----------------------------------------------------


def test_scalar_value_ruleset():
    rs = ColumnFilterRuleset(column="organization_uuid", value="7ef3")
    assert rs.kind == "column"
    assert rs.column == "organization_uuid"
    assert rs.value == "7ef3"
    assert rs.on_unapplicable == "block"


def test_list_value_coerced_to_tuple():
    rs = ColumnFilterRuleset(column="org", value=["a", "b"])
    assert rs.value == ("a", "b")
    assert isinstance(rs.value, tuple)


def test_numeric_and_bool_scalar_values():
    assert ColumnFilterRuleset(column="tenant_id", value=42).value == 42
    assert ColumnFilterRuleset(column="ratio", value=3.5).value == pytest.approx(3.5)
    assert ColumnFilterRuleset(column="is_active", value=True).value is True


def test_empty_list_value_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRuleset(column="org", value=[])


def test_empty_tuple_value_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRuleset(column="org", value=())


def test_blank_column_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRuleset(column="", value="x")


def test_whitespace_column_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRuleset(column="   ", value="x")


def test_on_unapplicable_pass_allowed():
    rs = ColumnFilterRuleset(column="org", value="x", on_unapplicable="pass")
    assert rs.on_unapplicable == "pass"


def test_on_unapplicable_invalid_rejected():
    with pytest.raises(ValidationError):
        ColumnFilterRuleset(column="org", value="x", on_unapplicable="skip")


def test_column_ruleset_extra_forbidden():
    with pytest.raises(ValidationError):
        ColumnFilterRuleset(column="org", value="x", op="=")


def test_column_ruleset_name_field_removed():
    """There is no ``name`` attribute anywhere, so passing one hits extra-forbid."""
    with pytest.raises(ValidationError):
        ColumnFilterRuleset(name="tenant", column="org", value="x")


def test_column_ruleset_is_frozen():
    rs = ColumnFilterRuleset(column="org", value="x")
    with pytest.raises(ValidationError):
        rs.column = "other"


def test_column_ruleset_kind_literal_enforced():
    with pytest.raises(ValidationError):
        ColumnFilterRuleset(kind="join", column="org", value="x")


# -- JoinHop / hop parsing helpers -------------------------------------------


def test_join_hop_removed_from_public_api():
    assert "JoinHop" not in policy_mod.__all__


def _hop(**kw):
    """Assemble a hop STRING from parts (default: orders -> customers)."""
    base = {
        "from_table": "orders",
        "from_column": "customer_id",
        "to_table": "customers",
        "to_column": "id",
    }
    base.update(kw)
    return (
        f"{base['from_table']}.{base['from_column']} = "
        f"{base['to_table']}.{base['to_column']}"
    )


def _join_rule(**kw):
    """JoinFilterRule factory (no column/value — hoisted to the ruleset)."""
    base = {"target_table": "orders", "join_path": [_hop()]}
    base.update(kw)
    return JoinFilterRule(**base)


# -- JoinFilterRule ----------------------------------------------------------


def test_join_rule_construction():
    rule = _join_rule()
    assert rule.target_table == "orders"
    assert rule.join_path == ("orders.customer_id = customers.id",)
    assert isinstance(rule.join_path, tuple)
    # no kind on the nested rule; no column/value/name
    assert not hasattr(rule, "column")
    assert not hasattr(rule, "value")


def test_join_rule_name_field_removed():
    with pytest.raises(ValidationError):
        _join_rule(name="by_customer")


def test_join_rule_column_value_removed():
    """column/value are hoisted to the ruleset — the nested rule rejects them."""
    hop = _hop()
    with pytest.raises(ValidationError):
        JoinFilterRule(
            target_table="orders",
            join_path=[hop],
            column="organization_uuid",
            value="7ef3",
        )


def test_join_path_elements_are_strings():
    assert all(isinstance(h, str) for h in _join_rule().join_path)


def test_join_rule_parses_hops():
    hops = _join_rule().parsed_hops
    assert len(hops) == 1
    assert hops[0].from_table == "orders"
    assert hops[0].from_column == "customer_id"
    assert hops[0].to_table == "customers"
    assert hops[0].to_column == "id"


def test_parsed_hops_absent_from_serialization():
    rule = _join_rule()
    assert "parsed_hops" not in rule.model_dump()
    json_dumped = rule.model_dump_json()
    assert "from_table" not in json_dumped
    assert "orders.customer_id = customers.id" in json_dumped


def test_join_rule_is_frozen():
    rule = _join_rule()
    with pytest.raises(ValidationError):
        rule.target_table = "other"


def test_join_rule_extra_forbidden():
    with pytest.raises(ValidationError):
        _join_rule(on_unapplicable="block")  # join rules have no such field


def test_join_rule_has_no_kind_field():
    """The nested rule has no discriminator — only the ruleset does."""
    with pytest.raises(ValidationError):
        _join_rule(kind="join")


def test_join_rule_join_path_list_coerced_to_tuple():
    assert isinstance(_join_rule(join_path=[_hop()]).join_path, tuple)


def test_join_rule_empty_join_path_rejected():
    with pytest.raises(ValidationError):
        _join_rule(join_path=[])


def test_bare_string_join_path_rejected():
    with pytest.raises(ValidationError):
        _join_rule(join_path="orders.customer_id = customers.id")


@pytest.mark.parametrize(
    "bad_element",
    [
        123,
        {
            "from_table": "orders",
            "from_column": "customer_id",
            "to_table": "customers",
            "to_column": "id",
        },
    ],
)
def test_non_string_hop_element_rejected(bad_element):
    with pytest.raises(ValidationError):
        _join_rule(join_path=[bad_element])


def test_joinhop_instance_hop_element_rejected():
    hop = JoinHop(
        from_table="orders", from_column="customer_id",
        to_table="customers", to_column="id",
    )
    with pytest.raises(ValidationError):
        _join_rule(join_path=[hop])


@pytest.mark.parametrize(
    "bad",
    [
        "orders.customer_id customers.id",  # no '='
        "a.b = c.d = e.f",  # more than one '='
        "orders = customers.id",  # left side has no dot
        "orders.customer_id = customers",  # right side has no dot
        ".customer_id = customers.id",  # blank left table
        "orders. = customers.id",  # blank left column
        "orders.customer_id = .id",  # blank right table
        "orders.customer_id = customers.",  # blank right column
        "",
        "   ",
    ],
)
def test_malformed_hop_string_rejected(bad):
    with pytest.raises(ValidationError):
        _join_rule(join_path=[bad])


def test_hop_whitespace_tolerant():
    rule = _join_rule(join_path=["   orders.customer_id   =   customers.id   "])
    assert rule.parsed_hops[0].from_table == "orders"
    assert rule.parsed_hops[0].to_column == "id"
    assert rule.join_path == ("   orders.customer_id   =   customers.id   ",)


# -- JoinFilterRule endpoint validation (either direction) -------------------


def test_join_rule_target_first_accepted():
    """Path written target-first (orders -> customers)."""
    rule = _join_rule(
        target_table="orders", join_path=["orders.customer_id = customers.id"]
    )
    assert rule.parsed_hops[0].from_table == "orders"


def test_join_rule_master_first_accepted():
    """A path may be written anchor-first, putting the target at the end."""
    rule = _join_rule(
        target_table="orders", join_path=["customers.id = orders.customer_id"]
    )
    # endpoints are {customers, orders}; target=orders is an endpoint
    assert rule._endpoints == ("customers", "orders")


def test_join_rule_target_not_an_endpoint_rejected():
    """target_table must be one of the path's two endpoints."""
    with pytest.raises(ValidationError):
        _join_rule(
            target_table="line_items",  # not an endpoint of orders<->customers
            join_path=["orders.customer_id = customers.id"],
        )


def test_join_rule_hops_must_chain():
    with pytest.raises(ValidationError):
        _join_rule(
            target_table="line_items",
            join_path=[
                "line_items.order_id = orders.id",
                "customers.region_id = regions.id",  # broken chain
            ],
        )


def test_join_rule_multihop_valid_chain():
    rule = _join_rule(
        target_table="line_items",
        join_path=[
            "line_items.order_id = orders.id",
            "orders.customer_id = customers.id",
        ],
    )
    assert len(rule.join_path) == 2
    assert rule._endpoints == ("line_items", "customers")


def test_endpoint_check_is_case_insensitive():
    rule = _join_rule(
        target_table="ORDERS", join_path=["orders.customer_id = customers.id"]
    )
    assert rule.target_table == "ORDERS"


# -- oriented_hops (target-first normalization) ------------------------------


def test_oriented_hops_target_first_unchanged():
    rule = _join_rule(
        target_table="orders", join_path=["orders.customer_id = customers.id"]
    )
    oriented = rule.oriented_hops()
    assert oriented[0].from_table == "orders"
    assert oriented[-1].to_table == "customers"


def test_oriented_hops_master_first_reversed():
    """An anchor-first path is reversed hop by hop so it ends up target-first."""
    rule = _join_rule(
        target_table="orders", join_path=["customers.id = orders.customer_id"]
    )
    oriented = rule.oriented_hops()
    assert oriented[0].from_table == "orders"
    assert oriented[0].from_column == "customer_id"
    assert oriented[-1].to_table == "customers"
    assert oriented[-1].to_column == "id"


def test_oriented_hops_multihop_master_first_reversed():
    rule = _join_rule(
        target_table="line_items",
        join_path=[
            "customers.id = orders.customer_id",
            "orders.id = line_items.order_id",
        ],
    )
    oriented = rule.oriented_hops()
    assert oriented[0].from_table == "line_items"
    assert oriented[-1].to_table == "customers"
    # chain stays valid after reversal
    for prev, cur in zip(oriented, oriented[1:]):
        assert cur.from_table == prev.to_table


# -- model_copy re-derivation / fail-closed ----------------------------------


def test_join_rule_model_copy_rederives_parsed_hops():
    rule = _join_rule()
    copied = rule.model_copy(
        update={
            "target_table": "line_items",
            "join_path": ("line_items.order_id = orders.id",),
        }
    )
    assert copied.parsed_hops[0].from_table == "line_items"


def test_join_rule_model_copy_breaking_chain_fails_closed():
    rule = _join_rule(
        target_table="line_items",
        join_path=[
            "line_items.order_id = orders.id",
            "orders.customer_id = customers.id",
        ],
    )
    broken = rule.model_copy(
        update={
            "join_path": (
                "line_items.order_id = orders.id",
                "customers.region_id = regions.id",  # non-chaining
            )
        }
    )
    with pytest.raises(ValueError):
        _ = broken.parsed_hops


# -- JoinFilterRuleset -------------------------------------------------------


def _join_ruleset(**kw):
    base = {
        "table": "customers",
        "column": "organization_uuid",
        "value": "7ef3",
        "joins": [_join_rule()],  # orders -> customers
    }
    base.update(kw)
    return JoinFilterRuleset(**base)


def test_join_ruleset_construction():
    rs = _join_ruleset()
    assert rs.kind == "join"
    assert rs.table == "customers"
    assert rs.column == "organization_uuid"
    assert rs.value == "7ef3"
    assert len(rs.joins) == 1
    assert isinstance(rs.joins, tuple)
    assert rs.whitelist == ()


def test_join_ruleset_name_field_removed():
    with pytest.raises(ValidationError):
        _join_ruleset(name="tenant")


def test_join_ruleset_value_list_coerced():
    rs = _join_ruleset(value=["a", "b"])
    assert rs.value == ("a", "b")


def test_join_ruleset_empty_value_rejected():
    with pytest.raises(ValidationError):
        _join_ruleset(value=[])


@pytest.mark.parametrize("field", ["table", "column"])
def test_join_ruleset_blank_field_rejected(field):
    with pytest.raises(ValidationError):
        _join_ruleset(**{field: "  "})


def test_join_ruleset_extra_forbidden():
    with pytest.raises(ValidationError):
        _join_ruleset(op="=")


def test_join_ruleset_is_frozen():
    rs = _join_ruleset()
    with pytest.raises(ValidationError):
        rs.table = "orders"


def test_join_ruleset_empty_joins_valid():
    """A ruleset with no joins (anchor + whitelist only) is valid."""
    rs = JoinFilterRuleset(
        table="customers", column="organization_uuid", value="7ef3",
        whitelist=["exchange_rates"],
    )
    assert rs.joins == ()
    assert rs.whitelist == ("exchange_rates",)


def test_join_ruleset_whitelist_list_coerced():
    rs = _join_ruleset(whitelist=["a", "b"])
    assert rs.whitelist == ("a", "b")
    assert isinstance(rs.whitelist, tuple)


# -- JoinFilterRuleset cross-rule validators ---------------------------------


def test_ruleset_non_target_endpoint_must_be_master():
    """The join path's non-target endpoint must equal the ruleset anchor."""
    # endpoints {line_items, orders}; master=customers absent
    rule = JoinFilterRule(
        target_table="line_items",
        join_path=["line_items.order_id = orders.id"],
    )
    with pytest.raises(ValidationError):
        JoinFilterRuleset(
            table="customers", column="organization_uuid", value="7ef3",
            joins=[rule],
        )


def test_ruleset_master_first_path_accepted():
    rs = JoinFilterRuleset(
        table="customers", column="organization_uuid", value="7ef3",
        joins=[
            JoinFilterRule(
                target_table="orders",
                join_path=["customers.id = orders.customer_id"],
            )
        ],
    )
    assert rs.joins[0].target_table == "orders"


def test_ruleset_master_as_intermediate_rejected():
    """The anchor mid-path is rejected even when the endpoints are {target, anchor}."""
    rule = JoinFilterRule(
        target_table="line_items",
        join_path=[
            "line_items.a = customers.b",   # master mid-path
            "customers.c = orders.d",
            "orders.e = customers.f",        # master terminal too
        ],
    )
    with pytest.raises(ValidationError):
        JoinFilterRuleset(
            table="customers", column="organization_uuid", value="7ef3",
            joins=[rule],
        )


def test_ruleset_join_targeting_anchor_rejected():
    rule = JoinFilterRule(
        target_table="customers",  # == anchor
        join_path=["customers.id = customers.id"],
    )
    with pytest.raises(ValidationError):
        JoinFilterRuleset(
            table="customers", column="organization_uuid", value="7ef3",
            joins=[rule],
        )


def test_ruleset_duplicate_target_rejected():
    rule_a = JoinFilterRule(
        target_table="orders",
        join_path=["orders.customer_id = customers.id"],
    )
    rule_b = JoinFilterRule(
        target_table="orders",  # duplicate
        join_path=["orders.region_id = customers.id"],
    )
    with pytest.raises(ValidationError):
        JoinFilterRuleset(
            table="customers", column="organization_uuid", value="7ef3",
            joins=[rule_a, rule_b],
        )


def test_ruleset_whitelist_intersects_target_rejected():
    rule = _join_rule()  # target orders
    with pytest.raises(ValidationError):
        JoinFilterRuleset(
            table="customers", column="organization_uuid", value="7ef3",
            joins=[rule],
            whitelist=["orders"],  # also whitelisted -> contradiction
        )


def test_ruleset_anchor_in_whitelist_rejected():
    with pytest.raises(ValidationError):
        _join_ruleset(whitelist=["customers"])  # anchor can't be whitelisted


def test_ruleset_qualified_anchor_requires_qualified_endpoint():
    """A qualified anchor reached via a bare endpoint is rejected: its schema would
    otherwise be dropped from the emitted SQL."""
    rule = JoinFilterRule(
        target_table="public.orders",
        join_path=["public.orders.customer_id = customers.id"],  # bare anchor endpoint
    )
    with pytest.raises(ValidationError):
        JoinFilterRuleset(
            table="public.customers", column="organization_uuid", value="7ef3",
            joins=[rule],
        )


def test_ruleset_qualified_anchor_qualified_endpoint_accepted():
    rule = JoinFilterRule(
        target_table="public.orders",
        join_path=["public.orders.customer_id = public.customers.id"],
    )
    rs = JoinFilterRuleset(
        table="public.customers", column="organization_uuid", value="7ef3",
        joins=[rule],
    )
    assert rs.table == "public.customers"


def test_ruleset_bare_anchor_qualified_endpoint_accepted():
    """Over-qualifying the endpoint of a bare anchor is safe and allowed."""
    rule = JoinFilterRule(
        target_table="orders",
        join_path=["orders.customer_id = public.customers.id"],
    )
    rs = JoinFilterRuleset(
        table="customers", column="organization_uuid", value="7ef3",
        joins=[rule],
    )
    assert rs.joins[0].target_table == "orders"


# -- SessionPolicy -----------------------------------------------------------


def test_bare_session_policy_rejected():
    """ruleset is required — a bare SessionPolicy() raises (no silent no-op)."""
    with pytest.raises(ValidationError):
        SessionPolicy()


def test_policy_with_column_ruleset():
    policy = SessionPolicy(ruleset=ColumnFilterRuleset(column="org", value="x"))
    assert policy.version == 1
    assert isinstance(policy.ruleset, ColumnFilterRuleset)


def test_policy_with_join_ruleset():
    policy = SessionPolicy(ruleset=_join_ruleset())
    assert isinstance(policy.ruleset, JoinFilterRuleset)


def test_policy_unknown_version_rejected():
    rs = ColumnFilterRuleset(column="org", value="x")
    with pytest.raises(ValidationError):
        SessionPolicy(version=2, ruleset=rs)


def test_policy_column_dict_with_kind():
    policy = SessionPolicy(ruleset={"kind": "column", "column": "org", "value": "x"})
    assert isinstance(policy.ruleset, ColumnFilterRuleset)


def test_policy_join_dict_with_kind():
    policy = SessionPolicy(
        ruleset={
            "kind": "join",
            "table": "customers",
            "column": "organization_uuid",
            "value": "7ef3",
            "joins": [
                {
                    "target_table": "orders",
                    "join_path": ["orders.customer_id = customers.id"],
                }
            ],
        }
    )
    assert isinstance(policy.ruleset, JoinFilterRuleset)
    assert policy.ruleset.joins[0].target_table == "orders"


def test_policy_kindless_dict_rejected():
    """kind is explicit — a kind-less dict ruleset cannot be discriminated."""
    with pytest.raises(ValidationError):
        SessionPolicy(ruleset={"column": "org", "value": "x"})


def test_policy_kindless_join_dict_rejected():
    with pytest.raises(ValidationError):
        SessionPolicy(
            ruleset={
                "table": "customers",
                "column": "organization_uuid",
                "value": "7ef3",
            }
        )


def test_policy_data_filters_rejected():
    """Hard break: the old data_filters= kwarg is gone (extra-forbid)."""
    rs = ColumnFilterRuleset(column="org", value="x")
    with pytest.raises(ValidationError):
        SessionPolicy(data_filters=[rs])


def test_policy_extra_forbidden():
    rs = ColumnFilterRuleset(column="org", value="x")
    with pytest.raises(ValidationError):
        SessionPolicy(ruleset=rs, extra_field=1)


def test_policy_is_frozen():
    policy = SessionPolicy(ruleset=ColumnFilterRuleset(column="org", value="x"))
    with pytest.raises(ValidationError):
        policy.ruleset = ColumnFilterRuleset(column="other", value="y")


def test_join_only_policy_needs_no_column_backstop():
    """A join ruleset needs no column-rule backstop; the whitelist subsumes it."""
    policy = SessionPolicy(ruleset=_join_ruleset())
    assert isinstance(policy.ruleset, JoinFilterRuleset)


def test_removed_symbols_absent_from_public_api():
    for gone in ("ColumnFilterRule", "DataFilterRule"):
        assert gone not in policy_mod.__all__
        assert not hasattr(policy_mod, gone)


def test_policy_full_round_trip():
    original = SessionPolicy(
        ruleset={
            "kind": "join",
            "table": "customers",
            "column": "organization_uuid",
            "value": "7ef3",
            "joins": [
                {
                    "target_table": "line_items",
                    "join_path": [
                        "line_items.order_id = orders.id",
                        "orders.customer_id = customers.id",
                    ],
                }
            ],
            "whitelist": ["exchange_rates"],
        }
    )
    rebuilt = SessionPolicy(**original.model_dump())
    assert isinstance(rebuilt.ruleset, JoinFilterRuleset)
    assert rebuilt.ruleset.joins[0].parsed_hops[1].to_table == "customers"
    assert rebuilt.ruleset.whitelist == ("exchange_rates",)
