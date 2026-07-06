"""Unit tests for the session-policy data model (DEV-1578).

``ColumnFilterRule`` / ``SessionPolicy`` are the immutable, agent-invisible
forced-filter configuration. These tests pin validation, immutability
(``frozen`` + tuple fields so list contents can't be mutated after init),
``extra="forbid"``, the empty-list / blank-column rejections, and the
scalar-vs-list value shape that drives ``=`` vs ``IN`` at the SQL layer.
"""

import pytest
from pydantic import ValidationError

import slayer.core.policy as policy_mod
from slayer.core.policy import (
    ColumnFilterRule,
    JoinFilterRule,
    JoinHop,  # internal parse product — imported only to pin strings-only input
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


# -- JoinFilterRule hop parsing (DEV-1627) -----------------------------------
#
# Hops are authored as strings "from_table.from_column = to_table.to_column".
# Each is parsed into an internal (never-serialized) representation, derived
# fresh from ``join_path`` on every access to ``parsed_hops`` (no cache); the
# public ``join_path`` stays a tuple of the original strings and round-trips
# symmetrically.


def test_join_hop_removed_from_public_api():
    """JoinHop is an internal parse product only (locked plan item)."""
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
    base = {
        "target_table": "orders",
        "join_path": [_hop()],
        "column": "organization_uuid",
        "value": "7ef3",
    }
    base.update(kw)
    return JoinFilterRule(**base)


def test_join_rule_construction():
    rule = _join_rule()
    assert rule.kind == "join"
    assert rule.target_table == "orders"
    assert rule.column == "organization_uuid"
    assert rule.value == "7ef3"
    assert rule.name is None
    assert rule.join_path == ("orders.customer_id = customers.id",)
    assert isinstance(rule.join_path, tuple)


def test_join_path_elements_are_strings():
    rule = _join_rule()
    assert all(isinstance(h, str) for h in rule.join_path)


def test_join_rule_parses_hops_into_internal_representation():
    hops = _join_rule().parsed_hops
    assert len(hops) == 1
    assert hops[0].from_table == "orders"
    assert hops[0].from_column == "customer_id"
    assert hops[0].to_table == "customers"
    assert hops[0].to_column == "id"


def test_parsed_hops_absent_from_serialization():
    rule = _join_rule()
    dumped = rule.model_dump()
    assert "parsed_hops" not in dumped
    assert "_parsed_hops" not in dumped
    # symmetric serialization: JSON carries only the hop strings, no hop object
    json_dumped = rule.model_dump_json()
    assert "parsed_hops" not in json_dumped
    assert "from_table" not in json_dumped
    assert "orders.customer_id = customers.id" in json_dumped


def test_join_rule_is_frozen():
    rule = _join_rule()
    with pytest.raises(ValidationError):
        rule.target_table = "other"


def test_join_rule_extra_forbidden():
    with pytest.raises(ValidationError):
        _join_rule(on_unapplicable="block")  # join rules have no such field


def test_join_rule_kind_literal_enforced():
    hop = _hop()
    with pytest.raises(ValidationError):
        JoinFilterRule(
            kind="column",
            target_table="orders",
            join_path=[hop],
            column="org",
            value="x",
        )


def test_join_rule_join_path_list_coerced_to_tuple():
    rule = _join_rule(join_path=[_hop()])
    assert isinstance(rule.join_path, tuple)


def test_join_rule_empty_join_path_rejected():
    with pytest.raises(ValidationError):
        _join_rule(join_path=[])


# -- hop string parsing: success ---------------------------------------------


def test_hop_whitespace_tolerant():
    rule = _join_rule(join_path=["   orders.customer_id   =   customers.id   "])
    assert rule.parsed_hops[0].from_table == "orders"
    assert rule.parsed_hops[0].from_column == "customer_id"
    assert rule.parsed_hops[0].to_table == "customers"
    assert rule.parsed_hops[0].to_column == "id"
    # the public field preserves the original string verbatim
    assert rule.join_path == ("   orders.customer_id   =   customers.id   ",)


def test_hop_schema_qualified_tables():
    rule = _join_rule(
        target_table="public.orders",
        join_path=["public.orders.customer_id = public.customers.id"],
    )
    assert rule.target_table == "public.orders"
    assert rule.parsed_hops[0].from_table == "public.orders"
    assert rule.parsed_hops[0].to_table == "public.customers"
    assert rule.parsed_hops[0].to_column == "id"


def test_hop_catalog_qualified_tables():
    rule = _join_rule(
        target_table="proj.dataset.orders",
        join_path=[
            "proj.dataset.orders.customer_id = proj.dataset.customers.id"
        ],
    )
    assert rule.parsed_hops[0].from_table == "proj.dataset.orders"
    assert rule.parsed_hops[0].to_table == "proj.dataset.customers"


def test_hop_dotted_column_name_not_expressible():
    """Documented out-of-scope narrowing: a column literally named with a dot
    can't be expressed — the last dot always splits table/column, so
    ``customers.weird.id`` parses as table ``customers.weird`` col ``id``."""
    rule = _join_rule(join_path=["orders.customer_id = customers.weird.id"])
    assert rule.parsed_hops[0].to_table == "customers.weird"
    assert rule.parsed_hops[0].to_column == "id"


# -- hop string parsing: failure ---------------------------------------------


@pytest.mark.parametrize(
    "bad",
    [
        "orders.customer_id customers.id",  # no '='
        "a.b = c.d = e.f",  # more than one '='
        "orders = customers.id",  # left side has no dot
        "orders.customer_id = customers",  # right side has no dot
        ".customer_id = customers.id",  # blank left table (leading dot)
        "orders. = customers.id",  # blank left column
        "orders.customer_id = .id",  # blank right table
        "orders.customer_id = customers.",  # blank right column
        "",  # empty
        "   ",  # whitespace only
    ],
)
def test_malformed_hop_string_rejected(bad):
    with pytest.raises(ValidationError):
        _join_rule(join_path=[bad])


def test_bare_string_join_path_rejected():
    """A single hop string (not wrapped in a list) is rejected, never silently
    iterated into a tuple of characters."""
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
        ["orders.customer_id = customers.id"],  # nested list
    ],
)
def test_non_string_hop_element_rejected(bad_element):
    """Strings-only input: a JoinHop dict / non-string hop is rejected."""
    with pytest.raises(ValidationError):
        _join_rule(join_path=[bad_element])


def test_joinhop_instance_hop_element_rejected():
    """Strings-only input: even the internal JoinHop instance is rejected as a
    hop element (callers must pass strings)."""
    hop = JoinHop(
        from_table="orders",
        from_column="customer_id",
        to_table="customers",
        to_column="id",
    )
    with pytest.raises(ValidationError):
        _join_rule(join_path=[hop])


# -- chain validators (operate on the parsed hops) ---------------------------


def test_join_rule_first_hop_must_start_at_target():
    """The first hop's from_table must equal target_table."""
    with pytest.raises(ValidationError):
        _join_rule(
            target_table="orders",
            join_path=["line_items.order_id = customers.id"],
        )


def test_join_rule_hops_must_chain():
    """Each hop's from_table must equal the previous hop's to_table."""
    with pytest.raises(ValidationError):
        _join_rule(
            target_table="line_items",
            join_path=[
                "line_items.order_id = orders.id",
                # broken chain: starts at "customers", not "orders"
                "customers.region_id = regions.id",
            ],
        )


def test_join_rule_multihop_valid_chain():
    rule = _join_rule(
        target_table="line_items",
        join_path=[
            "line_items.order_id = orders.id",
            "orders.customer_id = customers.id",
        ],
        column="organization_uuid",
        value="7ef3",
    )
    assert len(rule.join_path) == 2
    assert rule.parsed_hops[1].to_table == "customers"


def test_chain_check_is_case_insensitive():
    rule = _join_rule(
        target_table="ORDERS",
        join_path=["orders.customer_id = customers.id"],
    )
    assert rule.parsed_hops[0].to_table == "customers"


def test_inter_hop_chain_check_is_case_insensitive():
    """Consecutive hops chain case-insensitively (prev.to_table vs
    next.from_table differ only by case)."""
    rule = _join_rule(
        target_table="line_items",
        join_path=[
            "line_items.order_id = Orders.id",
            "ORDERS.customer_id = customers.id",
        ],
    )
    assert rule.parsed_hops[1].to_table == "customers"


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


# -- serialization round-trip ------------------------------------------------


def test_join_rule_serializes_join_path_as_strings():
    rule = _join_rule(
        target_table="line_items",
        join_path=[
            "line_items.order_id = orders.id",
            "orders.customer_id = customers.id",
        ],
    )
    dumped = rule.model_dump()
    assert list(dumped["join_path"]) == [
        "line_items.order_id = orders.id",
        "orders.customer_id = customers.id",
    ]
    assert all(isinstance(h, str) for h in dumped["join_path"])


def test_join_rule_json_round_trip_rebuilds_parsed_hops():
    rule = _join_rule()
    reloaded = JoinFilterRule.model_validate_json(rule.model_dump_json())
    assert reloaded.join_path == rule.join_path
    assert reloaded.parsed_hops[0].to_table == "customers"


def test_join_rule_model_copy_update_rederives_parsed_hops():
    """Bulletproof: parsed_hops derives from join_path on access, so a
    model_copy swapping join_path re-derives (no stale cache)."""
    rule = _join_rule()
    copied = rule.model_copy(
        update={
            "target_table": "line_items",
            "join_path": ("line_items.order_id = orders.id",),
        }
    )
    assert copied.parsed_hops[0].from_table == "line_items"
    assert copied.parsed_hops[0].to_table == "orders"


def test_join_rule_model_copy_breaking_chain_fails_closed():
    """model_copy(update=) bypasses Pydantic validation, but parsed_hops
    re-validates the chain on access, so a copy that swaps join_path to a
    non-chaining path fails closed (raises) rather than feeding SQL generation
    a bad correlation."""
    rule = _join_rule()  # target_table="orders"
    broken = rule.model_copy(
        update={"join_path": ("line_items.order_id = customers.id",)}
    )
    with pytest.raises(ValueError):
        _ = broken.parsed_hops


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
                "join_path": ["orders.customer_id = customers.id"],
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
                "join_path": ["orders.customer_id = customers.id"],
                "column": "organization_uuid",
                "value": "7ef3",
            },
        ]
    )
    assert any(isinstance(r, JoinFilterRule) for r in policy.data_filters)


def test_policy_join_rule_dict_hop_object_rejected():
    """Strings-only: a structured hop dict inside a policy dict is rejected."""
    with pytest.raises(ValidationError):
        SessionPolicy(
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


def test_policy_join_rule_full_round_trip():
    """Kind-less dict with string hops -> dump -> reconstruct -> parsed_hops
    rebuilt (Codex: exercise discriminator inference + re-derivation)."""
    original = SessionPolicy(
        data_filters=[
            _BLOCK_BACKSTOP,
            {
                "target_table": "line_items",
                "join_path": [
                    "line_items.order_id = orders.id",
                    "orders.customer_id = customers.id",
                ],
                "column": "organization_uuid",
                "value": "7ef3",
            },
        ]
    )
    dumped = original.model_dump()
    rebuilt = SessionPolicy(**dumped)
    join = next(r for r in rebuilt.data_filters if isinstance(r, JoinFilterRule))
    assert join.join_path == (
        "line_items.order_id = orders.id",
        "orders.customer_id = customers.id",
    )
    assert join.parsed_hops[1].to_table == "customers"


def test_policy_column_rule_dict_without_kind_still_inferred():
    """Backward-compat: a kind-less column dict still resolves to a column
    rule (pins the existing DEV-1578 dict shape under the new union)."""
    policy = SessionPolicy(data_filters=[{"column": "org", "value": "x"}])
    assert isinstance(policy.data_filters[0], ColumnFilterRule)


# -- mandatory block backstop (DEV-1627, decision 5) -------------------------


def test_join_only_policy_rejected_no_backstop():
    """A policy with a join rule but no block column rule fails closed at
    construction — an untargeted table could otherwise leak unfiltered."""
    rule = _join_rule()
    with pytest.raises(ValidationError):
        SessionPolicy(data_filters=[rule])


def test_join_plus_pass_only_column_rule_rejected():
    """A pass-only column rule is not a valid backstop: it leaves an absent-
    column table unfiltered, so it does not satisfy the requirement."""
    filters = [
        ColumnFilterRule(
            column="organization_uuid", value="x", on_unapplicable="pass"
        ),
        _join_rule(),
    ]
    with pytest.raises(ValidationError):
        SessionPolicy(data_filters=filters)


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
