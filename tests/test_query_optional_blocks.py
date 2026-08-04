"""Optional-block ``{? ... ?}`` substitution + structural variable extraction (DEV-1730).

These pin the Mode-A optional-filter idiom used to represent Cube FILTER_PARAMS
pushdowns: a delimited block that renders its content (parenthesised) when every
``{var}`` inside is supplied, and collapses to the neutral ``(1=1)`` when any is
missing. Plus ``extract_model_variables`` — the structural required/optional
classifier that backs inspect discoverability.
"""

import pytest

from slayer.core.models import Column, SlayerModel
from slayer.core.query import extract_model_variables, substitute_variables

# ── block rendering (escape="sql") ──────────────────────────────────────────


def test_block_renders_parenthesised_when_var_present():
    out = substitute_variables(
        "WHERE 1=1 AND {? brand IN ({brand}) ?}",
        {"brand": ["acme", "zeta"]},
        escape="sql",
    )
    assert out == "WHERE 1=1 AND (brand IN ('acme', 'zeta'))"


def test_block_collapses_to_one_equals_one_when_var_missing():
    out = substitute_variables(
        "WHERE 1=1 AND {? brand IN ({brand}) ?}",
        {},
        escape="sql",
    )
    assert out == "WHERE 1=1 AND (1=1)"


def test_block_collapses_if_any_inner_var_missing():
    # from present, to absent -> whole block collapses.
    out = substitute_variables(
        "AND {? d >= '{d_from}' AND d <= '{d_to}' ?}",
        {"d_from": "2025-01-01"},
        escape="sql",
    )
    assert out == "AND (1=1)"


def test_block_renders_range_when_all_present():
    out = substitute_variables(
        "AND {? d >= '{d_from}' AND d <= '{d_to}' ?}",
        {"d_from": "2025-01-01", "d_to": "2025-12-31"},
        escape="sql",
    )
    assert out == "AND (d >= '2025-01-01' AND d <= '2025-12-31')"


def test_scalar_position_block_collapse_reproduces_cube_cast_shape():
    # Optional arrow in scalar position -> the (1=1)::TYPE Cube "booby-trap".
    out = substitute_variables(
        "{? '{d_from}' ?}::TIMESTAMP AS d", {}, escape="sql"
    )
    assert out == "(1=1)::TIMESTAMP AS d"


def test_multiple_independent_blocks_mix_present_and_missing():
    out = substitute_variables(
        "WHERE 1=1 AND {? a IN ({a}) ?} AND {? b IN ({b}) ?}",
        {"a": ["x"]},
        escape="sql",
    )
    assert out == "WHERE 1=1 AND (a IN ('x')) AND (1=1)"


def test_string_value_inside_block_is_escaped():
    out = substitute_variables(
        "{? name = '{name}' ?}", {"name": "O'Brien"}, escape="sql"
    )
    assert out == "(name = 'O''Brien')"


def test_block_with_list_containing_apostrophe_and_comma():
    # Block + IN-list + SQL escaping interaction: commas inside a value must
    # not split it into two list elements; apostrophes are quote-doubled.
    out = substitute_variables(
        "{? brand IN ({brand}) ?}",
        {"brand": ["O'Reilly", "ACME, Inc."]},
        escape="sql",
    )
    assert out == "(brand IN ('O''Reilly', 'ACME, Inc.'))"


def test_plain_vars_still_substitute_alongside_blocks():
    out = substitute_variables(
        "d >= '{d_from}' AND {? brand IN ({brand}) ?}",
        {"d_from": "2025-01-01", "brand": ["a"]},
        escape="sql",
    )
    assert out == "d >= '2025-01-01' AND (brand IN ('a'))"


def test_brace_escapes_do_not_open_or_close_a_block():
    # {{ and }} stay literal; no block is parsed here.
    out = substitute_variables("{{not a block}}", {}, escape="sql")
    assert out == "{not a block}"


# ── block error cases ───────────────────────────────────────────────────────


def test_nested_block_raises():
    with pytest.raises(ValueError, match="[Nn]est"):
        substitute_variables("{? a {? {b} ?} ?}", {"b": "1"}, escape="sql")


def test_unterminated_block_raises():
    with pytest.raises(ValueError, match="[Uu]nterminated|unclosed"):
        substitute_variables("AND {? brand IN ({brand})", {"brand": ["a"]}, escape="sql")


def test_stray_block_close_raises():
    with pytest.raises(ValueError):
        substitute_variables("AND brand ?}", {}, escape="sql")


def test_block_with_no_variables_raises():
    with pytest.raises(ValueError, match="[Vv]ariable"):
        substitute_variables("{? 1=1 ?}", {}, escape="sql")


def test_block_in_python_mode_raises():
    # Mode-B query filters must reject the block syntax outright.
    with pytest.raises(ValueError):
        substitute_variables("{? x IN ({x}) ?}", {"x": ["a"]}, escape="python")


# ── extract_model_variables ─────────────────────────────────────────────────


def _sql_model(**kw) -> SlayerModel:
    kw.setdefault("name", "m")
    kw.setdefault("data_source", "ds")
    return SlayerModel(**kw)


def test_extract_bare_var_without_default_is_required():
    model = _sql_model(sql="SELECT * FROM t WHERE d >= '{d_from}'")
    v = extract_model_variables(model)
    assert v.required == ["d_from"]
    assert v.optional == []


def test_extract_bare_var_with_default_is_optional():
    model = _sql_model(
        sql="SELECT * FROM t WHERE region = '{region}'",
        query_variables={"region": "EU"},
    )
    v = extract_model_variables(model)
    assert v.required == []
    assert v.optional == ["region"]


def test_extract_in_block_var_is_optional():
    model = _sql_model(sql="SELECT * FROM t WHERE 1=1 AND {? brand IN ({brand}) ?}")
    v = extract_model_variables(model)
    assert v.required == []
    assert v.optional == ["brand"]


def test_extract_bare_occurrence_wins_over_in_block():
    # A var used bare somewhere AND inside a block classifies as required.
    model = _sql_model(
        sql="SELECT '{x}' AS a, CASE WHEN 1=1 THEN {? y = {x} ?} END FROM t"
    )
    v = extract_model_variables(model)
    assert "x" in v.required
    assert "x" not in v.optional


def test_extract_walks_all_four_mode_a_surfaces():
    model = _sql_model(
        sql="SELECT * FROM t WHERE {? a IN ({a}) ?}",
        filters=["b >= '{b_from}'"],
        columns=[
            Column(name="c1", sql="CASE WHEN {? c IN ({c}) ?} THEN 1 END"),
            Column(name="c2", sql="x", filter="d = '{d}'"),
        ],
    )
    v = extract_model_variables(model)
    assert set(v.optional) == {"a", "c"}
    assert set(v.required) == {"b_from", "d"}


def test_extract_on_malformed_block_does_not_raise():
    # Read-only inspection must never crash on a stored model whose SQL only
    # *looks* like a block (a literal '?}' e.g. inside a regex/JSON path). It is
    # classified as block-free; execution still raises (DEV-1730 review).
    model = _sql_model(sql="SELECT * FROM t WHERE tag ~ 'a?}' AND x = '{y}'")
    v = extract_model_variables(model)
    assert v.required == ["y"]


def test_extract_dedupes_and_sorts():
    model = _sql_model(
        sql="WHERE {? a IN ({a}) ?} AND {? a IN ({a}) ?}",
        columns=[Column(name="c", sql="z >= '{z_from}'")],
    )
    v = extract_model_variables(model)
    assert v.optional == ["a"]
    assert v.required == ["z_from"]
