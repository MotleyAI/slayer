"""Shared FILTER_PARAMS translation (slayer/cube/filter_params.py, DEV-1730).

The FILTER_PARAMS micro-grammar maps to SLayer Mode-A optional blocks / plain
vars. Ref construction is AST/text-agnostic (the JS parser feeds arrow segments,
the YAML path feeds a string col-expr); requiredness (block vs bare) is applied
downstream by the converter. These pin the emitted SQL text for each form.
"""


from slayer.cube.filter_params import (
    apply_filter_params,
    build_arrow_ref,
    build_string_ref,
    filter_param_sentinel,
    parse_string_filter_params,
    render_filter_param,
)
from slayer.cube.models import CubeFilterParamRef

# ── ref construction ────────────────────────────────────────────────────────


def test_string_ref_builds_in_list_body():
    ref = build_string_ref(
        cube="RrDrivers", member="brand", col_expr='pr."BRAND"',
        sentinel=filter_param_sentinel(0),
    )
    assert ref.kind == "string"
    assert ref.member == "brand"
    assert ref.body_template == 'pr."BRAND" IN ({brand})'
    assert ref.var_names == ["brand"]


def test_arrow_value_ref_single_prequoted_param():
    ref = build_arrow_ref(
        cube="RrDrivers", member="fulfillment_date",
        segments=[("param", "from")], sentinel=filter_param_sentinel(0),
    )
    assert ref.kind == "arrow_value"
    assert ref.body_template == "'{fulfillment_date_from}'"
    assert ref.var_names == ["fulfillment_date_from"]


def test_arrow_range_ref_concat_body():
    ref = build_arrow_ref(
        cube="RrDrivers", member="fulfillment_date",
        segments=[
            ("lit", 'fol."FD" >= '),
            ("param", "from"),
            ("lit", ' AND fol."FD" <= '),
            ("param", "to"),
        ],
        sentinel=filter_param_sentinel(0),
    )
    assert ref.kind == "arrow_range"
    assert ref.body_template == (
        'fol."FD" >= \'{fulfillment_date_from}\' '
        'AND fol."FD" <= \'{fulfillment_date_to}\''
    )
    assert ref.var_names == ["fulfillment_date_from", "fulfillment_date_to"]


def test_arrow_ref_preserves_ly_shifted_wrapper():
    ref = build_arrow_ref(
        cube="RrDrivers", member="fulfillment_date",
        segments=[
            ("lit", 'fol."FD" >= DATEADD(YEAR, -1, '),
            ("param", "from"),
            ("lit", ") AND fol.\"FD\" <= DATEADD(YEAR, -1, "),
            ("param", "to"),
            ("lit", ")"),
        ],
        sentinel=filter_param_sentinel(0),
    )
    assert "DATEADD(YEAR, -1, '{fulfillment_date_from}')" in ref.body_template
    assert "DATEADD(YEAR, -1, '{fulfillment_date_to}')" in ref.body_template


# ── render: optional (block) vs required (bare) ─────────────────────────────


def test_render_optional_wraps_in_block():
    ref = build_string_ref(
        cube="c", member="brand", col_expr="brand", sentinel=filter_param_sentinel(0)
    )
    assert render_filter_param(ref, required=False) == "{? brand IN ({brand}) ?}"


def test_render_required_is_bare():
    ref = build_string_ref(
        cube="c", member="brand", col_expr="brand", sentinel=filter_param_sentinel(0)
    )
    assert render_filter_param(ref, required=True) == "brand IN ({brand})"


def test_apply_filter_params_replaces_sentinels_by_requiredness():
    s0, s1 = filter_param_sentinel(0), filter_param_sentinel(1)
    r0 = build_arrow_ref(
        cube="c", member="fulfillment_date", segments=[("param", "from")], sentinel=s0
    )
    r1 = build_string_ref(cube="c", member="brand", col_expr="brand", sentinel=s1)
    text = f"SELECT {s0}::TIMESTAMP AS d FROM t WHERE 1=1 AND {s1}"
    out = apply_filter_params(
        text, [r0, r1], required_members={"fulfillment_date"}
    )
    assert out == (
        "SELECT '{fulfillment_date_from}'::TIMESTAMP AS d "
        "FROM t WHERE 1=1 AND {? brand IN ({brand}) ?}"
    )


# ── YAML text path (string-arg only) ────────────────────────────────────────


def test_parse_string_filter_params_extracts_and_sentinels():
    text = "WHERE 1=1 AND {FILTER_PARAMS.orders.status.filter('o.status')}"
    result = parse_string_filter_params(text, host_cube="orders")
    assert len(result.refs) == 1
    ref = result.refs[0]
    assert ref.member == "status"
    assert ref.body_template == "o.status IN ({status})"
    # the ref's sentinel replaced the FILTER_PARAMS ref in the returned text
    assert "FILTER_PARAMS" not in result.text
    assert ref.sentinel in result.text
    assert not result.unsupported


def test_parse_string_filter_params_arrow_in_yaml_is_unsupported():
    text = "AND {FILTER_PARAMS.orders.d.filter((from, to) => from)}"
    result = parse_string_filter_params(text, host_cube="orders")
    assert result.refs == []
    assert len(result.unsupported) == 1
    assert "d" in result.unsupported[0].raw or "d" == result.unsupported[0].member


def test_parse_string_filter_params_cross_cube_is_unsupported():
    # Stage-1: the cube segment must equal the host cube.
    text = "AND {FILTER_PARAMS.other.status.filter('status')}"
    result = parse_string_filter_params(text, host_cube="orders")
    assert result.refs == []
    assert len(result.unsupported) == 1


def test_parse_string_filter_params_no_filter_params_is_noop():
    text = "WHERE deleted_at IS NULL"
    result = parse_string_filter_params(text, host_cube="orders")
    assert result.text == text
    assert result.refs == []
    assert result.unsupported == []


def test_filter_param_ref_is_cube_model_validatable():
    # CubeCube must be able to carry these (JS parser populates them).
    d = {
        "cube": "c", "member": "brand", "kind": "string",
        "body_template": "brand IN ({brand})", "var_names": ["brand"],
        "sentinel": filter_param_sentinel(0),
    }
    ref = CubeFilterParamRef.model_validate(d)
    assert ref.member == "brand"
