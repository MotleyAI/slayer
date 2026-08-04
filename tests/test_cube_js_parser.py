"""JavaScript Cube-config front-end (slayer/cube/js_parser.py, DEV-1730).

Parses the declarative ``cube('Name', {...})`` / ``view(...)`` subset via an
esprima ESTree AST into the same ``CubeCube`` / ``CubeView`` shapes the YAML
front-end produces, so the converter is front-end-agnostic. FILTER_PARAMS
interpolations are captured as structured refs (sentinels in the surface text).
"""


from slayer.cube.js_parser import parse_cube_js
from slayer.cube.report import CubeIssueCategory


def _one_cube(source: str):
    result = parse_cube_js(source)
    assert not result.issues, [i.message for i in result.issues]
    assert len(result.cubes) == 1
    return result.cubes[0]


# ── basic shapes ────────────────────────────────────────────────────────────


def test_template_literal_cube_name_and_sql_table():
    cube = _one_cube(
        "cube(`Orders`, { sql_table: `public.orders`, "
        "dimensions: { id: { sql: `${CUBE}.id`, type: `number`, primaryKey: true } } });"
    )
    assert cube.name == "Orders"
    assert cube.sql_table == "public.orders"
    assert len(cube.dimensions) == 1
    assert cube.dimensions[0].name == "id"
    assert cube.dimensions[0].primary_key is True  # camelCase primaryKey normalised


def test_string_literal_cube_name():
    cube = _one_cube("cube('Orders', { sql_table: 'public.orders' });")
    assert cube.name == "Orders"


def test_dimensions_object_becomes_ordered_list_with_names():
    cube = _one_cube(
        "cube(`C`, { sql_table: `t`, dimensions: {"
        "  a: { sql: `${CUBE}.a`, type: `string` },"
        "  b: { sql: `${CUBE}.b`, type: `string` },"
        "  c: { sql: `${CUBE}.c`, type: `string` }"
        "} });"
    )
    assert [d.name for d in cube.dimensions] == ["a", "b", "c"]


def test_cube_ref_interpolation_becomes_single_brace():
    cube = _one_cube(
        "cube(`C`, { sql_table: `t`, dimensions: {"
        "  pk: { sql: `${CUBE}.\"ID\" || '|' || ${CUBE}.effect`, type: `string`, primaryKey: true, public: false }"
        "} });"
    )
    dim = cube.dimensions[0]
    assert dim.sql == '{CUBE}."ID" || \'|\' || {CUBE}.effect'
    assert dim.primary_key is True
    assert dim.public is False


def test_measure_named_count_and_max_with_format():
    cube = _one_cube(
        "cube(`C`, { sql_table: `t`, measures: {"
        "  count: { type: `count`, title: `Driver Count` },"
        "  share_ty: { sql: `${CUBE}.share_ty`, type: `max`, format: `percent`, description: `Share TY` }"
        "} });"
    )
    by_name = {m.name: m for m in cube.measures}
    assert by_name["count"].type == "count"
    assert by_name["count"].title == "Driver Count"
    assert by_name["share_ty"].type == "max"
    assert by_name["share_ty"].format == "percent"


def test_calc_measure_interpolation_becomes_brace_refs():
    cube = _one_cube(
        "cube(`C`, { sql_table: `t`, measures: {"
        "  share_change: { sql: `${share_ty} - ${share_ly}`, type: `number`, format: `percent` }"
        "} });"
    )
    m = cube.measures[0]
    assert m.sql == "{share_ty} - {share_ly}"
    assert m.type == "number"


def test_meta_object_preserved_verbatim_including_capitals():
    cube = _one_cube(
        "cube(`C`, { sql_table: `t`, dimensions: {"
        "  d: { sql: `${CUBE}.d`, type: `time`, meta: { required: true, camelKey: 42 } }"
        "} });"
    )
    meta = cube.dimensions[0].meta
    assert meta == {"required": True, "camelKey": 42}


def test_line_and_block_comments_tolerated():
    cube = _one_cube(
        "cube(`C`, {\n"
        "  // a line comment\n"
        "  sql_table: `t`, /* block */ dimensions: {"
        "    a: { sql: `${CUBE}.a`, type: `string` } // trailing\n"
        "  }\n"
        "});"
    )
    assert cube.name == "C"
    assert [d.name for d in cube.dimensions] == ["a"]


def test_empty_pre_aggregations_object_ok():
    cube = _one_cube("cube(`C`, { sql_table: `t`, pre_aggregations: {} });")
    assert cube.name == "C"


def test_negative_number_literal():
    cube = _one_cube(
        "cube(`C`, { sql_table: `t`, dimensions: {"
        "  a: { sql: `${CUBE}.a`, type: `number`, meta: { lo: -5 } }"
        "} });"
    )
    assert cube.dimensions[0].meta == {"lo": -5}


# ── multiple objects / views ────────────────────────────────────────────────


def test_multiple_cubes_in_one_file():
    result = parse_cube_js(
        "cube(`A`, { sql_table: `a` });\ncube(`B`, { sql_table: `b` });"
    )
    assert {c.name for c in result.cubes} == {"A", "B"}


def test_view_is_parsed_with_parity():
    result = parse_cube_js(
        "view(`MyView`, { cubes: [ { join_path: `orders`, includes: `*` } ] });"
    )
    assert not result.issues, [i.message for i in result.issues]
    assert len(result.views) == 1
    assert result.views[0].name == "MyView"
    assert result.views[0].cubes[0].join_path == "orders"


def test_module_exports_wrapper_recognised():
    result = parse_cube_js("module.exports = cube(`C`, { sql_table: `t` });")
    assert len(result.cubes) == 1
    assert result.cubes[0].name == "C"


# ── FILTER_PARAMS capture ───────────────────────────────────────────────────


def test_filter_params_string_form_captured_as_ref():
    cube = _one_cube(
        "cube(`RrDrivers`, { sql: `SELECT * FROM t WHERE 1=1 "
        "AND ${FILTER_PARAMS.RrDrivers.brand.filter('pr.\"BRAND\"')}`, "
        "dimensions: { brand: { sql: `${CUBE}.\"BRAND\"`, type: `string` } } });"
    )
    assert len(cube.filter_params) == 1
    ref = cube.filter_params[0]
    assert ref.member == "brand"
    assert ref.kind == "string"
    assert ref.body_template == 'pr."BRAND" IN ({brand})'
    # the sentinel replaced the FILTER_PARAMS interpolation in the sql text
    assert "FILTER_PARAMS" not in cube.sql
    assert ref.sentinel in cube.sql


def test_filter_params_arrow_value_and_range_captured():
    cube = _one_cube(
        "cube(`RrDrivers`, { sql: `SELECT "
        "${FILTER_PARAMS.RrDrivers.fulfillment_date.filter((from, to) => from)}::TIMESTAMP AS d "
        "FROM t WHERE "
        "${FILTER_PARAMS.RrDrivers.fulfillment_date.filter((from, to) => 'x >= ' + from + ' AND x <= ' + to)}`, "
        "dimensions: { fulfillment_date: { sql: `${CUBE}.d`, type: `time`, meta: { required: true } } } });"
    )
    kinds = sorted(r.kind for r in cube.filter_params)
    assert kinds == ["arrow_range", "arrow_value"]
    rng = next(r for r in cube.filter_params if r.kind == "arrow_range")
    assert rng.var_names == ["fulfillment_date_from", "fulfillment_date_to"]
    assert "x >= '{fulfillment_date_from}'" in rng.body_template


# ── error / report paths ────────────────────────────────────────────────────


def test_syntax_error_reports_parse_error():
    result = parse_cube_js("cube(`C`, { sql_table: ")
    assert result.cubes == []
    assert any(i.category == CubeIssueCategory.PARSE_ERROR for i in result.issues)


def test_dynamic_value_reports_and_skips_member_not_cube():
    # A dimension whose sql is a bare identifier reference (not a literal)
    # is dynamic -> that dimension is skipped with an issue, cube still built.
    result = parse_cube_js(
        "cube(`C`, { sql_table: `t`, dimensions: {"
        "  ok: { sql: `${CUBE}.ok`, type: `string` },"
        "  bad: { sql: someHelper(), type: `string` }"
        "} });"
    )
    assert len(result.cubes) == 1
    cube = result.cubes[0]
    assert [d.name for d in cube.dimensions] == ["ok"]
    assert result.issues  # a dynamic-construct issue was reported


def test_spread_in_object_reports_issue():
    result = parse_cube_js(
        "cube(`C`, { ...base, sql_table: `t` });"
    )
    assert result.issues


def test_bare_identifier_value_is_dynamic_and_skipped():
    result = parse_cube_js(
        "cube(`C`, { sql_table: `t`, dimensions: {"
        "  ok: { sql: `${CUBE}.ok`, type: `string` },"
        "  bad: { sql: someConst, type: `string` }"
        "} });"
    )
    assert len(result.cubes) == 1
    assert [d.name for d in result.cubes[0].dimensions] == ["ok"]
    assert result.issues


def test_unknown_capitalized_key_does_not_break_parse():
    # An unmodelled camelCase key must not be blanket-normalised into a known
    # field; the cube still parses (extra keys are ignored by the model).
    result = parse_cube_js(
        "cube(`C`, { sql_table: `t`, myCustomThing: `whatever`,"
        " dimensions: { a: { sql: `${CUBE}.a`, type: `string` } } });"
    )
    assert len(result.cubes) == 1
    assert result.cubes[0].name == "C"
    assert [d.name for d in result.cubes[0].dimensions] == ["a"]


# ── interpolation forms ─────────────────────────────────────────────────────


def test_member_and_dotted_interpolation_forms():
    cube = _one_cube(
        "cube(`C`, { sql_table: `t`, dimensions: {"
        "  a: { sql: `${member}`, type: `string` },"
        "  b: { sql: `${a.b}.x`, type: `string` }"
        "} });"
    )
    by_name = {d.name: d for d in cube.dimensions}
    assert by_name["a"].sql == "{member}"
    assert by_name["b"].sql == "{a.b}.x"


# ── template-literal escapes (cooked authoritative) ─────────────────────────


def test_newline_preserved_in_template():
    cube = _one_cube("cube(`C`, { sql: `line1\nline2` });")
    assert cube.sql == "line1\nline2"


def test_escaped_backslash_becomes_single_backslash():
    cube = _one_cube(r"cube(`C`, { sql: `a \\ b` });")
    assert cube.sql == r"a \ b"


def test_escaped_backtick_preserved():
    cube = _one_cube(r"cube(`C`, { sql: `a \` b` });")
    assert cube.sql == "a ` b"


def test_escaped_interpolation_is_literal_dollar_brace():
    # \${x} is not an interpolation — cooked to a literal ${x}.
    cube = _one_cube(r"cube(`C`, { sql: `SELECT \${x} FROM t` });")
    assert cube.sql == "SELECT ${x} FROM t"
