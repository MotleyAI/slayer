"""Converter integration for FILTER_PARAMS (slayer/cube/converter.py, DEV-1730).

Requiredness (block vs bare) is resolved here against ``meta.required`` +
``honor_required_meta``; sentinels from the front-end are replaced with the
emitted SLayer Mode-A text; variables are stashed in ``model.meta`` and reported.
"""


from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.js_parser import parse_cube_js
from slayer.cube.report import CubeIssueCategory

DS = "test_ds"

_RR = """
cube(`RrDrivers`, {{
  sql: `SELECT
      {value_arrow}::TIMESTAMP AS ty_start_date
    FROM fol
    WHERE 1 = 1
      AND {range_arrow}
      AND {brand_filter}
      AND {market_filter}`,
  dimensions: {{
    fulfillment_date: {{ sql: `${{CUBE}}.ty_start_date`, type: `time`,
      meta: {{ required: true }} }},
    brand: {{ sql: `${{CUBE}}."BRAND"`, type: `string` }},
    market: {{ sql: `${{CUBE}}."MARKET"`, type: `string` }}
  }}
}});
"""

_VALUE_ARROW = "${FILTER_PARAMS.RrDrivers.fulfillment_date.filter((from, to) => from)}"
_RANGE_ARROW = (
    "${FILTER_PARAMS.RrDrivers.fulfillment_date.filter((from, to) => "
    "'fol.\"FD\" >= ' + from + ' AND fol.\"FD\" <= ' + to)}"
)
_BRAND = "${FILTER_PARAMS.RrDrivers.brand.filter('pr.\"BRAND\"')}"
_MARKET = "${FILTER_PARAMS.RrDrivers.market.filter('fol.\"MARKET\"')}"


def _rr_source() -> str:
    return _RR.format(
        value_arrow=_VALUE_ARROW, range_arrow=_RANGE_ARROW,
        brand_filter=_BRAND, market_filter=_MARKET,
    )


def _convert(source: str, *, honor_required_meta: bool = True):
    result = parse_cube_js(source)
    conv = CubeToSlayerConverter(
        project=_as_project(result), data_source=DS,
        parse_issues=result.issues, honor_required_meta=honor_required_meta,
    )
    out = conv.convert()
    return {m.name: m for m in out.models}, out.report


def _as_project(js_result):
    from slayer.cube.models import CubeProject
    return CubeProject(cubes=js_result.cubes, views=js_result.views)


# ── required (honor meta) vs optional emission ──────────────────────────────


def test_required_date_is_bare_optional_categoricals_are_blocks():
    models, report = _convert(_rr_source())
    model = models["RrDrivers"]
    sql = model.sql
    assert "\x00" not in sql  # no leftover sentinels
    assert "FILTER_PARAMS" not in sql
    # required arrow (scalar + range) -> bare {var}s, raise-on-missing
    assert "'{fulfillment_date_from}'::TIMESTAMP AS ty_start_date" in sql
    assert 'fol."FD" >= \'{fulfillment_date_from}\'' in sql
    # optional categoricals -> collapse blocks
    assert '{? pr."BRAND" IN ({brand}) ?}' in sql
    assert '{? fol."MARKET" IN ({market}) ?}' in sql


def test_ignore_required_meta_makes_scalar_arrow_a_block():
    models, _ = _convert(_rr_source(), honor_required_meta=False)
    sql = models["RrDrivers"].sql
    # Now the scalar-position arrow is optional -> Cube's (1=1)::TIMESTAMP shape
    # after collapse. Here we assert the block survives to the model.
    assert "{? '{fulfillment_date_from}' ?}::TIMESTAMP AS ty_start_date" in sql


def test_variables_stashed_in_meta():
    models, _ = _convert(_rr_source())
    cube_vars = models["RrDrivers"].meta["cube_variables"]
    assert cube_vars["fulfillment_date_from"]["member"] == "fulfillment_date"
    assert cube_vars["fulfillment_date_from"]["required"] is True
    assert cube_vars["brand"]["required"] is False


def test_report_has_one_variable_entry_per_logical_variable():
    _, report = _convert(_rr_source())
    var_issues = report.by_category(CubeIssueCategory.FILTER_PARAMS_VARIABLE)
    reported = {i.member for i in var_issues}
    # fulfillment_date (used at 2 sites) reported once; brand, market once each
    assert reported == {"fulfillment_date", "brand", "market"}
    assert all(i.severity == "info" for i in var_issues)
    # Dedup is by variable NAME, so both fulfillment_date bounds are reported —
    # the report must agree with meta.cube_variables (DEV-1730 review).
    all_msgs = " ".join(i.message for i in var_issues)
    assert "fulfillment_date_from" in all_msgs
    assert "fulfillment_date_to" in all_msgs


def test_meta_cube_variables_full_shape():
    models, _ = _convert(_rr_source())
    cube_vars = models["RrDrivers"].meta["cube_variables"]
    fd = cube_vars["fulfillment_date_from"]
    assert fd["member"] == "fulfillment_date"
    assert fd["required"] is True
    assert fd["kind"] in ("arrow_value", "arrow_range")
    assert "description" in fd  # propagated (may be None if the member had none)
    brand = cube_vars["brand"]
    assert brand["kind"] == "string"
    assert brand["required"] is False


def test_model_variables_discoverable_required_and_optional():
    from slayer.core.query import extract_model_variables
    models, _ = _convert(_rr_source())
    v = extract_model_variables(models["RrDrivers"])
    # the range arrow references both from and to -> both required.
    assert set(v.required) == {"fulfillment_date_from", "fulfillment_date_to"}
    assert set(v.optional) == {"brand", "market"}


# ── validation / rejection paths ────────────────────────────────────────────


def test_unknown_member_drops_cube_with_error():
    src = (
        "cube(`C`, { sql: `SELECT * FROM t WHERE 1=1 "
        "AND ${FILTER_PARAMS.C.nonexistent.filter('x')}`, "
        "dimensions: { a: { sql: `${CUBE}.a`, type: `string` } } });"
    )
    models, report = _convert(src)
    assert "C" not in models
    assert report.by_category(CubeIssueCategory.FILTER_PARAMS_UNSUPPORTED)


def test_cross_cube_reference_drops_cube_with_error():
    src = (
        "cube(`orders`, { sql: `SELECT * FROM t WHERE 1=1 "
        "AND ${FILTER_PARAMS.other.status.filter('status')}`, "
        "dimensions: { status: { sql: `${CUBE}.status`, type: `string` } } });"
    )
    models, report = _convert(src)
    assert "orders" not in models
    assert report.by_category(CubeIssueCategory.FILTER_PARAMS_UNSUPPORTED)


def test_generated_name_collision_drops_cube():
    # member `d` (arrow value -> d_from) collides with member `d_from` (string).
    src = (
        "cube(`C`, { sql: `SELECT "
        "${FILTER_PARAMS.C.d.filter((from, to) => from)}::TIMESTAMP AS x "
        "FROM t WHERE 1=1 AND ${FILTER_PARAMS.C.d_from.filter('d_from')}`, "
        "dimensions: {"
        "  d: { sql: `${CUBE}.d`, type: `time` },"
        "  d_from: { sql: `${CUBE}.d_from`, type: `string` }"
        "} });"
    )
    models, report = _convert(src)
    assert "C" not in models
    assert report.by_category(CubeIssueCategory.FILTER_PARAMS_UNSUPPORTED)


def test_dropped_cube_emits_no_variable_entries():
    src = (
        "cube(`C`, { sql: `SELECT * FROM t WHERE 1=1 "
        "AND ${FILTER_PARAMS.C.nonexistent.filter('x')}`, "
        "dimensions: { a: { sql: `${CUBE}.a`, type: `string` } } });"
    )
    _, report = _convert(src)
    assert not report.by_category(CubeIssueCategory.FILTER_PARAMS_VARIABLE)


def test_generated_model_sql_parses_after_probe_render():
    # The converter must validate the sentinel-resolved SQL; a model that
    # survives conversion has parseable SQL once blocks collapse + vars fill.
    models, _ = _convert(_rr_source())
    assert "RrDrivers" in models  # survived offline validation
