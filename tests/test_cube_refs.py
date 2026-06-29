"""Tests for the Cube curly-reference translator (slayer/cube/refs.py).

DEV-1608 §3 / §4.4. `{CUBE}` / `{member}` / `{cube.member}` are Cube's
single-brace SQL-ref syntax — distinct from Jinja's `{{ }}` / `{% %}`.
"""


from slayer.cube.refs import contains_jinja, parse_join_on, translate_cube_refs


# ── Jinja detection ────────────────────────────────────────────────────────

def test_contains_jinja_double_brace():
    assert contains_jinja("{{ env_var('SCHEMA') }}.events")


def test_contains_jinja_block():
    assert contains_jinja("{% for t in tables %}")


def test_single_brace_cube_ref_is_not_jinja():
    assert not contains_jinja("{CUBE}.amount")
    assert not contains_jinja("{customers.id}")
    assert not contains_jinja("SUM({CUBE}.amount)")


# ── Mode A (SQL) translation ───────────────────────────────────────────────

def test_cube_dot_col_to_bare():
    assert translate_cube_refs("{CUBE}.amount", mode="sql", cube="orders") == "amount"


def test_same_cube_member_to_bare():
    out = translate_cube_refs(
        "{first_name} || ' ' || {last_name}", mode="sql", cube="people"
    )
    assert out == "first_name || ' ' || last_name"


def test_cross_cube_member_single_hop():
    assert (
        translate_cube_refs("{customers.name}", mode="sql", cube="orders")
        == "customers.name"
    )


def test_cross_cube_member_multi_hop():
    # `{a.b.c}` → SLayer multi-dot `a.b.c` (model-side `_fix_multidot_sql` later
    # rewrites it to `a__b.c`).
    assert (
        translate_cube_refs("{customers.regions.name}", mode="sql", cube="orders")
        == "customers.regions.name"
    )


def test_translation_skips_string_literals():
    # A `{CUBE}` inside a SQL string literal must be left untouched.
    out = translate_cube_refs(
        "CASE WHEN {CUBE}.status = '{CUBE}' THEN 1 END", mode="sql", cube="orders"
    )
    assert out == "CASE WHEN status = '{CUBE}' THEN 1 END"


# ── Mode B (DSL) translation ───────────────────────────────────────────────

def test_dsl_measure_refs_to_bare_names():
    out = translate_cube_refs("{revenue} / {count}", mode="dsl", cube="orders")
    assert out == "revenue / count"


# ── Join ON parsing ────────────────────────────────────────────────────────

def test_parse_join_on_simple_equality():
    pairs = parse_join_on(
        "{CUBE}.customer_id = {customers.id}",
        source_cube="orders",
        target_cube="customers",
    )
    assert pairs == [["customer_id", "id"]]


def test_parse_join_on_composite_key():
    pairs = parse_join_on(
        "{CUBE}.a = {t.x} AND {CUBE}.b = {t.y}",
        source_cube="o",
        target_cube="t",
    )
    assert pairs == [["a", "x"], ["b", "y"]]


def test_parse_join_on_non_equi_returns_none():
    assert (
        parse_join_on("{CUBE}.ts > {t.start}", source_cube="o", target_cube="t")
        is None
    )


def test_parse_join_on_function_call_returns_none():
    assert (
        parse_join_on(
            "LOWER({CUBE}.email) = {t.email}", source_cube="o", target_cube="t"
        )
        is None
    )
