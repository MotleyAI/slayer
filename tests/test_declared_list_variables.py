"""Declared list-valued ``{variable}`` coercion (DEV-1730 follow-up).

The generic Mode-A contract leaves quoting to the template author, so a scalar
string renders UNQUOTED — that is what makes ``order_total >= {floor}`` and
``{d}::TIMESTAMP`` work. A machine-generated ``col IN ({var})`` surface has no
author to write the quotes, so the importer declares the variable
``list_valued`` in ``meta.cube_variables`` and the engine wraps a bare scalar
into a one-element list before substitution.

Covers the two helpers, the engine choke point, and the boundaries that must
NOT change: hand-written models, undeclared variables, empty lists, and the
scalar-position arrow variables.
"""
import sqlite3
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import (
    SlayerQuery,
    coerce_declared_list_variables,
    declares_variables,
    list_valued_variable_names,
)
from slayer.engine.query_engine import (
    SlayerQueryEngine,
    _substitute_model_sql_surfaces,
)
from slayer.sql.dialects import SqliteDialect
from slayer.storage.yaml_storage import YAMLStorage


def _meta(**flags) -> dict:
    """Build a ``meta.cube_variables`` bag: name -> list_valued flag."""
    return {
        "cube_variables": {
            name: {
                "member": name, "required": False,
                "kind": "string" if flag else "arrow_value",
                "list_valued": flag, "description": None,
            }
            for name, flag in flags.items()
        }
    }


def _model(*, sql: str, meta: dict | None = None) -> SlayerModel:
    return SlayerModel(
        name="orders", sql=sql, data_source="ds", meta=meta,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )


# ── list_valued_variable_names ──────────────────────────────────────────────


def test_names_reads_only_the_neutral_flag():
    model = _model(sql="SELECT * FROM orders", meta=_meta(regions=True, cutoff=False))
    assert list_valued_variable_names(model) == {"regions"}


def test_names_empty_for_hand_written_model():
    assert list_valued_variable_names(_model(sql="SELECT * FROM orders")) == set()


def test_names_ignores_unrelated_or_malformed_meta():
    # A model carrying its own meta must not trip the accessor.
    assert list_valued_variable_names(
        _model(sql="SELECT 1", meta={"cube_variables": "not-a-dict", "owner": "x"})
    ) == set()
    assert list_valued_variable_names(
        _model(sql="SELECT 1", meta={"cube_variables": {"a": "not-a-dict"}})
    ) == set()


@pytest.mark.parametrize("flag", [1, "true", "false", "yes", [1], {"a": 1}])
def test_only_a_real_true_enables_coercion(flag):
    # meta is user-extensible, so a truthiness match would let a stray 1 — or
    # even the string "false" — silently switch substitution semantics.
    model = _model(
        sql="SELECT 1",
        meta={"cube_variables": {"regions": {"member": "r", "list_valued": flag}}},
    )
    assert list_valued_variable_names(model) == set()


def test_true_flag_enables_coercion():
    model = _model(
        sql="SELECT 1",
        meta={"cube_variables": {"regions": {"member": "r", "list_valued": True}}},
    )
    assert list_valued_variable_names(model) == {"regions"}


@pytest.mark.parametrize(
    "bag",
    [
        {"note": {}},                        # unrelated bag under the same key
        {"note": {"text": "hi"}},            # dict entries, but no 'member'
        {"note": {"member": 42}},            # 'member' present but not a string
        {"note": {"member": ""}},            # empty member — never importer output
    ],
)
def test_unrelated_meta_under_the_same_key_is_not_a_declaration(bag):
    """`meta` is free-form user data, so the bag must be SELF-IDENTIFYING.

    A false positive here would be a regression, not a nit: declaring disables
    the zero-variable fast path, so a hand-written model with raw brace literals
    would start raising on a query that used to work.
    """
    model = _model(sql="SELECT * FROM t WHERE tags = '{1,2,3}'", meta={"cube_variables": bag})
    assert declares_variables(model) is False
    assert list_valued_variable_names(model) == set()
    out = _substitute_model_sql_surfaces(
        model=model, variables={}, dialect=SqliteDialect()
    )
    assert out.sql == "SELECT * FROM t WHERE tags = '{1,2,3}'"  # still protected


# ── coerce_declared_list_variables ──────────────────────────────────────────


@pytest.mark.parametrize(
    "value,expected",
    [("US", ["US"]), (5, [5]), (2.5, [2.5]), (True, [True])],
)
def test_scalars_are_wrapped(value, expected):
    out = coerce_declared_list_variables({"regions": value}, list_valued={"regions"})
    assert out == {"regions": expected}


@pytest.mark.parametrize("value", [["US", "CA"], ("US",), []])
def test_sequences_pass_through_unchanged(value):
    # The empty list is deliberately NOT rescued here — it still raises at
    # render time, since 'no filter' belongs to an optional block / sentinel.
    out = coerce_declared_list_variables({"regions": value}, list_valued={"regions"})
    assert out["regions"] is value


def test_unsupported_types_left_for_the_renderer_to_reject():
    # None/dict keep their own scalar-path error message instead of being
    # wrapped into a list and reported as a bad list ELEMENT.
    for value in (None, {"a": 1}):
        out = coerce_declared_list_variables({"regions": value}, list_valued={"regions"})
        assert out["regions"] is value


def test_undeclared_variables_and_absent_names_untouched():
    variables = {"regions": "US", "floor": "500"}
    out = coerce_declared_list_variables(variables, list_valued={"regions", "missing"})
    assert out == {"regions": ["US"], "floor": "500"}


def test_input_never_mutated_and_returned_as_is_when_no_work():
    variables = {"floor": "500"}
    assert coerce_declared_list_variables(variables, list_valued=set()) is variables
    assert coerce_declared_list_variables(variables, list_valued={"regions"}) is variables
    declared = {"regions": "US"}
    coerce_declared_list_variables(declared, list_valued={"regions"})
    assert declared == {"regions": "US"}  # caller's dict intact


# ── declares_variables / the zero-variable fast-path hole ───────────────────


def test_declares_variables_true_only_for_a_declaring_model():
    assert declares_variables(_model(sql="SELECT 1", meta=_meta(regions=True))) is True
    assert declares_variables(_model(sql="SELECT 1", meta=_meta(cutoff=False))) is True
    assert declares_variables(_model(sql="SELECT 1")) is False
    assert declares_variables(_model(sql="SELECT 1", meta={"owner": "x"})) is False
    assert declares_variables(
        _model(sql="SELECT 1", meta={"cube_variables": {}})
    ) is False


def test_declared_required_var_with_zero_variables_raises():
    # The fast-path hole Codex flagged, closed for GENERATED models: a model
    # whose pushdowns are all required has no block to force the pass, so a
    # zero-variable call used to emit a bare {var} into the SQL.
    model = _model(
        sql="SELECT * FROM orders WHERE d >= '{cutoff}'", meta=_meta(cutoff=False)
    )
    dialect = SqliteDialect()  # hoisted: only one call may throw (sonar S5778)
    with pytest.raises(ValueError, match="Undefined variable 'cutoff'"):
        _substitute_model_sql_surfaces(model=model, variables={}, dialect=dialect)


def test_hand_written_model_keeps_the_brace_literal_protection():
    # The DEV-1625 contract is untouched for models that declare nothing: a
    # Postgres array literal must survive a zero-variable call verbatim.
    model = _model(sql="SELECT * FROM t WHERE tags = '{1,2,3}'")
    out = _substitute_model_sql_surfaces(
        model=model, variables={}, dialect=SqliteDialect()
    )
    assert out.sql == "SELECT * FROM t WHERE tags = '{1,2,3}'"
    assert out is model  # untouched, not even copied


# ── engine substitution choke point ─────────────────────────────────────────


def _sub(model: SlayerModel, variables: dict) -> str:
    return _substitute_model_sql_surfaces(
        model=model, variables=variables, dialect=SqliteDialect()
    ).sql


def test_declared_scalar_renders_quoted_in_list():
    model = _model(
        sql="SELECT * FROM orders WHERE region IN ({regions})", meta=_meta(regions=True)
    )
    assert _sub(model, {"regions": "US"}).endswith("IN ('US')")
    assert _sub(model, {"regions": ["US", "CA"]}).endswith("IN ('US', 'CA')")


def test_declared_scalar_is_still_escaped():
    model = _model(
        sql="SELECT * FROM orders WHERE region IN ({regions})", meta=_meta(regions=True)
    )
    assert _sub(model, {"regions": "O'Brien"}).endswith("IN ('O''Brien')")


def test_coercion_applies_inside_an_optional_block():
    model = _model(
        sql="SELECT * FROM orders WHERE 1=1 AND {? region IN ({regions}) ?}",
        meta=_meta(regions=True),
    )
    assert _sub(model, {"regions": "US"}).endswith("(region IN ('US'))")
    assert _sub(model, {}).endswith("(1=1)")


def test_undeclared_scalar_keeps_the_author_written_quote_convention():
    # The generic Mode-A rule is untouched: a hand-written template owns its
    # quotes, so {var} still works in numeric / fragment positions.
    model = _model(sql="SELECT * FROM orders WHERE amount >= {floor}")
    assert _sub(model, {"floor": "500"}).endswith(">= 500")
    assert _sub(model, {"floor": 500}).endswith(">= 500")


def test_declared_empty_list_still_raises():
    model = _model(
        sql="SELECT * FROM orders WHERE region IN ({regions})", meta=_meta(regions=True)
    )
    with pytest.raises(ValueError, match="cannot be an empty list"):
        _sub(model, {"regions": []})


def test_arrow_style_declared_variable_is_not_wrapped():
    # list_valued is False -> the pre-quoted scalar convention still applies.
    model = _model(
        sql="SELECT * FROM orders WHERE d >= '{cutoff}'", meta=_meta(cutoff=False)
    )
    assert _sub(model, {"cutoff": "2025-01-01"}).endswith(">= '2025-01-01'")


# ── end-to-end on SQLite (result data, not just text) ───────────────────────


def _seed(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT, amount REAL)")
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [(1, "US", 100.0), (2, "US", 60.0), (3, "EU", 200.0), (4, "CA", 300.0)],
    )
    conn.commit()
    conn.close()


async def _engine_with(model: SlayerModel):
    tmp = tempfile.TemporaryDirectory()
    _seed(f"{tmp.name}/orders.db")
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=f"{tmp.name}/orders.db")
    )
    await storage.save_model(model)
    return SlayerQueryEngine(storage=storage), tmp


async def test_e2e_scalar_and_list_return_identical_rows():
    model = _model(
        sql="SELECT * FROM orders WHERE 1=1 AND {? region IN ({regions}) ?}",
        meta=_meta(regions=True),
    )
    engine, tmp = await _engine_with(model)
    try:
        async def _total(regions):
            q = SlayerQuery(
                source_model="orders", measures=[{"formula": "amount:sum"}],
                variables={"regions": regions},
            )
            resp = await engine.execute(q)
            assert resp.row_count == 1, resp.data
            return resp.data[0]["orders.amount_sum"]

        assert await _total("US") == await _total(["US"]) == 160.0
    finally:
        tmp.cleanup()
