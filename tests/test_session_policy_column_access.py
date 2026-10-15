"""Column access and the inline-SQL switch of ``SessionPolicy``, end to end on SQLite."""

import sqlite3

import pytest
from pydantic import ValidationError

from slayer.core.errors import ForcedFilterError
from slayer.core.models import Column, DataType, DatasourceConfig, SlayerModel
from slayer.core.policy import ColumnAccessRule, ColumnFilterRuleset, SessionPolicy
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.session_policy import ScopedTable, apply_session_policy
from slayer.storage.yaml_storage import YAMLStorage


def _no_has_column(scoped: ScopedTable, column: str) -> bool:
    raise AssertionError("has_column must not be called")


def _columns(scoped: ScopedTable) -> list[str]:
    return {"employees": ["id", "org", "name", "salary"], "orgs": ["id", "title"]}[scoped.name]


def _rewrite(sql: str, policy: SessionPolicy) -> str:
    return apply_session_policy(
        sql, dialect="sqlite", policy=policy, has_column=_no_has_column, table_columns=_columns,
    )


# -- policy model ------------------------------------------------------------


def test_bare_policy_still_rejected():
    with pytest.raises(ValidationError):
        SessionPolicy()


def test_rule_needs_exactly_one_list():
    with pytest.raises(ValidationError):
        ColumnAccessRule(table="employees")
    with pytest.raises(ValidationError):
        ColumnAccessRule(table="employees", deny=("salary",), allow=("id",))


def test_column_access_alone_is_a_policy():
    policy = SessionPolicy(column_access=(ColumnAccessRule(table="employees", deny=("salary",)),))
    assert policy.ruleset is None


def test_inline_sql_switch_alone_is_a_policy():
    assert SessionPolicy(allow_inline_sql=False).column_access == ()


# -- SQL rewrite -------------------------------------------------------------


def test_deny_projects_remaining_columns():
    policy = SessionPolicy(column_access=(ColumnAccessRule(table="employees", deny=("salary",)),))
    out = _rewrite("SELECT e.name FROM employees AS e", policy)
    assert out == "SELECT e.name FROM (SELECT id, org, name FROM employees) AS e"


def test_allow_list_and_wildcard_table():
    policy = SessionPolicy(column_access=(
        ColumnAccessRule(table="*", deny=("org",)),
        ColumnAccessRule(table="employees", allow=("id", "name", "org")),
    ))
    out = _rewrite("SELECT * FROM employees JOIN orgs ON employees.id = orgs.id", policy)
    assert "(SELECT id, name FROM employees) AS employees" in out
    assert "(SELECT id, title FROM orgs) AS orgs" in out


def test_unrestricted_table_untouched():
    policy = SessionPolicy(column_access=(ColumnAccessRule(table="employees", deny=("salary",)),))
    assert _rewrite("SELECT title FROM orgs", policy) == "SELECT title FROM orgs"


def test_row_filter_wraps_inside_column_projection():
    policy = SessionPolicy(
        ruleset=ColumnFilterRuleset(column="org", value="acme"),
        column_access=(ColumnAccessRule(table="employees", deny=("salary",)),),
    )
    out = apply_session_policy(
        "SELECT name FROM employees", dialect="sqlite", policy=policy,
        has_column=lambda scoped, column: True, table_columns=_columns,
    )
    assert out == (
        "SELECT name FROM (SELECT id, org, name FROM "
        "(SELECT * FROM employees WHERE org = 'acme') AS employees) AS employees"
    )


def test_unlistable_table_fails_closed():
    policy = SessionPolicy(column_access=(ColumnAccessRule(table="employees", deny=("salary",)),))
    with pytest.raises(ForcedFilterError, match="could not list"):
        apply_session_policy(
            "SELECT name FROM employees", dialect="sqlite", policy=policy,
            has_column=_no_has_column, table_columns=lambda scoped: None,
        )


def test_nothing_visible_fails_closed():
    policy = SessionPolicy(column_access=(ColumnAccessRule(table="orgs", allow=("missing",)),))
    with pytest.raises(ForcedFilterError, match="no column"):
        _rewrite("SELECT title FROM orgs", policy)


# -- engine, end to end ------------------------------------------------------


@pytest.fixture
async def storage(tmp_path) -> YAMLStorage:
    db = tmp_path / "hr.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE employees (id INTEGER, org TEXT, name TEXT, salary INTEGER)")
        conn.executemany(
            "INSERT INTO employees VALUES (?, ?, ?, ?)",
            [(1, "acme", "Ann", 100), (2, "acme", "Bob", 200), (3, "globex", "Cid", 300)],
        )
    store = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await store.save_datasource(DatasourceConfig(name="hr", type="sqlite", database=str(db)))
    await store.save_model(SlayerModel(
        name="employees", sql_table="employees", data_source="hr",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="org"),
            Column(name="name"),
            Column(name="salary", type=DataType.INT),
        ],
    ))
    return store


def _engine(storage: YAMLStorage, **policy) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=storage, policy=SessionPolicy(**policy))


async def test_engine_allowed_columns_still_work(storage):
    engine = _engine(
        storage,
        ruleset=ColumnFilterRuleset(column="org", value="acme"),
        column_access=(ColumnAccessRule(table="employees", deny=("salary",)),),
    )
    resp = await engine.execute({"source_model": "employees", "measures": [{"formula": "count(*)", "name": "n"}]})
    assert resp.data == [{"employees.n": 2}]


async def test_engine_denied_column_fails(storage):
    engine = _engine(storage, column_access=(ColumnAccessRule(table="employees", deny=("salary",)),))
    with pytest.raises(Exception, match="salary"):
        await engine.execute({"source_model": "employees", "measures": [{"formula": "sum(salary)"}]})


async def test_engine_rejects_inline_model(storage):
    engine = _engine(storage, allow_inline_sql=False)
    inline = {"name": "x", "sql": "SELECT salary FROM employees", "data_source": "hr",
              "columns": [{"name": "salary", "type": "INT"}]}
    with pytest.raises(ForcedFilterError, match="Inline model"):
        await engine.execute({"source_model": inline, "measures": [{"formula": "sum(salary)"}]})


async def test_engine_rejects_extension_sql_column(storage):
    engine = _engine(storage, allow_inline_sql=False)
    extension = {"source_name": "employees", "columns": [{"name": "s2", "sql": "salary * 2"}]}
    with pytest.raises(ForcedFilterError, match="Extension column"):
        await engine.execute({"source_model": extension, "measures": [{"formula": "sum(s2)"}]})


async def test_engine_allows_extension_measures(storage):
    engine = _engine(storage, allow_inline_sql=False)
    extension = {"source_name": "employees", "measures": [{"formula": "count(*)", "name": "n"}]}
    resp = await engine.execute({"source_model": extension, "measures": [{"formula": "n"}]})
    assert resp.data == [{"employees.n": 3}]


# -- per-datasource row rulesets ---------------------------------------------


def test_datasource_ruleset_replaces_default():
    policy = SessionPolicy(
        ruleset=ColumnFilterRuleset(column="org", value="acme"),
        datasource_rulesets={"isolated": None, "other": ColumnFilterRuleset(column="tenant", value="t1")},
    )
    assert policy.ruleset_for("isolated") is None
    other, default = policy.ruleset_for("other"), policy.ruleset_for("hr")
    assert isinstance(other, ColumnFilterRuleset) and other.column == "tenant"
    assert isinstance(default, ColumnFilterRuleset) and default.column == "org"
    out = apply_session_policy(
        "SELECT name FROM employees", dialect="sqlite", policy=policy,
        has_column=_no_has_column, datasource="isolated",
    )
    assert out == "SELECT name FROM employees"


def test_only_unfiltered_datasources_is_a_no_op():
    with pytest.raises(ValidationError):
        SessionPolicy(datasource_rulesets={"isolated": None})


async def test_engine_uses_datasource_ruleset(storage):
    engine = _engine(
        storage,
        ruleset=ColumnFilterRuleset(column="missing", value="x"),
        datasource_rulesets={"hr": ColumnFilterRuleset(column="org", value="globex")},
    )
    resp = await engine.execute({"source_model": "employees", "measures": [{"formula": "count(*)", "name": "n"}]})
    assert resp.data == [{"employees.n": 1}]
