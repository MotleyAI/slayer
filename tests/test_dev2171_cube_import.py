"""Cube import preserves Cube's meaning (specs: importers/meaning-preservation, importers/cube-import)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.core.syntax import AggCall, Ref, parse_expr
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeProject
from slayer.sql import engine_factory
from tests._dev2171_fixtures import (
    DS, MASKED, by_id, column, counts_by, cube_issues, cube_report_messages, deletes, drift_entries, has_column,
    has_measure,
    import_cube, models_of, run_cli, save_datasource, seed, source_column, stored_model, typing_notes,
    unreachable_engine, value, write_yaml,
)

ID = {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True}
RENAMED_STATUS = {"name": "status", "sql": "{CUBE}.state"}
STATUS_UPPER = {"name": "status_upper", "sql": "UPPER({CUBE}.status)"}
SELF_NAMED = {"name": "status", "sql": "UPPER({CUBE}.status)"}
PAID = [{"sql": "{CUBE}.status = 'paid'"}]
CUSTOMERS = {"name": "customers", "sql_table": "customers", "dimensions": [
    ID, {"name": "name", "sql": "{CUBE}.name"}, {"name": "region", "sql": "{CUBE}.region"}]}


def dim(name: str, sql: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "sql": sql, **extra}


def meas(name: str, type_: str, sql: str | None = None, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, "type": type_, **extra}
    if sql is not None:
        out["sql"] = sql
    return out


def cube(name: str = "orders", *, dimensions: list[dict[str, Any]] | None = None,
         measures: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, "dimensions": [ID, *(dimensions or [])], "measures": measures or [],
                           **extra}
    if "sql" not in extra:
        out.setdefault("sql_table", "orders")
    return out


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed(tmp_path)


async def _measure(result, db: Path, name: str, model: str = "orders") -> Any:
    return await value(models=result.models, db=db, model=model, formula=name)


# ── physical references ───────────────────────────────────────────────────

class TestPhysicalReferences:
    @pytest.mark.parametrize("dims", [[RENAMED_STATUS, STATUS_UPPER], [STATUS_UPPER, RENAMED_STATUS]],
                             ids=["status-first", "derived-first"])
    @MASKED
    async def test_renamed_dimension_does_not_capture(self, db: Path, dims: list[dict[str, Any]]) -> None:
        result = import_cube([cube(dimensions=dims)], db=db)
        models = result.models
        assert await counts_by(models=models, db=db, model="orders", dimension="status_upper") == {
            "PAID": 2, "OPEN": 1}
        assert await counts_by(models=models, db=db, model="orders", dimension="status") == {
            "open": 1, "paid": 1, "x": 1}

    @pytest.mark.parametrize("sql", [
        'UPPER({CUBE}."status")', "UPPER({CUBE}.STATUS)", "UPPER({orders}.status)", "UPPER(status)",
    ], ids=["quoted", "case-folded", "own-cube-brace", "bare"])
    @MASKED
    async def test_every_physical_spelling(self, db: Path, sql: str) -> None:
        result = import_cube([cube(dimensions=[RENAMED_STATUS, dim("status_upper", sql)])], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status_upper") == {
            "PAID": 2, "OPEN": 1}

    @MASKED
    def test_quoting_is_kept_on_the_hidden_base_column(self) -> None:
        result = import_cube([cube(dimensions=[RENAMED_STATUS, dim("status_upper", 'UPPER({CUBE}."status")')])])
        hidden = [c for c in models_of(result)["orders"].columns if c.hidden and c.physical_name == "status"]
        assert [c.sql for c in hidden] == ['"status"']

    @pytest.mark.parametrize(("sql", "kept"), [
        ("({CUBE}.amount - (SELECT MIN(amount) FROM orders))", "SELECT MIN(amount) FROM orders"),
        ("({CUBE}.amount - (WITH m AS (SELECT MIN(amount) AS amount FROM orders) SELECT amount FROM m))",
         "WITH m AS (SELECT MIN(amount) AS amount FROM orders) SELECT amount FROM m"),
    ], ids=["subquery", "cte"])
    async def test_nested_scopes_are_left_alone(self, db: Path, sql: str, kept: str) -> None:
        result = import_cube([cube(dimensions=[
            dim("amount", "{CUBE}.amount * 2", type="number"), dim("spread", sql, type="number")])], db=db)
        assert kept in (column(models_of(result)["orders"], "spread").sql or "")
        assert await by_id(models=result.models, db=db, model="orders", dimension="spread") == [5, 15, 0]

    @MASKED
    async def test_measure_filter_reads_the_physical_column(self, db: Path) -> None:
        result = import_cube([cube(dimensions=[RENAMED_STATUS],
                                   measures=[meas("paid_amount", "sum", "{CUBE}.amount", filters=PAID)])], db=db)
        assert await _measure(result, db, "paid_amount") == 30

    @MASKED
    async def test_segment_reads_the_physical_column(self, db: Path) -> None:
        result = import_cube([cube(dimensions=[RENAMED_STATUS],
                                   segments=[{"name": "is_paid", "sql": "{CUBE}.status = 'paid'"}])], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="is_paid") == {
            True: 2, False: 1}

    @MASKED
    async def test_case_predicate_reads_the_physical_column(self, db: Path) -> None:
        bucket = {"name": "bucket", "case": {"when": [{"sql": "{CUBE}.status = 'paid'", "label": "P"}],
                                             "else": {"label": "O"}}}
        result = import_cube([cube(dimensions=[RENAMED_STATUS, bucket])], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="bucket") == {"P": 2, "O": 1}

    @pytest.mark.parametrize("word", ["select", "from"])
    def test_reserved_word_column_is_dropped_with_a_report(self, word: str) -> None:
        result = import_cube([cube(dimensions=[dim("k", f"{{CUBE}}.{word}")])])
        assert not has_column(result, "orders", "k")
        assert [i for i in cube_issues(result, "k") if "reserved word" in i.message], result.report.issues

    async def test_quoted_reserved_word_column_is_read(self, tmp_path: Path) -> None:
        db = seed(tmp_path, ['CREATE TABLE t (id INTEGER, "from" VARCHAR)', "INSERT INTO t VALUES (1, 'x'), (2, 'x')"])
        result = import_cube([cube("t", dimensions=[dim("k", '{CUBE}."from"')], sql_table="t")], db=db)
        assert await counts_by(models=result.models, db=db, model="t", dimension="k") == {"x": 2}


@MASKED
class TestSelfNamed:
    async def test_self_named_dimension(self, db: Path) -> None:
        result = import_cube([cube(dimensions=[SELF_NAMED])], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status") == {
            "PAID": 2, "OPEN": 1}

    async def test_hidden_column_name_already_taken(self, db: Path) -> None:
        result = import_cube([cube(dimensions=[dim("status_raw", "{CUBE}.state"), SELF_NAMED])], db=db)
        models = result.models
        assert await counts_by(models=models, db=db, model="orders", dimension="status") == {"PAID": 2, "OPEN": 1}
        assert await counts_by(models=models, db=db, model="orders", dimension="status_raw") == {
            "open": 1, "paid": 1, "x": 1}


class TestFilteredHelpers:
    async def test_filtered_measure_next_to_a_plain_reference(self, db: Path) -> None:
        result = import_cube([cube(dimensions=[dim("amount_x2", "{CUBE}.amount * 2", type="number")],
                                   measures=[meas("paid_amount", "sum", "{CUBE}.amount", filters=PAID)])], db=db)
        model = models_of(result)["orders"]
        assert source_column(model, "paid_amount").name == "paid_amount_col"
        assert all(c.filter is None for c in model.columns if c.name == "amount")
        assert await _measure(result, db, "paid_amount") == 30
        assert await by_id(models=result.models, db=db, model="orders", dimension="amount_x2") == [20, 40, 10]

    def test_helpers_are_shared_by_operand_and_filter(self) -> None:
        result = import_cube([cube(measures=[
            meas("paid_sum", "sum", "{CUBE}.amount", filters=PAID),
            meas("paid_avg", "avg", "{CUBE}.amount", filters=PAID),
            meas("open_sum", "sum", "{CUBE}.amount", filters=[{"sql": "{CUBE}.status = 'open'"}]),
        ])])
        model = models_of(result)["orders"]
        assert source_column(model, "paid_sum").name == source_column(model, "paid_avg").name
        assert source_column(model, "paid_sum").name != source_column(model, "open_sum").name

    def test_helpers_are_hidden(self) -> None:
        result = import_cube([cube(measures=[
            meas("paid_sum", "sum", "{CUBE}.amount", filters=PAID),
            meas("paid_cnt", "number", "SUM(CASE WHEN {CUBE}.status = 'paid' THEN 1 END)"),
        ])])
        model = models_of(result)["orders"]
        assert source_column(model, "paid_sum").hidden
        assert column(model, "paid_cnt_col").hidden

    def test_helpers_differ_by_type(self) -> None:
        result = import_cube([cube(measures=[
            meas("paid_sum", "sum", "{CUBE}.amount", filters=PAID),
            meas("paid_n", "count", "{CUBE}.amount", filters=PAID),
        ])])
        model = models_of(result)["orders"]
        summed, counted = source_column(model, "paid_sum"), source_column(model, "paid_n")
        assert (summed.type, counted.type) == (DataType.DOUBLE, DataType.TEXT)
        assert summed.name != counted.name


# ── member references ─────────────────────────────────────────────────────

class TestMemberReferences:
    async def test_same_cube_dotted_reference(self, db: Path) -> None:
        result = import_cube([cube(measures=[
            meas("paid_sum", "sum", "{CUBE}.amount", filters=PAID),
            meas("a", "number", "{orders.paid_sum}"), meas("b", "number", "{CUBE.paid_sum}"),
        ])], db=db)
        for name in ("paid_sum", "a", "b"):
            assert await _measure(result, db, name) == 30, name

    async def test_member_reference_under_its_imported_name(self, db: Path) -> None:
        result = import_cube([cube(measures=[
            meas("rate", "sum", "{CUBE}.rate"), meas("double_rate", "number", "{rate} * 2")])], db=db)
        assert await _measure(result, db, "double_rate") == pytest.approx(10.0)


class TestJoinedCubesPhysicalColumn:
    JOIN = {"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.customer_id = {customers}.id"}
    CUST_NAME = dim("cust_name", "{customers}.name")

    @pytest.mark.parametrize("customers", [CUSTOMERS, {**CUSTOMERS, "dimensions": [ID]}],
                             ids=["read-by-a-member", "read-by-no-member"])
    async def test_reads_the_joined_cubes_physical_column(self, db: Path, customers: dict[str, Any]) -> None:
        result = import_cube([cube(dimensions=[self.CUST_NAME], joins=[self.JOIN]), customers], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="cust_name") == {
            "Ann": 2, "Bo": 1}

    def test_masked_is_dropped_with_a_report(self) -> None:
        masked = {**CUSTOMERS, "dimensions": [ID, dim("name", "{CUBE}.region")]}
        result = import_cube([cube(dimensions=[self.CUST_NAME], joins=[self.JOIN]), masked])
        assert not has_column(result, "orders", "cust_name")
        assert [i for i in cube_issues(result, "cust_name") if "'name'" in i.message], result.report.issues

    def test_cube_not_joined_is_dropped_with_a_report(self) -> None:
        result = import_cube([cube(dimensions=[self.CUST_NAME]), CUSTOMERS])
        assert not has_column(result, "orders", "cust_name")
        assert [i for i in cube_issues(result, "cust_name") if "does not join" in i.message], result.report.issues

    @pytest.mark.parametrize("ref", ["{customers}.name", "{customers.name}"], ids=["physical", "member"])
    def test_join_dropped_later_drops_its_readers(self, ref: str) -> None:
        masked_key = dim("customer_id", "{CUBE}.legacy_ref", type="number")
        result = import_cube([cube(dimensions=[masked_key, dim("cn", ref), dim("cn_upper", "UPPER({cn})")],
                                   measures=[meas("n_names", "count_distinct", "{cn}")], joins=[self.JOIN]),
                              CUSTOMERS])
        assert not any(has_column(result, "orders", c) for c in ("cn", "cn_upper"))
        assert not has_measure(result, "orders", "n_names")
        assert [i for i in cube_issues(result, "cn") if "no join" in i.message], result.report.issues

    async def test_measure_over_the_joined_cubes_column(self, db: Path) -> None:
        result = import_cube([cube(measures=[meas("n_regions", "count_distinct", "{customers}.region")],
                                   joins=[self.JOIN]), CUSTOMERS], db=db)
        assert await _measure(result, db, "n_regions") == 2

    def test_filtered_measure_over_a_cube_not_joined_is_dropped(self) -> None:
        result = import_cube([cube(measures=[meas("m", "sum", "{customers}.id", filters=PAID)]), CUSTOMERS])
        assert not has_measure(result, "orders", "m")
        assert [i for i in cube_issues(result, "m") if "m_col" in i.message], result.report.issues

    def test_non_bare_aggregate_operand_is_dropped_with_a_report(self) -> None:
        result = import_cube([cube(measures=[meas("bad", "number", "SUM({CUBE}.amount + {customers}.id)")],
                                   joins=[self.JOIN]), CUSTOMERS])
        assert not has_measure(result, "orders", "bad")
        assert [i for i in cube_issues(result, "bad") if "reads another cube (customers)" in i.message], \
            result.report.issues


# ── calculated measures with raw SQL aggregates ───────────────────────────

class TestCalculatedMeasures:
    @pytest.mark.parametrize(("sql", "dims", "expected"), [
        pytest.param("SUM(CASE WHEN {CUBE}.status = 'paid' THEN 1 END)", [RENAMED_STATUS], 2, marks=MASKED),
        ("SUM(CASE WHEN {CUBE}.status = 'paid' THEN {CUBE}.amount END)", [], 30),
        ("SUM({CUBE}.amount)", [], 35),
        ("SUM({CUBE}.amount) FILTER (WHERE {CUBE}.status = 'paid') / COUNT(*)", [], 10),
        ("MAX({CUBE}.amount) - MIN({CUBE}.amount)", [], 15),
        pytest.param("COUNT(DISTINCT {CUBE}.status)", [RENAMED_STATUS], 2, marks=MASKED),
        ("AVG({CUBE}.amount * 3)", [dim("amount", "{CUBE}.amount * 2", type="number")], 35),
        ("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {CUBE}.amount)", [], 10),
    ], ids=["case-collision", "undeclared-column", "plain", "filter-clause", "arithmetic", "count-distinct",
            "masked-operand", "percentile"])
    async def test_translates_with_cubes_meaning(self, db: Path, sql: str, dims: list[dict[str, Any]],
                                                 expected: float) -> None:
        result = import_cube([cube(dimensions=dims, measures=[meas("m", "number", sql)])], db=db)
        assert await _measure(result, db, "m") == pytest.approx(expected)

    async def test_zero_divisor_yields_null(self, db: Path) -> None:
        sql = "SUM({CUBE}.amount) / SUM(CASE WHEN {CUBE}.status = 'void' THEN 1 ELSE 0 END)"
        result = import_cube([cube(measures=[meas("ratio", "number", sql)])], db=db)
        assert await _measure(result, db, "ratio") is None

    def test_plain_aggregate_is_the_sum_of_the_column(self) -> None:
        result = import_cube([cube(measures=[meas("total", "number", "SUM({CUBE}.amount)")])])
        model = models_of(result)["orders"]
        found = model.get_measure("total")
        assert found is not None
        parsed = parse_expr(found.formula)
        assert isinstance(parsed, AggCall)
        assert parsed.agg.lower() == "sum"
        assert isinstance(parsed.source, Ref)
        col = column(model, parsed.source.name)
        assert (col.physical_name, col.filter, col.is_base) == ("amount", None, True)

    @pytest.mark.parametrize("p", ["0.5", "0.9"], ids=["median", "percentile"])
    @pytest.mark.parametrize("target", ["mysql", "mssql"])
    def test_percentile_refused_by_the_target_is_dropped(self, p: str, target: str) -> None:
        project = CubeProject.model_validate({"cubes": [cube(measures=[
            meas("p", "number", f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {{CUBE}}.amount)")])]})
        result = CubeToSlayerConverter(project=project, data_source=DS, target_dialect=target).convert()
        assert not has_measure(result, "orders", "p")
        assert [i for i in cube_issues(result, "p") if "not supported" in i.message], result.report.issues

    def test_percentile_disc_is_dropped(self) -> None:
        result = import_cube([cube(measures=[
            meas("p", "number", "PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY {CUBE}.amount)")])])
        assert not has_measure(result, "orders", "p")
        assert [i for i in cube_issues(result, "p") if "PERCENTILE_DISC" in i.message], result.report.issues

    @pytest.mark.parametrize(("sql", "construct"), [
        ("SUM({CUBE}.amount) OVER ()", ""),
        ("SUM({missing_member})", "missing_member"),
        ("SUM({CUBE}.amount + {customers.id})", "customers"),
    ], ids=["window", "unknown-member", "operand-reads-another-cube"])
    def test_untranslatable_is_dropped_with_a_report(self, sql: str, construct: str) -> None:
        joins = [{"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.customer_id = {customers}.id"}]
        result = import_cube([cube(measures=[meas("bad", "number", sql)], joins=joins), CUSTOMERS])
        assert not has_measure(result, "orders", "bad")
        assert [i for i in cube_issues(result, "bad") if construct in i.message], result.report.issues


# ── joins ─────────────────────────────────────────────────────────────────

class TestJoinKeys:
    JOIN = {"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.customer_id = {customers}.id"}

    async def _by_customer(self, result, db: Path) -> dict[Any, int]:
        return await counts_by(models=result.models, db=db, model="orders", dimension="customers.name")

    @MASKED
    async def test_key_masked_by_a_renamed_dimension(self, db: Path) -> None:
        result = import_cube([cube(dimensions=[dim("customer_id", "{CUBE}.legacy_ref", type="number")],
                                   joins=[self.JOIN]), CUSTOMERS], db=db)
        assert await self._by_customer(result, db) == {"Ann": 2, "Bo": 1}

    async def test_composite_key_with_a_masked_side(self, db: Path) -> None:
        join = {**self.JOIN, "sql": "{CUBE}.customer_id = {customers}.id AND {CUBE}.region = {customers}.region"}
        result = import_cube([cube(dimensions=[dim("region", "UPPER({CUBE}.region)")], joins=[join]), CUSTOMERS],
                             db=db)
        assert await self._by_customer(result, db) == {"Ann": 2, "Bo": 1}

    def test_renamed_key_reuses_the_column_reading_it(self) -> None:
        result = import_cube([cube(dimensions=[dim("cust", "{CUBE}.customer_id", type="number")],
                                   joins=[self.JOIN]), CUSTOMERS])
        (join,) = models_of(result)["orders"].joins
        assert join.join_pairs == [["cust", "id"]]


# ── FILTER_PARAMS ─────────────────────────────────────────────────────────

class TestFilterParams:
    @pytest.mark.parametrize("required", [False, True], ids=["optional", "required"])
    async def test_pushdown_body_over_a_masked_column(self, db: Path, required: bool) -> None:
        status = {**SELF_NAMED, **({"meta": {"required": True}} if required else {})}
        source = "SELECT * FROM orders WHERE {FILTER_PARAMS.orders.status.filter('status')}"
        result = import_cube([cube(sql=source, dimensions=[status])], db=db)
        models = result.models
        assert await value(models=models, db=db, model="orders", formula="count(*)",
                           variables={"status": ["paid"]}) == 2
        if required:
            with pytest.raises(Exception, match="status"):
                await value(models=models, db=db, model="orders", formula="count(*)")
        else:
            assert await value(models=models, db=db, model="orders", formula="count(*)") == 3

    async def test_pushdown_body_in_a_column_surface(self, db: Path) -> None:
        flag = dim("paid_flag", "CASE WHEN {FILTER_PARAMS.orders.status.filter('status')} THEN 1 ELSE 0 END",
                   type="number")
        result = import_cube([cube(dimensions=[SELF_NAMED, flag])], db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="paid_flag",
                               variables={"status": ["paid"]}) == {1: 2, 0: 1}


# ── views ─────────────────────────────────────────────────────────────────

@MASKED
class TestViews:
    async def test_view_over_a_self_named_dimension(self, db: Path) -> None:
        view = {"name": "orders_view", "cubes": [{"join_path": "orders", "includes": ["id", "status", "paid_sum"]}]}
        result = import_cube([cube(dimensions=[SELF_NAMED],
                                   measures=[meas("paid_sum", "sum", "{CUBE}.amount", filters=PAID)])],
                             views=[view], db=db)
        models = result.models
        assert await counts_by(models=models, db=db, model="orders_view", dimension="status") == {
            "PAID": 2, "OPEN": 1}
        assert await value(models=models, db=db, model="orders_view", formula="paid_sum") == 30

    async def test_carried_hidden_column_does_not_collide_with_an_exported_member(self, db: Path) -> None:
        joins = [{"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.customer_id = {customers}.id"}]
        customers = {**CUSTOMERS, "dimensions": [*CUSTOMERS["dimensions"], dim("status_raw", "{CUBE}.name")]}
        view = {"name": "orders_view", "cubes": [
            {"join_path": "orders", "includes": ["id", "status"]},
            {"join_path": "orders.customers", "includes": ["status_raw"]}]}
        result = import_cube([cube(dimensions=[SELF_NAMED], joins=joins), customers], views=[view], db=db)
        models = result.models
        assert await counts_by(models=models, db=db, model="orders_view", dimension="status") == {
            "PAID": 2, "OPEN": 1}
        assert await counts_by(models=models, db=db, model="orders_view", dimension="status_raw") == {
            "Ann": 2, "Bo": 1}


# ── typing ────────────────────────────────────────────────────────────────

class TestTyping:
    async def test_list_measure_source_typed_from_the_database(self, db: Path) -> None:
        result = import_cube([cube(measures=[meas("tag_cnt", "count", "{CUBE}.tags")])], db=db)
        col = source_column(models_of(result)["orders"], "tag_cnt")
        assert (col.type, col.element_type) == (DataType.ARRAY, DataType.TEXT)
        assert await _measure(result, db, "tag_cnt") == 2

    def test_base_list_from_reflection_and_probed_list_unknown(self, db: Path) -> None:
        result = import_cube([cube(dimensions=[dim("tags", "{CUBE}.tags"),
                                               dim("tags_sorted", "list_sort({CUBE}.tags)")])], db=db)
        model = models_of(result)["orders"]
        assert (column(model, "tags").type, column(model, "tags").element_type) == (DataType.ARRAY, DataType.TEXT)
        probed = column(model, "tags_sorted")
        assert (probed.type, probed.element_type) == (DataType.ARRAY, DataType.UNKNOWN)

    def test_sql_mode_source_is_probed(self, db: Path) -> None:
        result = import_cube([cube(sql="SELECT * FROM orders", dimensions=[dim("tags", "{CUBE}.tags")])], db=db)
        col = column(models_of(result)["orders"], "tags")
        assert (col.type, col.element_type) == (DataType.ARRAY, DataType.TEXT)

    def test_offline_count_source(self) -> None:
        result = import_cube([cube(measures=[meas("tag_cnt", "count", "{CUBE}.tags")])])
        assert source_column(models_of(result)["orders"], "tag_cnt").type == DataType.TEXT
        notes = typing_notes(i.message for i in result.report.issues if i.severity == "info")
        assert len(notes) == 1, result.report.issues

    def test_unreachable_datasource_is_tried_once(self, tmp_path: Path) -> None:
        total = [meas("total", "sum", "{CUBE}.amount")]
        project = CubeProject.model_validate({"cubes": [cube(measures=total), cube("orders_b", measures=total)]})
        with unreachable_engine(tmp_path) as (engine, attempts):
            result = CubeToSlayerConverter(project=project, data_source=DS, sa_engine=engine).convert()
        assert len(attempts) == 1
        (note,) = typing_notes(i.message for i in result.report.issues if i.severity == "info")
        assert "could not be reached" in note

    def test_offline_max_over_an_untyped_source_is_dropped(self) -> None:
        result = import_cube([cube(measures=[meas("latest", "max", "{CUBE}.created_at")])])
        assert not has_measure(result, "orders", "latest")
        issues = cube_issues(result, "latest")
        assert [i for i in issues if "datasource" in i.message.lower() and "type" in i.message.lower()], issues

    def test_offline_max_over_a_declared_dimension_imports(self) -> None:
        result = import_cube([cube(dimensions=[dim("created_at", "{CUBE}.created_at", type="time")],
                                   measures=[meas("latest", "max", "{CUBE}.created_at")])])
        assert source_column(models_of(result)["orders"], "latest").type == DataType.TIMESTAMP

    @pytest.mark.parametrize("agg", ["count_distinct", "min", "max", "sum", "avg"])
    def test_aggregation_refused_over_a_list_is_dropped(self, db: Path, agg: str) -> None:
        result = import_cube([cube(measures=[meas("tag_agg", agg, "{CUBE}.tags")])], db=db)
        assert not has_measure(result, "orders", "tag_agg")
        issues = cube_issues(result, "tag_agg")
        assert [i for i in issues if "list" in i.message.lower() or "ARRAY" in i.message], issues

    async def test_validate_models_proposes_no_deletion(self, db: Path, tmp_path: Path) -> None:
        result = import_cube([cube(dimensions=[dim("tags", "{CUBE}.tags")],
                                   measures=[meas("tag_cnt", "count", "{CUBE}.tags")])], db=db)
        found = await drift_entries(models=result.models, db=db, base_dir=tmp_path / "store")
        assert deletes(found, model="orders") == [], found


# ── CLI ───────────────────────────────────────────────────────────────────

class TestCli:
    PROJECT = {"cubes": [cube(dimensions=[dim("tags", "{CUBE}.tags")],
                              measures=[meas("tag_cnt", "count", "{CUBE}.tags")])]}

    def _import(self, tmp_path: Path, *, datasource: str) -> tuple[int, Path]:
        project = tmp_path / "cube_project"
        write_yaml(project / "model" / "orders.yml", self.PROJECT)
        store = tmp_path / "store"
        code = run_cli("import-cube", str(project), "--datasource", datasource, "--storage", str(store))
        return code, store

    def test_import_against_a_reachable_datasource(self, db: Path, tmp_path: Path) -> None:
        save_datasource(tmp_path / "store", database=db)
        try:
            code, store = self._import(tmp_path, datasource=DS)
        finally:
            engine_factory.invalidate_engine(DatasourceConfig(name=DS, type="duckdb", database=str(db)))
        assert code == 0
        col = column(stored_model(store, "orders"), "tags")
        assert (col.type, col.element_type) == (DataType.ARRAY, DataType.TEXT)
        assert typing_notes(i["message"] for i in cube_report_messages(store)) == []

    def test_offline_imports_name_why_types_were_not_checked(self, tmp_path: Path) -> None:
        code, store = self._import(tmp_path / "absent", datasource="missing_ds")
        assert code == 0
        (absent,) = typing_notes(i["message"] for i in cube_report_messages(store) if i["severity"] == "info")

        unreachable = tmp_path / "unreachable"
        save_datasource(unreachable / "store", name="broken_ds", database=tmp_path / "no_such_dir" / "x.duckdb")
        code, store = self._import(unreachable, datasource="broken_ds")
        assert code == 0
        stored_model(store, "orders")
        (failed,) = typing_notes(i["message"] for i in cube_report_messages(store) if i["severity"] == "info")
        assert "missing_ds" in absent
        assert "broken_ds" in failed
        assert absent.replace("missing_ds", "") != failed.replace("broken_ds", "")


@MASKED
async def test_query_backed_source_with_a_collision(db: Path) -> None:
    result = import_cube([cube(sql="SELECT * FROM orders", dimensions=[RENAMED_STATUS, STATUS_UPPER])], db=db)
    assert await counts_by(models=result.models, db=db, model="orders", dimension="status_upper") == {
        "PAID": 2, "OPEN": 1}
