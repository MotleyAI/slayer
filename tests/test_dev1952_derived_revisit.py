"""DEV-1952 — a derived ``Column.sql`` / ``Column.filter`` whose join path revisits
a model already on it is a typed circular refusal, not a silent ``0.0``.

The shared walker (``core/join_walker.py::walk``) raises ``CircularJoinPathError``
on a revisit (``None`` now means only an unknown/unloaded hop); best-effort
consumers catch it and keep today's answer, while the two doors that classify a
user-typed definition fail closed. Save time rejects the model with
``DerivedColumnCircularError`` from both save doors; query time refuses a stored
circular definition in every position with the base class; the binder's
query-typed circular refusal adopts the same class.

The four unchanged save-time arity scenarios of the MODIFIED requirement (fanning
sql/filter rejected, to-one accepted, proof beats a to-many declaration) stay
covered by ``tests/test_dev1930_save_time_arity.py`` — this module owns the
revisit clause and the query-time backstop.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import (
    CircularJoinPathError,
    DerivedColumnCircularError,
    DerivedColumnFanningError,
    SlayerError,
    UnresolvableDimensionJoinError,
)
from slayer.core.join_walker import terminal_model, walk
from slayer.core.keys import Grain
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine import join_safety
from slayer.engine.compile.stages import _canonical_path
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.column_expansion import (
    _lenient_path,
    resolve_default_qualifier_path,
    resolve_ref_target,
)
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1832_fixtures import make_exec_engine as _dev1832_make_engine
from tests._dev1832_fixtures import orders_q as _dev1931_orders_q
from tests._dev1853_fixtures import chain_models
from tests._dev1900_fixtures import make_exec_engine as _dev1900_make_engine
from tests._dev1900_fixtures import orders_q as _dev1900_orders_q
from tests._dev1931_fixtures import dev1931_models
from tests._engine_helpers import seeded_exec_engine

DS = "ds"


# --------------------------------------------------------------------------- #
# Save-time model builders (``regions ← customers ← orders``; spend on customers,
# so ``regions.customers.spend`` from customers revisits customers).
# --------------------------------------------------------------------------- #
def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source=DS, sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="pop", type=DataType.DOUBLE),
        ],
    )


def _customers(*, cols: tuple[Column, ...] = (), joins: tuple[ModelJoin, ...] = ()) -> SlayerModel:
    return SlayerModel(
        name="customers", data_source=DS, sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="name", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
            *cols,
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                      cardinality=JoinCardinality.MANY_TO_ONE),
            *joins,
        ],
    )


def _orders(*, cols: tuple[Column, ...] = ()) -> SlayerModel:
    return SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            *cols,
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                         cardinality=JoinCardinality.MANY_TO_ONE)],
    )


def _storage(tmp_path) -> YAMLStorage:
    return YAMLStorage(base_dir=str(tmp_path))


# --------------------------------------------------------------------------- #
# Section A — Error vocabulary (task 1.1).
# --------------------------------------------------------------------------- #
class TestErrorVocabulary:
    def test_circular_error_fields_and_message(self) -> None:
        # Distinct synthetic values so each message clause is pinned individually.
        exc = CircularJoinPathError(
            reference="A.B.leaf", root_model="ROOT",
            revisited="REV", hop="HOP", via="VIA",
        )
        assert isinstance(exc, ValueError)
        assert isinstance(exc, SlayerError)
        assert exc.reference == "A.B.leaf"
        assert exc.root_model == "ROOT"
        assert exc.revisited == "REV"
        assert exc.hop == "HOP"
        assert exc.via == "VIA"
        msg = str(exc)
        assert "Circular join detected resolving 'A.B.leaf' from 'ROOT'" in msg
        assert "hop 'HOP' revisits model 'REV'" in msg

    def test_circular_error_names_the_column_when_set(self) -> None:
        exc = CircularJoinPathError(
            reference="A.B.leaf", root_model="ROOT", revisited="REV",
            hop="HOP", via="VIA", column="revisit_spend",
        )
        assert exc.column == "revisit_spend"
        assert "revisit_spend" in str(exc)

    def test_derived_error_is_a_circular_error_with_remedy(self) -> None:
        exc = DerivedColumnCircularError(
            column="revisit_spend", model="customers", kind="sql",
            reference="regions.customers.spend", root_model="customers",
            revisited="customers", hop="customers", via="regions",
        )
        assert isinstance(exc, CircularJoinPathError)
        assert isinstance(exc, SlayerError)
        assert isinstance(exc, ValueError)
        assert exc.column == "revisit_spend"
        assert exc.model == "customers"
        assert exc.kind == "sql"
        assert exc.reference == "regions.customers.spend"
        assert exc.root_model == "customers"
        assert exc.revisited == "customers"
        assert exc.hop == "customers"
        assert exc.via == "regions"
        msg = str(exc)
        assert "Derived column 'revisit_spend' on model 'customers'" in msg
        assert "sql reference 'regions.customers.spend'" in msg
        assert "revisits model 'customers'" in msg
        assert "not a column of 'customers'" in msg
        # Both remedy arms: reference the column directly, or aggregate on the via.
        assert "Reference the column on 'customers'" in msg
        assert "declare the aggregate on 'regions'" in msg
        assert "reaches 'customers'" in msg


# --------------------------------------------------------------------------- #
# Section B — Walker raise + best-effort consumers keep today's answer
# (tasks 2.1, 2.2, 2.4). The consumer guards already pass pre-fix (``walk``
# returns ``None``); they fail only if a consumer forgets to catch the new raise.
# --------------------------------------------------------------------------- #
def _chain() -> dict[str, SlayerModel]:
    return {m.name: m for m in chain_models()}


def _self_join_model() -> SlayerModel:
    """A model carrying a named ``parent`` self-join edge, built via
    ``model_construct`` to bypass ``SlayerModel._reject_self_joins`` — a real
    self-join is rejected at construction, so save/query paths never see one;
    this exercises the walker's first-hop revisit guard in isolation."""
    return SlayerModel.model_construct(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="parent_id", type=DataType.INT),
            Column(name="name", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["parent_id", "id"]],
                         name="parent", cardinality=JoinCardinality.MANY_TO_ONE)],
    )


class TestWalkerRaisesOnRevisit:
    def test_walk_raises_with_all_fields(self) -> None:
        models = _chain()
        with pytest.raises(CircularJoinPathError) as ei:
            walk(root=models["regions"], path=("customers", "regions"),
                 models_by_name=models)
        exc = ei.value
        assert exc.revisited == "regions"
        assert exc.hop == "regions"
        assert exc.via == "customers"
        assert exc.root_model == "regions"
        assert exc.reference == "customers.regions"

    def test_walk_returns_none_on_unknown_token(self) -> None:
        models = _chain()
        assert walk(root=models["orders"], path=("nope",),
                    models_by_name=models) is None

    def test_self_join_first_hop_raises(self) -> None:
        model = _self_join_model()
        with pytest.raises(CircularJoinPathError) as ei:
            walk(root=model, path=("parent",), models_by_name={"customers": model})
        assert ei.value.hop == "parent"
        assert ei.value.revisited == "customers"

    def test_terminal_model_catches_revisit_to_none(self) -> None:
        models = _chain()
        assert terminal_model(root=models["regions"], path=("customers", "regions"),
                              models_by_name=models) is None


class TestBestEffortConsumersUnchanged:
    """Every best-effort ``walk`` consumer keeps its documented answer on a revisit."""

    def test_safe_reachable_false(self) -> None:
        models = _chain()
        assert join_safety.safe_reachable(
            root=models["regions"], path=("customers", "regions"),
            models_by_name=models) is False

    def test_back_path_falls_back_to_host(self) -> None:
        models = _chain()
        assert join_safety._back_path(
            host_name="regions", target_path=("customers", "regions"),
            models_by_name=models) == ("regions",)

    def test_hop_walk_reason_none(self) -> None:
        models = _chain()
        assert join_safety._hop_walk_reason(
            root_model=models["regions"], path=("customers", "regions"),
            models_by_name=models) is None

    def test_path_grain_determined_false(self) -> None:
        models = _chain()
        assert join_safety._path_grain_determined(
            path=("customers", "regions"), grain=Grain(keys=frozenset()),
            host_model=models["regions"], models_by_name=models) is False

    def test_attributable_from_root_false(self) -> None:
        models = _chain()
        # A composed via-host reroot path that revisits stays unattributable (False),
        # never a raise — the reroot machinery relies on this answer.
        assert join_safety.attributable_from_root(
            host_path=("regions",), target_path=("customers",),
            root_model=models["orders"], models_by_name=models,
            host_name="customers") is False

    def test_resolve_ref_target_none(self) -> None:
        models = _chain()
        assert resolve_ref_target(
            qualifiers=("customers", "regions"), source_model=models["regions"],
            models_by_name=models) is None

    def test_resolve_default_qualifier_path_none(self) -> None:
        # A default circular from the speculative owner frame is a clean miss, so
        # owner-first/root-fallback default resolution is unchanged.
        models = _chain()
        assert resolve_default_qualifier_path(
            qualifiers=("customers", "regions"), leaf="pop",
            frame_model=models["regions"], models_by_name=models) is None

    def test_lenient_scanner_none(self) -> None:
        models = _chain()
        assert _lenient_path(
            qualifiers=("customers", "regions"), source_model=models["regions"],
            owner_alias="regions", models_by_name=models) is None

    def test_canonical_path_returns_input(self) -> None:
        models = _chain()
        assert _canonical_path(
            ("customers", "regions"), root=models["regions"],
            models_by_name=models) == ("customers", "regions")


# --------------------------------------------------------------------------- #
# Section C — Save-time rejection, storage door (task 4.1).
# --------------------------------------------------------------------------- #
class TestSaveTimeStorageDoor:
    async def test_sql_revisit_rejected(self, tmp_path) -> None:
        storage = _storage(tmp_path)
        await storage.save_model(_regions())
        model = _customers(cols=(Column(
            name="revisit_spend", type=DataType.DOUBLE, sql="regions.customers.spend"),))
        with pytest.raises(DerivedColumnCircularError) as ei:
            await storage.save_model(model)
        exc = ei.value
        assert isinstance(exc, CircularJoinPathError)
        assert isinstance(exc, ValueError)
        assert exc.column == "revisit_spend"
        assert exc.model == "customers"
        assert exc.kind == "sql"
        assert exc.reference == "regions.customers.spend"
        assert exc.revisited == "customers"
        assert exc.hop == "customers"
        assert exc.via == "regions"

    async def test_filter_revisit_rejected(self, tmp_path) -> None:
        storage = _storage(tmp_path)
        await storage.save_model(_regions())
        model = _customers(cols=(Column(
            name="rc", type=DataType.DOUBLE, sql="spend",
            filter="regions.customers.spend > 0"),))
        with pytest.raises(DerivedColumnCircularError) as ei:
            await storage.save_model(model)
        assert ei.value.kind == "filter"
        assert ei.value.reference == "regions.customers.spend"
        assert ei.value.revisited == "customers"

    async def test_revisit_declared_on_querying_model(self, tmp_path) -> None:
        storage = _storage(tmp_path)
        await storage.save_model(_regions())
        await storage.save_model(_customers())
        model = _orders(cols=(Column(
            name="rc", type=DataType.DOUBLE, sql="customers.regions.customers.spend"),))
        with pytest.raises(DerivedColumnCircularError) as ei:
            await storage.save_model(model)
        assert ei.value.revisited == "customers"
        assert ei.value.hop == "customers"
        assert ei.value.via == "regions"

    async def test_leading_declaring_qualifier_keeps_full_spelling(self, tmp_path) -> None:
        storage = _storage(tmp_path)
        await storage.save_model(_regions())
        model = _customers(cols=(Column(
            name="rc", type=DataType.DOUBLE, sql="customers.regions.customers.spend"),))
        with pytest.raises(DerivedColumnCircularError) as ei:
            await storage.save_model(model)
        assert ei.value.reference == "customers.regions.customers.spend"
        assert ei.value.revisited == "customers"
        assert ei.value.hop == "customers"
        assert ei.value.via == "regions"

    async def test_to_one_round_trip_is_circular(self, tmp_path) -> None:
        storage = _storage(tmp_path)
        a = SlayerModel(
            name="a", data_source=DS, sql_table="a",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="y", type=DataType.DOUBLE),
                Column(name="rt", type=DataType.DOUBLE, sql="b.a.y"),
            ],
            joins=[ModelJoin(target_model="b", join_pairs=[["id", "id"]],
                             cardinality=JoinCardinality.ONE_TO_ONE)],
        )
        b = SlayerModel(
            name="b", data_source=DS, sql_table="b",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        )
        await storage.save_model(b)
        with pytest.raises(DerivedColumnCircularError) as ei:
            await storage.save_model(a)
        assert ei.value.revisited == "a"

    async def test_revisit_beats_earlier_fanning_hop(self, tmp_path) -> None:
        storage = _storage(tmp_path)
        line_items = SlayerModel(
            name="line_items", data_source=DS, sql_table="line_items",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="order_id", type=DataType.INT),
            ],
        )
        await storage.save_model(line_items)
        orders = SlayerModel(
            name="orders", data_source=DS, sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="bf", type=DataType.DOUBLE, sql="line_items.orders.amount"),
            ],
            joins=[ModelJoin(
                target_model="line_items", join_pairs=[["id", "order_id"]],
                cardinality=JoinCardinality.ONE_TO_MANY)],
        )
        with pytest.raises(DerivedColumnCircularError) as ei:
            await storage.save_model(orders)
        # Circular, not fanning — the revisit precedes the fanning verdict.
        assert not isinstance(ei.value, DerivedColumnFanningError)
        assert ei.value.revisited == "orders"

    async def test_known_but_unloaded_intermediate_skipped(self, tmp_path) -> None:
        # ``regions`` is not loaded, so the revisit is unprovable and the save
        # succeeds (the query-time door stays the check).
        storage = _storage(tmp_path)
        model = _customers(cols=(Column(
            name="revisit_spend", type=DataType.DOUBLE, sql="regions.customers.spend"),))
        await storage.save_model(model)  # must not raise
        assert await storage.get_model("customers", data_source=DS) is not None

    async def test_validate_false_still_saves(self, tmp_path) -> None:
        storage = _storage(tmp_path)
        await storage.save_model(_regions())
        model = _customers(cols=(Column(
            name="revisit_spend", type=DataType.DOUBLE, sql="regions.customers.spend"),))
        await storage.save_model(model, _validate=False)  # migration write-back path
        assert await storage.get_model("customers", data_source=DS) is not None

    async def test_ambiguous_hop_on_revisit_path_skipped(self, tmp_path) -> None:
        # Two parallel customers→regions edges make the leading `regions` hop
        # ambiguous, so the revisit is unprovable and the save skips (no error).
        storage = _storage(tmp_path)
        await storage.save_model(_regions())
        model = SlayerModel(
            name="customers", data_source=DS, sql_table="customers",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region_id", type=DataType.INT),
                Column(name="spend", type=DataType.DOUBLE),
                Column(name="revisit_spend", type=DataType.DOUBLE,
                       sql="regions.customers.spend"),
            ],
            joins=[
                ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                          name="r1", cardinality=JoinCardinality.MANY_TO_ONE),
                ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                          name="r2", cardinality=JoinCardinality.MANY_TO_ONE),
            ],
        )
        await storage.save_model(model)  # must not raise
        assert await storage.get_model("customers", data_source=DS) is not None


# --------------------------------------------------------------------------- #
# Section D — Save-time rejection, engine door, identical error (task 4.2).
# --------------------------------------------------------------------------- #
class TestSaveTimeEngineDoor:
    async def test_engine_save_door_rejects_identically(self, tmp_path) -> None:
        storage = _storage(tmp_path)
        await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite"))
        await storage.save_model(_regions())
        model = _customers(cols=(Column(
            name="revisit_spend", type=DataType.DOUBLE, sql="regions.customers.spend"),))
        with pytest.raises(DerivedColumnCircularError) as storage_ei:
            await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        with pytest.raises(DerivedColumnCircularError) as engine_ei:
            await engine.save_model(model)
        storage_exc, engine_exc = storage_ei.value, engine_ei.value
        for exc in (storage_exc, engine_exc):
            assert exc.column == "revisit_spend"
            assert exc.model == "customers"
            assert exc.kind == "sql"
            assert exc.reference == "regions.customers.spend"
            assert exc.revisited == "customers"
            assert exc.hop == "customers"
            assert exc.via == "regions"
        assert str(storage_exc) == str(engine_exc)  # identical message + remedy

    async def test_model_filter_surface_propagates_base_error(self, tmp_path) -> None:
        # A model-level filter that revisits propagates the base error, not the
        # derived-column one (it is not a column surface).
        storage = _storage(tmp_path)
        await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite"))
        await storage.save_model(_regions())
        engine = SlayerQueryEngine(storage=storage)
        model = _customers().model_copy(
            update={"filters": ["regions.customers.spend > 0"]})
        with pytest.raises(CircularJoinPathError) as ei:
            await engine.save_model(model)
        assert not isinstance(ei.value, DerivedColumnCircularError)
        assert ei.value.revisited == "customers"


# --------------------------------------------------------------------------- #
# Section E — Query-time backstop, dual-engine (tasks 3.1, 3.2).
# A circular definition stored under ``_validate=False`` is refused in every
# position before any SQL runs, never a value / ``no such column`` / verbatim ref.
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, "North", 100.0), (2, "South", 200.0)]
_CUSTOMERS_ROWS = [(1, 1, "Alice", 100.0), (2, 1, "Bob", 150.0), (3, 2, "Cara", 60.0)]
_ORDERS_ROWS = [(1, 1), (2, 1), (3, 2)]


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
        cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
        cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
                    "name TEXT, spend REAL)")
        cur.executemany("INSERT INTO customers VALUES (?,?,?,?)", _CUSTOMERS_ROWS)
        cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")
        cur.executemany("INSERT INTO orders VALUES (?,?)", _ORDERS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR, pop DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    con.execute("CREATE TABLE customers (id INTEGER, region_id INTEGER, name VARCHAR, "
                "spend DOUBLE)")
    con.executemany("INSERT INTO customers VALUES (?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER)")
    con.executemany("INSERT INTO orders VALUES (?,?)", _ORDERS_ROWS)
    con.close()


def _revisit_query_models() -> list[SlayerModel]:
    """One rich graph; the query selects the circular definition under test."""
    customers = _customers(
        cols=(
            Column(name="revisit_spend", type=DataType.DOUBLE, sql="regions.customers.spend"),
            Column(name="revisit_lead", type=DataType.DOUBLE,
                   sql="customers.regions.customers.spend"),
            Column(name="chain", type=DataType.DOUBLE, sql="revisit_spend * 2"),
            Column(name="filt_revisit", type=DataType.DOUBLE, sql="spend",
                   filter="regions.customers.spend > 0"),
        ),
    )
    return [_regions(), customers, _orders()]


def _unresolved_query_models() -> list[SlayerModel]:
    """``ghost`` is declared but never saved — its path is refused as unresolvable
    before it could revisit."""
    customers = _customers(
        cols=(Column(name="ghost_spend", type=DataType.DOUBLE,
                     sql="ghost.customers.spend"),),
        joins=(ModelJoin(target_model="ghost", join_pairs=[["region_id", "id"]]),),
    )
    return [_regions(), customers, _orders()]


async def _make_engine(dialect: str, models: list[SlayerModel]):
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, models=models, datasource=DS, validate=False,
    ) as (engine, _db):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def qengine(request):
    async for e in _make_engine(request.param, _revisit_query_models()):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unresolved_engine(request):
    async for e in _make_engine(request.param, _unresolved_query_models()):
        yield e


def _cust_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "customers")
    return SlayerQuery(**kw)


def _orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


async def _assert_circular(
    engine, query: SlayerQuery, *,
    reference: str | None = "regions.customers.spend", revisited: str | None = "customers",
) -> CircularJoinPathError:
    """Refused with the circular error before any SQL runs — never a value, a
    ``no such column``, or the literal reference text."""
    with pytest.raises(CircularJoinPathError) as ei:
        await engine.execute(query)
    exc = ei.value
    msg = str(exc)
    assert "Circular join" in msg
    assert "revisits model" in msg
    assert "no such column" not in msg.lower()
    if reference is not None:
        assert exc.reference == reference
    if revisited is not None:
        assert exc.revisited == revisited
    return exc


class TestQueryTimeBackstop:
    async def test_aggregated_across_to_one_hop(self, qengine) -> None:
        await _assert_circular(qengine, _orders_q(
            measures=[ModelMeasure(formula="customers.revisit_spend:sum", name="w")]))

    async def test_aggregated_on_declaring_model(self, qengine) -> None:
        await _assert_circular(qengine, _cust_q(
            measures=[ModelMeasure(formula="revisit_spend:sum", name="w")]))

    async def test_as_dimension(self, qengine) -> None:
        await _assert_circular(qengine, _cust_q(
            dimensions=["revisit_spend"],
            measures=[ModelMeasure(formula="spend:sum", name="s")]))

    async def test_in_filter(self, qengine) -> None:
        await _assert_circular(qengine, _cust_q(
            measures=[ModelMeasure(formula="spend:sum", name="s")],
            filters=["revisit_spend > 0"]))

    async def test_raw_row_mode(self, qengine) -> None:
        await _assert_circular(qengine, _cust_q(
            dimensions=["revisit_spend"], distinct_dimension_values=False))

    async def test_through_a_derived_chain(self, qengine) -> None:
        exc = await _assert_circular(qengine, _cust_q(
            measures=[ModelMeasure(formula="chain:sum", name="w")]))
        assert exc.column == "revisit_spend"  # the innermost expanded definition

    async def test_filter_kind_definition(self, qengine) -> None:
        # reference comes from the filter fragment, proving the filter surface.
        await _assert_circular(qengine, _cust_q(
            measures=[ModelMeasure(formula="filt_revisit:sum", name="w")]))

    async def test_leading_host_qualifier_keeps_full_spelling(self, qengine) -> None:
        # The door keeps the complete pre-strip reference (leading host qualifier).
        await _assert_circular(
            qengine, _cust_q(measures=[ModelMeasure(formula="revisit_lead:sum", name="w")]),
            reference="customers.regions.customers.spend")

    async def test_refused_before_sql_executes(self, qengine) -> None:
        # dry_run builds SQL but does not execute it — a raise here proves the
        # refusal is at compile time, before any SQL runs.
        query = _cust_q(measures=[ModelMeasure(formula="revisit_spend:sum", name="w")])
        with pytest.raises(CircularJoinPathError):
            await qengine.execute(query, dry_run=True)

    async def test_query_typed_spelling_same_class(self, qengine) -> None:
        # The binder builds its own reference fields; pin only the class, the
        # revisited model, and that the message names the circular path.
        exc = await _assert_circular(
            qengine, _orders_q(measures=[ModelMeasure(
                formula="customers.regions.customers.spend:sum", name="w")]),
            reference=None)
        assert "customers.regions.customers" in str(exc)
        # The query-typed refusal is the base class, not the save-time derived one.
        assert not isinstance(exc, DerivedColumnCircularError)

    async def test_unresolved_model_refused_as_unresolvable(self, unresolved_engine) -> None:
        query = _cust_q(dimensions=["ghost_spend"],
                        measures=[ModelMeasure(formula="spend:sum", name="s")])
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await unresolved_engine.execute(query)
        msg = str(ei.value)
        assert "ghost" in msg
        assert not isinstance(ei.value, CircularJoinPathError)


# --------------------------------------------------------------------------- #
# Section F — Binder parity (task 5.1): the query-typed circular refusal adopts
# ``CircularJoinPathError`` while keeping its "Circular join" / "revisits" wording.
# --------------------------------------------------------------------------- #
class TestBinderParity:
    async def test_query_typed_revisit_raises_circular_class(self, qengine) -> None:
        exc = await _assert_circular(
            qengine, _orders_q(measures=[ModelMeasure(
                formula="customers.regions.customers.spend:sum", name="w")]),
            reference=None)
        assert isinstance(exc, CircularJoinPathError)
        assert isinstance(exc, ValueError)
        # The binder adopts the base class, not the save-time derived subclass.
        assert not isinstance(exc, DerivedColumnCircularError)


# --------------------------------------------------------------------------- #
# Section G — behaviour-unchanged regressions (design Risks): a composed via-host
# reroot path still resolves by association (never a new raise), and definition
# defaults still resolve owner-first with root fallback. Reuse the DEV-1900 /
# DEV-1931 graphs and their oracles; these pass before and after the fix.
# --------------------------------------------------------------------------- #
@pytest.fixture(params=["sqlite", "duckdb"])
async def reroot_engine(request):
    async for e in _dev1900_make_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def default_engine(request):
    async for e in _dev1832_make_engine(request=request, models=dev1931_models()):
        yield e


class TestViaHostRerootUnchanged:
    async def test_associate_over_composed_reroot_path(self, reroot_engine) -> None:
        # `customers.spend:sum(partition_by=status)` from orders exercises
        # reroot_from_root / attributable_from_root over a composed via-host path.
        resp = await reroot_engine.execute(_dev1900_orders_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:sum(partition_by=status)", name="w")],
            to_many_handling="associate"))
        got = {r["orders.status"]: r["orders.w"] for r in resp.data}
        assert got == pytest.approx({"new": 290.0, "ok": 420.0})


class TestDefinitionDefaultsUnchanged:
    @staticmethod
    async def _value(engine, formula: str) -> float:
        resp = await engine.execute(
            _dev1931_orders_q(measures=[ModelMeasure(formula=formula, name="w")]))
        return float(resp.data[0]["orders.w"])

    async def test_scalar_default_matches_explicit(self, default_engine) -> None:
        explicit = await self._value(
            default_engine, "customers.spend:wsum_host(weight=orders.amount)")
        default = await self._value(default_engine, "customers.spend:wsum_host")
        assert default == pytest.approx(explicit)

    async def test_expression_default_matches_explicit(self, default_engine) -> None:
        explicit = await self._value(
            default_engine, "customers.spend:wsum_host_expr(weight=orders.amount)")
        default = await self._value(default_engine, "customers.spend:wsum_host_expr")
        assert default == pytest.approx(explicit)
