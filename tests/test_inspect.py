"""DEV-1588: ``inspect`` — single-entity point-lookup primitive.

These tests pin the contract of :class:`slayer.inspect.service.InspectService`
(the shared core behind the MCP ``inspect`` tool + REST/CLI/SlayerClient
surfaces) and the ``include_hidden`` refactor of
:func:`slayer.search.render.collect_model_entity_pairs`.

Design (settled in the spec interview + two Codex review passes):

* ``inspect(reference, entity_type, ...)`` returns the rendered detail for
  EXACTLY one entity — no RRF / fusion / cypher / bundled memories.
* ``entity_type`` is REQUIRED, one of
  ``datasource/model/column/measure/aggregation/memory``. It disambiguates
  the 3-part canonical collision (a name shared by a column and an
  aggregation) and asserts the resolved kind; a mismatch raises a detailed
  error.
* The reference is normalized via ``resolve_entity`` (so join paths like
  ``orders.customers.region`` resolve to the owning model's canonical), and
  the normalized canonical id is echoed back (always in the JSON shape).
* ``compact=True`` (default) → description-only; ``compact=False`` → full
  render. ``format`` is ``markdown`` (default) | ``json``.
* ``inspect`` RENDERS hidden entities (deliberate escape-hatch lookup).
* Model-only args (``num_rows``/``show_sql``/``sections``/
  ``descriptions_max_chars``) apply where they map; otherwise ignored with a
  warning (warn only when set to a non-default value).
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.storage.yaml_storage import YAMLStorage

# The module under test does not exist yet — importing it is the first
# right-reason failure for this TDD suite.
from slayer.inspect.service import InspectService  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


async def _seed_basic(storage: YAMLStorage) -> None:
    """A ``mydb`` datasource with ``orders`` (+ a column, a measure, and an
    aggregation that collides by name with a column) and a joined
    ``customers`` model so join-path normalization is exercisable."""
    await storage.save_datasource(
        DatasourceConfig(
            name="mydb", type="sqlite", database=":memory:",
            description="Primary analytics warehouse.",
        )
    )
    await storage.save_model(SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="mydb",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(
                name="region", sql="region", type=DataType.TEXT,
                description="Customer billing region.",
            ),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="mydb",
        description="One row per placed order.",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(
                name="amount", sql="amount", type=DataType.DOUBLE,
                description="Order total in USD.",
            ),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            # Collides by name with the aggregation below.
            Column(
                name="big", sql="amount", type=DataType.DOUBLE,
                description="Order amount, aliased.",
            ),
        ],
        measures=[
            ModelMeasure(
                name="aov", formula="amount:sum / *:count",
                description="Average order value.",
            ),
        ],
        aggregations=[
            Aggregation(
                name="big", formula="MAX({col})",
                description="A custom aggregation named 'big'.",
            ),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[("customer_id", "id")]),
        ],
    ))
    await storage.set_datasource_priority(["mydb"])


@pytest.fixture
async def storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        await _seed_basic(st)
        yield st


@pytest.fixture
def svc(storage: YAMLStorage) -> InspectService:
    return InspectService(storage=storage)


# ---------------------------------------------------------------------------
# Argument validation (raise ValueError)
# ---------------------------------------------------------------------------


class TestArgValidation:
    async def test_invalid_entity_type_raises(self, svc: InspectService) -> None:
        with pytest.raises(ValueError, match="entity_type"):
            await svc.inspect(reference="mydb.orders", entity_type="banana")

    async def test_invalid_format_raises(self, svc: InspectService) -> None:
        with pytest.raises(ValueError, match="format"):
            await svc.inspect(
                reference="mydb.orders", entity_type="model", format="yaml",
            )

    async def test_negative_descriptions_max_chars_raises(
        self, svc: InspectService
    ) -> None:
        with pytest.raises(ValueError, match="descriptions_max_chars"):
            await svc.inspect(
                reference="mydb.orders.amount", entity_type="column",
                descriptions_max_chars=-1,
            )


# ---------------------------------------------------------------------------
# Point lookup per kind (full render)
# ---------------------------------------------------------------------------


class TestPerKindFullRender:
    async def test_datasource(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource", compact=False,
        )
        assert "Datasource: mydb" in out
        assert "Primary analytics warehouse." in out

    async def test_column(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=False,
        )
        assert "Column: mydb.orders.amount" in out
        assert "Order total in USD." in out
        assert "Type:" in out

    async def test_measure(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.aov", entity_type="measure", compact=False,
        )
        assert "Measure: mydb.orders.aov" in out
        assert "amount:sum / *:count" in out

    async def test_aggregation(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.big", entity_type="aggregation", compact=False,
        )
        assert "Aggregation: mydb.orders.big" in out
        assert "MAX({col})" in out

    async def test_model(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=False,
        )
        # Reuses the inspect_model renderer.
        assert "# Model: `orders`" in out
        assert "amount" in out


# ---------------------------------------------------------------------------
# compact semantics
# ---------------------------------------------------------------------------


class TestCompact:
    async def test_column_compact_is_description_only(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=True,
        )
        assert "Order total in USD." in out
        # Full-render-only fields must be absent in compact mode.
        assert "Type:" not in out
        assert "SQL:" not in out

    async def test_compact_is_default(self, svc: InspectService) -> None:
        default = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column",
        )
        explicit = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=True,
        )
        assert default == explicit


# ---------------------------------------------------------------------------
# Reference normalization (join paths) + normalized-id echo
# ---------------------------------------------------------------------------


class TestNormalization:
    async def test_join_path_normalizes_to_owning_model(
        self, svc: InspectService
    ) -> None:
        # orders -> customers join; region lives on customers.
        out = await svc.inspect(
            reference="mydb.orders.customers.region", entity_type="column",
            compact=False,
        )
        # Rendered against the OWNING model, not orders.
        assert "Column: mydb.customers.region" in out
        assert "Customer billing region." in out

    async def test_normalized_id_echoed_in_json(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.customers.region", entity_type="column",
            format="json", compact=False,
        )
        payload = json.loads(out)
        assert payload["canonical_id"] == "mydb.customers.region"
        assert payload["entity_type"] == "column"


# ---------------------------------------------------------------------------
# 3-part collision resolution via entity_type
# ---------------------------------------------------------------------------


class TestCollision:
    async def test_column_wins_when_entity_type_column(
        self, svc: InspectService
    ) -> None:
        # 'big' is both a column and an aggregation on orders.
        out = await svc.inspect(
            reference="mydb.orders.big", entity_type="column", compact=False,
        )
        assert "Column: mydb.orders.big" in out
        assert "Aggregation:" not in out

    async def test_aggregation_wins_when_entity_type_aggregation(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.big", entity_type="aggregation", compact=False,
        )
        assert "Aggregation: mydb.orders.big" in out
        assert "Column:" not in out

    async def test_duplicate_aggregation_errors(
        self, storage: YAMLStorage
    ) -> None:
        # Two aggregations share a name → entity_type cannot make it unique.
        await storage.save_model(SlayerModel(
            name="dupes",
            sql_table="dupes",
            data_source="mydb",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
            aggregations=[
                Aggregation(name="dup", formula="MAX({col})"),
                Aggregation(name="dup", formula="MIN({col})"),
            ],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.dupes.dup", entity_type="aggregation", compact=False,
        )
        assert "dup" in out
        # A "cannot uniquely identify"-style message, not a render.
        assert "uniquely" in out.lower()
        # Neither formula body should appear (it's an error, not a render).
        assert "MAX({col})" not in out
        assert "MIN({col})" not in out

    async def test_measure_vs_aggregation_collision(
        self, storage: YAMLStorage
    ) -> None:
        # A measure and an aggregation share the name 'same'.
        await storage.save_model(SlayerModel(
            name="ma",
            sql_table="ma",
            data_source="mydb",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
            measures=[ModelMeasure(name="same", formula="id:sum")],
            aggregations=[Aggregation(name="same", formula="MAX({col})")],
        ))
        svc = InspectService(storage=storage)
        as_measure = await svc.inspect(
            reference="mydb.ma.same", entity_type="measure", compact=False,
        )
        assert "Measure: mydb.ma.same" in as_measure
        assert "Aggregation:" not in as_measure
        as_agg = await svc.inspect(
            reference="mydb.ma.same", entity_type="aggregation", compact=False,
        )
        assert "Aggregation: mydb.ma.same" in as_agg
        assert "Measure:" not in as_agg


# ---------------------------------------------------------------------------
# entity_type mismatch → detailed error
# ---------------------------------------------------------------------------


class TestEntityTypeMismatch:
    async def test_wrong_kind_names_available_kind(
        self, svc: InspectService
    ) -> None:
        # amount is a column; asking for measure must error and name what's
        # actually there.
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="measure", compact=False,
        )
        assert "measure" in out.lower()
        assert "column" in out.lower()

    async def test_model_entity_type_on_three_part_id_errors(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="model", compact=False,
        )
        assert "model" in out.lower()

    async def test_memory_entity_type_on_non_memory_ref_errors(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="memory", compact=False,
        )
        assert "memory" in out.lower()


# ---------------------------------------------------------------------------
# Not found
# ---------------------------------------------------------------------------


class TestNotFound:
    async def test_unknown_column(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.nope", entity_type="column", compact=False,
        )
        assert "nope" in out
        assert "not" in out.lower()

    async def test_unknown_model(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.ghost", entity_type="model", compact=False,
        )
        assert "ghost" in out.lower()
        assert "not" in out.lower()

    async def test_unknown_datasource(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="ghostdb", entity_type="datasource", compact=False,
        )
        assert "not" in out.lower()


# ---------------------------------------------------------------------------
# Hidden entities are rendered (escape hatch), not errored
# ---------------------------------------------------------------------------


class TestHiddenRendered:
    async def test_hidden_column_renders(self, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="withsecret",
            sql_table="withsecret",
            data_source="mydb",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(
                    name="secret", sql="secret", type=DataType.TEXT,
                    description="Hidden but inspectable.", hidden=True,
                ),
            ],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.withsecret.secret", entity_type="column",
            compact=False,
        )
        assert "Column: mydb.withsecret.secret" in out
        assert "Hidden but inspectable." in out

    async def test_hidden_model_renders(self, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="hiddenmodel",
            sql_table="hiddenmodel",
            data_source="mydb",
            hidden=True,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            ],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.hiddenmodel", entity_type="model", compact=False,
        )
        assert "hiddenmodel" in out


# ---------------------------------------------------------------------------
# Datasource-vs-model collision (Case D): entity_type decides
# ---------------------------------------------------------------------------


class TestDatasourceModelCollision:
    async def _seed_collision(self, storage: YAMLStorage) -> None:
        # A datasource named "shared" AND a model named "shared" in mydb.
        await storage.save_datasource(
            DatasourceConfig(name="shared", type="sqlite", database=":memory:")
        )
        await storage.save_model(SlayerModel(
            name="shared",
            sql_table="shared",
            data_source="mydb",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        ))

    async def test_datasource_interpretation(self, storage: YAMLStorage) -> None:
        await self._seed_collision(storage)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="shared", entity_type="datasource", compact=False,
        )
        assert "Datasource: shared" in out

    async def test_model_interpretation_via_entity_type(
        self, storage: YAMLStorage
    ) -> None:
        await self._seed_collision(storage)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="shared", entity_type="model", compact=False,
        )
        # entity_type=model must win over the resolver's datasource default.
        assert "# Model: `shared`" in out


# ---------------------------------------------------------------------------
# Bare-reference resolver behavior (accepted point-lookup contract)
# ---------------------------------------------------------------------------


class TestBareReferences:
    async def test_bare_star_count_errors_without_model_context(
        self, svc: InspectService
    ) -> None:
        # '*:count' needs a model context; inspect passes source_model=None.
        out = await svc.inspect(
            reference="*:count", entity_type="model", compact=False,
        )
        assert "count" in out.lower()

    async def test_bare_unique_leaf_resolves(self, svc: InspectService) -> None:
        # 'region' is a unique column across mydb → resolves to its owner.
        out = await svc.inspect(
            reference="region", entity_type="column", compact=False,
        )
        assert "Column: mydb.customers.region" in out


# ---------------------------------------------------------------------------
# Memory
# ---------------------------------------------------------------------------


class TestMemory:
    async def test_memory_full_render(self, storage: YAMLStorage) -> None:
        mem = await storage.save_memory(
            learning="Refunds are excluded from net revenue.",
            entities=["mydb.orders.amount"],
        )
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference=f"memory:{mem.id}", entity_type="memory", compact=False,
        )
        assert "Refunds are excluded from net revenue." in out
        assert "mydb.orders.amount" in out

    async def test_memory_compact_uses_description(
        self, storage: YAMLStorage
    ) -> None:
        mem = await storage.save_memory(
            learning="Long body line one.\n\nSecond paragraph.",
            entities=["mydb.orders.amount"],
            description="Short preview.",
        )
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference=f"memory:{mem.id}", entity_type="memory", compact=True,
        )
        assert "Short preview." in out
        assert "Second paragraph." not in out

    async def test_memory_compact_first_paragraph_fallback(
        self, storage: YAMLStorage
    ) -> None:
        mem = await storage.save_memory(
            learning="First paragraph.\n\nSecond paragraph.",
            entities=["mydb.orders.amount"],
        )
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference=f"memory:{mem.id}", entity_type="memory", compact=True,
        )
        assert "First paragraph." in out
        assert "Second paragraph." not in out

    async def test_missing_memory_errors(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="memory:nope", entity_type="memory", compact=False,
        )
        assert "nope" in out
        assert "not" in out.lower() or "no memory" in out.lower()


# ---------------------------------------------------------------------------
# Model-only arg matrix
# ---------------------------------------------------------------------------


class TestModelOnlyArgs:
    async def test_sections_honored_on_model(self, svc: InspectService) -> None:
        # sections=["columns"] collapses other sections; "amount" still shows.
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=False,
            sections=["columns"],
        )
        assert "amount" in out

    async def test_num_rows_non_default_warns_on_column(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=False,
            num_rows=10,
        )
        assert "num_rows" in out
        assert "ignored" in out.lower() or "warning" in out.lower()

    async def test_num_rows_non_default_warns_on_measure(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.aov", entity_type="measure", compact=False,
            num_rows=10,
        )
        assert "num_rows" in out
        assert "ignored" in out.lower() or "warning" in out.lower()

    async def test_model_path_never_warns_for_its_own_args(
        self, svc: InspectService
    ) -> None:
        # All four args are valid for a model — none must produce a warning.
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=False,
            sections=["columns"], num_rows=10, show_sql=True,
            descriptions_max_chars=200,
        )
        assert "ignored" not in out.lower()

    async def test_descriptions_max_chars_truncates_model_description(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=False,
            descriptions_max_chars=4,
        )
        assert "One row per placed order." not in out

    async def test_sections_non_default_warns_on_column(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=False,
            sections=["columns"],
        )
        assert "sections" in out
        assert "ignored" in out.lower() or "warning" in out.lower()

    async def test_show_sql_on_column_is_no_op_no_warning(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=False,
            show_sql=True,
        )
        # The column's SQL is intrinsic to its render; flag is a silent no-op.
        assert "show_sql" not in out

    async def test_show_sql_on_datasource_warns(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource", compact=False,
            show_sql=True,
        )
        assert "show_sql" in out
        assert "ignored" in out.lower() or "warning" in out.lower()

    async def test_default_args_never_warn_on_column(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=False,
        )
        assert "ignored" not in out.lower()
        assert "warning" not in out.lower()

    async def test_descriptions_max_chars_truncates_column_description(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=False,
            descriptions_max_chars=5,
        )
        # "Order total in USD." → truncated; full text must be gone.
        assert "Order total in USD." not in out


# ---------------------------------------------------------------------------
# engine=None contract for the model path
# ---------------------------------------------------------------------------


class TestEngineNone:
    async def test_model_path_without_engine_does_not_crash(
        self, svc: InspectService
    ) -> None:
        # svc was constructed with no engine.
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=False,
            sections=["samples"], num_rows=3,
        )
        # Renders the model header; sample-data is skipped (no engine).
        assert "# Model: `orders`" in out


# ---------------------------------------------------------------------------
# JSON output shapes
# ---------------------------------------------------------------------------


class TestJsonShapes:
    async def test_column_json_shape(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column",
            format="json", compact=False,
        )
        payload = json.loads(out)
        assert payload["canonical_id"] == "mydb.orders.amount"
        assert payload["entity_type"] == "column"
        assert "text" in payload
        assert "warnings" in payload

    async def test_column_json_compact_empty_text(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column",
            format="json", compact=True,
        )
        payload = json.loads(out)
        assert payload["text"] == ""
        assert payload["description"] == "Order total in USD."

    async def test_model_json_shape(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model",
            format="json", compact=False,
        )
        payload = json.loads(out)
        # Reuses inspect_model JSON keys ...
        assert payload["model_name"] == "orders"
        # ... plus the inspect additions (normalized id always present).
        assert payload["canonical_id"] == "mydb.orders"
        assert "warnings" in payload


# ---------------------------------------------------------------------------
# Engine-backed model sample rows (real sqlite, non-integration)
# ---------------------------------------------------------------------------


def _make_sqlite_storage_with_data(
    tmpdir: str,
) -> Tuple[YAMLStorage, str]:
    db_path = os.path.join(tmpdir, "data.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?)",
        [(1, 10.0), (2, 20.0), (3, 30.0)],
    )
    conn.commit()
    conn.close()
    return YAMLStorage(base_dir=os.path.join(tmpdir, "store")), db_path


class TestModelSamplesWithEngine:
    async def test_model_samples_render_with_engine(self) -> None:
        from slayer.engine.query_engine import SlayerQueryEngine

        with tempfile.TemporaryDirectory() as tmp:
            st, db_path = _make_sqlite_storage_with_data(tmp)
            await st.save_datasource(
                DatasourceConfig(name="live", type="sqlite", database=db_path)
            )
            await st.save_model(SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="live",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                ],
            ))
            engine = SlayerQueryEngine(storage=st)
            svc = InspectService(storage=st, engine=engine)
            out = await svc.inspect(
                reference="live.orders", entity_type="model", compact=False,
                sections=["samples"], num_rows=3,
            )
            assert "# Model: `orders`" in out


# ---------------------------------------------------------------------------
# Refactor: collect_model_entity_pairs(include_hidden=...)
# ---------------------------------------------------------------------------


class TestCollectModelEntityPairsIncludeHidden:
    def _model(self) -> SlayerModel:
        return SlayerModel(
            name="m",
            sql_table="m",
            data_source="mydb",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="visible", sql="visible", type=DataType.TEXT),
                Column(name="secret", sql="secret", type=DataType.TEXT, hidden=True),
            ],
        )

    def test_default_excludes_hidden_columns(self) -> None:
        from slayer.search.render import collect_model_entity_pairs

        pairs = collect_model_entity_pairs(model=self._model())
        ids = {p.canonical_id for p in pairs}
        assert "mydb.m.visible" in ids
        assert "mydb.m.secret" not in ids

    def test_include_hidden_adds_hidden_columns(self) -> None:
        from slayer.search.render import collect_model_entity_pairs

        pairs = collect_model_entity_pairs(model=self._model(), include_hidden=True)
        ids = {p.canonical_id for p in pairs}
        assert "mydb.m.secret" in ids

    def test_default_returns_empty_for_hidden_model(self) -> None:
        from slayer.search.render import collect_model_entity_pairs

        hidden = self._model()
        hidden.hidden = True
        assert collect_model_entity_pairs(model=hidden) == []

    def test_include_hidden_emits_for_hidden_model(self) -> None:
        from slayer.search.render import collect_model_entity_pairs

        hidden = self._model()
        hidden.hidden = True
        pairs = collect_model_entity_pairs(model=hidden, include_hidden=True)
        ids = {p.canonical_id for p in pairs}
        assert "mydb.m" in ids
        assert "mydb.m.secret" in ids


class TestHiddenStaysOutOfSearch:
    """Regression: the include_hidden refactor must not leak hidden entities
    into the live search/index corpus (default include_hidden=False path)."""

    async def test_hidden_column_absent_from_search_results(
        self, storage: YAMLStorage
    ) -> None:
        from slayer.search.service import SearchService

        await storage.save_model(SlayerModel(
            name="leaky",
            sql_table="leaky",
            data_source="mydb",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(
                    name="topsecret", sql="topsecret", type=DataType.TEXT,
                    hidden=True,
                ),
            ],
        ))
        svc = SearchService(storage=storage)
        resp = await svc.search(question="topsecret", max_results=20)
        ids = {hit.id for hit in resp.results}
        assert "mydb.leaky.topsecret" not in ids
