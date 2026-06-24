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
* ``compact=True`` (default): leaf (column/measure/aggregation), datasource,
  and memory render description-only; the **model** kind renders a cheap
  schema *skeleton* (column / measure / aggregation **names** + join targets,
  zero DB calls). ``compact=False`` → full render; for the **datasource**
  kind, ``compact=False`` renders a per-model skeleton for each visible model.
  ``format`` is ``markdown`` (default) | ``json``.
* JSON ``text`` is present **iff non-empty**: ``compact=True`` JSON omits the
  ``text`` key entirely for every kind; ``compact=False`` JSON carries ``text``
  only where it holds a render (memory / leaf), ``models`` for the datasource
  kind, and the full ``inspect_model`` payload for the model kind.
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
from slayer.inspect.model_render import (  # noqa: E402
    model_skeleton_fields,
    render_model_skeleton,
)
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

    async def test_model_compact_is_skeleton(
        self, svc: InspectService
    ) -> None:
        # DEV-1588 follow-up: model compact is a cheap schema *skeleton* (names
        # only, zero DB), NOT description-only and NOT the full renderer.
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=True,
        )
        # Description still shown.
        assert "One row per placed order." in out
        # Skeleton lines present (all four, names only).
        assert "Columns: " in out
        assert "amount" in out and "customer_id" in out
        assert "Measures: aov" in out
        assert "Aggregations: big" in out
        assert "Joins to: customers" in out
        # Standalone skeleton heading is the backticked model name (no "Model:"
        # prefix, distinguishing it from the full render).
        assert "# `orders`" in out
        # Full-render-only markers must be absent (proves the skeleton path,
        # not render_model_inspection).
        assert "# Model: `orders`" not in out   # full-render heading
        assert "## Columns (" not in out         # full-render columns table
        assert "- **data_source:**" not in out   # metadata bullets
        assert "## Sample Data" not in out


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

    async def test_dotted_leaf_does_not_resolve_to_unrelated_model(
        self, storage: YAMLStorage
    ) -> None:
        # DEV-1588 review: a model literally named like the leaf must NOT be
        # returned for `inspect("mydb.orders.amount", "model")` — the dotted
        # leaf is a kind mismatch, not a bare-name fallback.
        await storage.save_model(SlayerModel(
            name="amount",  # collides with orders.amount leaf name
            sql_table="amount",
            data_source="mydb",
            description="UNRELATED model named amount.",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="model", compact=False,
        )
        assert "UNRELATED model named amount." not in out
        assert "# Model: `amount`" not in out

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

    async def test_column_json_compact_omits_text(
        self, svc: InspectService
    ) -> None:
        # DEV-1588 follow-up: ``text`` is present iff non-empty, so compact
        # JSON drops the key entirely (a consumer never sees ``text: ""``).
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column",
            format="json", compact=True,
        )
        payload = json.loads(out)
        assert "text" not in payload
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


# ---------------------------------------------------------------------------
# DEV-1588 follow-up: model/datasource compact skeleton
# ---------------------------------------------------------------------------


def _orders_like_model() -> SlayerModel:
    """A standalone (no-storage) ``orders`` model mirroring the seed fixture —
    columns id/amount/customer_id/big, one named measure, one aggregation, one
    join. Used to unit-test the pure skeleton helpers."""
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="mydb",
        description="One row per placed order.",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="big", sql="amount", type=DataType.DOUBLE),
        ],
        measures=[ModelMeasure(name="aov", formula="amount:sum / *:count")],
        aggregations=[Aggregation(name="big", formula="MAX({col})")],
        joins=[ModelJoin(target_model="customers", join_pairs=[("customer_id", "id")])],
    )


class TestModelSkeletonHelpers:
    """Pure helpers in ``slayer.inspect.model_render`` — no DB, no engine."""

    def test_render_model_skeleton_is_heading_less(self) -> None:

        md = render_model_skeleton(model=_orders_like_model())
        # The caller prepends the `#`/`##` heading — the helper body must not.
        assert not any(line.startswith("#") for line in md.splitlines())
        assert "One row per placed order." in md
        # Exact plain-CSV (no backticks) per the locked format.
        assert "Columns: id, amount, customer_id, big" in md
        assert "Measures: aov" in md
        assert "Aggregations: big" in md
        assert "Joins to: customers" in md

    def test_render_model_skeleton_empty_sections_render_none(self) -> None:

        bare = SlayerModel(
            name="customers", sql_table="customers", data_source="mydb",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        )
        md = render_model_skeleton(model=bare)
        lines = md.splitlines()
        # No description set → no description line; body starts at `Columns:`.
        assert lines[0] == "Columns: id, region"
        # All four lines always present; empty ones render `_(none)_`
        # (aligned to models_summary(compact)).
        assert "Measures: _(none)_" in md
        assert "Aggregations: _(none)_" in md
        assert "Joins to: _(none)_" in md
        # No blank trailing-space "Measures: " line.
        assert "Measures: \n" not in md and not md.endswith("Measures: ")

    def test_render_model_skeleton_hidden_only_columns_render_none(self) -> None:

        m = SlayerModel(
            name="m", sql_table="m", data_source="mydb",
            columns=[
                Column(name="secret", sql="secret", type=DataType.TEXT, hidden=True),
            ],
        )
        md = render_model_skeleton(model=m)
        assert "Columns: _(none)_" in md
        assert "secret" not in md

    def test_render_model_skeleton_unnamed_only_measures_render_none(
        self,
    ) -> None:

        # SlayerModel rejects unnamed measures at validation, so model_copy is
        # used to reach the defensive ``m.name is not None`` filter path.
        base = SlayerModel(
            name="m", sql_table="m", data_source="mydb",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
            measures=[ModelMeasure(name="tmp", formula="id:sum")],
        )
        m = base.model_copy(update={"measures": [ModelMeasure(formula="id:max")]})
        md = render_model_skeleton(model=m)
        assert "Measures: _(none)_" in md

    def test_render_model_skeleton_truncates_description(self) -> None:

        md = render_model_skeleton(model=_orders_like_model(), max_chars=4)
        assert "One row per placed order." not in md
        assert "Columns: " in md

    def test_model_skeleton_fields_shape(self) -> None:

        fields = model_skeleton_fields(model=_orders_like_model())
        assert fields["name"] == "orders"
        assert fields["canonical_id"] == "mydb.orders"
        assert fields["description"] == "One row per placed order."
        assert fields["column_names"] == ["id", "amount", "customer_id", "big"]
        assert fields["measure_names"] == ["aov"]
        assert fields["aggregation_names"] == ["big"]
        assert fields["joins_to"] == ["customers"]

    def test_model_skeleton_fields_excludes_hidden_columns(self) -> None:

        m = SlayerModel(
            name="m", sql_table="m", data_source="mydb",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="visible", sql="visible", type=DataType.TEXT),
                Column(name="secret", sql="secret", type=DataType.TEXT, hidden=True),
            ],
        )
        assert model_skeleton_fields(model=m)["column_names"] == ["id", "visible"]

    def test_model_skeleton_fields_excludes_unnamed_measures(self) -> None:

        # model_copy bypasses the SlayerModel "every measure must be named"
        # validator so we can prove the defensive filter in the helper.
        base = SlayerModel(
            name="m", sql_table="m", data_source="mydb",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
            measures=[ModelMeasure(name="named", formula="id:sum")],
        )
        m = base.model_copy(update={"measures": [
            ModelMeasure(name="named", formula="id:sum"),
            ModelMeasure(formula="id:max"),  # unnamed
        ]})
        assert model_skeleton_fields(model=m)["measure_names"] == ["named"]

    def test_model_skeleton_fields_joins_sorted_deduped(self) -> None:

        m = SlayerModel(
            name="m", sql_table="m", data_source="mydb",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="z_id", sql="z_id", type=DataType.INT),
                Column(name="a_id", sql="a_id", type=DataType.INT),
            ],
            joins=[
                ModelJoin(target_model="zebra", join_pairs=[("z_id", "id")]),
                ModelJoin(target_model="apple", join_pairs=[("a_id", "id")]),
            ],
        )
        assert model_skeleton_fields(model=m)["joins_to"] == ["apple", "zebra"]

    def test_canonical_id_has_no_leading_dot_without_data_source(self) -> None:

        # model_copy bypasses validators so we can simulate a not-yet-refined
        # (empty data_source) model and prove the guard.
        m = _orders_like_model().model_copy(update={"data_source": ""})
        assert model_skeleton_fields(model=m)["canonical_id"] == "orders"

    def test_model_render_module_does_not_import_slayer_mcp(self) -> None:
        # The skeleton helpers live in slayer.inspect.model_render, which
        # mcp/server.py imports — so the reverse would be a circular import.
        import ast
        import pathlib

        import slayer.inspect.model_render as mr

        src = pathlib.Path(mr.__file__).read_text()
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(
                    not a.name.startswith("slayer.mcp") for a in node.names
                )
            elif isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("slayer.mcp")


class TestModelCompactSkeletonJson:
    async def test_model_compact_json_shape(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model",
            format="json", compact=True,
        )
        p = json.loads(out)
        assert p["canonical_id"] == "mydb.orders"
        assert p["entity_type"] == "model"
        assert p["name"] == "orders"
        assert p["description"] == "One row per placed order."
        assert p["column_names"] == ["id", "amount", "customer_id", "big"]
        assert p["measure_names"] == ["aov"]
        assert p["aggregation_names"] == ["big"]
        assert p["joins_to"] == ["customers"]
        assert "text" not in p
        assert "warnings" in p

    async def test_model_compact_json_empty_lists_present(
        self, svc: InspectService
    ) -> None:
        # ``customers`` has no measures / aggregations / joins — the JSON keys
        # are still present as empty lists (stable shape).
        out = await svc.inspect(
            reference="mydb.customers", entity_type="model",
            format="json", compact=True,
        )
        p = json.loads(out)
        assert p["canonical_id"] == "mydb.customers"
        assert p["column_names"] == ["id", "region"]
        assert p["measure_names"] == []
        assert p["aggregation_names"] == []
        assert p["joins_to"] == []
        assert "text" not in p

    async def test_model_compact_markdown_empty_sections_render_none(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.customers", entity_type="model", compact=True,
        )
        assert "Columns: " in out and "region" in out
        assert "Measures: _(none)_" in out
        assert "Aggregations: _(none)_" in out
        assert "Joins to: _(none)_" in out

    async def test_model_compact_descriptions_max_chars(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=True,
            descriptions_max_chars=4,
        )
        assert "One row per placed order." not in out
        assert "Columns: " in out

    async def test_model_compact_json_truncates_description(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model",
            format="json", compact=True, descriptions_max_chars=4,
        )
        p = json.loads(out)
        assert p["description"] != "One row per placed order."
        assert p["description"].startswith("One ")
        # Structure intact despite truncation.
        assert p["column_names"] == ["id", "amount", "customer_id", "big"]


class TestDatasourceCompactFalseSkeletons:
    async def test_markdown_per_model_skeletons(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource", compact=False,
        )
        assert "Datasource: mydb" in out
        assert "Primary analytics warehouse." in out
        # Per-model heading + skeleton body for each visible model.
        assert "## `customers`" in out
        assert "## `orders`" in out
        assert "region" in out            # customers column name
        assert "Measures: aov" in out     # orders measure
        assert "Joins to: customers" in out
        # Sorted by name: customers heading precedes orders heading.
        assert out.index("## `customers`") < out.index("## `orders`")

    async def test_json_models_list(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource",
            format="json", compact=False,
        )
        p = json.loads(out)
        assert p["canonical_id"] == "mydb"
        assert p["entity_type"] == "datasource"
        assert p["description"] == "Primary analytics warehouse."
        assert "text" not in p
        names = [m["name"] for m in p["models"]]
        assert names == ["customers", "orders"]   # sorted by name
        orders = next(m for m in p["models"] if m["name"] == "orders")
        assert orders["canonical_id"] == "mydb.orders"
        assert orders["column_names"] == ["id", "amount", "customer_id", "big"]
        assert orders["measure_names"] == ["aov"]
        assert orders["aggregation_names"] == ["big"]
        assert orders["joins_to"] == ["customers"]

    async def test_hidden_model_excluded(self, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="hiddends", sql_table="hiddends", data_source="mydb",
            hidden=True,
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb", entity_type="datasource", compact=False,
        )
        assert "hiddends" not in out
        outj = await svc.inspect(
            reference="mydb", entity_type="datasource",
            format="json", compact=False,
        )
        assert all(m["name"] != "hiddends" for m in json.loads(outj)["models"])

    async def test_no_visible_models_empty_list(
        self, storage: YAMLStorage
    ) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="emptyds", type="sqlite", database=":memory:",
            description="Empty one.",
        ))
        await storage.save_model(SlayerModel(
            name="onlyhidden", sql_table="onlyhidden", data_source="emptyds",
            hidden=True,
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        ))
        svc = InspectService(storage=storage)
        outj = await svc.inspect(
            reference="emptyds", entity_type="datasource",
            format="json", compact=False,
        )
        assert json.loads(outj)["models"] == []
        outmd = await svc.inspect(
            reference="emptyds", entity_type="datasource", compact=False,
        )
        assert "Datasource: emptyds" in outmd
        assert "onlyhidden" not in outmd

    async def test_descriptions_max_chars_truncates_ds_and_models(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource", compact=False,
            descriptions_max_chars=4,
        )
        assert "Primary analytics warehouse." not in out   # ds description
        assert "One row per placed order." not in out       # model description
        # Skeleton structure still present.
        assert "## `orders`" in out

    async def test_json_descriptions_max_chars_truncates_ds_and_models(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource",
            format="json", compact=False, descriptions_max_chars=4,
        )
        p = json.loads(out)
        assert p["description"] != "Primary analytics warehouse."
        assert p["description"].startswith("Prim")
        orders = next(m for m in p["models"] if m["name"] == "orders")
        assert orders["description"] != "One row per placed order."
        assert orders["description"].startswith("One ")

    async def test_warnings_preserved_in_json(
        self, svc: InspectService
    ) -> None:
        # show_sql is a model-only arg → warns for the datasource kind, and the
        # warning must survive into the JSON ``warnings`` list.
        out = await svc.inspect(
            reference="mydb", entity_type="datasource",
            format="json", compact=False, show_sql=True,
        )
        p = json.loads(out)
        assert any("show_sql" in w for w in p["warnings"])


class TestDatasourceCompactTrueUnchanged:
    async def test_markdown_description_only(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource", compact=True,
        )
        assert "Primary analytics warehouse." in out
        assert "## `orders`" not in out
        assert "Columns:" not in out

    async def test_json_omits_text_and_models(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource",
            format="json", compact=True,
        )
        p = json.loads(out)
        assert "text" not in p
        assert "models" not in p
        assert p["description"] == "Primary analytics warehouse."


class TestCompactTextOmittedAllKinds:
    @pytest.mark.parametrize(
        "reference,entity_type",
        [
            ("mydb.orders.amount", "column"),
            ("mydb.orders.aov", "measure"),
            ("mydb.orders.big", "aggregation"),
        ],
    )
    async def test_leaf_compact_json_omits_text(
        self, svc: InspectService, reference: str, entity_type: str
    ) -> None:
        out = await svc.inspect(
            reference=reference, entity_type=entity_type,
            format="json", compact=True,
        )
        assert "text" not in json.loads(out)

    @pytest.mark.parametrize(
        "reference,entity_type",
        [
            ("mydb.orders.amount", "column"),
            ("mydb.orders.aov", "measure"),
            ("mydb.orders.big", "aggregation"),
        ],
    )
    async def test_leaf_noncompact_json_keeps_text(
        self, svc: InspectService, reference: str, entity_type: str
    ) -> None:
        out = await svc.inspect(
            reference=reference, entity_type=entity_type,
            format="json", compact=False,
        )
        p = json.loads(out)
        assert "text" in p and p["text"]

    async def test_memory_compact_json_omits_text(
        self, storage: YAMLStorage
    ) -> None:
        mem = await storage.save_memory(
            learning="Body.", entities=["mydb.orders.amount"],
            description="Preview.",
        )
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference=f"memory:{mem.id}", entity_type="memory",
            format="json", compact=True,
        )
        p = json.loads(out)
        assert "text" not in p
        assert p["description"] == "Preview."

    async def test_memory_noncompact_json_keeps_text(
        self, storage: YAMLStorage
    ) -> None:
        mem = await storage.save_memory(
            learning="Full body text here.",
            entities=["mydb.orders.amount"],
        )
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference=f"memory:{mem.id}", entity_type="memory",
            format="json", compact=False,
        )
        p = json.loads(out)
        assert "text" in p and "Full body text here." in p["text"]


class TestSkeletonZeroDB:
    async def test_model_compact_never_touches_engine(
        self, storage: YAMLStorage
    ) -> None:
        class _ExplodingEngine:
            def __getattr__(self, name: str):
                raise AssertionError(
                    f"engine.{name} must not be called in the compact "
                    f"skeleton path"
                )

        svc = InspectService(storage=storage, engine=_ExplodingEngine())
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=True,
        )
        assert "Columns: " in out
        outj = await svc.inspect(
            reference="mydb.orders", entity_type="model",
            format="json", compact=True,
        )
        assert json.loads(outj)["name"] == "orders"

    async def test_model_compact_does_not_call_full_renderer(
        self, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Direct proof the compact path short-circuits BEFORE the DB-hitting
        # full renderer: monkeypatch render_model_inspection to explode.
        import slayer.inspect.service as service_mod

        async def _boom(*args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError(
                "render_model_inspection must not run for compact=True"
            )

        monkeypatch.setattr(service_mod, "render_model_inspection", _boom)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=True,
        )
        assert "Columns: " in out
        # And the full path STILL uses it (guards against a stale patch /
        # wrong-symbol monkeypatch giving a false pass above).
        with pytest.raises(AssertionError, match="must not run"):
            await svc.inspect(
                reference="mydb.orders", entity_type="model", compact=False,
            )


# ---------------------------------------------------------------------------
# Ambiguous bare model name (DEV-1588 follow-up — Codex review)
# ---------------------------------------------------------------------------


class TestAmbiguousModelName:
    """A bare model name present in 2+ datasources with no priority winner
    makes ``resolve_entity`` raise ``AmbiguousModelError`` (a SlayerError
    sibling, NOT an EntityResolutionError). inspect must surface the message,
    not let it escape as an uncaught exception (which would 500 on REST)."""

    async def _seed_ambiguous(self, storage: YAMLStorage) -> None:
        # ``mydb`` already has ``orders``; add a second datasource that also
        # has ``orders``, then clear the priority so no winner resolves.
        await storage.save_datasource(DatasourceConfig(
            name="otherdb", type="sqlite", database=":memory:",
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="otherdb",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        ))
        await storage.set_datasource_priority([])

    @pytest.mark.parametrize(
        "entity_type", ["model", "datasource", "column"],
    )
    async def test_ambiguous_bare_model_returns_message_not_raise(
        self, storage: YAMLStorage, entity_type: str
    ) -> None:
        await self._seed_ambiguous(storage)
        svc = InspectService(storage=storage)
        # Must NOT raise — returns the actionable ambiguity message.
        out = await svc.inspect(
            reference="orders", entity_type=entity_type, compact=False,
        )
        assert "multiple datasources" in out.lower()
        assert "mydb" in out and "otherdb" in out

    async def test_ambiguous_bare_model_json_does_not_crash(
        self, storage: YAMLStorage
    ) -> None:
        await self._seed_ambiguous(storage)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="orders", entity_type="model",
            format="json", compact=True,
        )
        # The ambiguity message is returned as a plain string (not JSON, not a
        # raised exception) — the surface stays alive.
        assert "multiple datasources" in out.lower()
