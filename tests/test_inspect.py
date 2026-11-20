"""``inspect`` — single-entity point-lookup primitive.

These tests pin the contract of :class:`slayer.inspect.service.InspectService`
(the shared core behind the MCP ``inspect`` tool + REST/CLI/SlayerClient
surfaces) and the ``include_hidden`` refactor of
:func:`slayer.search.render.collect_dataset_entity_pairs`.

Design (settled in the spec interview + two Codex review passes):

* ``inspect(reference, entity_type, ...)`` returns the rendered detail for
  EXACTLY one entity — no RRF / fusion / cypher / bundled memories.
* ``entity_type`` is REQUIRED, one of
  ``datasource/dataset/field/metric/aggregation/memory``. It disambiguates
  the 3-part canonical collision (a name shared by a field and an
  aggregation) and asserts the resolved kind; a mismatch raises a detailed
  error.
* The reference is normalized via ``resolve_entity`` (so relationship paths like
  ``orders.customers.region`` resolve to the owning dataset's canonical), and
  the normalized canonical id is echoed back (always in the JSON shape).
* ``compact=True`` (default): a **field**, datasource, and memory render
  description-only; **metric** / **aggregation** leaves also include their full
  text (expression / params / type — no sample data, so cheap); the **dataset** kind
  renders a cheap schema *skeleton* (field / metric / aggregation **names** +
  relationship targets, zero DB calls). ``compact=False`` → full render; for the
  **datasource** kind, ``compact=False`` renders a per-dataset skeleton for each
  visible dataset. ``format`` is ``markdown`` (default) | ``json``.
* JSON ``text`` is present **iff non-empty**: ``compact=True`` JSON omits the
  ``text`` key for fields / datasource / memory but keeps it for metrics /
  aggregations; ``compact=False`` JSON carries ``text`` wherever it holds a
  render (memory / leaf), ``datasets`` for the datasource kind, and the full
  ``inspect_model`` payload for the dataset kind.
* ``inspect`` RENDERS hidden entities (deliberate escape-hatch lookup).
* Dataset-only args (``num_rows``/``show_sql``/``sections``/
  ``descriptions_max_chars``) apply where they map; otherwise ignored with a
  warning (warn only when set to a non-default value).
"""

from __future__ import annotations

import ast
import json
import os
import pathlib
import tempfile
from typing import AsyncIterator, Tuple

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.storage.yaml_storage import YAMLStorage

# The module under test does not exist yet — importing it is the first
# right-reason failure for this TDD suite.
from slayer.inspect.dataset_render import (  # noqa: E402
    dataset_skeleton_fields,
    render_dataset_skeleton,
)
from slayer.inspect.service import InspectService  # noqa: E402
from slayer.storage.sqlite_conn import transaction

from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect import dataset_render as mr
from slayer.inspect import service as service_mod
from slayer.search.render import collect_dataset_entity_pairs
from slayer.search.service import SearchService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


async def _seed_basic(storage: YAMLStorage) -> None:
    """A ``mydb`` datasource with ``orders`` (+ a field, a metric, and an
    aggregation that collides by name with a field) and a joined
    ``customers`` dataset so relationship-path normalization is exercisable."""
    await storage.save_datasource(
        DatasourceConfig(
            name="mydb", type="sqlite", database=":memory:",
            description="Primary analytics warehouse.",
        )
    )
    await storage.save_dataset(SlayerDataset(
        name="customers",
        sql_table="customers",
        data_source="mydb",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(
                name="region", sql="region", datatype=DataType.STRING,
                description="Customer billing region.",
            ),
        ],
    ))
    await storage.save_dataset(SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="mydb",
        description="One row per placed order.",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(
                name="amount", sql="amount", datatype=DataType.FLOAT,
                description="Order total in USD.",
            ),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INTEGER),
            # Collides by name with the aggregation below.
            DatasetField(
                name="big", sql="amount", datatype=DataType.FLOAT,
                description="Order amount, aliased.",
            ),
        ],
        metrics=[
            DatasetMetric(
                name="aov", expression="sum(amount) / count(*)",
                description="Average order value.",
            ),
        ],
        aggregations=[
            Aggregation(
                name="big", expression="MAX({col})",
                description="A custom aggregation named 'big'.",
            ),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
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
                reference="mydb.orders", entity_type="dataset", format="yaml",
            )

    async def test_negative_descriptions_max_chars_raises(
        self, svc: InspectService
    ) -> None:
        with pytest.raises(ValueError, match="descriptions_max_chars"):
            await svc.inspect(
                reference="mydb.orders.amount", entity_type="field",
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
            reference="mydb.orders.amount", entity_type="field", compact=False,
        )
        assert "Field: mydb.orders.amount" in out
        assert "Order total in USD." in out
        assert "Type:" in out

    async def test_measure(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.aov", entity_type="metric", compact=False,
        )
        assert "Metric: mydb.orders.aov" in out
        assert "sum(amount) / count(*)" in out

    async def test_aggregation(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.big", entity_type="aggregation", compact=False,
        )
        assert "Aggregation: mydb.orders.big" in out
        assert "MAX({col})" in out

    async def test_model(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset", compact=False,
        )
        # Reuses the inspect_model renderer.
        assert "# Dataset: `orders`" in out
        assert "amount" in out


# ---------------------------------------------------------------------------
# compact semantics
# ---------------------------------------------------------------------------


class TestCompact:
    async def test_column_compact_is_description_only(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field", compact=True,
        )
        assert "Order total in USD." in out
        # Full-render-only fields must be absent in compact mode.
        assert "Type:" not in out
        assert "SQL:" not in out

    async def test_compact_is_default(self, svc: InspectService) -> None:
        default = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field",
        )
        explicit = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field", compact=True,
        )
        assert default == explicit

    async def test_model_compact_is_skeleton(
        self, svc: InspectService
    ) -> None:
        # Dataset compact is a cheap schema *skeleton* (names
        # only, zero DB), NOT description-only and NOT the full renderer.
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset", compact=True,
        )
        # Description still shown.
        assert "One row per placed order." in out
        # Skeleton lines present (all four, names only).
        assert "Fields: " in out
        assert "amount" in out
        assert "customer_id" in out
        assert "Metrics: aov" in out
        assert "Aggregations: big" in out
        assert "Relationships to: customers" in out
        # Standalone skeleton heading is the backticked dataset name (no "Dataset:"
        # prefix, distinguishing it from the full render).
        assert "# `orders`" in out
        # Full-render-only markers must be absent (proves the skeleton path,
        # not render_model_inspection).
        assert "# Dataset: `orders`" not in out   # full-render heading
        assert "## Fields (" not in out         # full-render fields table
        assert "- **data_source:**" not in out   # metadata bullets
        assert "## Data Profile" not in out


# ---------------------------------------------------------------------------
# Reference normalization (relationship paths) + normalized-id echo
# ---------------------------------------------------------------------------


class TestNormalization:
    async def test_join_path_normalizes_to_owning_model(
        self, svc: InspectService
    ) -> None:
        # orders -> customers relationship; region lives on customers.
        out = await svc.inspect(
            reference="mydb.orders.customers.region", entity_type="field",
            compact=False,
        )
        # Rendered against the OWNING dataset, not orders.
        assert "Field: mydb.customers.region" in out
        assert "Customer billing region." in out

    async def test_normalized_id_echoed_in_json(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.customers.region", entity_type="field",
            format="json", compact=False,
        )
        payload = json.loads(out)
        assert payload["canonical_id"] == "mydb.customers.region"
        assert payload["entity_type"] == "field"


# ---------------------------------------------------------------------------
# 3-part collision resolution via entity_type
# ---------------------------------------------------------------------------


class TestCollision:
    async def test_column_wins_when_entity_type_column(
        self, svc: InspectService
    ) -> None:
        # 'big' is both a field and an aggregation on orders.
        out = await svc.inspect(
            reference="mydb.orders.big", entity_type="field", compact=False,
        )
        assert "Field: mydb.orders.big" in out
        assert "Aggregation:" not in out

    async def test_aggregation_wins_when_entity_type_aggregation(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.big", entity_type="aggregation", compact=False,
        )
        assert "Aggregation: mydb.orders.big" in out
        assert "Field:" not in out

    async def test_duplicate_aggregation_errors(
        self, storage: YAMLStorage
    ) -> None:
        # Two aggregations share a name → entity_type cannot make it unique.
        await storage.save_dataset(SlayerDataset(
            name="dupes",
            sql_table="dupes",
            data_source="mydb",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
            aggregations=[
                Aggregation(name="dup", expression="MAX({col})"),
                Aggregation(name="dup", expression="MIN({col})"),
            ],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.dupes.dup", entity_type="aggregation", compact=False,
        )
        assert "dup" in out
        # A "cannot uniquely identify"-style message, not a render.
        assert "uniquely" in out.lower()
        # Neither expression body should appear (it's an error, not a render).
        assert "MAX({col})" not in out
        assert "MIN({col})" not in out

    async def test_measure_vs_aggregation_collision(
        self, storage: YAMLStorage
    ) -> None:
        # A metric and an aggregation share the name 'same'.
        await storage.save_dataset(SlayerDataset(
            name="ma",
            sql_table="ma",
            data_source="mydb",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
            metrics=[DatasetMetric(name="same", expression="sum(id)")],
            aggregations=[Aggregation(name="same", expression="MAX({col})")],
        ))
        svc = InspectService(storage=storage)
        as_measure = await svc.inspect(
            reference="mydb.ma.same", entity_type="metric", compact=False,
        )
        assert "Metric: mydb.ma.same" in as_measure
        assert "Aggregation:" not in as_measure
        as_agg = await svc.inspect(
            reference="mydb.ma.same", entity_type="aggregation", compact=False,
        )
        assert "Aggregation: mydb.ma.same" in as_agg
        assert "Metric:" not in as_agg


# ---------------------------------------------------------------------------
# entity_type mismatch → detailed error
# ---------------------------------------------------------------------------


class TestEntityTypeMismatch:
    async def test_wrong_kind_names_available_kind(
        self, svc: InspectService
    ) -> None:
        # amount is a field; asking for metric must error and name what's
        # actually there.
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="metric", compact=False,
        )
        assert "metric" in out.lower()
        assert "field" in out.lower()

    async def test_model_entity_type_on_three_part_id_errors(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="dataset", compact=False,
        )
        assert "dataset" in out.lower()

    async def test_dotted_leaf_does_not_resolve_to_unrelated_model(
        self, storage: YAMLStorage
    ) -> None:
        # A dataset literally named like the leaf must NOT be
        # returned for `inspect("mydb.orders.amount", "dataset")` — the dotted
        # leaf is a kind mismatch, not a bare-name fallback.
        await storage.save_dataset(SlayerDataset(
            name="amount",  # collides with orders.amount leaf name
            sql_table="amount",
            data_source="mydb",
            description="UNRELATED model named amount.",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="dataset", compact=False,
        )
        assert "UNRELATED model named amount." not in out
        assert "# Dataset: `amount`" not in out

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
            reference="mydb.orders.nope", entity_type="field", compact=False,
        )
        assert "nope" in out
        assert "not" in out.lower()

    async def test_unknown_model(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.ghost", entity_type="dataset", compact=False,
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
        await storage.save_dataset(SlayerDataset(
            name="withsecret",
            sql_table="withsecret",
            data_source="mydb",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(
                    name="secret", sql="secret", datatype=DataType.STRING,
                    description="Hidden but inspectable.", hidden=True,
                ),
            ],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.withsecret.secret", entity_type="field",
            compact=False,
        )
        assert "Field: mydb.withsecret.secret" in out
        assert "Hidden but inspectable." in out

    async def test_hidden_model_renders(self, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="hiddenmodel",
            sql_table="hiddenmodel",
            data_source="mydb",
            hidden=True,
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            ],
        ))
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.hiddenmodel", entity_type="dataset", compact=False,
        )
        assert "hiddenmodel" in out


# ---------------------------------------------------------------------------
# Datasource-vs-dataset collision (Case D): entity_type decides
# ---------------------------------------------------------------------------


class TestDatasourceModelCollision:
    async def _seed_collision(self, storage: YAMLStorage) -> None:
        # A datasource named "shared" AND a dataset named "shared" in mydb.
        await storage.save_datasource(
            DatasourceConfig(name="shared", type="sqlite", database=":memory:")
        )
        await storage.save_dataset(SlayerDataset(
            name="shared",
            sql_table="shared",
            data_source="mydb",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
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
            reference="shared", entity_type="dataset", compact=False,
        )
        # entity_type=dataset must win over the resolver's datasource default.
        assert "# Dataset: `shared`" in out


# ---------------------------------------------------------------------------
# Bare-reference resolver behavior (accepted point-lookup contract)
# ---------------------------------------------------------------------------


class TestBareReferences:
    async def test_bare_star_count_errors_without_model_context(
        self, svc: InspectService
    ) -> None:
        # '*:count' needs a dataset context; inspect passes population=None.
        out = await svc.inspect(
            reference="count(*)", entity_type="dataset", compact=False,
        )
        assert "count" in out.lower()

    async def test_bare_unique_leaf_resolves(self, svc: InspectService) -> None:
        # 'region' is a unique field across mydb → resolves to its owner.
        out = await svc.inspect(
            reference="region", entity_type="field", compact=False,
        )
        assert "Field: mydb.customers.region" in out


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
# Dataset-only arg matrix
# ---------------------------------------------------------------------------


class TestModelOnlyArgs:
    async def test_sections_honored_on_model(self, svc: InspectService) -> None:
        # sections=["fields"] collapses other sections; "amount" still shows.
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset", compact=False,
            sections=["fields"],
        )
        assert "amount" in out

    async def test_num_rows_non_default_warns_on_column(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field", compact=False,
            num_rows=10,
        )
        assert "num_rows" in out
        assert "ignored" in out.lower() or "warning" in out.lower()

    async def test_num_rows_non_default_warns_on_measure(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.aov", entity_type="metric", compact=False,
            num_rows=10,
        )
        assert "num_rows" in out
        assert "ignored" in out.lower() or "warning" in out.lower()

    async def test_model_path_never_warns_for_its_own_args(
        self, svc: InspectService
    ) -> None:
        # All four args are valid for a dataset — none must produce a warning.
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset", compact=False,
            sections=["fields"], num_rows=10, show_sql=True,
            descriptions_max_chars=200,
        )
        assert "ignored" not in out.lower()

    async def test_descriptions_max_chars_truncates_model_description(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset", compact=False,
            descriptions_max_chars=4,
        )
        assert "One row per placed order." not in out

    async def test_sections_non_default_warns_on_column(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field", compact=False,
            sections=["fields"],
        )
        assert "sections" in out
        assert "ignored" in out.lower() or "warning" in out.lower()

    async def test_show_sql_on_column_is_no_op_no_warning(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field", compact=False,
            show_sql=True,
        )
        # The field's SQL is intrinsic to its render; flag is a silent no-op.
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
            reference="mydb.orders.amount", entity_type="field", compact=False,
        )
        assert "ignored" not in out.lower()
        assert "warning" not in out.lower()

    async def test_descriptions_max_chars_truncates_column_description(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field", compact=False,
            descriptions_max_chars=5,
        )
        # "Order total in USD." → truncated; full text must be gone.
        assert "Order total in USD." not in out


# ---------------------------------------------------------------------------
# engine=None contract for the dataset path
# ---------------------------------------------------------------------------


class TestEngineNone:
    async def test_model_path_without_engine_does_not_crash(
        self, svc: InspectService
    ) -> None:
        # svc was constructed with no engine.
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset", compact=False,
            sections=["samples"], num_rows=3,
        )
        # Renders the dataset header; sample-data is skipped (no engine).
        assert "# Dataset: `orders`" in out


# ---------------------------------------------------------------------------
# JSON output shapes
# ---------------------------------------------------------------------------


class TestJsonShapes:
    async def test_column_json_shape(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field",
            format="json", compact=False,
        )
        payload = json.loads(out)
        assert payload["canonical_id"] == "mydb.orders.amount"
        assert payload["entity_type"] == "field"
        assert "text" in payload
        assert "warnings" in payload

    async def test_column_json_compact_omits_text(
        self, svc: InspectService
    ) -> None:
        # ``text`` is present iff non-empty, so compact
        # JSON drops the key entirely (a consumer never sees ``text: ""``).
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="field",
            format="json", compact=True,
        )
        payload = json.loads(out)
        assert "text" not in payload
        assert payload["description"] == "Order total in USD."

    async def test_model_json_shape(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset",
            format="json", compact=False,
        )
        payload = json.loads(out)
        # Reuses inspect_model JSON keys ...
        assert payload["dataset_name"] == "orders"
        # ... plus the inspect additions (normalized id always present).
        assert payload["canonical_id"] == "mydb.orders"
        assert "warnings" in payload


# ---------------------------------------------------------------------------
# Engine-backed dataset sample rows (real sqlite, non-integration)
# ---------------------------------------------------------------------------


def _make_sqlite_storage_with_data(
    tmpdir: str,
) -> Tuple[YAMLStorage, str]:
    db_path = os.path.join(tmpdir, "data.db")
    with transaction(db_path) as conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?)",
            [(1, 10.0), (2, 20.0), (3, 30.0)],
        )
    return YAMLStorage(base_dir=os.path.join(tmpdir, "store")), db_path


class TestModelSamplesWithEngine:
    async def test_model_samples_render_with_engine(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st, db_path = _make_sqlite_storage_with_data(tmp)
            await st.save_datasource(
                DatasourceConfig(name="live", type="sqlite", database=db_path)
            )
            await st.save_dataset(SlayerDataset(
                name="orders",
                sql_table="orders",
                data_source="live",
                fields=[
                    DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                ],
            ))
            engine = SlayerQueryEngine(storage=st)
            svc = InspectService(storage=st, engine=engine)
            out = await svc.inspect(
                reference="live.orders", entity_type="dataset", compact=False,
                sections=["samples"], num_rows=3,
            )
            assert "# Dataset: `orders`" in out


# ---------------------------------------------------------------------------
# Refactor: collect_model_entity_pairs(include_hidden=...)
# ---------------------------------------------------------------------------


class TestCollectModelEntityPairsIncludeHidden:
    def _dataset(self) -> SlayerDataset:
        return SlayerDataset(
            name="m",
            sql_table="m",
            data_source="mydb",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="visible", sql="visible", datatype=DataType.STRING),
                DatasetField(name="secret", sql="secret", datatype=DataType.STRING, hidden=True),
            ],
        )

    def test_default_excludes_hidden_columns(self) -> None:
        pairs = collect_dataset_entity_pairs(dataset=self._dataset())
        ids = {p.canonical_id for p in pairs}
        assert "mydb.m.visible" in ids
        assert "mydb.m.secret" not in ids

    def test_include_hidden_adds_hidden_columns(self) -> None:
        pairs = collect_dataset_entity_pairs(dataset=self._dataset(), include_hidden=True)
        ids = {p.canonical_id for p in pairs}
        assert "mydb.m.secret" in ids

    def test_default_returns_empty_for_hidden_model(self) -> None:
        hidden = self._dataset()
        hidden.hidden = True
        assert collect_dataset_entity_pairs(dataset=hidden) == []

    def test_include_hidden_emits_for_hidden_model(self) -> None:
        hidden = self._dataset()
        hidden.hidden = True
        pairs = collect_dataset_entity_pairs(dataset=hidden, include_hidden=True)
        ids = {p.canonical_id for p in pairs}
        assert "mydb.m" in ids
        assert "mydb.m.secret" in ids


class TestHiddenStaysOutOfSearch:
    """Regression: the include_hidden refactor must not leak hidden entities
    into the live search/index corpus (default include_hidden=False path)."""

    async def test_hidden_column_absent_from_search_results(
        self, storage: YAMLStorage
    ) -> None:
        await storage.save_dataset(SlayerDataset(
            name="leaky",
            sql_table="leaky",
            data_source="mydb",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(
                    name="topsecret", sql="topsecret", datatype=DataType.STRING,
                    hidden=True,
                ),
            ],
        ))
        svc = SearchService(storage=storage)
        resp = await svc.search(question="topsecret", max_results=20)
        ids = {hit.id for hit in resp.results}
        assert "mydb.leaky.topsecret" not in ids


# ---------------------------------------------------------------------------
# Dataset/datasource compact skeleton
# ---------------------------------------------------------------------------


def _orders_like_dataset() -> SlayerDataset:
    """A standalone (no-storage) ``orders`` dataset mirroring the seed fixture —
    fields id/amount/customer_id/big, one named metric, one aggregation, one
    relationship. Used to unit-test the pure skeleton helpers."""
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="mydb",
        description="One row per placed order.",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="big", sql="amount", datatype=DataType.FLOAT),
        ],
        metrics=[DatasetMetric(name="aov", expression="sum(amount) / count(*)")],
        aggregations=[Aggregation(name="big", expression="MAX({col})")],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )


class TestModelSkeletonHelpers:
    """Pure helpers in ``slayer.inspect.dataset_render`` — no DB, no engine."""

    def test_render_model_skeleton_is_heading_less(self) -> None:

        md = render_dataset_skeleton(dataset=_orders_like_dataset())
        # The caller prepends the `#`/`##` heading — the helper body must not.
        assert not any(line.startswith("#") for line in md.splitlines())
        assert "One row per placed order." in md
        # Exact plain-CSV (no backticks) per the locked format.
        assert "Fields: id, amount, customer_id, big" in md
        assert "Metrics: aov" in md
        assert "Aggregations: big" in md
        assert "Relationships to: customers" in md

    def test_render_model_skeleton_empty_sections_render_none(self) -> None:

        bare = SlayerDataset(
            name="customers", sql_table="customers", data_source="mydb",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
            ],
        )
        md = render_dataset_skeleton(dataset=bare)
        lines = md.splitlines()
        # No description set → no description line; body starts at `Fields:`.
        assert lines[0] == "Fields: id, region"
        # All four lines always present; empty ones render `_(none)_`
        # (aligned to datasets_summary(compact)).
        assert "Metrics: _(none)_" in md
        assert "Aggregations: _(none)_" in md
        assert "Relationships to: _(none)_" in md
        # No blank trailing-space "Metrics: " line.
        assert "Metrics: \n" not in md
        assert not md.endswith("Metrics: ")

    def test_render_model_skeleton_hidden_only_columns_render_none(self) -> None:

        m = SlayerDataset(
            name="m", sql_table="m", data_source="mydb",
            fields=[
                DatasetField(name="secret", sql="secret", datatype=DataType.STRING, hidden=True),
            ],
        )
        md = render_dataset_skeleton(dataset=m)
        assert "Fields: _(none)_" in md
        assert "secret" not in md

    def test_render_model_skeleton_truncates_description(self) -> None:

        md = render_dataset_skeleton(dataset=_orders_like_dataset(), max_chars=4)
        assert "One row per placed order." not in md
        assert "Fields: " in md

    def test_model_skeleton_fields_shape(self) -> None:

        fields = dataset_skeleton_fields(dataset=_orders_like_dataset())
        assert fields["name"] == "orders"
        assert fields["canonical_id"] == "mydb.orders"
        assert fields["description"] == "One row per placed order."
        assert fields["field_names"] == ["id", "amount", "customer_id", "big"]
        assert fields["metric_names"] == ["aov"]
        assert fields["aggregation_names"] == ["big"]
        assert fields["relationships_to"] == ["customers"]

    def test_model_skeleton_fields_excludes_hidden_columns(self) -> None:

        m = SlayerDataset(
            name="m", sql_table="m", data_source="mydb",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="visible", sql="visible", datatype=DataType.STRING),
                DatasetField(name="secret", sql="secret", datatype=DataType.STRING, hidden=True),
            ],
        )
        assert dataset_skeleton_fields(dataset=m)["field_names"] == ["id", "visible"]

    def test_model_skeleton_fields_joins_sorted_deduped(self) -> None:

        m = SlayerDataset(
            name="m", sql_table="m", data_source="mydb",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="z_id", sql="z_id", datatype=DataType.INTEGER),
                DatasetField(name="a_id", sql="a_id", datatype=DataType.INTEGER),
            ],
            relationships=[
                DatasetRelationship(to="zebra", from_columns=["z_id"], to_columns=["id"]),
                DatasetRelationship(to="apple", from_columns=["a_id"], to_columns=["id"]),
            ],
        )
        assert dataset_skeleton_fields(dataset=m)["relationships_to"] == ["apple", "zebra"]

    def test_canonical_id_has_no_leading_dot_without_data_source(self) -> None:

        # model_copy bypasses validators so we can simulate a not-yet-refined
        # (empty data_source) dataset and prove the guard.
        m = _orders_like_dataset().model_copy(update={"data_source": ""})
        assert dataset_skeleton_fields(dataset=m)["canonical_id"] == "orders"

    def test_model_render_module_does_not_import_slayer_mcp(self) -> None:
        # The skeleton helpers live in slayer.inspect.dataset_render, which
        # mcp/server.py imports — so the reverse would be a circular import.
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
            reference="mydb.orders", entity_type="dataset",
            format="json", compact=True,
        )
        p = json.loads(out)
        assert p["canonical_id"] == "mydb.orders"
        assert p["entity_type"] == "dataset"
        assert p["name"] == "orders"
        assert p["description"] == "One row per placed order."
        assert p["field_names"] == ["id", "amount", "customer_id", "big"]
        assert p["metric_names"] == ["aov"]
        assert p["aggregation_names"] == ["big"]
        assert p["relationships_to"] == ["customers"]
        assert "text" not in p
        assert "warnings" in p

    async def test_model_compact_json_empty_lists_present(
        self, svc: InspectService
    ) -> None:
        # ``customers`` has no metrics / aggregations / relationships — the JSON keys
        # are still present as empty lists (stable shape).
        out = await svc.inspect(
            reference="mydb.customers", entity_type="dataset",
            format="json", compact=True,
        )
        p = json.loads(out)
        assert p["canonical_id"] == "mydb.customers"
        assert p["field_names"] == ["id", "region"]
        assert p["metric_names"] == []
        assert p["aggregation_names"] == []
        assert p["relationships_to"] == []
        assert "text" not in p

    async def test_model_compact_markdown_empty_sections_render_none(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.customers", entity_type="dataset", compact=True,
        )
        assert "Fields: " in out
        assert "region" in out
        assert "Metrics: _(none)_" in out
        assert "Aggregations: _(none)_" in out
        assert "Relationships to: _(none)_" in out

    async def test_model_compact_descriptions_max_chars(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset", compact=True,
            descriptions_max_chars=4,
        )
        assert "One row per placed order." not in out
        assert "Fields: " in out

    async def test_model_compact_json_truncates_description(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset",
            format="json", compact=True, descriptions_max_chars=4,
        )
        p = json.loads(out)
        assert p["description"] != "One row per placed order."
        assert p["description"].startswith("One ")
        # Structure intact despite truncation.
        assert p["field_names"] == ["id", "amount", "customer_id", "big"]


class TestDatasourceCompactFalseSkeletons:
    async def test_markdown_per_model_skeletons(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource", compact=False,
        )
        assert "Datasource: mydb" in out
        assert "Primary analytics warehouse." in out
        # Per-dataset heading + skeleton body for each visible dataset.
        assert "## `customers`" in out
        assert "## `orders`" in out
        assert "region" in out            # customers field name
        assert "Metrics: aov" in out     # orders metric
        assert "Relationships to: customers" in out
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
        names = [m["name"] for m in p["datasets"]]
        assert names == ["customers", "orders", "time_spine"]   # sorted by name
        orders = next(m for m in p["datasets"] if m["name"] == "orders")
        assert orders["canonical_id"] == "mydb.orders"
        assert orders["field_names"] == ["id", "amount", "customer_id", "big"]
        assert orders["metric_names"] == ["aov"]
        assert orders["aggregation_names"] == ["big"]
        assert orders["relationships_to"] == ["customers"]

    async def test_hidden_model_excluded(self, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="hiddends", sql_table="hiddends", data_source="mydb",
            hidden=True,
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
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
        assert all(m["name"] != "hiddends" for m in json.loads(outj)["datasets"])

    async def test_no_visible_models_empty_list(
        self, storage: YAMLStorage
    ) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="emptyds", type="sqlite", database=":memory:",
            description="Empty one.",
        ))
        await storage.save_dataset(SlayerDataset(
            name="onlyhidden", sql_table="onlyhidden", data_source="emptyds",
            hidden=True,
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
        ))
        svc = InspectService(storage=storage)
        outj = await svc.inspect(
            reference="emptyds", entity_type="datasource",
            format="json", compact=False,
        )
        assert [m["name"] for m in json.loads(outj)["datasets"]] == ["time_spine"]
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
        assert "One row per placed order." not in out       # dataset description
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
        orders = next(m for m in p["datasets"] if m["name"] == "orders")
        assert orders["description"] != "One row per placed order."
        assert orders["description"].startswith("One ")

    async def test_warnings_preserved_in_json(
        self, svc: InspectService
    ) -> None:
        # show_sql is a dataset-only arg → warns for the datasource kind, and the
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
        assert "Fields:" not in out

    async def test_json_omits_text_and_models(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb", entity_type="datasource",
            format="json", compact=True,
        )
        p = json.loads(out)
        assert "text" not in p
        assert "datasets" not in p
        assert p["description"] == "Primary analytics warehouse."


class TestCompactTextOmittedAllKinds:
    @pytest.mark.parametrize(
        "reference,entity_type",
        [
            ("mydb.orders.amount", "field"),
        ],
    )
    async def test_leaf_compact_json_omits_text(
        self, svc: InspectService, reference: str, entity_type: str
    ) -> None:
        # Fields stay description-only in compact (their text can carry
        # sampled values / DB reads).
        out = await svc.inspect(
            reference=reference, entity_type=entity_type,
            format="json", compact=True,
        )
        assert "text" not in json.loads(out)

    @pytest.mark.parametrize(
        "reference,entity_type",
        [
            ("mydb.orders.aov", "metric"),
            ("mydb.orders.big", "aggregation"),
        ],
    )
    async def test_leaf_compact_json_keeps_text_for_measure_and_aggregation(
        self, svc: InspectService, reference: str, entity_type: str
    ) -> None:
        # Expression / params / type are essential and carry no sample data, so
        # the compact leaf render includes them for metrics/aggregations.
        out = await svc.inspect(
            reference=reference, entity_type=entity_type,
            format="json", compact=True,
        )
        p = json.loads(out)
        assert "text" in p
        assert p["text"]

    @pytest.mark.parametrize(
        "reference,entity_type",
        [
            ("mydb.orders.amount", "field"),
            ("mydb.orders.aov", "metric"),
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
        assert "text" in p
        assert p["text"]

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
        assert "text" in p
        assert "Full body text here." in p["text"]


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
            reference="mydb.orders", entity_type="dataset", compact=True,
        )
        assert "Fields: " in out
        outj = await svc.inspect(
            reference="mydb.orders", entity_type="dataset",
            format="json", compact=True,
        )
        assert json.loads(outj)["name"] == "orders"

    async def test_model_compact_does_not_call_full_renderer(
        self, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Direct proof the compact path short-circuits BEFORE the DB-hitting
        # full renderer: monkeypatch render_model_inspection to explode.
        async def _boom(*args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError(
                "render_model_inspection must not run for compact=True"
            )

        monkeypatch.setattr(service_mod, "render_dataset_inspection", _boom)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.orders", entity_type="dataset", compact=True,
        )
        assert "Fields: " in out
        # And the full path STILL uses it (guards against a stale patch /
        # wrong-symbol monkeypatch giving a false pass above).
        with pytest.raises(AssertionError, match="must not run"):
            await svc.inspect(
                reference="mydb.orders", entity_type="dataset", compact=False,
            )


# ---------------------------------------------------------------------------
# Ambiguous bare dataset name
# ---------------------------------------------------------------------------


class TestAmbiguousModelName:
    """A bare dataset name present in 2+ datasources with no priority winner
    makes ``resolve_entity`` raise ``AmbiguousDatasetError`` (a SlayerError
    sibling, NOT an EntityResolutionError). inspect must surface the message,
    not let it escape as an uncaught exception (which would 500 on REST)."""

    async def _seed_ambiguous(self, storage: YAMLStorage) -> None:
        # ``mydb`` already has ``orders``; add a second datasource that also
        # has ``orders``, then clear the priority so no winner resolves.
        await storage.save_datasource(DatasourceConfig(
            name="otherdb", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="otherdb",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
        ))
        await storage.set_datasource_priority([])

    @pytest.mark.parametrize(
        "entity_type", ["dataset", "datasource", "field"],
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
        assert "mydb" in out
        assert "otherdb" in out

    async def test_ambiguous_bare_model_json_does_not_crash(
        self, storage: YAMLStorage
    ) -> None:
        await self._seed_ambiguous(storage)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="orders", entity_type="dataset",
            format="json", compact=True,
        )
        # The ambiguity message is returned as a plain string (not JSON, not a
        # raised exception) — the surface stays alive.
        assert "multiple datasources" in out.lower()

    async def test_datasource_prefix_not_blocked_by_ambiguous_model(
        self, storage: YAMLStorage
    ) -> None:
        # A name that is BOTH a known datasource AND an ambiguous dataset (same
        # name in >=2 datasources, no priority winner). resolve_entity's
        # Case-D collision probe must not let that ambiguity abort the valid
        # datasource interpretation (regression from the AmbiguousDatasetError
        # handling — Codex re-review).
        await storage.save_datasource(DatasourceConfig(
            name="shared", type="sqlite", database=":memory:",
            description="The shared datasource.",
        ))
        await storage.save_datasource(DatasourceConfig(
            name="otherdb", type="sqlite", database=":memory:",
        ))
        await storage.save_dataset(SlayerDataset(
            name="shared", sql_table="shared", data_source="mydb",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
        ))
        await storage.save_dataset(SlayerDataset(
            name="shared", sql_table="shared", data_source="otherdb",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True)],
        ))
        await storage.set_datasource_priority([])  # no dataset-leg winner
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="shared", entity_type="datasource", compact=False,
        )
        assert "Datasource: shared" in out
        assert "multiple datasources" not in out.lower()
