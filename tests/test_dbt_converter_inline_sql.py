"""Tests for inline-SQL ingestion in DbtToSlayerConverter.

Covers the behaviour added alongside slayer.dbt.sql_resolver: when a
semantic_model points at a regular dbt model with raw SQL on disk, the
resulting SlayerModel carries sql= (the resolved SELECT) instead of
sql_table=. Semantic models pointing at source tables (no .sql file)
keep sql_table= exactly as before.
"""

import textwrap
from pathlib import Path

from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtEntity, DbtProject, DbtRegularModel, DbtSemanticModel
from slayer.dbt.parser import parse_dbt_project


def _semantic_model(name: str, ref_name: str) -> DbtSemanticModel:
    return DbtSemanticModel(
        name=name,
        model=ref_name,
        entities=[DbtEntity(name="id", type="primary")],
        dimensions=[],
        measures=[],
    )


class TestSemanticOverSource:
    def test_preserves_sql_table_for_sources(self) -> None:
        """When the referenced name has no SQL body, fall back to sql_table."""
        project = DbtProject(
            semantic_models=[_semantic_model("orders", "orders")],
            metrics=[],
            regular_models=[],
        )
        result = DbtToSlayerConverter(project=project, data_source="db").convert()
        m = next(m for m in result.models if m.name == "orders")
        assert m.sql_table == "orders"
        assert m.sql is None


class TestSemanticOverRegularModel:
    def test_inlines_sql_body(self) -> None:
        project = DbtProject(
            semantic_models=[_semantic_model("orders", "orders")],
            metrics=[],
            regular_models=[
                DbtRegularModel(name="orders", raw_code="select id from raw_orders"),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="db").convert()
        m = next(m for m in result.models if m.name == "orders")
        assert m.sql_table is None
        assert m.sql == "select id from raw_orders"

    def test_resolves_nested_refs(self) -> None:
        bridge_sql = textwrap.dedent("""\
            select c.id
            from {{ ref('claim') }} c
            inner join {{ ref('policy') }} p on c.policy_id = p.id
        """)
        project = DbtProject(
            semantic_models=[_semantic_model("claim_policy_bridge", "claim_policy_bridge")],
            metrics=[],
            regular_models=[
                DbtRegularModel(name="claim_policy_bridge", raw_code=bridge_sql),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="db").convert()
        m = next(m for m in result.models if m.name == "claim_policy_bridge")
        assert m.sql_table is None
        assert m.sql is not None
        # refs to sources become bare names
        assert "from claim" in m.sql
        assert "join policy" in m.sql
        # No Jinja leftovers
        assert "{{" not in m.sql

    def test_inlines_transitive_regular_refs(self) -> None:
        # bridge refs 'claim' which is itself a regular model wrapping raw 'Claim' source
        claim_sql = "select * from {{ ref('Claim') }}"
        bridge_sql = "select * from {{ ref('claim') }}"
        project = DbtProject(
            semantic_models=[_semantic_model("bridge", "bridge")],
            metrics=[],
            regular_models=[
                DbtRegularModel(name="bridge", raw_code=bridge_sql),
                DbtRegularModel(name="claim", raw_code=claim_sql),
            ],
        )
        result = DbtToSlayerConverter(project=project, data_source="db").convert()
        m = next(m for m in result.models if m.name == "bridge")
        assert m.sql is not None
        assert "claim_ref_sub" in m.sql
        assert "from Claim" in m.sql


class TestEndToEndFromDisk:
    def test_parse_then_convert_with_sql_files(self, tmp_path: Path) -> None:
        """Write YAML + .sql files, parse, convert, check inlining."""
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        (models_dir / "orders.yaml").write_text(textwrap.dedent("""\
            semantic_models:
              - name: orders
                model: ref('orders')
                entities:
                  - name: order_id
                    type: primary
                    expr: id
                dimensions: []
                measures: []
        """))

        (models_dir / "orders.sql").write_text(textwrap.dedent("""\
            {{ config(materialized='table') }}
            select id, customer_id, amount
            from {{ source('raw', 'orders') }}
        """))

        project = parse_dbt_project(str(tmp_path))
        result = DbtToSlayerConverter(project=project, data_source="db").convert()

        m = next(m for m in result.models if m.name == "orders")
        assert m.sql_table is None
        assert m.sql is not None
        assert "config" not in m.sql
        assert "raw.orders" in m.sql
