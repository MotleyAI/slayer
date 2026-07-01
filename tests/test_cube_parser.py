"""Tests for the Cube project parser (slayer/cube/parser.py).

DEV-1608 §2. Walk a directory, parse cubes:/views:, skip + report Jinja
(file-level and member-level) and malformed files without aborting the run.
"""

import os
import textwrap

from slayer.cube.parser import parse_cube_project
from slayer.cube.report import CubeIssueCategory

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "cube_project")


def test_parses_cubes_and_views():
    project, _issues = parse_cube_project(FIXTURE)
    cube_names = {c.name for c in project.cubes}
    view_names = {v.name for v in project.views}
    assert {"orders", "customers", "base_events", "clicks"} <= cube_names
    assert "orders_overview" in view_names


def test_orders_cube_fields_populated():
    project, _ = parse_cube_project(FIXTURE)
    orders = next(c for c in project.cubes if c.name == "orders")
    assert orders.sql_table == "public.orders"
    assert {m.name for m in orders.measures} >= {"count", "total_revenue", "completed_revenue"}
    assert orders.joins[0].name == "customers"
    assert orders.pre_aggregations  # captured for unmapped-infra stashing


def test_file_level_jinja_is_skipped_and_reported():
    _project, issues = parse_cube_project(FIXTURE)
    assert any(i.category == CubeIssueCategory.REQUIRES_TEMPLATING for i in issues)


def test_member_level_jinja_skips_member_keeps_cube():
    project, _issues = parse_cube_project(FIXTURE)
    tenant = next((c for c in project.cubes if c.name == "tenant_scoped"), None)
    assert tenant is not None
    dim_names = {d.name for d in tenant.dimensions}
    assert "id" in dim_names          # plain member kept
    assert "tenant" not in dim_names  # templated member dropped


def test_malformed_cube_is_reported_not_fatal():
    project, issues = parse_cube_project(FIXTURE)
    # The nameless cube in malformed.yml is dropped...
    assert all(c.name != "orphan" for c in project.cubes)
    # ...via a parse_error, and the rest of the project still parsed.
    assert any(i.category == CubeIssueCategory.PARSE_ERROR for i in issues)
    assert any(c.name == "orders" for c in project.cubes)


def test_single_object_cubes_block_tolerated(tmp_path):
    (tmp_path / "single.yml").write_text(textwrap.dedent("""
        cubes:
          name: solo
          sql_table: public.solo
          dimensions:
            - name: id
              sql: "{CUBE}.id"
              type: number
    """))
    project, _ = parse_cube_project(str(tmp_path))
    assert any(c.name == "solo" for c in project.cubes)


def test_single_object_views_block_tolerated(tmp_path):
    (tmp_path / "v.yml").write_text(textwrap.dedent("""
        views:
          name: solo_view
          cubes:
            - join_path: orders
              includes: ["status"]
    """))
    project, _ = parse_cube_project(str(tmp_path))
    assert any(v.name == "solo_view" for v in project.views)


def test_member_level_jinja_in_measure_sql_skips_measure(tmp_path):
    (tmp_path / "m.yml").write_text(textwrap.dedent("""
        cubes:
          - name: orders
            sql_table: public.orders
            measures:
              - name: plain
                type: count
              - name: templated
                type: sum
                sql: "{{ user_attr('scale') }} * {CUBE}.amount"
            dimensions:
              - name: id
                sql: "{CUBE}.id"
                type: number
    """))
    project, issues = parse_cube_project(str(tmp_path))
    orders = next(c for c in project.cubes if c.name == "orders")
    names = {m.name for m in orders.measures}
    assert "plain" in names
    assert "templated" not in names
    assert any(i.category == CubeIssueCategory.REQUIRES_TEMPLATING for i in issues)


def test_hidden_dirs_and_target_skipped(tmp_path):
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "x.yml").write_text(
        "cubes:\n  - name: ghost\n    sql_table: public.ghost\n")
    project, _ = parse_cube_project(str(tmp_path))
    assert all(c.name != "ghost" for c in project.cubes)


def test_unreadable_file_is_reported_not_fatal(tmp_path):
    # A broken symlink with a .yml extension raises OSError on open — it must be
    # reported like a malformed file, not abort the whole import.
    (tmp_path / "broken.yml").symlink_to(tmp_path / "nonexistent.yml")
    (tmp_path / "good.yml").write_text(
        "cubes:\n  - name: ok\n    sql_table: public.ok\n")
    project, issues = parse_cube_project(str(tmp_path))
    assert any(c.name == "ok" for c in project.cubes)
    assert any(i.category == CubeIssueCategory.PARSE_ERROR for i in issues)


def test_empty_dir_yields_empty_project(tmp_path):
    project, _ = parse_cube_project(str(tmp_path))
    assert project.cubes == []
    assert project.views == []
