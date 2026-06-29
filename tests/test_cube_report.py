"""Tests for the Cube conversion report shapes (slayer/cube/report.py).

DEV-1608 §10.
"""

import json

from slayer.cube.report import (
    CubeConversionIssue,
    CubeConversionReport,
    CubeConversionResult,
    CubeIssueCategory,
)


def test_issue_context_prefers_cube_then_view_then_member():
    assert CubeConversionIssue(
        category=CubeIssueCategory.NO_SOURCE, message="x", cube="orders"
    ).context == "orders"
    assert CubeConversionIssue(
        category=CubeIssueCategory.DISCONNECTED_VIEW, message="x", view="ov"
    ).context == "ov"
    assert CubeConversionIssue(
        category=CubeIssueCategory.COMPLEX_MEASURE, message="x", member="aov"
    ).context == "aov"
    assert CubeConversionIssue(
        category=CubeIssueCategory.PARSE_ERROR, message="x"
    ).context == "general"


def test_report_filters_by_category_and_severity():
    report = CubeConversionReport()
    report.add(CubeConversionIssue(
        category=CubeIssueCategory.NO_SOURCE, severity="error", message="a"))
    report.add(CubeConversionIssue(
        category=CubeIssueCategory.LOSSY_MAPPING, severity="info", message="b"))
    report.add(CubeConversionIssue(
        category=CubeIssueCategory.LOSSY_MAPPING, severity="info", message="c"))

    assert len(report.by_category(CubeIssueCategory.LOSSY_MAPPING)) == 2
    assert len(report.by_severity("info")) == 2
    assert len(report.by_severity("error")) == 1
    assert report.has_errors


def test_report_has_no_errors_when_all_info_or_warning():
    report = CubeConversionReport()
    report.add(CubeConversionIssue(
        category=CubeIssueCategory.SEGMENT_AS_COLUMN, severity="info", message="x"))
    assert not report.has_errors


def test_result_json_round_trips():
    result = CubeConversionResult(
        report=CubeConversionReport(
            issues=[CubeConversionIssue(
                category=CubeIssueCategory.UNMAPPED_INFRA,
                severity="warning",
                cube="orders",
                message="pre_aggregations dropped",
                raw="name: main",
            )],
            model_count=3,
            hidden_count=1,
            view_count=1,
        )
    )
    blob = result.model_dump_json()
    parsed = json.loads(blob)
    assert parsed["report"]["model_count"] == 3
    assert parsed["report"]["issues"][0]["category"] == "unmapped_infra"

    # Re-validate from the JSON to confirm the categories survive the round-trip.
    restored = CubeConversionResult.model_validate_json(blob)
    assert restored.report.issues[0].category == CubeIssueCategory.UNMAPPED_INFRA
    assert restored.report.hidden_count == 1
    # `raw` fragment is preserved.
    assert restored.report.issues[0].raw == "name: main"


def test_converter_derives_report_counts():
    """The converter must populate model/hidden/view counts on the report."""
    from slayer.cube.converter import CubeToSlayerConverter
    from slayer.cube.models import CubeCube, CubeDimension, CubeProject, CubeView, CubeViewCubeRef

    project = CubeProject(
        cubes=[
            CubeCube(name="orders", sql_table="public.orders",
                     dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
            CubeCube(name="internal", sql_table="public.internal", public=False,
                     dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")]),
        ],
        views=[CubeView(name="ov", cubes=[
            CubeViewCubeRef(join_path="orders", includes=["id"])])],
    )
    result = CubeToSlayerConverter(project=project, data_source="ds").convert()
    assert result.report.model_count == len(result.models)
    assert result.report.hidden_count == 1
    assert result.report.view_count == 1
