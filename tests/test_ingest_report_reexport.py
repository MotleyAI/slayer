"""The conversion-report classes move to a neutral module (DEV-1643).

``ConversionResult`` / ``ConversionWarning`` are extracted from
``slayer.dbt.converter`` into ``slayer.ingest_report`` so the OSI importer can
reuse them without importing the dbt package. The dbt module re-exports them,
so existing imports keep working and it's the *same* class object.
"""

from slayer.dbt.converter import ConversionResult as DbtConversionResult
from slayer.dbt.converter import ConversionWarning as DbtConversionWarning
from slayer.ingest_report import ConversionResult, ConversionWarning


def test_reexport_is_same_class() -> None:
    assert DbtConversionResult is ConversionResult
    assert DbtConversionWarning is ConversionWarning


def test_render_report_and_tally() -> None:
    result = ConversionResult(
        models=[],
        unconverted_metrics=[
            ConversionWarning(
                metric_name="m1",
                message="cannot convert",
                category="expr",
                severity="unconverted",
            )
        ],
        warnings=[
            ConversionWarning(
                model_name="mdl",
                message="dropped a thing",
                category="shape",
                severity="dropped",
            )
        ],
    )
    report = result.render_report()
    assert "m1" in report and "cannot convert" in report
    assert result.tally() == (1, 1)  # (unconverted, dropped)
