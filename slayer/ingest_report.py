"""Shared conversion-report types for semantic-layer importers and the Ossie exporter.

``slayer.dbt.converter`` re-exports ``ConversionWarning`` / ``ConversionResult``
for back-compat, so the class objects are shared across both import paths.
"""

from collections import defaultdict
from typing import Literal

from pydantic import BaseModel, Field

from slayer.core.models import SlayerModel


class ConversionWarning(BaseModel):
    """A structured entry in a conversion report.

    ``category`` groups entries in ``render_report``; ``severity`` is one of
    ``"unconverted"`` (tried to convert, couldn't), ``"dropped"`` (intentional
    clean-fail of an inexpressible construct), or ``"info"`` (a caveat — the
    construct imports but has a runtime limitation). ``suggestion`` carries the
    documented workaround.
    """
    model_name: str | None = None
    metric_name: str | None = None
    message: str
    category: str = "general"
    severity: Literal["unconverted", "dropped", "info"] = "unconverted"
    suggestion: str | None = None


class ConversionReport(BaseModel):
    """A list of report entries, rendered grouped by category."""
    warnings: list[ConversionWarning] = Field(default_factory=list)

    @property
    def entries(self) -> list[ConversionWarning]:
        return list(self.warnings)

    def render_report(self) -> str:
        """Each category as a heading with a count; each entry with its entity, severity, reason and workaround."""
        entries = self.entries
        if not entries:
            return "No conversion issues."
        by_cat: dict[str, list[ConversionWarning]] = defaultdict(list)
        for e in entries:
            by_cat[e.category or "general"].append(e)
        lines: list[str] = []
        for cat in sorted(by_cat):
            items = by_cat[cat]
            lines.append(f"## {cat} ({len(items)})")
            for e in items:
                entity = e.metric_name or e.model_name or "general"
                lines.append(f"  - [{e.severity}] {entity}: {e.message}")
                if e.suggestion:
                    lines.append(f"      workaround: {e.suggestion}")
            lines.append("")
        return "\n".join(lines).rstrip()

    def tally(self) -> tuple[int, int]:
        """``(unconverted, dropped)`` counts by severity for the CLI summary."""
        entries = self.entries
        unconverted = sum(1 for e in entries if e.severity == "unconverted")
        dropped = sum(1 for e in entries if e.severity == "dropped")
        return unconverted, dropped


class ConversionResult(ConversionReport):
    """Result of converting a semantic-layer project into SLayer models."""
    models: list[SlayerModel] = Field(default_factory=list)
    unconverted_metrics: list[ConversionWarning] = Field(default_factory=list)

    @property
    def entries(self) -> list[ConversionWarning]:
        return [*self.unconverted_metrics, *self.warnings]
