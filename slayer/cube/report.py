"""Structured report for Cube → SLayer conversion (DEV-1608, §10).

Everything the converter cannot map cleanly is recorded as a
``CubeConversionIssue`` rather than silently dropped or raised. The full
``CubeConversionResult`` (models + report) is returned by the converter and the
report is also written to JSON by the CLI.
"""

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from slayer.core.models import SlayerModel


class CubeIssueCategory(str, Enum):
    REQUIRES_TEMPLATING = "requires_templating"
    PARSE_ERROR = "parse_error"
    COMPLEX_SQL = "complex_sql"
    COMPLEX_MEASURE = "complex_measure"
    LOSSY_MAPPING = "lossy_mapping"
    UNSUPPORTED_JOIN = "unsupported_join"
    UNSUPPORTED_ROLLING_WINDOW = "unsupported_rolling_window"
    UNSUPPORTED_FORMAT = "unsupported_format"
    UNSUPPORTED_DEFAULT_FILTER = "unsupported_default_filter"
    SEGMENT_AS_COLUMN = "segment_as_column"
    UNMAPPED_INFRA = "unmapped_infra"
    GEO_UNMAPPED = "geo_unmapped"
    SUBQUERY_UNMAPPED = "subquery_unmapped"
    GRANULARITY_UNMAPPED = "granularity_unmapped"
    DISCONNECTED_VIEW = "disconnected_view"
    AMBIGUOUS_VIEW_ROOT = "ambiguous_view_root"
    VIEW_FANOUT_RISK = "view_fanout_risk"
    FOLDERS_UNMAPPED = "folders_unmapped"
    EXTENDS_CYCLE = "extends_cycle"
    NO_SOURCE = "no_source"
    DEFERRED_STAGE2 = "deferred_stage2"


Severity = Literal["info", "warning", "error"]


class CubeConversionIssue(BaseModel):
    category: CubeIssueCategory
    severity: Severity = "warning"
    cube: str | None = None
    view: str | None = None
    member: str | None = None
    message: str
    raw: str | None = None  # raw Cube fragment when useful

    @property
    def context(self) -> str:
        return self.cube or self.view or self.member or "general"


class CubeConversionReport(BaseModel):
    issues: list[CubeConversionIssue] = Field(default_factory=list)
    model_count: int = 0
    hidden_count: int = 0
    view_count: int = 0

    def add(self, issue: CubeConversionIssue) -> None:
        self.issues.append(issue)

    def by_category(self, category: CubeIssueCategory) -> list[CubeConversionIssue]:
        return [i for i in self.issues if i.category == category]

    def by_severity(self, severity: Severity) -> list[CubeConversionIssue]:
        return [i for i in self.issues if i.severity == severity]

    @property
    def has_errors(self) -> bool:
        return any(i.severity == "error" for i in self.issues)


class CubeConversionResult(BaseModel):
    """Return value of ``CubeToSlayerConverter.convert``."""

    models: list[SlayerModel] = Field(default_factory=list)
    report: CubeConversionReport = Field(default_factory=CubeConversionReport)
