"""Pydantic v2 models for parsed Cube (Cube.js / Cube.dev) YAML objects.

Lightweight representations of Cube's ``cubes:`` and ``views:`` YAML. We don't
depend on any Cube runtime — these shapes are populated directly from the YAML
by ``slayer.cube.parser`` and consumed by ``slayer.cube.converter``.

Unknown keys are tolerated (Cube's schema evolves); Pydantic's default
``extra="ignore"`` drops them, except for the fields explicitly captured below
so the converter can either map them or stash the raw value under
``meta.cube_unmapped.<feature>`` (see the spec on DEV-1608, §7).
"""

from typing import Any

from pydantic import BaseModel, Field


class CubeMeasureFilter(BaseModel):
    """One entry of a Cube measure ``filters:`` list — a conditional-aggregation
    predicate (``{sql: "..."}``)."""

    sql: str


class CubeMeasure(BaseModel):
    name: str
    type: str  # count, count_distinct, count_distinct_approx, sum, avg, min, max,
    #            number, string, time, boolean, number_agg (Tesseract)
    sql: str | None = None
    title: str | None = None
    description: str | None = None
    public: bool = True
    meta: dict[str, Any] | None = None
    format: Any | None = None  # str ("percent"/"currency"/…) or object
    filters: list[CubeMeasureFilter] = Field(default_factory=list)
    drill_members: list[str] = Field(default_factory=list)
    rolling_window: dict[str, Any] | None = None
    # Multi-stage / Tesseract-leaning fields (mostly Stage-2 — see §9).
    multi_stage: bool = False
    time_shift: Any | None = None
    grain: Any | None = None
    filter: Any | None = None  # Tesseract grain filter (exclude/keep_only/mode)
    case: Any | None = None  # Tesseract conditional measure (switch-keyed)


class CubeDimension(BaseModel):
    name: str
    sql: str | None = None
    type: str = "string"  # string, number, boolean, time, geo, switch (Tesseract)
    title: str | None = None
    description: str | None = None
    public: bool = True
    meta: dict[str, Any] | None = None
    format: Any | None = None
    primary_key: bool = False
    sub_query: bool = False
    case: dict[str, Any] | None = None  # CASE-WHEN dimension (Stage 1)
    granularities: list[dict[str, Any]] | None = None  # custom time granularities
    latitude: dict[str, Any] | None = None  # geo
    longitude: dict[str, Any] | None = None  # geo
    links: Any | None = None  # presentation
    order: str | None = None  # presentation


class CubeJoin(BaseModel):
    name: str  # target cube name
    relationship: str = "many_to_one"
    sql: str  # ON clause, e.g. "{CUBE}.customer_id = {customers.id}"


class CubeSegment(BaseModel):
    name: str
    sql: str
    title: str | None = None
    description: str | None = None
    public: bool = True
    meta: dict[str, Any] | None = None


class CubeCube(BaseModel):
    name: str
    sql_table: str | None = None
    sql: str | None = None
    sql_alias: str | None = None
    extends: str | None = None
    data_source: str | None = None
    title: str | None = None
    description: str | None = None
    public: bool = True
    meta: dict[str, Any] | None = None
    refresh_key: dict[str, Any] | None = None
    calendar: bool | None = None
    measures: list[CubeMeasure] = Field(default_factory=list)
    dimensions: list[CubeDimension] = Field(default_factory=list)
    joins: list[CubeJoin] = Field(default_factory=list)
    segments: list[CubeSegment] = Field(default_factory=list)
    hierarchies: list[dict[str, Any]] | None = None
    pre_aggregations: list[dict[str, Any]] | None = None
    access_policy: list[dict[str, Any]] | None = None


class CubeViewCubeRef(BaseModel):
    """One entry of a view's ``cubes:`` list — a cube reached via ``join_path``
    contributing a set of members to the view.

    ``includes`` is a list of member names, the string ``"*"``, or Cube's
    per-member override object form (``[{name, alias, title, format, meta}, …]``).
    The converter extracts the member names and reports per-member overrides as
    unsupported (Stage 1) rather than silently dropping them.
    """

    join_path: str
    includes: list[str | dict[str, Any]] | str | None = None
    excludes: list[str] = Field(default_factory=list)
    prefix: bool = False
    alias: str | None = None  # renames the cube for member prefixing


class CubeView(BaseModel):
    name: str
    cubes: list[CubeViewCubeRef] = Field(default_factory=list)
    extends: str | None = None
    title: str | None = None
    description: str | None = None
    public: bool = True
    meta: dict[str, Any] | None = None
    folders: list[dict[str, Any]] | None = None
    default_filters: list[dict[str, Any]] | None = None
    access_policy: list[dict[str, Any]] | None = None


class CubeProject(BaseModel):
    """Aggregated result of parsing all YAML files in a Cube project."""

    cubes: list[CubeCube] = Field(default_factory=list)
    views: list[CubeView] = Field(default_factory=list)
