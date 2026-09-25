"""ResolvedSourceBundle: eagerly resolved query inputs (P11).
The orchestrator builds this once at execute start; the binder reads it purely.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    SlayerModel,
    _check_column_measure_namespace,
    _check_join_keys,
)
from slayer.core.query import ModelExtension, SlayerQuery, SourceSpec

from slayer.core.scope import ModelScope, StageDisplay, StageSchema

__all__ = [
    "resolve_scope",
    "ResolvedSourceBundle",
    "apply_extension_overlay",
    "as_extension_over_nonsibling",
    "follow_sibling_chain",
    "model_from_stage_schema",
    "source_name_if_sibling",
    "spec_adds_measures",
    "stage_bundle_with_siblings",
]


class ResolvedSourceBundle(BaseModel):
    """Eagerly resolved inputs to one query execution (P11)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_model: Optional[SlayerModel] = None
    referenced_models: List[SlayerModel] = Field(default_factory=list)
    inline_extensions: List[ModelExtension] = Field(default_factory=list)
    named_queries: Dict[str, SlayerQuery] = Field(default_factory=dict)
    # Per-named-stage source model (sibling-sourced stages omitted).
    stage_source_models: Dict[str, SlayerModel] = Field(default_factory=dict)
    query_variables: Dict[str, Any] = Field(default_factory=dict)
    datasource_hint: Optional[str] = None
    dialect: str  # sqlglot dialect the query renders in
    # Minted stage identity → its user-facing spelling.
    stage_displays: Dict[str, StageDisplay] = Field(default_factory=dict)
    # Stored query-backed models the statement may splice (stored form, joins dropped).
    query_backed: Dict[str, SlayerModel] = Field(default_factory=dict)
    # Query-backed models whose stages are being planned, outermost first.
    splice_chain: Tuple[str, ...] = ()
    runtime_variables: Dict[str, Any] = Field(default_factory=dict)
    dry_run_placeholders: bool = False

    def get_referenced_model(self, name: str) -> Optional[SlayerModel]:
        """Linear lookup by name (list is small, O(n) scan is fine)."""
        for m in self.referenced_models:
            if m.name == name:
                return m
        return None

    def rerooted(self, new_source: SlayerModel) -> "ResolvedSourceBundle":
        """Re-root at ``new_source``, keeping the former source in the universe
        (a reverse hop can target it — the map must not shrink)."""
        refs = self.referenced_models
        old = self.source_model
        if old is not None and self.get_referenced_model(old.name) is None:
            refs = [*refs, old]
        return self.model_copy(
            update={"source_model": new_source, "referenced_models": refs},
        )

    @property
    def models_by_name(self) -> Dict[str, SlayerModel]:
        """The host-inclusive model map every walker and scanner consumes: the
        source model first (so a source→host reverse hop resolves deterministically,
        not incidentally), then the referenced models, de-duplicated by name with
        the source winning. Production bundles already list the source among
        ``referenced_models``; a hand-built bundle that omits it still exposes it."""
        out: Dict[str, SlayerModel] = {}
        if self.source_model is not None:
            out[self.source_model.name] = self.source_model
        for m in self.referenced_models:
            out.setdefault(m.name, m)
        return out


def apply_extension_overlay(
    base: SlayerModel, ext: ModelExtension
) -> SlayerModel:
    """Extend ``base`` with the extra columns / measures / joins of ``ext``."""
    # Query-backed base defers its measure overlay to post-expansion; overlaying
    # now would build the source_queries+measures combo the validator rejects.
    overlay_measures = list(base.measures) if base.source_queries else (
        list(base.measures) + list(ext.measures or [])
    )
    merged = base.model_copy(
        update={
            "columns": list(base.columns) + list(ext.columns or []),
            "measures": overlay_measures,
            "joins": list(base.joins) + list(ext.joins or []),
        }
    )
    # model_copy runs no validators; re-run the namespace and join-key checks
    # (the latter deferred, like measures, until a query-backed base expands).
    if not base.awaits_columns:
        _check_join_keys(model_name=merged.name, columns=merged.columns, joins=merged.joins)
    if not merged.source_queries:
        _check_column_measure_namespace(
            model_name=merged.name,
            columns=merged.columns,
            measures=merged.measures,
        )
    return merged


def source_name_if_sibling(
    spec: SourceSpec | None, sibling_names: "set[str] | Dict[str, Any]"
) -> Optional[str]:
    """The sibling stage a bare-name or extension spec reads from, else ``None``."""
    if isinstance(spec, str):
        return spec if spec in sibling_names else None
    if isinstance(spec, ModelExtension):
        return spec.source_name if spec.source_name in sibling_names else None
    return None


def spec_adds_measures(spec: SourceSpec | None) -> bool:
    """True when a ``source_model`` spec is a ``ModelExtension`` carrying measures."""
    return isinstance(spec, ModelExtension) and bool(spec.measures)


def follow_sibling_chain(
    spec: SourceSpec | None, named_queries: Dict[str, SlayerQuery]
) -> SourceSpec | None:
    """Resolve a sibling-pointing ``source_model`` to the real base spec (cycle raises ``ValueError``)."""
    seen: List[str] = []
    while True:
        sib = source_name_if_sibling(spec, named_queries)
        if sib is None:
            return spec
        if sib in seen:
            chain = " -> ".join([*seen, sib])
            raise ValueError(
                f"Circular reference detected in source_queries DAG: {chain}"
            )
        seen.append(sib)
        spec = named_queries[sib].source_model


def as_extension_over_nonsibling(
    spec: SourceSpec | None, sibling_names: "set[str]"
) -> Optional[ModelExtension]:
    """``spec`` itself when it is an extension over a NON-sibling base, else ``None``."""
    if isinstance(spec, ModelExtension) and spec.source_name not in sibling_names:
        return spec
    return None


def model_from_stage_schema(
    *,
    name: str,
    schema: "StageSchema",
    data_source: str,
    sql: Optional[str] = None,
    column_sql: Optional[Dict[str, str]] = None,
    default_time_dimension: Optional[str] = None,
) -> SlayerModel:
    """The model a stage's output is read as: over its CTE (``sql=None``) or over ``sql``.

    A single-column grain is stamped ``unique``; a composite grain ``primary_key`` on each member.
    """
    grain = schema.grain or []
    composite = len(grain) > 1
    column_sql = column_sql or {}
    model = SlayerModel(
        name="_stage",
        data_source=data_source,
        sql_table="_stage" if sql is None else None,
        sql=sql,
        columns=[
            Column(
                name=c.name,
                sql=column_sql.get(c.name),
                type=c.type or DataType.DOUBLE,
                granularity=c.granularity,
                label=c.label,
                format=c.format,
                description=c.description,
                primary_key=composite and c.name in grain,
                unique=not composite and c.name in grain,
            ).with_respellings(c.respellings)
            for c in schema.columns
        ],
    )
    # A minted stage identity carries the reserved prefix the name validator rejects.
    return model.model_copy(update={
        "name": name, "sql_table": name if sql is None else None,
        "default_time_dimension": default_time_dimension or schema.default_time_dimension,
    }).with_spelling(schema.display_name if schema.display_name != name else None)


def stage_bundle_with_siblings(
    *,
    bundle: ResolvedSourceBundle,
    source_model: SlayerModel,
    sibling_schemas: Dict[str, "StageSchema"],
    data_source: str,
) -> ResolvedSourceBundle:
    """Per-stage bundle: ``source_model`` is the stage's own host; synthetic
    sibling models (one per emitted ``StageSchema``) are threaded into
    ``referenced_models`` so a join / cross-model ref to a sibling resolves.

    Order: host first, then synthetic siblings, then the original bundle's
    referenced models minus the host's own entry. A sibling identity meeting a
    referenced model is an invariant violation (identities are minted unique).
    """
    synths = [
        model_from_stage_schema(
            name=n, schema=s, data_source=data_source
        )
        for n, s in sibling_schemas.items()
    ]
    clashes = sorted({s.name for s in synths} & {m.name for m in bundle.referenced_models})
    if clashes:
        raise ValueError(
            f"Stage identities {clashes} collide with models in the source bundle; "
            f"stage identities must be minted apart from model names."
        )
    referenced = (
        [source_model]
        + synths
        + [m for m in bundle.referenced_models if m.name != source_model.name]
    )
    return bundle.model_copy(
        update={"source_model": source_model, "referenced_models": referenced}
    )




def resolve_scope(
    *,
    query,
    bundle: "ResolvedSourceBundle",
    stage_schemas: Optional[Dict[str, "StageSchema"]],
):
    """The scope a query binds against: its named sibling stage's schema, else the bundle's model."""
    source = query.source_model
    if isinstance(source, str) and source in (stage_schemas or {}):
        return (stage_schemas or {})[source]
    return ModelScope(source_model=bundle.source_model)
