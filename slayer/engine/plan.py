"""Planning orchestration: elaborate → compile.

``plan_query`` is the planning door — it elaborates (bind + type + typing
environment) and compiles the result; ``plan_stages`` orders and threads a
multi-stage DAG through it, splicing the stored query-backed models its stages read.
"""

from __future__ import annotations

from typing import Any, Dict, Hashable, List, Optional, Set, Tuple, Union

from pydantic import BaseModel, Field

from slayer.core.errors import QueryBackedCycleError
from slayer.core.join_walker import observe_traversals
from slayer.core.models import SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery, extract_placeholder_names
from slayer.core.scope import (
    ModelScope,
    StageDisplay,
    StageSchema,
    collect_stale_spellings,
    stale_spelling_stage,
)
from slayer.engine.compile import compile_query
from slayer.engine.elaborate import elaborate_query
from slayer.engine.normalization import normalize_query
from slayer.engine.stage_ordering import (
    localize_stages,
    stage_sibling_reads,
    topologically_order_stages,
)
from slayer.ir.planned import PlannedQuery
from slayer.ir.prebound import PreboundQuery
from slayer.ir.source_bundle import (
    ResolvedSourceBundle,
    apply_extension_overlay,
    follow_sibling_chain,
    model_from_stage_schema,
    source_name_if_sibling,
    stage_bundle_with_siblings,
)
from slayer.ir.variables import (
    apply_variables_to_query,
    model_placeholder_names,
    substitute_model_sql_surfaces,
)
from slayer.sql.dialects import get_dialect

__all__ = [
    "plan_query",
    "plan_stages",
]


def plan_query(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    prebound: Optional[PreboundQuery] = None,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Plan one user-authored query stage: elaborate, then compile."""
    elaborated = elaborate_query(
        query=query,
        bundle=bundle,
        scope=scope,
        stage_schemas=stage_schemas,
        prebound=prebound,
    )
    return compile_query(
        elaborated=elaborated,
        producer_registry=producer_registry,
    )


def _stage_scope_and_bundle(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    data_source: str,
    stage_model: Optional[SlayerModel],
) -> "Tuple[Union[ModelScope, StageSchema], ResolvedSourceBundle]":
    """Resolve one DAG stage's ``(scope, per-stage bundle)``; each stage binds against its OWN source, with sibling synthetic models threaded in."""
    src = query.source_model
    sibling_names = set(stage_schemas)
    sib = source_name_if_sibling(spec=src, sibling_names=sibling_names)

    # 1. ModelExtension OVER a sibling: overlay the extra columns onto a synthetic sibling model.
    if sib is not None and isinstance(src, ModelExtension):
        base = model_from_stage_schema(
            name=sib, schema=stage_schemas[sib], data_source=data_source,
        )
        overlaid = apply_extension_overlay(base, src)
        others = {n: s for n, s in stage_schemas.items() if n != sib}
        sb = stage_bundle_with_siblings(
            bundle=bundle, source_model=overlaid,
            sibling_schemas=others, data_source=data_source,
        )
        return ModelScope(source_model=overlaid), sb

    # 2. Bare-string sibling source (chain): bind against the upstream flat StageSchema.
    if isinstance(src, str) and src in stage_schemas:
        synth = model_from_stage_schema(
            name=src, schema=stage_schemas[src], data_source=data_source,
        )
        others = {n: s for n, s in stage_schemas.items() if n != src}
        sb = stage_bundle_with_siblings(
            bundle=bundle, source_model=synth,
            sibling_schemas=others, data_source=data_source,
        )
        return stage_schemas[src], sb

    # 3. Model-scoped: the stage's own resolved source model.
    assert stage_model is not None
    sb = stage_bundle_with_siblings(
        bundle=bundle, source_model=stage_model,
        sibling_schemas=stage_schemas, data_source=data_source,
    )
    return ModelScope(source_model=stage_model), sb


class _Splice(BaseModel):
    """One spliced stored query-backed model: its stage identities and variable context."""

    model: str
    identities: List[str] = Field(default_factory=list)
    context: Dict[str, Any] = Field(default_factory=dict)
    footprint: Set[str] = Field(default_factory=set)
    children: List[str] = Field(default_factory=list)


class _State(BaseModel):
    """The mutable planning state a failed speculative splice rolls back."""

    planned: List[PlannedQuery] = Field(default_factory=list)
    schemas: Dict[str, StageSchema] = Field(default_factory=dict)
    displays: Dict[str, StageDisplay] = Field(default_factory=dict)
    spliced: Dict[str, _Splice] = Field(default_factory=dict)

    def snapshot(self) -> "_State":
        return _State(
            planned=list(self.planned), schemas=dict(self.schemas),
            displays=dict(self.displays),
            spliced={n: s.model_copy(deep=True) for n, s in self.spliced.items()},
        )


class _StagePlanner:
    """Plans a stage list, splicing each stored query-backed model a stage reads
    before that stage (design decision 6): a stage is planned in attempts, and an
    attempt whose traversals touched an unspliced placeholder splices it and retries."""

    def __init__(self, *, bundle: ResolvedSourceBundle, outer_variables: Dict[str, Any]) -> None:
        self.bundle = bundle.model_copy(update={"referenced_models": [
            m for m in bundle.referenced_models if m.name not in bundle.query_backed
        ]})
        self.outer_variables = outer_variables
        self.state = _State(displays=dict(bundle.stage_displays))
        self.inert: Set[str] = set()
        # A merely touched model that failed to splice: its error, raised if it is read.
        self.failures: Dict[str, Exception] = {}
        self.data_source = (
            (bundle.source_model.data_source if bundle.source_model else None) or "_stage"
        )

    # -- stages ---------------------------------------------------------------
    def plan_stage(
        self, query: SlayerQuery, *, chain: Tuple[str, ...], stage_model: Optional[SlayerModel],
        single: bool = False, owner: Optional[str] = None,
    ) -> None:
        """Plan ``query`` after every query-backed model it names or reads is spliced."""
        for name in self._explicit_demands(query, chain=chain):
            self.ensure(name, chain=chain, explicit=True)
        while True:
            alone = single and not self.state.schemas
            scope, stamped = self._universe(query, stage_model=stage_model, single=alone)
            attempt = stamped.model_copy(update={"referenced_models": [
                *stamped.referenced_models, *self._placeholders(),
            ]})
            failure: Optional[Exception] = None
            planned: Optional[PlannedQuery] = None
            with observe_traversals() as seen:
                try:
                    planned = self._plan_once(query, bundle=attempt, scope=scope, single=alone)
                except Exception as exc:  # noqa: BLE001 — a miss may be a missing splice; retried below
                    failure = exc
            demand = sorted(
                n for n in seen
                if n in self.bundle.query_backed and n not in chain
                and n not in self.state.spliced and n not in self.inert
            )
            if demand and any([self.ensure(n, chain=chain, explicit=False) for n in demand]):
                continue
            on_chain = sorted(n for n, strict in seen.items() if strict and n in chain)
            unspliceable = sorted(n for n, strict in seen.items() if strict and n in self.failures)
            if failure is not None:
                if unspliceable:
                    # The stage reads a query-backed model that cannot be spliced: its error is the cause.
                    raise self.failures[unspliceable[0]] from failure
                if on_chain:
                    failure.add_note(
                        f"(it reads query-backed model(s) {on_chain} still being planned: "
                        f"{' -> '.join(chain)})"
                    )
                raise failure
            assert planned is not None
            self._record(query, planned=planned, stamped=stamped, seen=seen, chain=chain,
                         raw=[*on_chain, *unspliceable], owner=owner)
            return

    def _plan_once(
        self, query: SlayerQuery, *, bundle: ResolvedSourceBundle, scope, single: bool,
    ) -> PlannedQuery:
        display = self.state.displays.get(query.name) if query.name else None
        label = display.label if display else (
            f"stage {query.name!r}" if query.name else f"stages[{len(self.state.planned)}]"
        )
        with collect_stale_spellings() as spellings:
            if single:
                planned = plan_query(query=query, bundle=bundle)
            else:
                with stale_spelling_stage(label):
                    planned = plan_query(
                        query=query, bundle=bundle, scope=scope, stage_schemas=self.state.schemas,
                    )
        return planned.model_copy(update={"stale_spellings": spellings})

    def _universe(
        self, query: SlayerQuery, *, stage_model: Optional[SlayerModel], single: bool,
    ) -> "Tuple[Union[ModelScope, StageSchema, None], ResolvedSourceBundle]":
        base = self.bundle.model_copy(update={"stage_displays": dict(self.state.displays)})
        if single:
            return None, base
        return _stage_scope_and_bundle(
            query=query, bundle=base, stage_schemas=self.state.schemas,
            data_source=self.data_source,
            stage_model=stage_model or self.bundle.source_model,
        )

    def _placeholders(self) -> List[SlayerModel]:
        return [
            m for n, m in self.bundle.query_backed.items()
            if n not in self.state.spliced and n not in self.state.schemas
        ]

    def _record(
        self, query: SlayerQuery, *, planned: PlannedQuery, stamped: ResolvedSourceBundle,
        seen: Dict[str, bool], chain: Tuple[str, ...], raw: List[str],
        owner: Optional[str],
    ) -> None:
        position = {
            p.stage_schema.relation_name: i
            for i, p in enumerate(self.state.planned) if p.stage_schema is not None
        }
        reads = stage_sibling_reads(query=query, siblings=set(self.state.schemas))
        reads |= {n for n, strict in seen.items() if strict and n in self.state.spliced}
        # A read of a model still in flight, or one that failed to splice, stays in the
        # universe in stored form so its emission raises the cause.
        if raw:
            stamped = stamped.model_copy(update={"referenced_models": [
                *stamped.referenced_models, *(self.bundle.query_backed[n] for n in raw),
            ]})
        update: Dict[str, Any] = {
            "stage_reads": sorted(reads, key=position.__getitem__),
            "stage_bundle": stamped.model_copy(update={
                "splice_chain": chain,
                "splice_failures": {n: self.failures[n] for n in raw if n in self.failures},
            }),
        }
        display = self.state.displays.get(query.name) if query.name else None
        if planned.stage_schema is not None:
            schema_update: Dict[str, Any] = {}
            if display is not None:
                schema_update["display"] = display
            if owner is not None and query.name == owner:
                schema_update["default_time_dimension"] = self._base_time_dimension(owner)
            if schema_update:
                update["stage_schema"] = planned.stage_schema.model_copy(update=schema_update)
        planned = planned.model_copy(update=update)
        self.state.planned.append(planned)
        if query.name and planned.stage_schema is not None:
            self.state.schemas[query.name] = planned.stage_schema

    # -- splicing -------------------------------------------------------------
    def _explicit_demands(self, query: SlayerQuery, *, chain: Tuple[str, ...]) -> List[str]:
        spec = query.source_model
        names: List[str] = []
        if isinstance(spec, str):
            names.append(spec)
        elif spec is not None:
            if isinstance(spec, ModelExtension):
                names.append(spec.source_name)
            names.extend(j.target_model for j in spec.joins or [])
        return [n for n in dict.fromkeys(names) if n in chain or n in self.bundle.query_backed]

    def ensure(self, name: str, *, chain: Tuple[str, ...], explicit: bool) -> bool:
        """Splice query-backed model ``name`` under ``chain``; ``False`` if it stays a placeholder."""
        if name in chain:
            if explicit:
                raise QueryBackedCycleError(path=[*chain[chain.index(name):], name])
            return False
        if chain and chain[-1] in self.state.spliced:
            self.state.spliced[chain[-1]].children.append(name)
        context = self._context(chain=(*chain, name))
        spliced = self.state.spliced.get(name)
        if spliced is not None:
            self._check_context(spliced, context)
            return True
        model = self.bundle.query_backed.get(name)
        if name in self.inert or model is None:
            return False
        if model.data_source and model.data_source != self.data_source:
            if explicit:
                raise ValueError(
                    f"Query-backed model {name!r} is in datasource {model.data_source!r}; "
                    f"a statement reads one datasource ({self.data_source!r})."
                )
            self.inert.add(name)
            return False
        before = self.state.snapshot()
        try:
            self._splice(name, model=model, chain=(*chain, name), context=context)
        except Exception as exc:
            if explicit:
                raise
            self.state = before
            self.inert.add(name)
            self.failures[name] = exc
            return False
        return True

    def _splice(
        self, name: str, *, model: SlayerModel, chain: Tuple[str, ...], context: Dict[str, Any],
    ) -> None:
        stages, displays = localize_stages(
            topologically_order_stages(list(model.source_queries or [])), model=name,
        )
        self.state.displays.update(displays)
        record = _Splice(
            model=name, context=context, identities=[q.name for q in stages if q.name],
            footprint=set().union(*(extract_placeholder_names(q) for q in stages)),
        )
        self.state.spliced[name] = record
        own = {q.name for q in stages if q.name}
        for stage in stages:
            prepared, source, placeholders = self._prepare(stage, chain=chain, private=own)
            record.footprint |= placeholders
            self.plan_stage(prepared, chain=chain, stage_model=source, owner=name)
        for child in record.children:
            if child in self.state.spliced:
                record.footprint |= self.state.spliced[child].footprint
        record.context = self._context(chain=chain)

    def _prepare(
        self, stage: SlayerQuery, *, chain: Tuple[str, ...], private: Set[str],
    ) -> "Tuple[SlayerQuery, Optional[SlayerModel], Set[str]]":
        """A spliced stage, normalized and with its lexically layered variables
        substituted; its own source model with its Mode-A surfaces substituted
        (``None`` when it reads a sibling); and the placeholders that source reads."""
        stage = stage.strip_source_model_prefix()
        base_name = stage.source_model_name
        source = None
        if base_name not in private and base_name not in self.bundle.query_backed \
                and base_name not in chain:
            source = self._resolve_stage_model(stage.source_model)
        norm = normalize_query(stage, model=source)
        stage = norm.query if norm.query is not None else stage
        variables = {
            **((source.query_variables if source is not None else None) or {}),
            **self._chain_variables(chain),
            **self.outer_variables,
            **(stage.variables or {}),
            **self.bundle.runtime_variables,
        }
        stage = apply_variables_to_query(
            query=stage, variables=variables,
            dry_run_placeholders=self.bundle.dry_run_placeholders,
        )
        if source is None:
            return stage, None, set()
        return stage, substitute_model_sql_surfaces(
            model=source, variables=variables,
            backslash_escapes=get_dialect(self.bundle.dialect).backslash_escapes_strings,
        ), model_placeholder_names(source)

    def _resolve_stage_model(self, spec) -> Optional[SlayerModel]:
        """A spliced stage's own source model; ``None`` when it reads a sibling."""
        if source_name_if_sibling(spec=spec, sibling_names=set(self.state.schemas)) is not None:
            return None
        if isinstance(spec, SlayerModel):
            return spec
        name = spec.source_name if isinstance(spec, ModelExtension) else spec
        base = self.bundle.models_by_name.get(name) if isinstance(name, str) else None
        if base is None or base.source_queries:
            raise ValueError(f"Model {name!r} not found")
        return apply_extension_overlay(base, spec) if isinstance(spec, ModelExtension) else base

    def _base_time_dimension(self, name: str) -> Optional[str]:
        model = self.bundle.query_backed[name]
        private = {q.name: q for q in model.source_queries or [] if q.name}
        final = topologically_order_stages(list(model.source_queries or []))[-1]
        spec = follow_sibling_chain(spec=final.source_model, named_queries=private)
        base_name = spec.source_name if isinstance(spec, ModelExtension) else spec
        if isinstance(spec, SlayerModel):
            return spec.default_time_dimension
        base = self.bundle.models_by_name.get(base_name) if isinstance(base_name, str) else None
        return base.default_time_dimension if base is not None else None

    def _chain_variables(self, chain: Tuple[str, ...]) -> Dict[str, Any]:
        """The enclosing query-backed models' variables, outer ones overriding inner."""
        out: Dict[str, Any] = {}
        for name in reversed(chain):
            model = self.bundle.query_backed.get(name)
            out.update(model.query_variables if model is not None else {})
        return out

    def _context(self, *, chain: Tuple[str, ...]) -> Dict[str, Any]:
        return {**self._chain_variables(chain), **self.outer_variables, **self.bundle.runtime_variables}

    def _check_context(self, spliced: _Splice, context: Dict[str, Any]) -> None:
        """A model spliced once must not be reached under disagreeing variables."""
        differing = sorted(
            k for k in spliced.footprint if spliced.context.get(k) != context.get(k)
        )
        if not differing:
            return
        message = (
            f"Query-backed model {spliced.model!r} is read under conflicting values of "
            f"variable(s) {differing}; one statement splices it once."
        )
        self.state.planned = [
            p.model_copy(update={"splice_conflict": message})
            if p.stage_schema is not None and p.stage_schema.relation_name in spliced.identities
            else p
            for p in self.state.planned
        ]


def plan_stages(
    *,
    queries: List[SlayerQuery],
    bundle: ResolvedSourceBundle,
) -> List[PlannedQuery]:
    """Plan a multi-stage DAG: topo sort, then plan each stage against its own resolved
    source + already-planned siblings' synthetic models, splicing every stored
    query-backed model a stage reads before it; each plan records its sibling reads
    and is stamped with its per-stage bundle."""
    ordered = topologically_order_stages(queries)
    planner = _StagePlanner(bundle=bundle, outer_variables=dict(ordered[-1].variables or {}))
    for q in ordered:
        planner.plan_stage(
            q, chain=bundle.splice_chain, single=len(ordered) == 1,
            stage_model=(
                bundle.stage_source_models.get(q.name) if q.name and q is not ordered[-1] else None
            ),
        )
    return planner.state.planned
