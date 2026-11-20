"""Entity resolution for the unified Memory surface.

Maps every input form valid inside a ``SlayerQuery`` to the canonical
``<datasource>.<dataset>[.<leaf>]`` string described in the spec, §3-4. The
canonical form is what the storage layer indexes against, so two callers
referencing the same entity in different shapes (``sum(revenue)`` and
``customers.revenue``) end up keyed by the same string.

The leaf rule (§3.2):

* Strip aggregation suffix; the aggregation itself is never an entity.
* ``count(*)`` collapses to the source dataset.
* For dotted paths through relationships, only the leaf segment is tagged; the
  intermediates are discarded.
* The named-entity SQL (``DatasetField.sql`` or ``DatasetMetric.expression``) is
  opaque — we never recurse into it, except for a query's own extension
  fields, which stand for the stored entities their definitions read.

Two public entry points:

* ``resolve_entity(raw, *, storage, source_dataset=None)`` — single-token
  resolution; used by ``save_memory`` (entity-list path) and as the
  backbone of ``extract_entities_from_query``.
* ``extract_entities_from_query(query, *, storage)`` — walks every
  field of a ``SlayerQuery`` (population, dimensions, time_dimensions,
  metrics, filters) and returns the deduplicated canonical entity set
  plus any non-fatal warnings.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import (
    AmbiguousRelationshipPathError,
    AmbiguousDatasetError,
    ColonSpellingError,
    CircularRelationshipPathError,
    EntityResolutionError,
    PopulationInferenceError,
    UnknownFunctionError,
)
from slayer.core.relationship_walker import walk_miss, walk_terminal
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery, extension_dataset_names
from slayer.core.time_spine import is_spine_query
from slayer.engine.bundle_builder import stored_dataset
from slayer.engine.field_dependency import definition_reads
from slayer.engine.extensions import FieldKind, QueryScope, query_field_texts, query_scopes
from slayer.engine.population.inference import infer_rows, infer_spine_population
from slayer.engine.population.row_supply import plan_rows
from slayer.core.syntax import (
    AggCall,
    DottedRef,
    ParsedExpr,
    Ref,
    StarSource,
    drop_root_hop,
    drop_root_qualifier,
    parse_expr,
    split_entity_agg_ref,
    walk_parsed_refs,
)
from slayer.memories.models import (
    MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX,
    _validate_memory_id_charset,
)
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures, all_or_raise


def canonical_id_rooted_at(canonical_id: str, datasource: str) -> bool:
    """Return ``True`` iff ``canonical_id`` belongs to ``datasource``
    under the dotted-namespace rule.

    The rule mirrors the cascade-delete semantics:
    a canonical id is rooted at ``datasource`` when it is exactly the
    datasource name, OR a strict dotted-path descendant
    (``<datasource>.<...>``). Datasource names cannot contain ``.``
    (enforced by ``DatasourceConfig.name`` and ``SlayerDataset.data_source``
    validators), so the prefix match is unambiguous.

    ``memory:<int>`` canonical ids are datasource-agnostic — they never
    match any datasource, even one named ``memory``. Memory eligibility
    under a datasource filter is computed at the service layer by
    walking the memory's ``entities`` list and checking each entry with
    this helper.

    An empty ``datasource`` is rejected upstream by the validators; the
    helper still degrades gracefully (returns ``False`` for any input).
    """
    if not datasource:
        return False
    if canonical_id.startswith(_MEMORY_PREFIX):
        return False
    return canonical_id == datasource or canonical_id.startswith(
        f"{datasource}."
    )


class EntityResolution(BaseModel):
    """Output of ``resolve_entity`` and ``extract_entities_from_query``.

    ``canonical_forms`` is a list (not a set) so the order is stable —
    callers that union resolutions from multiple inputs can still
    deduplicate. ``warnings`` carries non-fatal diagnostics like the
    Case A dataset-vs-field collision and the Case D
    datasource-vs-dataset collision; fatal failures raise instead.
    """

    canonical_forms: list[str]
    warnings: list[str] = []


def _dataset_has_leaf(dataset: SlayerDataset, leaf: str) -> bool:
    """``True`` if ``leaf`` is a field / named metric / custom
    aggregation on ``dataset``."""
    if dataset.get_field(leaf) is not None:
        return True
    if dataset.get_metric(leaf) is not None:
        return True
    if dataset.get_aggregation(leaf) is not None:
        return True
    return False


async def _all_datasets_in_datasource(
    storage: StorageBackend, data_source: str
) -> list[SlayerDataset]:
    """Every dataset of ``data_source``; an unloadable one fails closed."""
    return all_or_raise(await storage.load_datasets(data_source=data_source))


async def _find_leaf_in_priority_winner(
    storage: StorageBackend, leaf: str
) -> tuple[str | None, list[SlayerDataset]]:
    """Walk the priority list; return ``(data_source, matches)`` for the
    first datasource that has ≥1 dataset carrying ``leaf`` as a field /
    metric / custom aggregation. ``matches`` may have multiple datasets
    (Case B1, ambiguous).
    """
    priority = await storage.get_datasource_priority()
    all_dses = await storage.list_datasources()
    # Walk priority list first, then any unlisted datasources.
    for ds in priority + [d for d in all_dses if d not in priority]:
        datasets = await _all_datasets_in_datasource(storage, ds)
        matches = [m for m in datasets if _dataset_has_leaf(m, leaf)]
        if matches:
            return ds, matches
    return None, []


async def _datasource_datasets(storage: StorageBackend, dataset: SlayerDataset) -> dict[str, SlayerDataset]:
    """``dataset``'s datasource peers by name, ``dataset`` itself included; unloadable peers skipped."""
    peers = DocumentLoadFailures().skip(await storage.load_datasets(data_source=dataset.data_source))
    return {**{m.name: m for m in peers}, dataset.name: dataset}


def _walk_relationship_path(
    starting_dataset: SlayerDataset, path: list[str], *, datasets_by_name: dict[str, SlayerDataset],
) -> SlayerDataset:
    """Walk a chain of relationship targets over ``datasets_by_name``, returning the leaf dataset.

    Raises ``EntityResolutionError`` when a segment doesn't match any relationship on the current dataset.
    """
    try:
        walked = walk_terminal(root=starting_dataset, path=path, datasets_by_name=datasets_by_name)
        miss = walk_miss(root=starting_dataset, path=path, datasets_by_name=datasets_by_name) if walked is None else None
    except CircularRelationshipPathError as exc:
        raise EntityResolutionError(str(exc)) from exc
    if walked is not None:
        return walked[0]
    assert miss is not None
    via = walk_terminal(root=starting_dataset, path=path[:miss.index], datasets_by_name=datasets_by_name)
    raise EntityResolutionError(
        f"'{miss.token}' is not a relationship target on dataset "
        f"'{via[0].name if via is not None else starting_dataset.name}'."
    )


def _dotted_canonical(
    starting_dataset: SlayerDataset, rest: list[str], *, datasets_by_name: dict[str, SlayerDataset],
) -> str:
    """Apply the leaf rule to a path ``[hop, hop, ..., leaf?]`` rooted at
    ``starting_dataset``. Returns the canonical form."""
    if not rest:
        return f"{starting_dataset.data_source}.{starting_dataset.name}"
    # Try interpreting the last segment as a leaf attribute on the
    # dataset reached by walking the rest.
    leaf_segment = rest[-1]
    try:
        leaf_dataset = _walk_relationship_path(starting_dataset, rest[:-1], datasets_by_name=datasets_by_name)
    except EntityResolutionError:
        # The walk failed before reaching the last segment — fall
        # through to the "every segment is a relationship" interpretation
        # below.
        leaf_dataset = None
    if leaf_dataset is not None and _dataset_has_leaf(leaf_dataset, leaf_segment):
        return f"{leaf_dataset.data_source}.{leaf_dataset.name}.{leaf_segment}"
    # No leaf attribute — every segment must be a relationship target, with
    # the path terminating at the joined dataset itself (§3.2 step 3).
    final_dataset = _walk_relationship_path(starting_dataset, rest, datasets_by_name=datasets_by_name)
    return f"{final_dataset.data_source}.{final_dataset.name}"


async def _resolve_dotted_against_dataset(storage: StorageBackend, starting_dataset: SlayerDataset, rest: list[str]) -> str:
    """``_dotted_canonical`` over ``starting_dataset``'s stored datasource peers."""
    if not rest:
        return f"{starting_dataset.data_source}.{starting_dataset.name}"
    return _dotted_canonical(starting_dataset, rest, datasets_by_name=await _datasource_datasets(storage, starting_dataset))


async def resolve_entity(
    raw: str,
    *,
    storage: StorageBackend,
    source_dataset: SlayerDataset | None = None,
) -> EntityResolution:
    """Resolve a single entity reference.

    Accepts every input form valid inside a ``SlayerQuery``:
    bare names, dotted paths through relationships, datasource-qualified paths,
    aggregations (``sum(x)``, ``weighted_avg(x, weight=qty)``), and
    ``count(*)``.

    With ``source_dataset`` set, bare-name resolution prefers attributes
    on the given source dataset — matching the engine's runtime behaviour
    inside a query body.
    """
    raw = raw.strip()
    if not raw:
        raise EntityResolutionError("entity reference is empty")

    # ``memory:<id>`` runs before the parser, which would refuse it as a colon spelling. Memory ids are
    # opaque strings (with a small charset forbidden); the resolver checks existence via ``get_memory_row``.
    if raw.startswith(_MEMORY_PREFIX):
        memory_id = raw[len(_MEMORY_PREFIX):]
        try:
            _validate_memory_id_charset(memory_id)
        except ValueError as exc:
            raise EntityResolutionError(str(exc)) from exc
        row = await storage.get_memory_row(memory_id)
        if row is None:
            raise EntityResolutionError(
                f"No memory with id {memory_id!r}."
            )
        return EntityResolution(canonical_forms=[raw])

    # Expression text is not an entity; a colon spelling is refused.
    try:
        prefix, suffix = split_entity_agg_ref(raw)
    except ColonSpellingError:
        raise
    except ValueError as exc:  # UnknownFunctionError is a ValueError subclass
        raise EntityResolutionError(str(exc)) from exc
    agg = suffix.split("(", 1)[0] if suffix is not None else None
    return await _resolve_reference(
        raw=raw, prefix=prefix, agg=agg, storage=storage, source_dataset=source_dataset)


async def _resolve_reference(  # NOSONAR(S3776) — single linear dispatch matching the spec's resolution-case table; splitting per-case helpers would obscure the shared error-message style and warning aggregation
    *, raw: str, prefix: str, agg: str | None, storage: StorageBackend, source_dataset: SlayerDataset | None,
) -> EntityResolution:
    """Resolve the reference ``prefix`` (aggregated by ``agg``, if any) written as ``raw``."""

    # ``count(*)`` special case (§3.1): collapses to the source dataset.
    # Only ``count`` is valid for the wildcard; ``sum(*)`` etc. would
    # silently get tagged as the dataset and corrupt the canonical-entity
    # index, so reject them explicitly.
    if prefix == "*":
        if agg != "count":
            raise EntityResolutionError(
                f"'{raw}' is not a valid entity reference; use 'count(*)' "
                "to refer to a dataset's row count."
            )
        if source_dataset is None:
            raise EntityResolutionError(
                "'count(*)' requires a dataset context: write "
                "'count(<dataset>.*)' or invoke from a query that has "
                "a population."
            )
        return EntityResolution(
            canonical_forms=[
                f"{source_dataset.data_source}.{source_dataset.name}"
            ]
        )

    # Detect ``count(<dataset>.*)`` shape — collapse to the dataset.
    # Same wildcard rule as above: only ``count`` is valid here.
    if prefix.endswith(".*"):
        if agg != "count":
            raise EntityResolutionError(
                f"'{raw}' is not a valid entity reference; use the "
                f"'count(<dataset>.*)' form."
            )
        dataset_part = prefix[:-2]
        return await resolve_entity(
            dataset_part, storage=storage, source_dataset=source_dataset
        )

    segments = prefix.split(".")
    # Hyphens are permitted in a segment: a datasource name may embed a UUID
    # (e.g. ``source_019f4323-cb59-77c3-...``), so the datasource segment can
    # contain hyphens. Dataset/leaf segments that don't match a stored entity
    # simply resolve to not-found below, so allowing the character is safe.
    if not all(re.match(r"^[a-zA-Z_][\w-]*$", s) for s in segments):
        raise EntityResolutionError(
            f"'{raw}' contains an invalid identifier segment."
        )

    warnings: list[str] = []
    known_dses = set(await storage.list_datasources())

    # ----- step 3: datasource-prefix detection ---------------------------
    if segments[0] in known_dses:
        ds = segments[0]
        # Case D: same name is also a dataset in some other datasource. This
        # probe is best-effort (it only drives a warning) — an *ambiguous*
        # dataset leg (same name in ≥2 datasources, no priority winner) must
        # NOT abort a perfectly valid datasource-prefixed reference, so we
        # treat the ambiguity as "yes, also a dataset" and keep going.
        try:
            ds_as_dataset = await storage.resolve_dataset_identity(ds)
        except AmbiguousDatasetError:
            ds_as_dataset = (ds, ds)
        if ds_as_dataset is not None:
            warnings.append(
                f"'{ds}' is both a datasource and a dataset; interpreted "
                f"as datasource. To force the dataset interpretation, "
                f"qualify as <other_ds>.{ds}..."
            )
        rest = segments[1:]
        if not rest:
            return EntityResolution(canonical_forms=[ds], warnings=warnings)
        dataset_name = rest[0]
        dataset = await storage.get_dataset(dataset_name, data_source=ds)
        if dataset is None:
            raise EntityResolutionError(
                f"No dataset '{dataset_name}' in datasource '{ds}'."
            )
        canonical = await _resolve_dotted_against_dataset(
            storage, dataset, rest[1:]
        )
        return EntityResolution(
            canonical_forms=[canonical], warnings=warnings
        )

    # ----- step 5: bare name (n=1) ---------------------------------------
    if len(segments) == 1:
        leaf = segments[0]

        # source_dataset context → prefer attributes on the source dataset.
        if source_dataset is not None and _dataset_has_leaf(source_dataset, leaf):
            return EntityResolution(
                canonical_forms=[
                    f"{source_dataset.data_source}.{source_dataset.name}.{leaf}"
                ]
            )

        # Try as a dataset first (§4.3 step 5).
        identity = await storage.resolve_dataset_identity(leaf)
        if identity is not None:
            ds_winner, dataset_name = identity
            # Case A: any other dataset in the priority-winner ds with
            # this name as a field / metric / custom-agg?
            other_datasets = [
                m
                for m in await _all_datasets_in_datasource(storage, ds_winner)
                if m.name != dataset_name and _dataset_has_leaf(m, leaf)
            ]
            if other_datasets:
                names = sorted(m.name for m in other_datasets)
                warnings.append(
                    f"'{leaf}' is both a dataset and a field on "
                    f"{names}; resolved as the dataset. Qualify as "
                    f"<dataset>.{leaf} to refer to the field."
                )
            return EntityResolution(
                canonical_forms=[f"{ds_winner}.{dataset_name}"],
                warnings=warnings,
            )

        # No dataset match → search for field / metric / custom-agg in
        # priority-winner datasource.
        ds_match, matches = await _find_leaf_in_priority_winner(
            storage, leaf
        )
        if ds_match is None:
            raise EntityResolutionError(
                f"Entity '{leaf}' not found in any datasource "
                f"(checked all known datasources)."
            )
        if len(matches) > 1:
            names = sorted(m.name for m in matches)
            raise EntityResolutionError(
                f"Ambiguous bare name '{leaf}' matches fields / "
                f"metrics / aggregations on {names} in datasource "
                f"'{ds_match}'. Qualify as '<dataset>.{leaf}'."
            )
        owner = matches[0]
        return EntityResolution(
            canonical_forms=[f"{ds_match}.{owner.name}.{leaf}"],
            warnings=warnings,
        )

    # ----- step 6: dotted form (n≥2) -------------------------------------
    head = segments[0]
    identity = await storage.resolve_dataset_identity(head)
    if identity is None:
        raise EntityResolutionError(
            f"'{head}' is neither a known datasource nor a saved "
            f"dataset. (segment of '{raw}')."
        )
    ds_winner, dataset_name = identity
    dataset = await storage.get_dataset(dataset_name, data_source=ds_winner)
    assert dataset is not None, "resolve_dataset_identity returned a stale match"
    canonical = await _resolve_dotted_against_dataset(
        storage, dataset, segments[1:]
    )
    return EntityResolution(
        canonical_forms=[canonical], warnings=warnings
    )


# ---------------------------------------------------------------------------
# Query-walking helpers
# ---------------------------------------------------------------------------


def _ref_token(node: Ref | DottedRef) -> str:
    """Textual form of a bare or dotted ref."""
    return node.name if isinstance(node, Ref) else ".".join(node.parts)


#: An entity token: a reference's text, or an aggregation's ``(source, agg)``.
EntityToken = str | tuple[str, str]


def _agg_call_tokens(node: AggCall) -> Iterable[EntityToken]:
    """Entity token(s) for one aggregation call."""
    source = node.source
    if isinstance(source, (Ref, DottedRef)):
        yield _ref_token(source), node.agg
    elif isinstance(source, StarSource):
        yield "*", node.agg
    else:
        # Expression source (``sum(amount - cost)``): the expression
        # itself is not an entity — tag each operand ref.
        for inner in walk_parsed_refs(source):
            if isinstance(inner, (Ref, DottedRef)):
                yield _ref_token(inner)


def _expression_entity_tokens(parsed: ParsedExpr) -> Iterable[EntityToken]:
    """Yield every entity *token* referenced by a parsed Mode-B expression.

    Aggregations surface as ``(source, agg)`` (``("*", "count")`` included), bare and dotted refs as their
    textual form. Each token is resolved downstream (the aggregation dropped, ``count(*)`` collapsed to the
    dataset, dotted relationship paths walked). No binding — the resolver does its own resolution.

    Aggregation args / kwargs (e.g. ``weighted_avg(weight=quantity)``) and
    transform partition fields are opaque (legacy parity) — only the
    aggregated source / inner value surfaces.
    """
    for node in walk_parsed_refs(parsed):
        if isinstance(node, AggCall):
            yield from _agg_call_tokens(node)
        else:  # Ref or DottedRef
            yield _ref_token(node)


def _field_entity_tokens(kind: FieldKind, text: str) -> list[EntityToken]:
    """Entity tokens of one query field; an unparseable expression is a single token."""
    if kind == "ref":
        return [text]
    try:
        parsed = parse_expr(text)
    # IllegalWindowInFilterError is-a ValueError, so ValueError covers it.
    except (ValueError, UnknownFunctionError):
        return [text]
    return list(_expression_entity_tokens(parsed))


class _ExtensionScope(BaseModel):
    """Resolves query tokens over the query's scope, replacing each extension-added field by the stored entities its
    definition reads."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    storage: StorageBackend
    scope: QueryScope
    stored: dict[str, SlayerDataset]
    expanded: set[tuple[str, str, str]] = set()

    def _extended(self, name: str) -> SlayerDataset | None:
        """Dataset ``name`` when the query changes it (stored datasets resolve through storage, as outside a query)."""
        dataset = self.scope.datasets.get(name)
        return dataset if dataset is not None and dataset is not self.stored.get(name) else None

    async def resolve(self, token: EntityToken, *, host: SlayerDataset) -> list[EntityResolution]:
        source, agg = token if isinstance(token, tuple) else (token, None)
        field = self._extension_field(source, host=host)
        if field is not None:
            return await self._field_reads(owner=field[0], name=field[1])
        scoped = self._scoped_form(source, agg=agg, host=host)
        if scoped is not None:
            return [EntityResolution(canonical_forms=[scoped])]
        if agg is not None:
            return [await _resolve_reference(
                raw=f"{agg}({source})", prefix=source, agg=agg, storage=self.storage, source_dataset=host)]
        return [await resolve_entity(source, storage=self.storage, source_dataset=host)]

    def _extension_field(self, source: str, *, host: SlayerDataset) -> tuple[SlayerDataset, str] | None:
        """``(extended dataset, field)`` when ``source`` names a field the query's extensions add."""
        *prefix, leaf = source.split(".")
        if not any(leaf in m.query_local_names for m in self.scope.datasets.values()):
            return None
        owner = self._dataset(prefix, host=host)
        return (owner, leaf) if owner is not None and leaf in owner.query_local_names else None

    def _walked(self, start: SlayerDataset, path: list[str]) -> SlayerDataset | None:
        try:
            return _walk_relationship_path(start, path, datasets_by_name={**self.scope.datasets, start.name: start})
        except (EntityResolutionError, AmbiguousRelationshipPathError):
            return None

    def _dataset(self, path: list[str], *, host: SlayerDataset) -> SlayerDataset | None:
        """The dataset ``path`` names: edge tokens from ``host`` (a lone hop short-form routed, as the binder does),
        else a dataset the query extends and its edge tokens."""
        path = list(drop_root_hop(path, host.spelling, head_quoted=False))
        if not path:
            return host
        walked = self._walked(host, path)
        if walked is not None:
            return walked
        routed = self.scope.resolve(path, root=host)
        if routed is not None and routed[1]:
            return self.scope.datasets.get(routed[1][-1].to)
        head = self._extended(path[0])
        return self._walked(head, path[1:]) if head is not None else None

    def _scoped_form(self, source: str, *, agg: str | None, host: SlayerDataset) -> str | None:
        """Canonical form of a dotted ``source`` over the scope — its head an edge token from ``host`` or short-form
        routed from it, else a dataset the query extends — or ``None``."""
        star = source.endswith(".*")
        segments = source.removesuffix(".*").split(".")
        if (star and agg != "count") or (not star and len(segments) < 2):
            return None
        if star:
            dataset = self._dataset(segments, host=host)
            return f"{dataset.data_source}.{dataset.name}" if dataset is not None else None
        heads = [(host, list(drop_root_qualifier(segments, host.spelling, quoted=())))]
        routed = self.scope.resolve(segments[:-1], root=host)
        if routed is not None and routed[1]:
            heads.append((self.scope.datasets[routed[1][-1].to], segments[-1:]))
        head = self._extended(segments[0])
        if head is not None:
            heads.append((head, segments[1:]))
        for start, rest in heads:
            try:
                return _dotted_canonical(start, rest, datasets_by_name={**self.scope.datasets, start.name: start})
            except (EntityResolutionError, AmbiguousRelationshipPathError):
                continue
        return None

    async def _field_reads(self, *, owner: SlayerDataset, name: str) -> list[EntityResolution]:
        mark = (owner.data_source, owner.name, name)
        if mark in self.expanded:
            return []
        self.expanded.add(mark)
        field = owner.get_field(name)
        if field is not None:
            reachable = {**self.scope.datasets, owner.name: owner}
            out: list[EntityResolution] = []
            for dataset_name, leaf in definition_reads(field=field, host=owner, reachable=reachable):
                target = reachable[dataset_name]
                if leaf in target.query_local_names:
                    out.extend(await self._field_reads(owner=target, name=leaf))
                else:
                    out.append(EntityResolution(canonical_forms=[f"{target.data_source}.{target.name}.{leaf}"]))
            return out
        metric = owner.get_metric(name)
        if metric is None or not metric.text:
            return []
        return [r for token in _field_entity_tokens("expression", metric.text) for r in await self.resolve(token, host=owner)]


async def _inferred_host(query: SlayerQuery, *, storage: StorageBackend) -> tuple[str, str]:
    """The dataset a rootless ``query`` reports (its dimension anchor), else the one the engine plans it from (the
    unit's supplying home), and its datasource."""
    if is_spine_query(query):
        choice = await infer_spine_population(query=query, storage=storage)
        return choice.dataset_name, choice.data_source
    rows = await infer_rows(query=query, storage=storage, raw_rows=not query.distinct_dimension_values)
    plan = plan_rows(query, anchor=rows.anchor, datasets_by_name=rows.datasets_by_name)
    reported = plan.reported if plan.reported in rows.datasets_by_name else None
    return reported or plan.population, rows.data_source


async def _rooted(query: SlayerQuery, *, storage: StorageBackend) -> tuple[SlayerQuery, SlayerDataset]:
    """``query`` with its source dataset; a rootless query is rooted at the population the engine infers."""
    src = query.population
    data_source = None
    if src is None:
        try:
            src, data_source = await _inferred_host(query, storage=storage)
        except PopulationInferenceError as exc:
            raise EntityResolutionError(f"Could not infer the query's population: {exc}") from exc
        query = query.model_copy(update={"population": src})
    dataset = await stored_dataset(storage, name=src, data_source=data_source)
    if dataset is None:
        raise EntityResolutionError(f"Source dataset not found: {src!r}.")
    return query, dataset


async def extract_entities_from_query(  # NOSONAR(S3776) — straight-line walk over each SlayerQuery field (population → dimensions → time_dimensions → metrics → filters); each branch is independently simple and parallels the SlayerQuery shape
    query: SlayerQuery,
    *,
    storage: StorageBackend,
) -> EntityResolution:
    """Extract every canonical entity referenced by ``query``: the source dataset, then each token of each field ``query_field_texts`` yields. Resolution failures bubble up unchanged."""
    canonical: list[str] = []
    warnings: list[str] = []
    seen: set[str] = set()

    def _add(forms: Iterable[str]) -> None:
        for f in forms:
            if f not in seen:
                seen.add(f)
                canonical.append(f)

    # 1. population — a saved dataset name or the population the engine infers — then the datasets
    # its extensions name.
    query, source_dataset = await _rooted(query, storage=storage)
    ds = source_dataset.data_source
    stored = await _datasource_datasets(storage, source_dataset)
    query_scope = query_scopes([query], datasets_by_ds={ds: stored}, data_source=ds)[0][0]
    query = query_scope.query or query
    _add([f"{source_dataset.data_source}.{source_dataset.name}"])
    for name in extension_dataset_names(query):
        extended = await storage.get_dataset(name, data_source=source_dataset.data_source or None)
        if extended is not None:
            _add([f"{extended.data_source}.{extended.name}"])

    # 2. every field the reads walk enumerates; aggregation arguments and the query's own names are not entities.
    local_names = {m.alias for m in query.metrics or [] if m.alias} | {
        d.name for d in query.dimensions or [] if d.is_computed
    } | {td.alias for td in query.time_dimensions or [] if td.alias}
    scope = _ExtensionScope(storage=storage, scope=query_scope, stored=stored)
    root = query_scope.host or source_dataset
    for kind, text in query_field_texts(query):
        for token in _field_entity_tokens(kind, text):
            if isinstance(token, str) and token in local_names:
                continue
            for result in await scope.resolve(token, host=root):
                _add(result.canonical_forms)
                warnings.extend(result.warnings)

    # Deduplicate warnings while preserving order.
    seen_warn: set[str] = set()
    deduped_warnings = []
    for w in warnings:
        if w not in seen_warn:
            seen_warn.add(w)
            deduped_warnings.append(w)

    return EntityResolution(
        canonical_forms=canonical, warnings=deduped_warnings
    )
