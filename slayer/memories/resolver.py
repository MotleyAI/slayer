"""Entity resolution for the unified Memory surface (DEV-1357 v2).

Maps every input form valid inside a ``SlayerQuery`` to the canonical
``<datasource>.<model>[.<leaf>]`` string described in the spec, §3-4. The
canonical form is what the storage layer indexes against, so two callers
referencing the same entity in different shapes (``revenue:sum`` and
``customers.revenue``) end up keyed by the same string.

The leaf rule (§3.2):

* Strip aggregation suffix; the aggregation itself is never an entity.
* ``*:count`` collapses to the source model.
* For dotted paths through joins, only the leaf segment is tagged; the
  intermediates are discarded.
* The named-entity SQL (``Column.sql`` or ``ModelMeasure.formula``) is
  opaque — we never recurse into it.

Two public entry points:

* ``resolve_entity(raw, *, storage, source_model=None)`` — single-token
  resolution; used by ``save_memory`` (entity-list path) and as the
  backbone of ``extract_entities_from_query``.
* ``extract_entities_from_query(query, *, storage)`` — walks every
  field of a ``SlayerQuery`` (source_model, dimensions, time_dimensions,
  measures, filters) and returns the deduplicated canonical entity set
  plus any non-fatal warnings.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Set, Tuple

from pydantic import BaseModel

from slayer.core.errors import (
    EntityResolutionError,
    UnknownFunctionError,
)
from slayer.core.models import SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.core.refs import strip_agg_suffix as _strip_agg_suffix
from slayer.engine.normalization import func_style_agg_to_colon
from slayer.engine.syntax import (
    AggCall,
    ParsedExpr,
    Ref,
    StarSource,
    parse_expr,
    walk_parsed_refs,
)
from slayer.memories.models import (
    MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX,
    _validate_memory_id_charset,
)
from slayer.storage.base import StorageBackend


# SQL keywords / function names that look like identifiers — skip when
# scanning filter expressions for entity refs.
_FILTER_TOKEN_BLACKLIST: frozenset[str] = frozenset({
    "and", "or", "not", "is", "null", "in", "like", "between", "true",
    "false", "case", "when", "then", "else", "end", "exists",
    # Common SQL functions agents might call — these aren't entities.
    "coalesce", "nullif", "cast", "ln", "log", "log2", "log10", "exp",
    "sqrt", "pow", "power", "abs", "round", "floor", "ceil", "ceiling",
    "current_date", "current_timestamp", "now",
})


def canonical_id_rooted_at(canonical_id: str, datasource: str) -> bool:
    """Return ``True`` iff ``canonical_id`` belongs to ``datasource``
    under the dotted-namespace rule (DEV-1409).

    The rule mirrors the cascade-delete semantics established in DEV-1405:
    a canonical id is rooted at ``datasource`` when it is exactly the
    datasource name, OR a strict dotted-path descendant
    (``<datasource>.<...>``). Datasource names cannot contain ``.``
    (enforced by ``DatasourceConfig.name`` and ``SlayerModel.data_source``
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
    Case A model-vs-column collision and the Case D
    datasource-vs-model collision; fatal failures raise instead.
    """

    canonical_forms: List[str]
    warnings: List[str] = []


def _model_has_leaf(model: SlayerModel, leaf: str) -> bool:
    """``True`` if ``leaf`` is a column / named measure / custom
    aggregation on ``model``."""
    if model.get_column(leaf) is not None:
        return True
    if model.get_measure(leaf) is not None:
        return True
    if model.get_aggregation(leaf) is not None:
        return True
    return False


async def _all_models_in_datasource(
    storage: StorageBackend, data_source: str
) -> List[SlayerModel]:
    identities = await storage._list_all_model_identities()
    out: List[SlayerModel] = []
    for ds, name in identities:
        if ds != data_source:
            continue
        m = await storage.get_model(name, data_source=ds)
        if m is not None:
            out.append(m)
    return out


async def _find_leaf_in_priority_winner(
    storage: StorageBackend, leaf: str
) -> Tuple[Optional[str], List[SlayerModel]]:
    """Walk the priority list; return ``(data_source, matches)`` for the
    first datasource that has ≥1 model carrying ``leaf`` as a column /
    measure / custom aggregation. ``matches`` may have multiple models
    (Case B1, ambiguous).
    """
    priority = await storage.get_datasource_priority()
    all_dses = await storage.list_datasources()
    # Walk priority list first, then any unlisted datasources.
    for ds in priority + [d for d in all_dses if d not in priority]:
        models = await _all_models_in_datasource(storage, ds)
        matches = [m for m in models if _model_has_leaf(m, leaf)]
        if matches:
            return ds, matches
    return None, []


async def _resolve_join_path(
    storage: StorageBackend,
    starting_model: SlayerModel,
    path: List[str],
) -> SlayerModel:
    """Walk a chain of join targets, returning the leaf model.

    Targets are looked up within the parent model's ``data_source``, per
    DEV-1330's join-scoping rule. Raises ``EntityResolutionError`` when
    a segment doesn't match any join on the current model.
    """
    current = starting_model
    for seg in path:
        join = next(
            (j for j in current.joins if j.target_model == seg), None
        )
        if join is None:
            raise EntityResolutionError(
                f"'{seg}' is not a join target on model "
                f"'{current.name}'."
            )
        target = await storage.get_model(seg, data_source=current.data_source)
        if target is None:
            raise EntityResolutionError(
                f"Join target '{seg}' on model '{current.name}' "
                f"resolves to no saved model in datasource "
                f"'{current.data_source}'."
            )
        current = target
    return current


async def _resolve_dotted_against_model(
    storage: StorageBackend,
    starting_model: SlayerModel,
    rest: List[str],
) -> str:
    """Apply the leaf rule to a path ``[hop, hop, ..., leaf?]`` rooted at
    ``starting_model``. Returns the canonical form."""
    if not rest:
        return f"{starting_model.data_source}.{starting_model.name}"
    # Try interpreting the last segment as a leaf attribute on the
    # model reached by walking the rest.
    leaf_segment = rest[-1]
    walk_segments = rest[:-1]
    try:
        leaf_model = await _resolve_join_path(
            storage, starting_model, walk_segments
        )
    except EntityResolutionError:
        # The walk failed before reaching the last segment — fall
        # through to the "every segment is a join" interpretation
        # below.
        leaf_model = None  # type: ignore[assignment]
    if leaf_model is not None and _model_has_leaf(leaf_model, leaf_segment):
        return (
            f"{leaf_model.data_source}.{leaf_model.name}.{leaf_segment}"
        )
    # No leaf attribute — every segment must be a join target, with
    # the path terminating at the join'd model itself (§3.2 step 3).
    final_model = await _resolve_join_path(
        storage, starting_model, rest
    )
    return f"{final_model.data_source}.{final_model.name}"


async def resolve_entity(  # NOSONAR(S3776) — single linear dispatch matching the spec's resolution-case table; splitting per-case helpers would obscure the shared error-message style and warning aggregation
    raw: str,
    *,
    storage: StorageBackend,
    source_model: Optional[SlayerModel] = None,
) -> EntityResolution:
    """Resolve a single entity reference.

    Accepts every input form valid inside a ``SlayerQuery``:
    bare names, dotted paths through joins, datasource-qualified paths,
    aggregation suffixes (``:sum``, ``:weighted_avg(weight=qty)``),
    and the ``*:count`` form.

    With ``source_model`` set, bare-name resolution prefers attributes
    on the given source model — matching the engine's runtime behaviour
    inside a query body.
    """
    raw = raw.strip()
    if not raw:
        raise EntityResolutionError("entity reference is empty")

    # DEV-1428: ``memory:<id>`` branch must run BEFORE ``_strip_agg_suffix``
    # — otherwise ``memory:abc`` would be parsed as prefix ``memory`` plus
    # agg ``abc``. Memory ids are opaque strings (with a small charset
    # forbidden); the resolver checks existence via ``get_memory_row``.
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

    prefix, agg = _strip_agg_suffix(raw)

    # ``*:count`` special case (§3.1): collapses to the source model.
    # Only ``count`` is valid for the wildcard; ``*:sum`` etc. would
    # silently get tagged as the model and corrupt the canonical-entity
    # index, so reject them explicitly.
    if prefix == "*":
        if agg != "count":
            raise EntityResolutionError(
                f"'{raw}' is not a valid entity reference; use '*:count' "
                "to refer to a model's row count."
            )
        if source_model is None:
            raise EntityResolutionError(
                "'*:count' requires a model context: write "
                "'<model>.*:count' or invoke from a query that has "
                "a source_model."
            )
        return EntityResolution(
            canonical_forms=[
                f"{source_model.data_source}.{source_model.name}"
            ]
        )

    # Detect ``<model>.*:count`` shape — collapse to the model.
    # Same wildcard rule as above: only ``count`` is valid here.
    if prefix.endswith(".*"):
        if agg != "count":
            raise EntityResolutionError(
                f"'{raw}' is not a valid entity reference; use the "
                f"'<model>.*:count' form."
            )
        model_part = prefix[:-2]
        return await resolve_entity(
            model_part, storage=storage, source_model=source_model
        )

    segments = prefix.split(".")
    if not all(re.match(r"^[a-zA-Z_]\w*$", s) for s in segments):
        raise EntityResolutionError(
            f"'{raw}' contains an invalid identifier segment."
        )

    warnings: List[str] = []
    known_dses = set(await storage.list_datasources())

    # ----- step 3: datasource-prefix detection ---------------------------
    if segments[0] in known_dses:
        ds = segments[0]
        # Case D: same name is also a model in some other datasource.
        ds_as_model = await storage.resolve_model_identity(ds)
        if ds_as_model is not None:
            warnings.append(
                f"'{ds}' is both a datasource and a model; interpreted "
                f"as datasource. To force the model interpretation, "
                f"qualify as <other_ds>.{ds}..."
            )
        rest = segments[1:]
        if not rest:
            return EntityResolution(canonical_forms=[ds], warnings=warnings)
        model_name = rest[0]
        model = await storage.get_model(model_name, data_source=ds)
        if model is None:
            raise EntityResolutionError(
                f"No model '{model_name}' in datasource '{ds}'."
            )
        canonical = await _resolve_dotted_against_model(
            storage, model, rest[1:]
        )
        return EntityResolution(
            canonical_forms=[canonical], warnings=warnings
        )

    # ----- step 5: bare name (n=1) ---------------------------------------
    if len(segments) == 1:
        leaf = segments[0]

        # source_model context → prefer attributes on the source model.
        if source_model is not None and _model_has_leaf(source_model, leaf):
            return EntityResolution(
                canonical_forms=[
                    f"{source_model.data_source}.{source_model.name}.{leaf}"
                ]
            )

        # Try as a model first (§4.3 step 5).
        identity = await storage.resolve_model_identity(leaf)
        if identity is not None:
            ds_winner, model_name = identity
            # Case A: any other model in the priority-winner ds with
            # this name as a column / measure / custom-agg?
            other_models = [
                m
                for m in await _all_models_in_datasource(storage, ds_winner)
                if m.name != model_name and _model_has_leaf(m, leaf)
            ]
            if other_models:
                names = sorted(m.name for m in other_models)
                warnings.append(
                    f"'{leaf}' is both a model and a column on "
                    f"{names}; resolved as the model. Qualify as "
                    f"<model>.{leaf} to refer to the column."
                )
            return EntityResolution(
                canonical_forms=[f"{ds_winner}.{model_name}"],
                warnings=warnings,
            )

        # No model match → search for column / measure / custom-agg in
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
                f"Ambiguous bare name '{leaf}' matches columns / "
                f"measures / aggregations on {names} in datasource "
                f"'{ds_match}'. Qualify as '<model>.{leaf}'."
            )
        owner = matches[0]
        return EntityResolution(
            canonical_forms=[f"{ds_match}.{owner.name}.{leaf}"],
            warnings=warnings,
        )

    # ----- step 6: dotted form (n≥2) -------------------------------------
    head = segments[0]
    identity = await storage.resolve_model_identity(head)
    if identity is None:
        raise EntityResolutionError(
            f"'{head}' is neither a known datasource nor a saved "
            f"model. (segment of '{raw}')."
        )
    ds_winner, model_name = identity
    model = await storage.get_model(model_name, data_source=ds_winner)
    assert model is not None, "resolve_model_identity returned a stale match"
    canonical = await _resolve_dotted_against_model(
        storage, model, segments[1:]
    )
    return EntityResolution(
        canonical_forms=[canonical], warnings=warnings
    )


# ---------------------------------------------------------------------------
# Query-walking helpers
# ---------------------------------------------------------------------------


def _formula_entity_tokens(parsed: ParsedExpr) -> Iterable[str]:
    """Yield every entity *token* referenced by a parsed Mode-B formula.

    Colon-syntax aggregations surface as ``"<source>:<agg>"`` (``*:count``
    included verbatim), bare and dotted refs surface as their textual form.
    Each token is fed one-by-one into ``resolve_entity`` downstream, which
    canonicalises it (stripping the agg suffix, collapsing ``*:count`` to the
    model, walking dotted join paths). No binding — the resolver does its own
    resolution.

    Aggregation args / kwargs (e.g. ``weighted_avg(weight=quantity)``) and
    transform partition columns are opaque (legacy parity) — only the
    aggregated source / inner value surfaces.
    """
    for node in walk_parsed_refs(parsed):
        if isinstance(node, AggCall):
            source = node.source
            if isinstance(source, StarSource):
                src_name = "*"
            elif isinstance(source, Ref):
                src_name = source.name
            else:  # DottedRef
                src_name = ".".join(source.parts)
            yield f"{src_name}:{node.agg}"
        elif isinstance(node, Ref):
            yield node.name
        else:  # DottedRef
            yield ".".join(node.parts)


_FILTER_AGG_SUFFIX_RE = re.compile(r":\w+(?:\([^)]*\))?")
_FILTER_LITERAL_RE = re.compile(r"'[^']*'|\"[^\"]*\"")
_FILTER_VAR_RE = re.compile(r"\{[^}]*\}")
_FILTER_TOKEN_RE = re.compile(r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*")


def _extract_filter_tokens(filter_text: str) -> List[str]:
    """Extract identifier-shaped tokens from a filter expression that
    might be entity references.

    Strips quoted literals, ``{variable}`` placeholders, and ``:agg``
    aggregation suffixes (handled separately by the caller). Skips SQL
    keywords and common SQL function names.
    """
    cleaned = _FILTER_AGG_SUFFIX_RE.sub("", filter_text)
    cleaned = _FILTER_LITERAL_RE.sub("", cleaned)
    cleaned = _FILTER_VAR_RE.sub("", cleaned)
    out: List[str] = []
    for m in _FILTER_TOKEN_RE.finditer(cleaned):
        token = m.group(0)
        # Skip identifiers immediately followed by '(' — they're SQL
        # function names like coalesce(...), nullif(...).
        end = m.end()
        if end < len(cleaned) and cleaned[end] == "(":
            continue
        if token.lower() in _FILTER_TOKEN_BLACKLIST:
            continue
        if token.replace(".", "").isdigit():
            continue
        out.append(token)
    return out


def _column_ref_path(ref: ColumnRef) -> str:
    return ref.full_name


async def extract_entities_from_query(  # NOSONAR(S3776) — straight-line walk over each SlayerQuery field (source_model → dimensions → time_dimensions → measures → filters); each branch is independently simple and parallels the SlayerQuery shape
    query: SlayerQuery,
    *,
    storage: StorageBackend,
) -> EntityResolution:
    """Extract every canonical entity referenced by ``query``.

    Walks ``source_model``, ``dimensions``, ``time_dimensions``,
    ``measures`` (formula bodies), and ``filters``. The source model is
    always tagged, even if no field references it explicitly.
    Resolution failures bubble up unchanged.
    """
    canonical: List[str] = []
    warnings: List[str] = []
    seen: Set[str] = set()

    def _add(forms: Iterable[str]) -> None:
        for f in forms:
            if f not in seen:
                seen.add(f)
                canonical.append(f)

    # 1. source_model — must already be a saved model name (str). For
    # inline SlayerModel / ModelExtension, fall back to its name attr.
    src = query.source_model
    if isinstance(src, str):
        source_model = await storage.get_model(src)
    elif isinstance(src, SlayerModel):
        source_model = src
    else:
        # ModelExtension: resolve its source by name.
        source_name = getattr(src, "source_name", None) or getattr(
            src, "name", None
        )
        if not isinstance(source_name, str):
            raise EntityResolutionError(
                "Could not derive a model name from query.source_model."
            )
        source_model = await storage.get_model(source_name)
    if source_model is None:
        raise EntityResolutionError(
            f"Source model not found: {src!r}."
        )
    _add([f"{source_model.data_source}.{source_model.name}"])

    # 2. dimensions
    for dim in query.dimensions or []:
        result = await resolve_entity(
            _column_ref_path(dim),
            storage=storage,
            source_model=source_model,
        )
        _add(result.canonical_forms)
        warnings.extend(result.warnings)

    # 3. time_dimensions
    for td in query.time_dimensions or []:
        ref: ColumnRef = td.dimension if isinstance(td, TimeDimension) else td
        result = await resolve_entity(
            _column_ref_path(ref),
            storage=storage,
            source_model=source_model,
        )
        _add(result.canonical_forms)
        warnings.extend(result.warnings)

    # 4. measures — parse each formula (Mode-B DSL) and resolve each
    # referenced entity token. Function-style aggregations are rewritten to
    # colon syntax first (quiet FUNC_STYLE_AGG slack helper), including the
    # source model's custom aggregations — matching the legacy parse path.
    custom_agg_names = frozenset(a.name for a in source_model.aggregations)
    for m in query.measures or []:
        if m.formula is None:
            continue
        try:
            parsed = parse_expr(
                func_style_agg_to_colon(
                    m.formula, custom_agg_names=custom_agg_names
                )
            )
        # IllegalWindowInFilterError is-a ValueError, so ValueError covers it.
        except (ValueError, UnknownFunctionError):
            # Not parseable as Mode-B DSL — fall back to treating the whole
            # formula text as a single entity reference.
            result = await resolve_entity(
                m.formula,
                storage=storage,
                source_model=source_model,
            )
            _add(result.canonical_forms)
            warnings.extend(result.warnings)
            continue
        for token in _formula_entity_tokens(parsed):
            result = await resolve_entity(
                token,
                storage=storage,
                source_model=source_model,
            )
            _add(result.canonical_forms)
            warnings.extend(result.warnings)

    # 5. filters — extract identifier tokens, resolve each.
    for f in query.filters or []:
        for tok in _extract_filter_tokens(f):
            result = await resolve_entity(
                tok,
                storage=storage,
                source_model=source_model,
            )
            _add(result.canonical_forms)
            warnings.extend(result.warnings)

    # Deduplicate warnings while preserving order.
    seen_warn: Set[str] = set()
    deduped_warnings = []
    for w in warnings:
        if w not in seen_warn:
            seen_warn.add(w)
            deduped_warnings.append(w)

    return EntityResolution(
        canonical_forms=canonical, warnings=deduped_warnings
    )
