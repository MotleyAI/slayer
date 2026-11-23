"""Python client for SLayer API."""

import logging
from collections.abc import (
    Coroutine,
    Mapping,
    Mapping as ABCMapping,
    Sequence,
    Sequence as ABCSequence,
)
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
)
from urllib.parse import quote

import yaml
from typing_extensions import deprecated

from slayer.async_utils import run_sync
from slayer.core.enums import Conformance
from slayer.core.errors import OssieErrorCode, OssieQueryError
from slayer.core.format import NumberFormat
from slayer.core.ossie_conformance import parse_conformance
from slayer.core.query import QueryRefinement, SlayerQuery, deprecated_inputs_of
from slayer.core.recommend import PopulationRecommendation
from slayer.core.vocabulary import method_twin
from slayer.core.models import SlayerDataset
from slayer.core.warnings import DeprecatedInputWarning, SlayerWarning
from slayer.engine.query_engine import (
    REFINE_REQUIRES_NAME,
    ClientFactory,
    FieldMetadata,
    ResponseAttributes,
    SavedQueryRun,
    SlayerQueryEngine,
    SlayerResponse,
    resolve_run_input,
)
from slayer.inspect.service import InspectService
from slayer.memories.models import (
    ForgetMemoryResponse,
    SaveMemoryResponse,
)
from slayer.memories.service import MemoryService
from slayer.search.service import SearchResponse, SearchService
from slayer.storage.base import StorageBackend

# httpx is the optional ``client`` extra; keep it import-safe so local-engine
# mode loads without it. pandas is heavier and only ``query_df`` needs it, so it
# stays a lazy import inside that method.
try:
    import httpx
except ImportError:
    httpx = None

if TYPE_CHECKING:
    from slayer.core.policy import SessionPolicy

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_QUERY_PATH = "/query"


# Input union for every public query entry point (mirrors ``engine.execute``):
# ``str`` = run-by-name, list = multi-stage DAG. ``Mapping``/``Sequence`` keep
# pyright's invariance check from rejecting ``list[dict[str, str]]``.
QueryInput = (
    SlayerQuery
    | Mapping[str, Any]
    | Sequence[SlayerQuery | Mapping[str, Any]]
    | str
)
Refinement = QueryRefinement | Mapping[str, Any] | None
Variables = Mapping[str, Any] | None


def _raise_for_status(resp: Any) -> None:
    """``raise_for_status``, a strict refusal (a 400 whose ``detail`` carries an Ossie code) raised as itself."""
    if resp.status_code == 400:
        try:
            detail = resp.json().get("detail")
        except ValueError:
            detail = None
        if isinstance(detail, dict) and detail.get("code") in set(OssieErrorCode):
            raise OssieQueryError.from_detail(detail)
    resp.raise_for_status()


def _semantic_model_path(*, data_source: str, name: str) -> str:
    return f"/semantic-models/{quote(data_source, safe='')}/{quote(name, safe='')}"

class SlayerClient:
    """Async-first client for the SLayer REST API, or direct local mode.

    Remote: ``SlayerClient(url=...)``. Local (no server):
    ``SlayerClient(storage=YAMLStorage(...))``. Async ``query`` / sync
    ``query_sync``.
    """

    def __init__(
        self,
        url: str = "http://localhost:5143",
        storage: StorageBackend | None = None,
        *,
        policy: "SessionPolicy | None" = None,
        client_factory: ClientFactory | None = None,
    ):
        self.url = url.rstrip("/")
        self._storage: StorageBackend | None = storage
        self._engine = None
        if storage is not None:
            self._engine = SlayerQueryEngine(storage=storage, policy=policy, client_factory=client_factory)
            self._storage = self._engine.storage
        elif policy is not None:
            # Forced-filter policy is local-engine only; fail fast rather than
            # silently disable a security control over HTTP.
            raise ValueError(
                "policy= is only supported in local-engine mode (pass "
                "storage=...); server-side policy over HTTP is not yet "
                "available."
            )
        elif client_factory is not None:
            raise ValueError("client_factory= is only supported in local-engine mode (pass storage=...).")

    def _run_sync(self, workflow: Coroutine[Any, Any, _T]) -> _T:
        """One synchronous call's whole workflow, bridged through the local engine when there is one."""
        if self._engine is not None:
            return self._engine.run_sync(workflow)
        return run_sync(workflow)

    async def _request(
        self,
        method: str,
        path: str,
        json: dict | None = None,
        params: dict | None = None,
    ) -> Any:
        if httpx is None:
            raise ImportError("Client requires httpx: pip install motley-slayer[client]")
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method=method, url=f"{self.url}{path}", json=json, params=params
            )
            _raise_for_status(resp)
            return resp.json()

    async def _request_or_none(self, *, method: str, path: str) -> Any | None:
        """``_request``, ``None`` when the server answers 404."""
        if httpx is None:
            return await self._request(method=method, path=path)
        try:
            return await self._request(method=method, path=path)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise

    def _request_sync(
        self,
        method: str,
        path: str,
        json: dict | None = None,
        params: dict | None = None,
    ) -> Any:
        if httpx is None:
            raise ImportError("Client requires httpx: pip install motley-slayer[client]")
        with httpx.Client() as client:
            resp = client.request(
                method=method, url=f"{self.url}{path}", json=json, params=params
            )
            _raise_for_status(resp)
            return resp.json()

    @staticmethod
    def _dump(query: SlayerQuery) -> dict[str, Any]:
        """A query as the server's ``QueryRequest`` accepts it (string shorthands already lifted)."""
        return query.model_dump(mode="json", exclude_none=True)

    @staticmethod
    def _stage(*, index: int, item: Any) -> SlayerQuery:
        if isinstance(item, SlayerQuery):
            return item
        if isinstance(item, ABCMapping):
            return SlayerQuery.model_validate(dict(item))
        raise TypeError(
            f"query[{index}] must be SlayerQuery or Mapping; got "
            f"{type(item).__name__}"
        )

    @staticmethod
    def _build_query_body(
        query: QueryInput,
        *,
        refine: Refinement = None,
        variables: Variables = None,
        dry_run: bool = False,
        explain: bool = False,
        deprecations_out: list[DeprecatedInputWarning] | None = None,
    ) -> dict[str, Any]:
        """Build the ``POST /query`` JSON body from any accepted input shape
        (``str`` run-by-name with optional ``refine``, list DAG, ``Mapping``, or
        ``SlayerQuery``); never mutates caller-owned data. Shared by sync + async transports.
        The deprecations of the query objects and refinement it sends go to ``deprecations_out``."""
        run = resolve_run_input(query, SlayerClient._normalize_refine(refine))
        sent: list[SlayerQuery | QueryRefinement | None] = []
        if isinstance(run, SavedQueryRun):
            body: dict[str, Any] = {"name": run.name}
            sent.append(run.refinement)
            if run.refinement is not None:
                body["refine"] = run.refinement.model_dump(mode="json", exclude_unset=True)
        elif isinstance(query, SlayerQuery):
            sent.append(query)
            body = SlayerClient._dump(query)
        elif isinstance(query, ABCSequence) and not isinstance(
            query, (bytes, bytearray)
        ):
            # bytes are Sequences too; route them to the else-branch below.
            stages = [SlayerClient._stage(index=i, item=item) for i, item in enumerate(query)]
            sent.extend(stages)
            body = {"queries": [SlayerClient._dump(q) for q in stages]}
        elif isinstance(query, ABCMapping):
            validated = SlayerQuery.model_validate(dict(query))
            sent.append(validated)
            body = SlayerClient._dump(validated)
        else:
            raise TypeError(
                "query must be SlayerQuery, Mapping, Sequence, or str; got "
                f"{type(query).__name__}"
            )
        if deprecations_out is not None:
            deprecations_out.extend(deprecated_inputs_of(sent))
        if variables:
            # Runtime wins over a single query's own ``variables``, as in ``engine.execute``.
            body["variables"] = {**body.get("variables", {}), **variables}
        if dry_run:
            body["dry_run"] = True
        if explain:
            body["explain"] = True
        return body

    @staticmethod
    def _strict_body(
        query: QueryInput, *, refine: Refinement, variables: Variables, dry_run: bool, explain: bool,
    ) -> Any:
        """The ``POST /query?conformance=ossie-l3`` body: the query as given (the server judges it), wrapper keys
        beside it."""
        if not isinstance(query, (str, SlayerQuery, ABCMapping)):
            return [SlayerClient._strict_parts(q) for q in query]
        if isinstance(query, str):
            body: dict[str, Any] = {"name": query, **SlayerClient._strict_refine(refine)}
        elif refine is not None:
            raise ValueError(REFINE_REQUIRES_NAME)
        else:
            body = SlayerClient._strict_parts(query)
        body.update({k: v for k, v in (("variables", dict(variables) if variables else None),
                                       ("dry_run", dry_run or None), ("explain", explain or None)) if v})
        return body

    @staticmethod
    def _strict_parts(query: Any) -> Any:
        """A query object as the server judges it: the parts it sets (``version`` is bookkeeping), an explicit ``None``
        kept for refusal."""
        if isinstance(query, SlayerQuery):
            parts = query.model_dump(mode="json", exclude_unset=True, exclude={"version"})
            parts = {**parts, **{n: None for n in query.model_fields_set if getattr(query, n) is None}}
        elif isinstance(query, ABCMapping):
            parts = dict(query)
        else:
            return query
        if "name" in parts:
            # On the wire a top-level ``name`` runs a saved query; in a query object it is no Ossie part.
            raise OssieQueryError(OssieErrorCode.E_INVALID_QUERY,
                                  "'name' is not a part of an Ossie query (dimensions, metrics, filter)")
        return parts

    @staticmethod
    def _strict_refine(refine: Refinement) -> dict[str, Any]:
        if refine is None:
            return {}
        return {"refine": dict(refine) if isinstance(refine, ABCMapping) else refine.model_dump(
            mode="json", exclude_unset=True)}

    @staticmethod
    def _normalize_for_engine(query: QueryInput) -> Any:
        """Coerce ``Mapping``/``Sequence`` inputs to concrete ``dict``/``list``
        so ``engine.execute``'s ``isinstance`` dispatch resolves them correctly."""
        if isinstance(query, str) or isinstance(query, SlayerQuery):
            return query
        if isinstance(query, ABCSequence) and not isinstance(
            query, (bytes, bytearray)
        ):
            return [
                item if isinstance(item, SlayerQuery)
                else dict(item) if isinstance(item, ABCMapping)
                else item  # engine raises with the per-item context.
                for item in query
            ]
        if isinstance(query, ABCMapping):
            return dict(query)
        return query  # engine raises with the per-input context.

    @staticmethod
    def _normalize_refine(refine: Refinement) -> QueryRefinement | dict | None:
        return dict(refine) if isinstance(refine, ABCMapping) else refine

    @staticmethod
    def _normalize_variables(variables: Variables) -> dict[str, Any] | None:
        return dict(variables) if variables else None

    @staticmethod
    def _parse_response(result: dict, *, deprecations: list[DeprecatedInputWarning] | None = None) -> SlayerResponse:
        """Parse an API JSON response into a SlayerResponse; its warnings are the server's plus the client's ``deprecations``."""
        def _parse_meta_dict(d: dict | None) -> dict[str, FieldMetadata]:
            out = {}
            for k, v in (d or {}).items():
                fmt = None
                if v.get("format"):
                    fmt = NumberFormat.model_validate(v["format"])
                out[k] = FieldMetadata(label=v.get("label"), format=fmt)
            return out

        attrs_raw = result.get("attributes") or {}
        attributes = ResponseAttributes(
            dimensions=_parse_meta_dict(attrs_raw.get("dimensions")),
            metrics=_parse_meta_dict(attrs_raw.get("metrics")),
        )
        return SlayerResponse(
            data=result["data"],
            columns=result.get("columns") or [],
            sql=result.get("sql"),
            attributes=attributes,
            warnings=[*(result.get("warnings") or []), *(deprecations or [])],
            population=result.get("population"),
            population_inferred=bool(result.get("population_inferred", False)),
            row_sources=list(result.get("row_sources") or []),
        )

    # ----- Async API -----

    async def query(
        self,
        query: QueryInput,
        *,
        refine: Refinement = None,
        variables: Variables = None,
        dry_run: bool = False,
        explain: bool = False,
        conformance: Conformance | str = Conformance.SLAYER,
    ) -> SlayerResponse:
        """Execute a query. Accepts ``SlayerQuery`` / ``dict`` / list-DAG /
        ``str`` (run-by-name, optionally ``refine``-d) — the same union as ``engine.execute``;
        ``variables`` are runtime placeholder values; ``conformance="ossie-l3"`` runs the strict Ossie binding."""
        if self._engine is not None:
            return await self._engine.execute(
                query=self._normalize_for_engine(query),
                variables=self._normalize_variables(variables),
                dry_run=dry_run,
                explain=explain,
                refine=self._normalize_refine(refine),
                conformance=conformance,
            )
        if parse_conformance(conformance) is Conformance.OSSIE_L3:
            body = self._strict_body(query, refine=refine, variables=variables, dry_run=dry_run, explain=explain)
            return self._parse_response(await self._request(method="POST", path=_QUERY_PATH, json=body,
                                                            params={"conformance": Conformance.OSSIE_L3.value}))
        deprecations: list[DeprecatedInputWarning] = []
        body = self._build_query_body(
            query, refine=refine, variables=variables, dry_run=dry_run, explain=explain,
            deprecations_out=deprecations,
        )
        result = await self._request(method="POST", path=_QUERY_PATH, json=body)
        return self._parse_response(result, deprecations=deprecations)

    async def sql(
        self, query: QueryInput, *, refine: Refinement = None, variables: Variables = None,
        conformance: Conformance | str = Conformance.SLAYER,
    ) -> str:
        """Generate SQL for a query without executing it (same input union)."""
        sql = (await self.query(query=query, refine=refine, variables=variables, dry_run=True,
                                conformance=conformance)).sql
        assert sql is not None  # a dry run always renders SQL
        return sql

    async def explain(
        self, query: QueryInput, *, refine: Refinement = None, variables: Variables = None,
        conformance: Conformance | str = Conformance.SLAYER,
    ) -> SlayerResponse:
        """Run the database's EXPLAIN on a query (same input union)."""
        return await self.query(query=query, refine=refine, variables=variables, explain=True, conformance=conformance)

    async def list_datasets(self, data_source: str | None = None) -> list[str]:
        if self._storage is not None:
            names = await self._storage.list_datasets(data_source=data_source)
            return list(names)
        params = {"data_source": data_source} if data_source else None
        return await self._request(method="GET", path="/datasets", params=params)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def get_dataset(
        self,
        name: str,
        data_source: str | None = None,
    ) -> Any | None:
        if self._storage is not None:
            return await self._storage.get_dataset(name, data_source=data_source)
        params = {"data_source": data_source} if data_source else None
        return await self._request(method="GET", path=f"/datasets/{name}", params=params)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def create_dataset(self, dataset: dict[str, Any]) -> dict[str, Any]:
        if self._engine is not None:
            validated = SlayerDataset.model_validate(dataset)
            notes: list[SlayerWarning] = []
            await self._engine.save_dataset(validated, warnings_out=notes)
            return {"status": "created", "name": validated.name, "warnings": [n.model_dump(mode="json") for n in notes]}
        return await self._request(method="POST", path="/datasets", json=dataset)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def list_datasources(self) -> list[str]:
        return await self._request(method="GET", path="/datasources")

    async def create_datasource(self, datasource: dict[str, Any]) -> dict[str, str]:
        return await self._request(method="POST", path="/datasources", json=datasource)

    async def get_datasource_priority(self) -> list[str]:
        if self._storage is not None:
            return await self._storage.get_datasource_priority()
        body = await self._request(method="GET", path="/datasources/priority")
        return list(body.get("priority", []))

    async def set_datasource_priority(self, priority: list[str]) -> None:
        if self._storage is not None:
            await self._storage.set_datasource_priority(list(priority))
            return
        await self._request(
            method="PUT",
            path="/datasources/priority",
            json={"priority": list(priority)},
        )

    # ----- Semantic models -----

    async def list_semantic_models(self, data_source: str | None = None) -> list[dict[str, str]]:
        """Every stored semantic model as ``{data_source, name}``."""
        if self._storage is not None:
            return [{"data_source": ds, "name": n}
                    for ds, n in await self._storage.list_semantic_models(data_source=data_source)]
        params = {"data_source": data_source} if data_source else None
        return await self._request(method="GET", path="/semantic-models", params=params)

    async def get_semantic_model(self, name: str, *, data_source: str) -> str | None:
        """A semantic model's text as stored; ``None`` when absent."""
        if self._storage is not None:
            return await self._storage.get_semantic_model_text(name, data_source=data_source)
        found = await self._request_or_none(method="GET", path=_semantic_model_path(data_source=data_source, name=name))
        return found["text"] if found is not None else None

    async def create_semantic_model(
        self, *, data_source: str, name: str | None = None, content: str | dict[str, Any] | None = None,
        datasets: list[str] | None = None,
    ) -> None:
        """Create a semantic model from Ossie ``content``, or from standalone ``datasets`` moved into it."""
        if self._storage is not None:
            await self._storage.create_semantic_model(data_source=data_source, name=name, content=content,
                                                      datasets=datasets)
            return
        await self._request(method="POST", path="/semantic-models", json={
            "data_source": data_source, "name": name, "content": content, "datasets": datasets})

    async def replace_semantic_model(self, name: str, *, data_source: str, content: str | dict[str, Any]) -> None:
        """Replace a semantic model's text."""
        if self._storage is not None:
            text = content if isinstance(content, str) else yaml.safe_dump(content, sort_keys=False)
            await self._storage.save_semantic_model_text(data_source=data_source, name=name, text=text)
            return
        await self._request(method="PUT", path=_semantic_model_path(data_source=data_source, name=name), json={"content": content})

    async def delete_semantic_model(self, name: str, *, data_source: str) -> bool:
        """Delete a semantic model and its datasets; ``False`` when absent."""
        if self._storage is not None:
            return await self._storage.delete_semantic_model(name, data_source=data_source)
        return await self._request_or_none(method="DELETE", path=_semantic_model_path(data_source=data_source, name=name)) is not None

    def list_semantic_models_sync(self, data_source: str | None = None) -> list[dict[str, str]]:
        return self._run_sync(self.list_semantic_models(data_source=data_source))

    def get_semantic_model_sync(self, name: str, *, data_source: str) -> str | None:
        return self._run_sync(self.get_semantic_model(name, data_source=data_source))

    def create_semantic_model_sync(
        self, *, data_source: str, name: str | None = None, content: str | dict[str, Any] | None = None,
        datasets: list[str] | None = None,
    ) -> None:
        self._run_sync(self.create_semantic_model(data_source=data_source, name=name, content=content,
                                                  datasets=datasets))

    def replace_semantic_model_sync(self, name: str, *, data_source: str, content: str | dict[str, Any]) -> None:
        self._run_sync(self.replace_semantic_model(name, data_source=data_source, content=content))

    def delete_semantic_model_sync(self, name: str, *, data_source: str) -> bool:
        return self._run_sync(self.delete_semantic_model(name, data_source=data_source))

    # ----- Memory API -----

    def _memory_service(self):
        if self._storage is None:
            raise RuntimeError(
                "Memory operations need a storage backend; remote-mode "
                "callers go through the HTTP code path."
            )
        return MemoryService(storage=self._storage)

    @staticmethod
    def _coerce_linked_entities(value):
        # SlayerQuery → dict; lists/dicts pass through (service revalidates).
        if isinstance(value, SlayerQuery):
            return value.model_dump(mode="json", exclude_none=True)
        return value

    async def save_memory(
        self,
        *,
        learning: str,
        linked_entities: list[str] | SlayerQuery | dict[str, Any],
        id: str | None = None,  # noqa: A002 — public kwarg matching MCP / REST
        description: str | None = None,
    ) -> SaveMemoryResponse:
        """Save a memory (learning + linked entities, or an inline SlayerQuery
        to extract them from). ``id`` pins the canonical id; ``description`` is
        a ≤500-char preview surfaced by search/inspect."""
        if self._storage is not None:
            # A prebuilt SlayerQuery goes through as is, so the service reports its input deprecations.
            return await self._memory_service().save_memory(
                learning=learning, linked_entities=linked_entities, id=id, description=description,
            )
        body: dict[str, Any] = {
            "learning": learning,
            "linked_entities": self._coerce_linked_entities(linked_entities),
        }
        if id is not None:
            body["id"] = id
        if description is not None:
            body["description"] = description
        result = await self._request(method="POST", path="/memories", json=body)
        response = SaveMemoryResponse.model_validate(result)
        if isinstance(linked_entities, SlayerQuery):
            # The server only sees the converted form; the deprecations are the client's to report.
            response.warnings.extend(p.human_message() for p in linked_entities.deprecated_inputs)
        return response

    async def forget_memory(
        self, identifier: int | str
    ) -> ForgetMemoryResponse:
        if self._storage is not None:
            return await self._memory_service().forget_memory(
                identifier=identifier
            )
        # Percent-encode: ids are arbitrary strings that may hold reserved
        # URL characters.
        encoded = quote(str(identifier), safe="")
        result = await self._request(
            method="DELETE", path=f"/memories/{encoded}",
        )
        return ForgetMemoryResponse.model_validate(result)

    # ----- Search API -----

    async def search(
        self,
        *,
        entities: list[str] | None = None,
        query: SlayerQuery | dict[str, Any] | None = None,
        question: str | None = None,
        datasource: str | None = None,
        max_results: int = 10,
        cypher_filter: str | None = None,
        compact: bool = True,
    ) -> "SearchResponse":
        """Up-to-three-channel semantic search (BM25 + full-text + optional
        embeddings, RRF-fused) over memories and canonical entities.
        ``datasource`` scopes results to one datasource; an unknown one raises
        ``ValueError`` in local mode, HTTP 400 in remote mode."""
        coerced_query: Any = None
        if query is not None:
            coerced_query = (
                query.model_dump(mode="json", exclude_none=True)
                if isinstance(query, SlayerQuery) else query
            )
        if self._storage is not None:
            return await SearchService(storage=self._storage).search(
                entities=entities,
                query=coerced_query,
                question=question,
                datasource=datasource,
                max_results=max_results,
                cypher_filter=cypher_filter,
                compact=compact,
            )
        body: dict[str, Any] = {
            "max_results": max_results,
            "compact": compact,
        }
        if entities is not None:
            body["entities"] = entities
        if coerced_query is not None:
            body["query"] = coerced_query
        if question is not None:
            body["question"] = question
        if datasource is not None:
            body["datasource"] = datasource
        if cypher_filter is not None:
            body["cypher_filter"] = cypher_filter
        result = await self._request(method="POST", path="/search", json=body)
        return SearchResponse.model_validate(result)

    async def inspect(
        self,
        *,
        reference: str | list[str] | None = None,
        entity_type: str,
        compact: bool = True,
        format: str = "markdown",
        num_rows: int = 3,
        show_sql: bool = False,
        sections: list[str] | None = None,
        descriptions_max_chars: int | None = None,
    ) -> str:
        """Point-lookup one entity, a same-kind batch (``reference`` a list), or
        a whole collection (``reference`` ``None``/``[]``; ``dataset``/
        ``datasource`` only). ``entity_type`` is required and applies to every
        id; a list returns one block per id with per-id error isolation."""
        if self._storage is not None:
            return await InspectService(
                storage=self._storage, engine=self._engine,
            ).inspect(
                reference=reference,
                entity_type=entity_type,
                compact=compact,
                format=format,
                num_rows=num_rows,
                show_sql=show_sql,
                sections=sections,
                descriptions_max_chars=descriptions_max_chars,
            )
        body = self._build_inspect_body(
            reference=reference,
            entity_type=entity_type,
            compact=compact,
            format=format,
            num_rows=num_rows,
            show_sql=show_sql,
            sections=sections,
            descriptions_max_chars=descriptions_max_chars,
        )
        resp = await self._request(method="POST", path="/inspect", json=body)
        return resp["result"]

    @staticmethod
    def _build_inspect_body(
        *,
        reference: str | list[str] | None,
        entity_type: str,
        compact: bool,
        format: str,
        num_rows: int,
        show_sql: bool,
        sections: list[str] | None,
        descriptions_max_chars: int | None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "reference": reference,
            "entity_type": entity_type,
            "compact": compact,
            "format": format,
            "num_rows": num_rows,
            "show_sql": show_sql,
        }
        if sections is not None:
            body["sections"] = sections
        if descriptions_max_chars is not None:
            body["descriptions_max_chars"] = descriptions_max_chars
        return body

    # ----- Sync API (for notebooks, scripts, CLI) -----

    def query_sync(
        self,
        query: QueryInput,
        *,
        refine: Refinement = None,
        variables: Variables = None,
        dry_run: bool = False,
        explain: bool = False,
        conformance: Conformance | str = Conformance.SLAYER,
    ) -> SlayerResponse:
        """Execute a query synchronously (same arguments as ``query``)."""
        if self._engine is not None:
            return self._engine.execute_sync(
                query=self._normalize_for_engine(query),
                variables=self._normalize_variables(variables),
                dry_run=dry_run,
                explain=explain,
                refine=self._normalize_refine(refine),
                conformance=conformance,
            )
        if parse_conformance(conformance) is Conformance.OSSIE_L3:
            body = self._strict_body(query, refine=refine, variables=variables, dry_run=dry_run, explain=explain)
            return self._parse_response(self._request_sync(method="POST", path=_QUERY_PATH, json=body,
                                                           params={"conformance": Conformance.OSSIE_L3.value}))
        deprecations: list[DeprecatedInputWarning] = []
        body = self._build_query_body(
            query, refine=refine, variables=variables, dry_run=dry_run, explain=explain,
            deprecations_out=deprecations,
        )
        result = self._request_sync(method="POST", path=_QUERY_PATH, json=body)
        return self._parse_response(result, deprecations=deprecations)

    def sql_sync(
        self, query: QueryInput, *, refine: Refinement = None, variables: Variables = None,
        conformance: Conformance | str = Conformance.SLAYER,
    ) -> str:
        """Generate SQL synchronously (same input union)."""
        sql = self.query_sync(query=query, refine=refine, variables=variables, dry_run=True,
                              conformance=conformance).sql
        assert sql is not None  # a dry run always renders SQL
        return sql

    def explain_sync(
        self, query: QueryInput, *, refine: Refinement = None, variables: Variables = None,
        conformance: Conformance | str = Conformance.SLAYER,
    ) -> SlayerResponse:
        """Run the database's EXPLAIN synchronously (same input union)."""
        return self.query_sync(query=query, refine=refine, variables=variables, explain=True, conformance=conformance)

    def inspect_sync(
        self,
        *,
        reference: str | list[str] | None = None,
        entity_type: str,
        compact: bool = True,
        format: str = "markdown",
        num_rows: int = 3,
        show_sql: bool = False,
        sections: list[str] | None = None,
        descriptions_max_chars: int | None = None,
    ) -> str:
        """Synchronous variant of :meth:`inspect`."""
        if self._storage is not None:
            return self._run_sync(self.inspect(
                reference=reference,
                entity_type=entity_type,
                compact=compact,
                format=format,
                num_rows=num_rows,
                show_sql=show_sql,
                sections=sections,
                descriptions_max_chars=descriptions_max_chars,
            ))
        body = self._build_inspect_body(
            reference=reference,
            entity_type=entity_type,
            compact=compact,
            format=format,
            num_rows=num_rows,
            show_sql=show_sql,
            sections=sections,
            descriptions_max_chars=descriptions_max_chars,
        )
        resp = self._request_sync(method="POST", path="/inspect", json=body)
        return resp["result"]

    async def recommend_population(
        self, items: list[str], *, data_source: str | None = None,
        root_hint: str | None = None,
    ) -> "PopulationRecommendation":
        """Recommend the query root (``population``) for a set of
        ``dataset.field``/``dataset.metric`` items, plus each item's path from it.
        ``root_hint`` forces the root when it reaches every item."""
        if self._engine is not None:
            return await self._engine.recommend_population(
                items, data_source=data_source, root_hint=root_hint
            )
        body: dict[str, Any] = {"items": items}
        if data_source is not None:
            body["data_source"] = data_source
        if root_hint is not None:
            body["root_hint"] = root_hint
        result = await self._request(
            method="POST", path="/recommend-population", json=body
        )
        return PopulationRecommendation.model_validate(result)

    def recommend_population_sync(
        self, items: list[str], *, data_source: str | None = None,
        root_hint: str | None = None,
    ) -> "PopulationRecommendation":
        """Synchronous variant of :meth:`recommend_population`."""
        return self._run_sync(self.recommend_population(
            items, data_source=data_source, root_hint=root_hint
        ))

    def query_df(self, query: QueryInput, *, refine: Refinement = None, variables: Variables = None):
        """Execute a query and return a pandas DataFrame (sync; same input union)."""
        try:
            import pandas as pd  # ALLOW(import-not-top): heavy optional dep, only this method needs it
        except ImportError as e:
            raise ImportError("DataFrame support requires pandas: pip install motley-slayer[client]") from e
        result = self.query_sync(query=query, refine=refine, variables=variables)
        return pd.DataFrame(result.data)

    def list_datasets_sync(self) -> list[str]:
        return self._request_sync(method="GET", path="/datasets")

    def get_dataset_sync(self, name: str) -> dict[str, Any]:
        return self._request_sync("GET", f"/datasets/{name}")

    @method_twin("list_datasets")
    @deprecated("list_models is deprecated; use list_datasets", category=None)
    async def list_models(self, data_source: str | None = None) -> list[str]: ...

    @method_twin("list_datasets_sync")
    @deprecated("list_models_sync is deprecated; use list_datasets_sync", category=None)
    def list_models_sync(self) -> list[str]: ...

    @method_twin("get_dataset")
    @deprecated("get_model is deprecated; use get_dataset", category=None)
    async def get_model(self, name: str, data_source: str | None = None) -> Any | None: ...

    @method_twin("get_dataset_sync")
    @deprecated("get_model_sync is deprecated; use get_dataset_sync", category=None)
    def get_model_sync(self, name: str) -> dict[str, Any]: ...

    @method_twin("create_dataset", arguments={"model": "dataset"})
    @deprecated("create_model is deprecated; use create_dataset", category=None)
    async def create_model(self, dataset: dict[str, Any]) -> dict[str, Any]: ...

    @method_twin("recommend_population")
    @deprecated("recommend_root_model is deprecated; use recommend_population", category=None)
    async def recommend_root_model(
        self, items: list[str], *, data_source: str | None = None, root_hint: str | None = None,
    ) -> "PopulationRecommendation": ...

    @method_twin("recommend_population_sync")
    @deprecated("recommend_root_model_sync is deprecated; use recommend_population_sync", category=None)
    def recommend_root_model_sync(
        self, items: list[str], *, data_source: str | None = None, root_hint: str | None = None,
    ) -> "PopulationRecommendation": ...
