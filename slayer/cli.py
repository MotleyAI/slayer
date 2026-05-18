"""CLI entry point for SLayer."""

import argparse
import copy
import json
import os
import sys
from typing import List, Optional

from pydantic import BaseModel, Field

from slayer.async_utils import run_sync
from slayer.core.errors import (
    AmbiguousModelError,
    EntityResolutionError,
    MemoryNotFoundError,
)
from slayer.core.models import SlayerModel
from slayer.engine.ingestion import (
    _print_ingest_addition,
    _print_ingest_drift_and_errors,
)
from slayer.engine.profiling import (
    refresh_all_table_backed_sampled,
    refresh_table_backed_model_sampled,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.service import SearchService
from slayer.storage import migrations as _mig
from slayer.storage.base import default_storage_path
from slayer.storage.type_refinement import (
    has_refineable_columns,
    refine_dict_with_live_schema,
)

_STORAGE_DEFAULT = default_storage_path()
_STORAGE_HELP = (
    "Storage path: directory for YAML storage, or .db/.sqlite file for SQLite storage "
    f"(default: {_STORAGE_DEFAULT})"
)
_INGEST_ON_STARTUP_HELP = (
    "Walk every configured datasource and run idempotent auto-ingestion before "
    "starting the server. Per-datasource errors are logged to stderr and never "
    "abort startup. See docs/concepts/ingestion.md."
)


def _env_ingest_on_startup() -> bool:
    """Truthy check for the ``SLAYER_INGEST_ON_STARTUP`` env var.

    Truthy values (case-insensitive, with surrounding whitespace stripped):
    ``1``, ``true``, ``yes``. Anything else — including unset, empty,
    ``0``, ``false``, ``no``, ``garbage`` — returns False.
    """
    return os.environ.get("SLAYER_INGEST_ON_STARTUP", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


class RefreshSamplesResult(BaseModel):
    """Result envelope for ``slayer search refresh-samples``. ``errors``
    accumulates per-column profile / persist failures (best-effort);
    ``unresolved_models`` lists any user-specified ``--model`` names
    that didn't resolve in the requested scope — those are reported as
    a hard error so typos fail fast."""

    errors: List[str] = Field(default_factory=list)
    unresolved_models: List[str] = Field(default_factory=list)


def _add_storage_arg(parser):
    """Add --storage and legacy --models-dir flags to a parser."""
    parser.add_argument("--storage", default=None, help=_STORAGE_HELP)
    parser.add_argument(
        "--models-dir",
        default=None,
        help="(deprecated, use --storage) Path to YAML models directory",
    )


def _resolve_storage(args):
    """Resolve storage backend from --storage or --models-dir flags."""
    from slayer.storage.base import resolve_storage

    path = args.storage or args.models_dir or _STORAGE_DEFAULT
    return resolve_storage(path)


def main():
    parser = argparse.ArgumentParser(
        prog="slayer",
        description="SLayer — a lightweight semantic layer for AI agents",
        epilog="""\
common workflows:
  # 1. Create a datasource config, ingest models, start the server
  slayer ingest --datasource my_postgres
  slayer serve

  # 2. Query from the command line
  slayer query '{"source_model": "orders", "measures": [{"formula": "*:count"}]}'

  # 3. Start the MCP server for AI agents
  slayer mcp

  # 4. Use SQLite storage instead of YAML files
  slayer serve --storage slayer.db
  slayer ingest --datasource my_pg --storage slayer.db

docs: https://motley-slayer.readthedocs.io/
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    # ── serve ─────────────────────────────────────────────────────────
    serve_parser = subparsers.add_parser(
        "serve",
        help="Start the REST API server",
        epilog="""\
examples:
  slayer serve
  slayer serve --port 8080 --storage ./my_data
  slayer serve --storage slayer.db

  # Instant demo: auto-ingest the bundled Jaffle Shop dataset, then serve
  slayer serve --demo
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    serve_parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    serve_parser.add_argument("--port", type=int, default=5143, help="Port number (default: 5143)")
    serve_parser.add_argument(
        "--demo",
        action="store_true",
        help="Generate and ingest the bundled Jaffle Shop demo dataset before starting (idempotent).",
    )
    serve_parser.add_argument(
        "--ingest-on-startup",
        action="store_true",
        help=_INGEST_ON_STARTUP_HELP,
    )
    _add_storage_arg(serve_parser)

    # ── flight-serve ──────────────────────────────────────────────────
    # DEV-1390: Arrow Flight SQL endpoint, wire-compatible with the
    # dbt Semantic Layer JDBC driver.
    from slayer.flight.cli import add_flight_serve_subparser
    add_flight_serve_subparser(subparsers)
    # Storage flag is shared with the rest of the subcommands.
    flight_parser = subparsers._name_parser_map["flight-serve"]
    _add_storage_arg(flight_parser)

    # ── mcp ───────────────────────────────────────────────────────────
    mcp_parser = subparsers.add_parser(
        "mcp",
        help="Start the MCP server (stdio transport for AI agents)",
        epilog="""\
examples:
  slayer mcp
  slayer mcp --storage slayer.db

  # Add to Claude Code:
  claude mcp add slayer -- slayer mcp --storage ./slayer_data

  # Instant demo: auto-ingest the bundled Jaffle Shop dataset, then serve over MCP
  slayer mcp --demo
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mcp_parser.add_argument(
        "--demo",
        action="store_true",
        help="Generate and ingest the bundled Jaffle Shop demo dataset before starting (idempotent).",
    )
    mcp_parser.add_argument(
        "--ingest-on-startup",
        action="store_true",
        help=_INGEST_ON_STARTUP_HELP,
    )
    _add_storage_arg(mcp_parser)

    # ── query ─────────────────────────────────────────────────────────
    query_parser = subparsers.add_parser(
        "query",
        help="Execute a query from JSON",
        epilog="""\
examples:
  # Inline JSON
  slayer query '{"source_model": "orders", "measures": [{"formula": "*:count"}]}'

  # From a file
  slayer query @query.json

  # Preview SQL without executing
  slayer query '{"source_model": "orders", "measures": [{"formula": "*:count"}]}' --dry-run

  # Show execution plan
  slayer query @query.json --explain

  # Output as JSON
  slayer query @query.json --format json
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    query_parser.add_argument(
        "query_json",
        help=(
            "JSON query (e.g. '{\"source_model\": ...}'), @file.json to read "
            "from a file, or a model name to run a query-backed model's "
            "stored backing query."
        ),
    )
    _add_storage_arg(query_parser)
    query_parser.add_argument(
        "--format",
        choices=["json", "table"],
        default="table",
        help="Output format (default: table)",
    )
    query_parser.add_argument("--dry-run", action="store_true", help="Generate SQL without executing")
    query_parser.add_argument("--explain", action="store_true", help="Run EXPLAIN ANALYZE on the query")
    query_parser.add_argument(
        "--variables",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help=(
            "Set a runtime variable (repeatable). Overrides query.variables and "
            "model.query_variables. Example: --variables threshold=100 --variables region=US."
        ),
    )
    query_parser.add_argument(
        "--variables-json",
        default=None,
        help="Set runtime variables from a JSON object string. Mutually exclusive with --variables.",
    )

    # ── ingest ────────────────────────────────────────────────────────
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Auto-ingest models from a datasource",
        epilog="""\
examples:
  slayer ingest --datasource my_postgres
  slayer ingest --datasource my_postgres --schema public
  slayer ingest --datasource my_postgres --include orders,customers
  slayer ingest --datasource my_postgres --exclude migrations,django_session
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ingest_parser.add_argument("--datasource", required=True, help="Name of the datasource to ingest from")
    ingest_parser.add_argument("--schema", default=None, help="Database schema to introspect (e.g., public)")
    ingest_parser.add_argument(
        "--include",
        default=None,
        help="Comma-separated list of tables to include (default: all)",
    )
    ingest_parser.add_argument(
        "--exclude",
        default=None,
        help="Comma-separated list of tables to exclude",
    )
    _add_storage_arg(ingest_parser)

    # ── validate-models ───────────────────────────────────────────────
    validate_parser = subparsers.add_parser(
        "validate-models",
        help="Diff persisted models against live DB schemas (read-only)",
        epilog="""\
examples:
  slayer validate-models                      # check every datasource
  slayer validate-models --datasource my_pg
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    validate_parser.add_argument(
        "--datasource",
        default=None,
        help="Datasource name. If omitted, every datasource is validated.",
    )
    validate_parser.add_argument(
        "--force-clean",
        action="store_true",
        help=(
            "After printing the diff, prompt to apply each delete via "
            "edit_model / delete_model. Destructive; opt-in only."
        ),
    )
    validate_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="With --force-clean, skip the confirmation prompt.",
    )
    _add_storage_arg(validate_parser)

    # ── import-dbt ────────────────────────────────────────────────────
    import_dbt_parser = subparsers.add_parser(
        "import-dbt",
        help="Import dbt semantic layer definitions into SLayer models",
        epilog="""\
examples:
  slayer import-dbt ./my_dbt_project --datasource my_postgres
  slayer import-dbt ./my_dbt_project/models --datasource my_postgres --storage ./slayer_data
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    import_dbt_parser.add_argument("dbt_project_path", help="Path to dbt project root or models directory")
    import_dbt_parser.add_argument("--datasource", required=True, help="SLayer datasource name for the imported models")
    import_dbt_parser.add_argument(
        "--include-hidden-models",
        action="store_true",
        help=(
            "Also import regular dbt models (those not wrapped by a semantic_model) "
            "as hidden SLayer models via SQL introspection. Requires dbt-core "
            "(pip install 'motley-slayer[dbt]') and a working connection on --datasource."
        ),
    )
    _add_storage_arg(import_dbt_parser)

    # ── models ────────────────────────────────────────────────────────
    models_parser = subparsers.add_parser(
        "models",
        help="Manage models",
        epilog="""\
examples:
  slayer models list
  slayer models show orders
  slayer models create model.yaml
  slayer models delete old_model
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_storage_arg(models_parser)
    models_subparsers = models_parser.add_subparsers(dest="models_command")

    models_subparsers.add_parser("list", help="List all models")

    models_show_parser = models_subparsers.add_parser("show", help="Show a model definition (YAML)")
    models_show_parser.add_argument("name", help="Model name")

    models_create_parser = models_subparsers.add_parser("create", help="Create a model from a YAML file")
    models_create_parser.add_argument("file", help="Path to YAML model definition")

    models_delete_parser = models_subparsers.add_parser("delete", help="Delete a model")
    models_delete_parser.add_argument("name", help="Model name")

    # ── datasources ───────────────────────────────────────────────────
    datasources_parser = subparsers.add_parser(
        "datasources",
        help="Manage datasources",
        epilog="""\
examples:
  slayer datasources list
  slayer datasources show my_postgres

  # Create from a connection string (name derived from the URL)
  slayer datasources create postgresql://user:${DB_PASSWORD}@localhost/analytics

  # Create and immediately ingest models from the schema
  slayer datasources create postgresql://localhost/analytics --ingest

  # SQLite / DuckDB (filename stem used as the name)
  slayer datasources create sqlite:///path/to/app.db --ingest

  # Override the auto-derived name
  slayer datasources create duckdb:///tmp/data.duckdb --name warehouse --ingest

  # Spin up the bundled Jaffle Shop demo DuckDB (idempotent — safe to re-run)
  slayer datasources create demo --ingest

  slayer datasources delete my_postgres
  slayer datasources test my_postgres
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_storage_arg(datasources_parser)
    datasources_subparsers = datasources_parser.add_subparsers(dest="datasources_command")

    datasources_subparsers.add_parser("list", help="List all datasources")

    datasources_show_parser = datasources_subparsers.add_parser(
        "show", help="Show datasource config (passwords masked)"
    )
    datasources_show_parser.add_argument("name", help="Datasource name")

    datasources_create_parser = datasources_subparsers.add_parser(
        "create",
        help="Create a datasource from a connection string",
    )
    datasources_create_parser.add_argument(
        "connection_string",
        help="Database connection URL, e.g. postgresql://user:pass@host/db or sqlite:///path/to/file.db. "
        "${ENV_VAR} references are resolved at use time. "
        "Pass the literal 'demo' to spin up the bundled Jaffle Shop demo dataset.",
    )
    datasources_create_parser.add_argument(
        "--name",
        default=None,
        help="Datasource name (default: derived from the database portion of the URL)",
    )
    datasources_create_parser.add_argument(
        "--description", default=None, help="Human-readable description"
    )
    datasources_create_parser.add_argument(
        "--ingest",
        action="store_true",
        help="Run auto-ingestion immediately after creating the datasource",
    )
    datasources_create_parser.add_argument(
        "--schema", default=None, help="(with --ingest) Schema to ingest from"
    )
    datasources_create_parser.add_argument(
        "--include",
        default=None,
        help="(with --ingest) Comma-separated list of tables to include",
    )
    datasources_create_parser.add_argument(
        "--exclude",
        default=None,
        help="(with --ingest) Comma-separated list of tables to exclude",
    )
    datasources_create_parser.add_argument(
        "--years",
        type=int,
        default=2,
        help="(demo only) Years of synthetic data to generate (default: 2)",
    )
    datasources_create_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Overwrite existing datasource / colliding models without prompting",
    )

    datasources_delete_parser = datasources_subparsers.add_parser("delete", help="Delete a datasource")
    datasources_delete_parser.add_argument("name", help="Datasource name")

    datasources_test_parser = datasources_subparsers.add_parser("test", help="Test datasource connectivity")
    datasources_test_parser.add_argument("name", help="Datasource name")

    # ── memory ────────────────────────────────────────────────────────
    memory_parser = subparsers.add_parser(
        "memory",
        help="Manage agent memories (write side: save / forget)",
        epilog="""\
examples:
  # Save a learning indexed by entities
  slayer memory save --learning "amount is in cents" --entities mydb.orders.amount

  # Save a learning that comes with an example SlayerQuery
  slayer memory save --learning "Paid revenue" --query @paid_revenue.json

  # Forget a memory by id
  slayer memory forget 42

  # For retrieval, use `slayer search` (memories + canonical entities).
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_storage_arg(memory_parser)
    memory_subparsers = memory_parser.add_subparsers(dest="memory_command")

    memory_save_parser = memory_subparsers.add_parser(
        "save",
        help="Save a memory (learning + linked entities or example query)",
    )
    memory_save_parser.add_argument(
        "--learning",
        required=True,
        help="The free-form note text. Required.",
    )
    memory_save_parser.add_argument(
        "--entities",
        default=None,
        help="Comma-separated list of entity references (e.g. mydb.orders.amount).",
    )
    memory_save_parser.add_argument(
        "--query",
        default=None,
        help="Inline JSON SlayerQuery (or @file.json) to extract entities from and persist alongside the learning.",
    )

    memory_forget_parser = memory_subparsers.add_parser(
        "forget", help="Delete a memory by id"
    )
    memory_forget_parser.add_argument("id", type=int, help="Memory id (positive int)")

    # ── storage ──────────────────────────────────────────────────────
    storage_parser = subparsers.add_parser(
        "storage",
        help="Storage maintenance (DEV-1361: migrate-types refines DOUBLE→INT for legacy models)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    storage_subparsers = storage_parser.add_subparsers(dest="subcommand")

    migrate_types_parser = storage_subparsers.add_parser(
        "migrate-types",
        help=(
            "Walk every persisted model, introspect its datasource, and refine "
            "DOUBLE → INT on base columns whose live SQL type is integer. "
            "Hard-fails if a datasource is unreachable."
        ),
    )
    migrate_types_parser.add_argument(
        "--data-source",
        default=None,
        dest="data_source",
        help="Optional datasource filter; defaults to all datasources.",
    )
    migrate_types_parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Report planned refinements without writing them back to storage.",
    )
    _add_storage_arg(migrate_types_parser)

    # ── search (DEV-1375) ────────────────────────────────────────────
    search_parser = subparsers.add_parser(
        "search",
        help="Semantic search over memories + canonical entities (DEV-1375)",
        epilog="""\
examples:
  # Two-channel search by entity overlap + tantivy full-text
  poetry run slayer search --entity mydb.orders.amount_paid --question "paid revenue"

  # Refresh persisted Column.sampled values for every table-backed model
  poetry run slayer search refresh-samples --data-source mydb
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_storage_arg(search_parser)
    search_subparsers = search_parser.add_subparsers(dest="search_command")
    # ``slayer search`` (no subcommand) runs the search query directly.
    search_parser.add_argument(
        "--entity",
        action="append",
        default=None,
        dest="entities",
        help="Canonical entity reference(s) (repeatable).",
    )
    search_parser.add_argument(
        "--query",
        default=None,
        help="Inline JSON SlayerQuery (or @file.json) to extract entities from.",
    )
    search_parser.add_argument(
        "--question",
        default=None,
        help="Free-text query for the tantivy full-text channel.",
    )
    search_parser.add_argument(
        "--datasource",
        default=None,
        help=(
            "Scope memories + entities to this datasource. Entity hits "
            "limited to docs rooted at the datasource (exact or dotted "
            "descendant); memories surface when any tagged entity is "
            "rooted there."
        ),
    )
    search_parser.add_argument(
        "--max-memories",
        type=int,
        default=5,
        dest="max_memories",
        help="Cap on returned learning-only memory hits (default 5).",
    )
    search_parser.add_argument(
        "--max-example-queries",
        type=int,
        default=2,
        dest="max_example_queries",
        help="Cap on returned query-bearing memory hits (default 2 — bulky).",
    )
    search_parser.add_argument(
        "--max-entities",
        type=int,
        default=5,
        dest="max_entities",
        help="Cap on returned entity hits (default 5).",
    )
    search_parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="text",
        help="Output format (default: text).",
    )
    refresh_parser = search_subparsers.add_parser(
        "refresh-samples",
        help="Re-profile and persist Column.sampled for table-backed models.",
    )
    _add_storage_arg(refresh_parser)
    refresh_parser.add_argument(
        "--data-source",
        default=None,
        dest="data_source",
        help="Limit refresh to one datasource (default: all).",
    )
    refresh_parser.add_argument(
        "--model",
        action="append",
        default=None,
        dest="models",
        help="Model name(s) to refresh (repeatable; default: all in scope).",
    )

    # ── help ──────────────────────────────────────────────────────────
    from slayer.help import TOPIC_SUMMARY_LINE

    help_parser = subparsers.add_parser(
        "help",
        help="Show conceptual help on SLayer (concepts, query composition, transforms, joins, workflow)",
        epilog=(
            f"{TOPIC_SUMMARY_LINE}\n\n"
            "examples:\n"
            "  slayer help                  # intro\n"
            "  slayer help queries          # deep dive on a topic\n"
            "  slayer help transforms\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    help_parser.add_argument(
        "topic",
        nargs="?",
        default=None,
        help="Topic name (optional). If omitted, prints the intro.",
    )

    args = parser.parse_args()

    if args.command == "serve":
        _run_serve(args)
    elif args.command == "flight-serve":
        from slayer.flight.cli import run_flight_serve
        run_flight_serve(args, resolve_storage=_resolve_storage, prepare_demo=_prepare_demo)
    elif args.command == "mcp":
        _run_mcp(args)
    elif args.command == "query":
        _run_query(args)
    elif args.command == "ingest":
        _run_ingest(args)
    elif args.command == "validate-models":
        _run_validate_models(args)
    elif args.command == "import-dbt":
        _run_import_dbt(args)
    elif args.command == "models":
        _run_models(args)
    elif args.command == "datasources":
        _run_datasources(args)
    elif args.command == "memory":
        _run_memory(args)
    elif args.command == "search":
        _run_search(args)
    elif args.command == "storage":
        _run_storage(args)
    elif args.command == "help":
        _run_help(args)
    else:
        parser.print_help()
        sys.exit(1)


def _run_search(args) -> None:
    """Dispatch ``slayer search [...]`` and ``slayer search refresh-samples``."""
    storage = _resolve_storage(args)
    sub = getattr(args, "search_command", None)
    if sub == "refresh-samples":
        _run_search_refresh_samples(args=args, storage=storage)
        return
    _run_search_query(args=args, storage=storage)


async def _refresh_samples_async(*, args, storage) -> "RefreshSamplesResult":
    """Async core of ``slayer search refresh-samples`` — loop datasources
    and (optional) model filters, accumulate per-column errors and the
    names of any user-specified models that didn't resolve."""
    engine = SlayerQueryEngine(storage=storage)
    errors: List[str] = []
    unresolved_models: List[str] = []
    data_source = args.data_source
    models = args.models
    if data_source is None:
        datasources = await storage.list_datasources()
    else:
        datasources = [data_source]
    for ds in datasources:
        if models:
            for model_name in models:
                m = await storage.get_model(model_name, data_source=ds)
                if m is None:
                    unresolved_models.append(f"{ds}.{model_name}")
                    continue
                errs = await refresh_table_backed_model_sampled(
                    model=m, engine=engine, storage=storage,
                )
                errors.extend(errs)
        else:
            errors.extend(
                await refresh_all_table_backed_sampled(
                    engine=engine, storage=storage, data_source=ds,
                )
            )
    return RefreshSamplesResult(errors=errors, unresolved_models=unresolved_models)


def _run_search_refresh_samples(*, args, storage) -> None:
    """``slayer search refresh-samples`` — re-profile + persist
    ``Column.sampled`` for every table-backed model in scope. Exits
    non-zero on unresolved user-specified ``--model`` names so typos
    don't masquerade as a clean run.

    Honors the shared ``--format`` flag: ``json`` emits the
    ``RefreshSamplesResult`` envelope; otherwise the human-readable
    text path runs.
    """
    result = run_sync(_refresh_samples_async(args=args, storage=storage))
    fmt = getattr(args, "format", "text")
    if fmt == "json":
        print(result.model_dump_json(indent=2))
        if result.unresolved_models:
            sys.exit(1)
        return
    if result.unresolved_models:
        print(
            "Sample-value refresh: requested model(s) not found in scope:",
            file=sys.stderr,
        )
        for m in result.unresolved_models:
            print(f"  - {m}", file=sys.stderr)
        sys.exit(1)
    if result.errors:
        print("Sample-value refresh completed with errors:")
        for e in result.errors:
            print(f"  - {e}")
    else:
        print("Sample-value refresh completed successfully.")


def _print_search_response_text(response) -> None:
    """Pretty-print a ``SearchResponse`` for the default text format."""
    for w in response.warnings:
        print(f"[warning] {w}")
    if response.resolved_input_entities:
        print(
            "\nResolved input entities: "
            + ", ".join(response.resolved_input_entities)
        )
    print(f"\nMemories ({len(response.memories)}):")
    for hit in response.memories:
        print(f"  M{hit.id} (score={hit.score:.4f})")
        print(f"    {hit.text.splitlines()[0] if hit.text else ''}")
    print(f"\nExample queries ({len(response.example_queries)}):")
    for hit in response.example_queries:
        print(f"  M{hit.id} (score={hit.score:.4f})")
        print(f"    {hit.text.splitlines()[0] if hit.text else ''}")
    print(f"\nEntities ({len(response.entities)}):")
    for hit in response.entities:
        print(f"  [{hit.kind}] {hit.id} (score={hit.score:.4f})")


def _run_search_query(args, storage) -> None:
    """``slayer search [...]`` — call the SearchService and emit JSON or
    pretty text."""
    service = SearchService(storage=storage)
    query_input = _load_query_arg(args.query) if args.query else None
    try:
        response = run_sync(service.search(
            entities=args.entities,
            query=query_input,
            question=args.question,
            datasource=args.datasource,
            max_memories=args.max_memories,
            max_example_queries=args.max_example_queries,
            max_entities=args.max_entities,
        ))
    except (EntityResolutionError, AmbiguousModelError, ValueError) as exc:
        _exit_with_error(exc)
        return
    if args.format == "json":
        print(response.model_dump_json(indent=2))
    else:
        _print_search_response_text(response)


def _run_storage(args) -> None:
    """Dispatch the ``slayer storage <subcommand>`` group."""
    subcommand = getattr(args, "subcommand", None)
    if subcommand == "migrate-types":
        _run_storage_migrate_types(args)
    else:
        print(
            "Usage: slayer storage <subcommand>\n  available: migrate-types"
        )
        sys.exit(1)


def _run_storage_migrate_types(args) -> None:
    """DEV-1361: refine DOUBLE → INT on every base column whose live SQL
    type is integer. Iterates models in storage, calls
    ``refine_dict_with_live_schema`` per model, optionally writes the
    refined v5 dict back. Hard-fails if a datasource is unreachable.
    """
    storage = _resolve_storage(args)
    # Unwrap JoinSyncStorage so we can list identities and read raw dicts
    # without triggering the get_model auto-refinement path (which would
    # rewrite the file before our dry-run gets to inspect it).
    inner = getattr(storage, "_inner", storage)
    data_source_filter = getattr(args, "data_source", None)
    dry_run = bool(getattr(args, "dry_run", False))

    identities = run_sync(inner._list_all_model_identities())

    refined_total = 0
    for ds_name, model_name in identities:
        if data_source_filter and ds_name != data_source_filter:
            continue
        if _refine_one_model_for_cli(
            inner=inner,
            ds_name=ds_name,
            model_name=model_name,
            dry_run=dry_run,
        ):
            refined_total += 1

    if refined_total == 0:
        print("No models needed refinement.")
    elif dry_run:
        print(f"\nDry run: {refined_total} model(s) would be refined.")
    else:
        print(f"\nDone: refined {refined_total} model(s).")


def _refine_one_model_for_cli(
    *, inner, ds_name: str, model_name: str, dry_run: bool,
) -> bool:
    """Per-model refinement loop body for ``slayer storage migrate-types``.

    Returns True iff the model needed refinement. Reads the raw on-disk
    dict, runs the v5 migrator chain, applies live-schema refinement, and
    optionally persists.

    Mirrors ``StorageBackend._migrate_and_refine_on_load``: if the migrated
    dict has refineable DOUBLE base columns AND the datasource entry is
    missing, raises ``ValueError`` rather than silently reporting "nothing
    to refine" for a model the CLI never had enough information to inspect.
    Models with no refineable columns (text-only, query-backed, sql-mode,
    already-narrowed) skip silently and don't require a live datasource.
    """
    raw = run_sync(_load_raw_model_dict(inner, ds_name, model_name))
    if raw is None:
        return False
    # Snapshot the original column types before migration mutates the
    # shared inner dicts, so we can show before/after diffs.
    original_types = {
        (c.get("name") or "?"): c.get("type", "?")
        for c in raw.get("columns", []) or []
        if isinstance(c, dict)
    }
    upgraded = _mig.migrate("SlayerModel", copy.deepcopy(raw))
    if not has_refineable_columns(upgraded):
        return False
    ds = run_sync(inner.get_datasource(ds_name))
    if ds is None:
        raise ValueError(
            f"Cannot refine model {ds_name!r}.{model_name!r}: datasource "
            f"{ds_name!r} is unavailable for type refinement. Restore the "
            f"datasource entry or remove the stale model file."
        )
    if not refine_dict_with_live_schema(upgraded, ds):
        return False
    print(f"refined {ds_name}.{model_name}:")
    for col in upgraded.get("columns", []) or []:
        if col.get("type") != "INT":
            continue
        before = original_types.get(col.get("name") or "", None)
        if before != "INT":
            print(f"  - {col['name']}: {before or '?'} → INT")
    if not dry_run:
        model = SlayerModel.model_validate(upgraded)
        # Save through inner so we don't re-trigger the load-time
        # refinement / join-sync mirror loop.
        run_sync(inner.save_model(model))
    return True


async def _load_raw_model_dict(storage, data_source: str, name: str) -> Optional[dict]:
    """Read a model's raw on-disk dict bypassing Pydantic's validator chain."""
    import json as _json
    import os as _os

    import yaml as _yaml

    if hasattr(storage, "_model_path"):  # YAMLStorage
        path = storage._model_path(data_source, name)
        if not _os.path.exists(path):
            return None
        with open(path) as f:  # NOSONAR(S7493) — sync I/O in async by design (CLAUDE.md, Async Architecture)
            return _yaml.safe_load(f)
    if hasattr(storage, "_get_model_sync"):  # SQLiteStorage
        import asyncio as _asyncio

        raw = await _asyncio.to_thread(storage._get_model_sync, data_source, name)
        return _json.loads(raw) if raw else None
    return None


def _run_help(args):
    from slayer.help import render_help

    print(render_help(topic=args.topic))


def _parse_cli_variables(args) -> dict:
    """Combine ``--variables KEY=VALUE`` (repeatable) and ``--variables-json``
    into a single dict. Errors out if both forms are mixed.
    """
    has_kv = bool(args.variables)
    has_json = args.variables_json is not None
    if has_kv and has_json:
        raise SystemExit(
            "--variables and --variables-json are mutually exclusive."
        )
    if has_json:
        try:
            parsed = json.loads(args.variables_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"--variables-json contains invalid JSON: {exc}") from None
        if not isinstance(parsed, dict):
            raise SystemExit("--variables-json must decode to a JSON object.")
        return parsed
    out: dict = {}
    for raw in args.variables or []:
        if "=" not in raw:
            raise SystemExit(
                f"--variables expects KEY=VALUE form, got: {raw!r}"
            )
        key, value = raw.split("=", 1)
        out[key] = value
    return out


def _run_query(args):  # NOSONAR S3776 — argparse-driven dispatch; one straight-line function reads better than threaded helpers
    from slayer.engine.query_engine import SlayerQueryEngine

    query_input = args.query_json
    runtime_kwarg = _parse_cli_variables(args)

    storage = _resolve_storage(args)
    engine = SlayerQueryEngine(storage=storage)

    if query_input.startswith("@"):
        filepath = query_input[1:]
        try:
            with open(filepath) as f:
                query_input = f.read()
        except FileNotFoundError:
            raise SystemExit(f"Query file not found: {filepath}") from None
        except OSError as e:
            raise SystemExit(f"Error reading query file: {e}") from None
        is_json = True
    else:
        # Heuristic: a JSON query starts with '{' or '['; anything else
        # is treated as a model name for run-by-name dispatch.
        stripped = query_input.lstrip()
        is_json = stripped.startswith(("{", "["))

    if is_json:
        data = json.loads(query_input)
        if isinstance(data, list) and not data:
            raise SystemExit("Query list cannot be empty.")
        result = engine.execute_sync(
            query=data,
            variables=runtime_kwarg or None,
            dry_run=bool(args.dry_run),
            explain=bool(args.explain),
        )
    else:
        # Run-by-name: the positional arg is a model name.
        result = engine.execute_sync(
            query=query_input,
            variables=runtime_kwarg or None,
            dry_run=bool(args.dry_run),
            explain=bool(args.explain),
        )

    if args.dry_run:
        print(result.sql)
        return

    if args.format == "json":
        print(json.dumps(result.data, indent=2, default=str))
    else:
        if args.explain:
            print(f"SQL:\n{result.sql}\n")
            print("Query Plan:")
        if not result.data:
            print("No results.")
            return
        header = " | ".join(result.columns)
        separator = " | ".join("-" * len(c) for c in result.columns)
        print(header)
        print(separator)
        for row in result.data:
            print(" | ".join(str(row.get(c, "")) for c in result.columns))
        if not args.explain:
            print(f"\n{result.row_count} row(s)")


def _prepare_demo(args, storage, *, stream=None):
    """Ensure the Jaffle Shop demo is set up before a long-running server starts.

    Writes status messages to ``stream`` (default: stderr) so stdio-based
    transports (``slayer mcp``) remain protocol-safe.
    """
    from slayer.demo import ensure_demo_datasource

    out = stream if stream is not None else sys.stderr
    storage_path = args.storage or args.models_dir or _STORAGE_DEFAULT
    try:
        ds, models, db_built = ensure_demo_datasource(
            storage,
            storage_path=storage_path,
            ingest_models=True,
            assume_yes=True,
            stream=out,
        )
    except Exception as e:
        print(f"Failed to set up the Jaffle Shop demo: {e}", file=out)
        sys.exit(1)

    state = "generated" if db_built else "reused"
    print(
        f"Demo ready: {state} {ds.database}; datasource '{ds.name}', "
        f"{len(models)} model(s) available.",
        file=out,
    )


def _run_serve(args):
    from slayer.api.server import create_app

    storage = _resolve_storage(args)
    if getattr(args, "demo", False):
        _prepare_demo(args, storage)
    ingest_on_startup = (
        getattr(args, "ingest_on_startup", False) or _env_ingest_on_startup()
    )
    app = create_app(storage=storage, ingest_on_startup=ingest_on_startup)

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


def _run_mcp(args):
    from slayer.mcp.server import create_mcp_server

    storage = _resolve_storage(args)
    if getattr(args, "demo", False):
        _prepare_demo(args, storage)
    ingest_on_startup = (
        getattr(args, "ingest_on_startup", False) or _env_ingest_on_startup()
    )
    mcp = create_mcp_server(storage=storage, ingest_on_startup=ingest_on_startup)
    mcp.run()


def _parse_csv_arg(value):
    if not value:
        return None
    return [t for t in (s.strip() for s in value.split(",")) if t]


def _run_ingest(args):
    from slayer.engine.ingestion import ingest_datasource_idempotent

    storage = _resolve_storage(args)
    ds = run_sync(storage.get_datasource(args.datasource))
    if ds is None:
        storage_path = args.storage or args.models_dir or _STORAGE_DEFAULT
        print(f"Datasource '{args.datasource}' not found in {storage_path}")
        sys.exit(1)

    result = run_sync(
        ingest_datasource_idempotent(
            datasource=ds,
            storage=storage,
            schema=args.schema,
            include_tables=_parse_csv_arg(args.include),
            exclude_tables=_parse_csv_arg(args.exclude),
        )
    )
    for addition in result.additions:
        _print_ingest_addition(addition)
    _print_ingest_drift_and_errors(result)
    if result.errors:
        sys.exit(1)


_REMOVE_SECTIONS = (
    ("columns", "drop columns"),
    ("measures", "drop measures"),
    ("aggregations", "drop aggregations"),
    ("joins", "drop joins"),
)


def _format_edit_entry_lines(entry) -> List[str]:
    lines = [f"EDIT MODEL: {entry.model_name} (datasource: {entry.data_source})"]
    for attr, label in _REMOVE_SECTIONS:
        values = getattr(entry.remove, attr)
        if values:
            lines.append(f"  {label}: {', '.join(values)}")
    if entry.remove_filters:
        lines.append("  remove filters: " + "; ".join(entry.remove_filters))
    return lines


def _format_validate_models_output(entries) -> str:
    """Render a List[ToDeleteEntry] as human-readable text for CLI output."""
    if not entries:
        return "No drift detected."
    lines: List[str] = []
    for entry in entries:
        if entry.tool == "delete_model":
            lines.append(
                f"DELETE MODEL: {entry.model_name} (datasource: {entry.data_source})"
            )
        else:
            lines.extend(_format_edit_entry_lines(entry))
        for r in entry.reasons:
            lines.append(f"    - {r.target}: {r.reason}")
    return "\n".join(lines)


def _run_validate_models(args):
    from slayer.engine.query_engine import SlayerQueryEngine

    storage = _resolve_storage(args)
    if args.datasource:
        # Fail fast on a typoed name. Without this check, ``validate_models``
        # returns ``[]`` for an unknown datasource (no models match), which
        # is indistinguishable from "no drift" and silently exits 0.
        ds = run_sync(storage.get_datasource(args.datasource))
        if ds is None:
            storage_path = args.storage or args.models_dir or _STORAGE_DEFAULT
            print(f"Datasource '{args.datasource}' not found in {storage_path}")
            sys.exit(1)
    engine = SlayerQueryEngine(storage=storage)
    try:
        entries = run_sync(engine.validate_models(data_source=args.datasource))
    except Exception as exc:  # noqa: BLE001 — surface DB/auth/introspection failures cleanly
        print(f"validate-models failed: {exc}")
        sys.exit(1)
    print(_format_validate_models_output(entries))

    force_clean = bool(getattr(args, "force_clean", False))
    if not force_clean:
        return

    if not entries:
        return

    if not _confirm(
        f"\nApply {len(entries)} delete(s) to storage?",
        assume_yes=bool(getattr(args, "yes", False)),
    ):
        print("Aborted; storage unchanged.")
        return

    result = run_sync(engine.apply_drift_deletes(entries))
    print(f"\nApplied {len(result.applied)} entry/entries.")
    if result.errors:
        print(f"Errors ({len(result.errors)}):")
        for err in result.errors:
            print(f"  - {err.tool} {err.model_name}: {err.error}")
    if result.residual:
        print("\nResidual drift after apply:")
        print(_format_validate_models_output(result.residual))
        sys.exit(1)
    if result.errors:
        sys.exit(1)
    print("\n✓ no remaining drift")


def _run_import_dbt(args):
    import sqlalchemy as sa

    from slayer.dbt.converter import DbtToSlayerConverter
    from slayer.dbt.parser import parse_dbt_project

    storage = _resolve_storage(args)
    include_hidden = bool(args.include_hidden_models)
    project = parse_dbt_project(
        args.dbt_project_path,
        include_regular_models=include_hidden,
    )

    if not project.semantic_models and not (include_hidden and project.regular_models):
        print(f"No semantic models found in {args.dbt_project_path}")
        sys.exit(1)

    sa_engine = None
    if include_hidden:
        ds = run_sync(storage.get_datasource(args.datasource))
        if ds is None:
            storage_path = args.storage or args.models_dir or _STORAGE_DEFAULT
            print(
                f"Datasource '{args.datasource}' not found in {storage_path}; "
                "required for --include-hidden-models."
            )
            sys.exit(1)
        sa_engine = sa.create_engine(ds.resolve_env_vars().get_connection_string())

    try:
        converter = DbtToSlayerConverter(
            project=project,
            data_source=args.datasource,
            sa_engine=sa_engine,
            include_hidden_models=include_hidden,
        )
        result = converter.convert()
    finally:
        if sa_engine is not None:
            sa_engine.dispose()

    hidden_count = 0
    for model in result.models:
        run_sync(storage.save_model(model))
        suffix = " [hidden]" if model.hidden else ""
        if model.hidden:
            hidden_count += 1
        print(
            f"Imported model: {model.name}{suffix} "
            f"({len(model.columns)} columns, {len(model.measures)} measures)"
        )

    for u in result.unconverted_metrics:
        context = u.model_name or u.metric_name or "general"
        print(f"  UNCONVERTED [{context}]: {u.message}")

    for w in result.warnings:
        context = w.model_name or w.metric_name or "general"
        print(f"  WARNING [{context}]: {w.message}")

    visible_count = len(result.models) - hidden_count
    print(
        f"\nDone: {visible_count} models, {hidden_count} hidden, "
        f"{len(result.unconverted_metrics)} unconverted metrics, "
        f"{len(result.warnings)} warnings"
    )


def _run_models(args):
    import yaml

    from slayer.core.models import SlayerModel

    storage = _resolve_storage(args)

    if args.models_command == "list":
        names = run_sync(storage.list_models())
        if not names:
            print("No models found.")
            return
        for name in names:
            model = run_sync(storage.get_model(name))
            if model and model.hidden:
                continue
            desc = f"  — {model.description}" if model and model.description else ""
            print(f"{name}{desc}")

    elif args.models_command == "show":
        model = run_sync(storage.get_model(args.name))
        if model is None:
            print(f"Model '{args.name}' not found.")
            sys.exit(1)
        data = model.model_dump(mode="json", exclude_none=True)
        print(yaml.dump(data, sort_keys=False, default_flow_style=False).rstrip())

    elif args.models_command == "create":
        from slayer.engine.query_engine import SlayerQueryEngine

        with open(args.file) as f:
            data = yaml.safe_load(f)
        model = SlayerModel.model_validate(data)
        # Route through engine.save_model so query-backed models get cache
        # populated (and user-supplied cache fields are rejected).
        engine = SlayerQueryEngine(storage=storage)
        try:
            run_sync(engine.save_model(model))
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
        print(f"Created model '{model.name}'.")

    elif args.models_command == "delete":
        deleted = run_sync(storage.delete_model(args.name))
        if deleted:
            print(f"Deleted model '{args.name}'.")
        else:
            print(f"Model '{args.name}' not found.")
            sys.exit(1)

    else:
        print("Usage: slayer models {list,show,create,delete}")
        sys.exit(1)


def _run_datasources(args):
    import yaml

    storage = _resolve_storage(args)

    if args.datasources_command == "list":
        names = run_sync(storage.list_datasources())
        if not names:
            print("No datasources found.")
            return
        for name in names:
            ds = run_sync(storage.get_datasource(name))
            ds_type = ds.type if ds and ds.type else "unknown"
            print(f"{name}  ({ds_type})")

    elif args.datasources_command == "show":
        ds = run_sync(storage.get_datasource(args.name))
        if ds is None:
            print(f"Datasource '{args.name}' not found.")
            sys.exit(1)
        data = ds.model_dump(mode="json", exclude_none=True)
        if "password" in data:
            data["password"] = "********"
        if "connection_string" in data:
            data["connection_string"] = "********"
        print(yaml.dump(data, sort_keys=False, default_flow_style=False).rstrip())

    elif args.datasources_command == "create":
        _run_datasources_create(args, storage)

    elif args.datasources_command == "delete":
        deleted = run_sync(storage.delete_datasource(args.name))
        if deleted:
            print(f"Deleted datasource '{args.name}'.")
        else:
            print(f"Datasource '{args.name}' not found.")
            sys.exit(1)

    elif args.datasources_command == "test":
        ds = run_sync(storage.get_datasource(args.name))
        if ds is None:
            print(f"Datasource '{args.name}' not found.")
            sys.exit(1)
        import sqlalchemy as sa

        try:
            engine = sa.create_engine(ds.resolve_env_vars().get_connection_string())
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            engine.dispose()
            print(f"OK — connected to '{args.name}' ({ds.type}).")
        except Exception as e:
            print(f"FAILED — {e}")
            sys.exit(1)

    else:
        print("Usage: slayer datasources {list,show,create,delete,test}")
        sys.exit(1)


def _parse_connection_string(url: str) -> tuple[str, str]:
    """Parse a database URL into (type, derived_name).

    - Strips any ``+driver`` suffix from the scheme (``mysql+pymysql`` → ``mysql``).
    - Normalizes ``postgresql`` → ``postgres``.
    - For file-based backends (sqlite, duckdb), the derived name is the file stem.
    - For networked backends, the derived name is the database portion of the path.

    Raises ``ValueError`` if the scheme is missing or no name can be derived.
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if not parsed.scheme:
        raise ValueError(f"Connection string '{url}' is missing a scheme (e.g. postgresql://…)")

    ds_type = parsed.scheme.split("+", 1)[0].lower()
    if ds_type == "postgresql":
        ds_type = "postgres"

    if ds_type in ("sqlite", "duckdb"):
        # Path may start with ``/`` (netloc empty) or be relative.
        raw_path = parsed.path or parsed.netloc
        if not raw_path:
            raise ValueError(
                f"Cannot derive a name from '{url}': no file path provided. Pass --name explicitly."
            )
        stem = os.path.splitext(os.path.basename(raw_path.rstrip("/")))[0]
        if not stem:
            raise ValueError(
                f"Cannot derive a name from '{url}': empty filename. Pass --name explicitly."
            )
        return ds_type, stem

    # Networked: take the first non-empty path segment (Postgres/MySQL/ClickHouse all put db there).
    segments = [s for s in parsed.path.split("/") if s]
    if not segments:
        raise ValueError(
            f"Cannot derive a name from '{url}': no database in path. Pass --name explicitly."
        )
    return ds_type, segments[0]


def _confirm(prompt: str, *, assume_yes: bool) -> bool:
    """Yes/no prompt. Returns True if user confirms or ``assume_yes`` is set.

    Aborts (returns False) on a non-tty when ``assume_yes`` is not set — the caller
    should treat that as a declined confirmation.
    """
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        print(f"{prompt} (non-interactive session; pass --yes to proceed)")
        return False
    try:
        answer = input(f"{prompt} [y/N] ").strip().lower()
    except EOFError:
        return False
    return answer in ("y", "yes")


def _persist_ingested_models(models, storage, *, assume_yes: bool, pre_save=None) -> None:
    """Persist freshly-ingested models, after a collision-confirmation prompt.

    Used by both ``datasources create`` (with ``--ingest``) and
    ``datasources create demo --ingest``. ``pre_save`` is an optional hook
    called once per model just before saving — the demo path uses it to
    apply ``default_time_dimension`` overrides.
    """
    if not models:
        print("No models were generated.")
        return

    colliding = [m.name for m in models if run_sync(storage.get_model(m.name)) is not None]
    if colliding and not _confirm(
        f"Models already exist and will be overwritten: {', '.join(colliding)}. Continue?",
        assume_yes=assume_yes,
    ):
        print("Aborted before writing models.")
        sys.exit(1)

    for model in models:
        if pre_save is not None:
            pre_save(model)
        run_sync(storage.save_model(model))
        print(f"Ingested: {model.name} ({len(model.columns)} columns, {len(model.measures)} measures)")


def _run_datasources_create(args, storage):
    if (args.connection_string or "").strip().lower() == "demo":
        _run_datasources_create_demo(args, storage)
        return

    from slayer.core.models import DatasourceConfig

    try:
        ds_type, derived_name = _parse_connection_string(args.connection_string)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    name = args.name or derived_name
    ds = DatasourceConfig.model_validate(
        {
            "name": name,
            "type": ds_type,
            "connection_string": args.connection_string,
            "description": args.description,
        }
    )

    existing = run_sync(storage.get_datasource(name))
    if existing is not None and not _confirm(
        f"Datasource '{name}' already exists. Overwrite?", assume_yes=args.yes
    ):
        print("Aborted.")
        sys.exit(1)

    run_sync(storage.save_datasource(ds))
    print(f"Created datasource '{ds.name}' ({ds.type}).")

    if not args.ingest:
        return

    from slayer.engine.ingestion import ingest_datasource

    include = [t for t in (s.strip() for s in args.include.split(",")) if t] if args.include else None
    exclude = [t for t in (s.strip() for s in args.exclude.split(",")) if t] if args.exclude else None

    try:
        models = ingest_datasource(
            datasource=ds,
            schema=args.schema,
            include_tables=include,
            exclude_tables=exclude,
        )
    except Exception as e:
        print(f"Ingestion failed: {e}")
        sys.exit(1)

    _persist_ingested_models(models, storage, assume_yes=args.yes)


def _run_datasources_create_demo(args, storage):  # NOSONAR S3776 — linear demo-bootstrap flow (build → confirm → save → optional ingest); branches are sequential UX guards, not nested logic
    from slayer.demo import (
        DEFAULT_TIME_DIMENSIONS,
        DEMO_NAME,
        build_jaffle_shop,
        resolve_demo_db_path,
    )

    storage_path = args.storage or args.models_dir or _STORAGE_DEFAULT
    name = args.name or DEMO_NAME
    db_path = resolve_demo_db_path(storage_path)

    try:
        db_built = build_jaffle_shop(
            db_path=db_path, years=max(1, args.years), stream=sys.stderr
        )
    except Exception as e:
        print(f"Failed to build Jaffle Shop demo: {e}")
        sys.exit(1)

    if db_built:
        print(f"Generated Jaffle Shop DuckDB at {db_path}")
    else:
        print(f"Reusing existing Jaffle Shop DuckDB at {db_path}")

    from slayer.core.models import DatasourceConfig

    ds = DatasourceConfig.model_validate(
        {
            "name": name,
            "type": "duckdb",
            "database": db_path,
            "description": args.description or "Jaffle Shop demo (synthetic data via jafgen)",
        }
    )

    existing = run_sync(storage.get_datasource(name))
    if existing is not None and not _confirm(
        f"Datasource '{name}' already exists. Overwrite?", assume_yes=args.yes
    ):
        print("Aborted.")
        sys.exit(1)

    run_sync(storage.save_datasource(ds))
    print(f"Created datasource '{ds.name}' (duckdb).")

    if not args.ingest:
        print("Run with --ingest to also auto-generate models.")
        return

    from slayer.engine.ingestion import ingest_datasource

    try:
        models = ingest_datasource(datasource=ds)
    except Exception as e:
        print(f"Ingestion failed: {e}")
        sys.exit(1)

    def _apply_demo_time_dim(model):
        if model.name in DEFAULT_TIME_DIMENSIONS:
            model.default_time_dimension = DEFAULT_TIME_DIMENSIONS[model.name]

    _persist_ingested_models(
        models, storage, assume_yes=args.yes, pre_save=_apply_demo_time_dim
    )


def _load_query_arg(value):
    """Resolve a CLI ``--query`` / ``--about-query`` argument.

    Accepts inline JSON or ``@/path/to/file.json``. Returns the parsed
    dict so the caller can hand it to the service layer (which validates
    it as a ``SlayerQuery``).
    """
    if value.startswith("@"):
        path = value[1:]
        if not os.path.exists(path):
            print(f"Error: Query file not found: {path}")
            sys.exit(1)
        with open(path) as f:
            text = f.read()
    else:
        text = value
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        print(f"Error: invalid JSON for query argument: {exc}")
        sys.exit(1)


def _exit_with_error(exc: Exception) -> None:
    print(f"Error: {exc}")
    sys.exit(1)


def _run_memory_save(args, service):
    if not args.entities and not args.query:
        print("Error: --entities or --query must be supplied (one or the other).")
        sys.exit(1)
    if args.entities and args.query:
        print("Error: --entities and --query are mutually exclusive.")
        sys.exit(1)
    linked = (
        [e.strip() for e in args.entities.split(",") if e.strip()]
        if args.entities
        else _load_query_arg(args.query)
    )
    try:
        response = run_sync(
            service.save_memory(
                learning=args.learning,
                linked_entities=linked,
            )
        )
    except (EntityResolutionError, AmbiguousModelError, ValueError) as exc:
        _exit_with_error(exc)
        return  # for type checkers; _exit_with_error never returns
    print(f"Saved memory {response.memory_id}.")
    if response.resolved_entities:
        print("Resolved entities: " + ", ".join(response.resolved_entities))
    for warning in response.warnings:
        print(f"Warning: {warning}")


def _run_memory_forget(args, service):
    try:
        response = run_sync(service.forget_memory(identifier=args.id))
    except (MemoryNotFoundError, ValueError) as exc:
        _exit_with_error(exc)
        return
    print(f"Forgot memory {response.deleted_id}.")


_MEMORY_DISPATCH = {
    "save": _run_memory_save,
    "forget": _run_memory_forget,
}


def _run_memory(args):
    """Dispatcher for ``slayer memory <save|forget>``. Memory retrieval
    is handled by ``slayer search``."""
    from slayer.memories.service import MemoryService

    handler = _MEMORY_DISPATCH.get(args.memory_command)
    if handler is None:
        print("Usage: slayer memory {save,forget}")
        sys.exit(1)
    storage = _resolve_storage(args)
    handler(args, MemoryService(storage=storage))


if __name__ == "__main__":
    main()
