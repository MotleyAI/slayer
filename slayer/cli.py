"""CLI entry point for SLayer."""

import argparse
import json
import os
import sys
from typing import List

from slayer.async_utils import run_sync
from slayer.storage.base import default_storage_path

_STORAGE_DEFAULT = default_storage_path()
_STORAGE_HELP = (
    "Storage path: directory for YAML storage, or .db/.sqlite file for SQLite storage "
    f"(default: {_STORAGE_DEFAULT})"
)


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
    _add_storage_arg(serve_parser)

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
        default=4,
        help="(demo only) Years of synthetic data to generate (default: 4)",
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
        help="Manage agent memories (learnings + saved queries)",
        epilog="""\
examples:
  # Save a learning indexed by entities
  slayer memory save --learning "amount is in cents" --entities mydb.orders.amount

  # Save a learning that comes with an example SlayerQuery
  slayer memory save --learning "Paid revenue" --query @paid_revenue.json

  # Forget a memory by id
  slayer memory forget 42

  # Recall memories about specific entities
  slayer memory recall --about mydb.orders.amount,mydb.orders.status

  # Recall memories relevant to a query
  slayer memory recall --about-query @paid_revenue.json
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

    memory_recall_parser = memory_subparsers.add_parser(
        "recall", help="Look up memories by entity overlap"
    )
    memory_recall_parser.add_argument(
        "--about",
        default=None,
        help="Comma-separated list of entity references to search by.",
    )
    memory_recall_parser.add_argument(
        "--about-query",
        default=None,
        dest="about_query",
        help="Inline JSON SlayerQuery (or @file.json) whose entities augment --about.",
    )
    memory_recall_parser.add_argument(
        "--max-learnings",
        type=int,
        default=None,
        dest="max_learnings",
        help="Cap on returned learning-shaped memories (default: all).",
    )
    memory_recall_parser.add_argument(
        "--max-queries",
        type=int,
        default=2,
        dest="max_queries",
        help="Cap on returned query-bearing memories (default: 2).",
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
    elif args.command == "help":
        _run_help(args)
    else:
        parser.print_help()
        sys.exit(1)


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
        is_json = stripped.startswith("{") or stripped.startswith("[")

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

    app = create_app(storage=storage)

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


def _run_mcp(args):
    from slayer.mcp.server import create_mcp_server

    storage = _resolve_storage(args)
    if getattr(args, "demo", False):
        _prepare_demo(args, storage)

    mcp = create_mcp_server(storage=storage)
    mcp.run()


def _print_ingest_addition(addition) -> None:
    if addition.created:
        print(
            f"Created: {addition.model_name} ({len(addition.new_columns)} columns)"
        )
        return
    if not (addition.new_columns or addition.new_joins):
        return
    details = []
    if addition.new_columns:
        details.append(f"+columns: {', '.join(addition.new_columns)}")
    if addition.new_joins:
        details.append(f"+joins: {', '.join(addition.new_joins)}")
    print(f"Updated: {addition.model_name} ({'; '.join(details)})")


def _print_ingest_drift_and_errors(result) -> None:
    if result.to_delete:
        print("\nPending drift (run `slayer validate-models` to inspect):")
        for entry in result.to_delete:
            print(f"  - {entry.tool}: {entry.model_name}")
    if result.errors:
        print(f"\nErrors ({len(result.errors)}):")
        for err in result.errors:
            print(f"  - {err.model_name}: {err.error}")


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
        db_built = build_jaffle_shop(db_path=db_path, years=max(1, args.years))
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
    from slayer.core.errors import AmbiguousModelError, EntityResolutionError

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
    from slayer.core.errors import MemoryNotFoundError

    try:
        response = run_sync(service.forget_memory(identifier=args.id))
    except (MemoryNotFoundError, ValueError) as exc:
        _exit_with_error(exc)
        return
    print(f"Forgot memory {response.deleted_id}.")


def _run_memory_recall(args, service):
    from slayer.core.errors import AmbiguousModelError, EntityResolutionError

    if args.about and args.about_query:
        print("Error: --about and --about-query are mutually exclusive.")
        sys.exit(1)
    if args.about:
        about = [a.strip() for a in args.about.split(",") if a.strip()]
    elif args.about_query:
        about = _load_query_arg(args.about_query)
    else:
        about = []
    try:
        response = run_sync(
            service.recall_memories(
                about=about,
                max_learnings=args.max_learnings,
                max_queries=args.max_queries,
            )
        )
    except (EntityResolutionError, AmbiguousModelError, ValueError) as exc:
        _exit_with_error(exc)
        return
    for warning in response.warnings:
        print(f"Warning: {warning}")
    _print_recall_section("Learnings", response.learnings)
    _print_recall_section("Queries", response.queries)
    if not response.learnings and not response.queries:
        print("No matching memories.")


def _print_recall_section(title, hits):
    if not hits:
        return
    print(f"{title} ({len(hits)}):")
    for hit in hits:
        tags = ", ".join(hit.matched_entities) or "—"
        print(f"  M{hit.id} [{tags}]: {hit.learning}")


_MEMORY_DISPATCH = {
    "save": _run_memory_save,
    "forget": _run_memory_forget,
    "recall": _run_memory_recall,
}


def _run_memory(args):
    """Dispatcher for ``slayer memory <save|forget|recall>``."""
    from slayer.memories.service import MemoryService

    handler = _MEMORY_DISPATCH.get(args.memory_command)
    if handler is None:
        print("Usage: slayer memory {save,forget,recall}")
        sys.exit(1)
    storage = _resolve_storage(args)
    handler(args, MemoryService(storage=storage))


if __name__ == "__main__":
    main()
