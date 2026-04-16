"""CLI entry point for SLayer."""

import argparse
import os
import sys

from slayer.async_utils import run_sync

_STORAGE_DEFAULT = os.environ.get("SLAYER_STORAGE", os.environ.get("SLAYER_MODELS_DIR", "./slayer_data"))
_STORAGE_HELP = (
    "Storage path: directory for YAML storage, or .db/.sqlite file for SQLite storage "
    "(default: $SLAYER_STORAGE or $SLAYER_MODELS_DIR or ./slayer_data)"
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
  slayer query '{"source_model": "orders", "fields": [{"formula": "count"}]}'

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
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    serve_parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    serve_parser.add_argument("--port", type=int, default=5143, help="Port number (default: 5143)")
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
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_storage_arg(mcp_parser)

    # ── query ─────────────────────────────────────────────────────────
    query_parser = subparsers.add_parser(
        "query",
        help="Execute a query from JSON",
        epilog="""\
examples:
  # Inline JSON
  slayer query '{"source_model": "orders", "fields": [{"formula": "count"}]}'

  # From a file
  slayer query @query.json

  # Preview SQL without executing
  slayer query '{"source_model": "orders", "fields": [{"formula": "count"}]}' --dry-run

  # Show execution plan
  slayer query @query.json --explain

  # Output as JSON
  slayer query @query.json --format json
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    query_parser.add_argument(
        "query_json",
        help="JSON query string, or @file.json to read from a file",
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
        "--no-strict-aggregations",
        action="store_true",
        help="Don't restrict measures to their dbt-defined aggregation types",
    )
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

  # Create from YAML file
  slayer datasources create datasource.yaml

  # Create inline (quick setup — use env vars for secrets)
  slayer datasources create-inline my_pg --type postgres --host localhost --database mydb --username user --password '${DB_PASSWORD}'

  # Or read password interactively
  slayer datasources create-inline my_pg --type postgres --host localhost --database mydb --username user --password-stdin

  # Create SQLite/DuckDB (just needs a path)
  slayer datasources create-inline my_sqlite --type sqlite --database /path/to/data.db

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

    datasources_create_parser = datasources_subparsers.add_parser("create", help="Create a datasource from a YAML file")
    datasources_create_parser.add_argument("file", help="Path to YAML datasource config")

    ds_inline_parser = datasources_subparsers.add_parser(
        "create-inline", help="Create a datasource from command-line flags"
    )
    ds_inline_parser.add_argument("name", help="Datasource name")
    ds_inline_parser.add_argument("--type", required=True, help="Database type (postgres, mysql, sqlite, duckdb, ...)")
    ds_inline_parser.add_argument("--host", default=None, help="Database host")
    ds_inline_parser.add_argument("--port", type=int, default=None, help="Database port")
    ds_inline_parser.add_argument("--database", default=None, help="Database name or file path")
    ds_inline_parser.add_argument("--username", default=None, help="Database username")
    ds_inline_parser.add_argument(
        "--password", default=None, help="Database password (prefer --password-stdin or ${ENV_VAR} in YAML configs)"
    )
    ds_inline_parser.add_argument(
        "--password-stdin", action="store_true", help="Read password from stdin (more secure than --password)"
    )
    ds_inline_parser.add_argument(
        "--connection-string", default=None, help="Full connection string (overrides other flags)"
    )
    ds_inline_parser.add_argument("--description", default=None, help="Human-readable description")

    datasources_delete_parser = datasources_subparsers.add_parser("delete", help="Delete a datasource")
    datasources_delete_parser.add_argument("name", help="Datasource name")

    datasources_test_parser = datasources_subparsers.add_parser("test", help="Test datasource connectivity")
    datasources_test_parser.add_argument("name", help="Datasource name")

    args = parser.parse_args()

    if args.command == "serve":
        _run_serve(args)
    elif args.command == "mcp":
        _run_mcp(args)
    elif args.command == "query":
        _run_query(args)
    elif args.command == "ingest":
        _run_ingest(args)
    elif args.command == "import-dbt":
        _run_import_dbt(args)
    elif args.command == "models":
        _run_models(args)
    elif args.command == "datasources":
        _run_datasources(args)
    else:
        parser.print_help()
        sys.exit(1)


def _run_query(args):
    import json

    from slayer.core.query import SlayerQuery
    from slayer.engine.query_engine import SlayerQueryEngine

    query_input = args.query_json
    if query_input.startswith("@"):
        with open(query_input[1:]) as f:
            query_input = f.read()
    data = json.loads(query_input)
    if args.dry_run:
        data["dry_run"] = True
    if args.explain:
        data["explain"] = True
    slayer_query = SlayerQuery.model_validate(data)

    storage = _resolve_storage(args)
    engine = SlayerQueryEngine(storage=storage)
    result = engine.execute_sync(query=slayer_query)

    if slayer_query.dry_run:
        print(result.sql)
        return

    if args.format == "json":
        print(json.dumps(result.data, indent=2, default=str))
    else:
        if slayer_query.explain:
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
        if not slayer_query.explain:
            print(f"\n{result.row_count} row(s)")


def _run_serve(args):
    from slayer.api.server import create_app

    storage = _resolve_storage(args)
    app = create_app(storage=storage)

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


def _run_mcp(args):
    from slayer.mcp.server import create_mcp_server

    storage = _resolve_storage(args)
    mcp = create_mcp_server(storage=storage)
    mcp.run()


def _run_ingest(args):
    from slayer.engine.ingestion import ingest_datasource

    storage = _resolve_storage(args)
    ds = run_sync(storage.get_datasource(args.datasource))
    if ds is None:
        storage_path = args.storage or args.models_dir or _STORAGE_DEFAULT
        print(f"Datasource '{args.datasource}' not found in {storage_path}")
        sys.exit(1)

    include = [t for t in (s.strip() for s in args.include.split(",")) if t] if args.include else None
    exclude = [t for t in (s.strip() for s in args.exclude.split(",")) if t] if args.exclude else None

    models = ingest_datasource(
        datasource=ds,
        schema=args.schema,
        include_tables=include,
        exclude_tables=exclude,
    )
    for model in models:
        run_sync(storage.save_model(model))
        print(f"Ingested: {model.name} ({len(model.dimensions)} dims, {len(model.measures)} measures)")


def _run_import_dbt(args):
    import sqlalchemy as sa
    import yaml as _yaml

    from slayer.dbt.converter import DbtToSlayerConverter
    from slayer.dbt.parser import parse_dbt_project

    storage = _resolve_storage(args)
    project = parse_dbt_project(args.dbt_project_path)

    include_hidden = bool(args.include_hidden_models)
    if not project.semantic_models and not (include_hidden and project.regular_models):
        print(f"No semantic models found in {args.dbt_project_path}")
        sys.exit(1)

    sa_engine = None
    if include_hidden:
        ds = storage.get_datasource(args.datasource)
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
            strict_aggregations=not args.no_strict_aggregations,
            sa_engine=sa_engine,
            include_hidden_models=include_hidden,
        )
        result = converter.convert()
    finally:
        if sa_engine is not None:
            sa_engine.dispose()

    # Save models
    hidden_count = 0
    for model in result.models:
        storage.save_model(model)
        suffix = " [hidden]" if model.hidden else ""
        if model.hidden:
            hidden_count += 1
        print(
            f"Imported model: {model.name}{suffix} "
            f"({len(model.dimensions)} dims, {len(model.measures)} measures)"
        )

    # Save queries to queries.yaml if any
    if result.queries:
        storage_path = args.storage or args.models_dir or _STORAGE_DEFAULT
        queries_path = os.path.join(storage_path, "queries.yaml")
        with open(queries_path, "w") as f:
            _yaml.dump(result.queries, f, sort_keys=False, default_flow_style=False)
        print(f"Generated {len(result.queries)} metric queries → {queries_path}")

    # Print warnings
    for w in result.warnings:
        context = w.model_name or w.metric_name or "general"
        print(f"  WARNING [{context}]: {w.message}")

    visible_count = len(result.models) - hidden_count
    print(
        f"\nDone: {visible_count} models, {hidden_count} hidden, "
        f"{len(result.queries)} queries, {len(result.warnings)} warnings"
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
        with open(args.file) as f:
            data = yaml.safe_load(f)
        model = SlayerModel.model_validate(data)
        run_sync(storage.save_model(model))
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

    from slayer.core.models import DatasourceConfig

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
        with open(args.file) as f:
            data = yaml.safe_load(f)
        ds = DatasourceConfig.model_validate(data)
        run_sync(storage.save_datasource(ds))
        print(f"Created datasource '{ds.name}' ({ds.type}).")

    elif args.datasources_command == "create-inline":
        ds_data = {"name": args.name, "type": args.type}
        for field in ("host", "port", "database", "username", "password", "connection_string", "description"):
            val = getattr(args, field.replace("-", "_"), None)
            if val is not None:
                ds_data[field] = val
        if args.password_stdin:
            import getpass

            ds_data["password"] = (
                getpass.getpass("Password: ") if sys.stdin.isatty() else sys.stdin.readline().rstrip("\n")
            )
        ds = DatasourceConfig.model_validate(ds_data)
        run_sync(storage.save_datasource(ds))
        print(f"Created datasource '{ds.name}' ({ds.type}).")

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
        print("Usage: slayer datasources {list,show,create,create-inline,delete,test}")
        sys.exit(1)


if __name__ == "__main__":
    main()
