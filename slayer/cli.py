"""CLI entry point for SLayer."""

import argparse
import os
import sys


def main():
    parser = argparse.ArgumentParser(prog="slayer", description="SLayer — semantic layer for AI agents")
    subparsers = parser.add_subparsers(dest="command")

    # serve command
    serve_parser = subparsers.add_parser("serve", help="Start the REST API server")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=5143)
    serve_parser.add_argument("--models-dir", default=os.environ.get("SLAYER_MODELS_DIR", "./slayer_data"))

    # mcp command
    mcp_parser = subparsers.add_parser("mcp", help="Start the MCP server")
    mcp_parser.add_argument("--models-dir", default=os.environ.get("SLAYER_MODELS_DIR", "./slayer_data"))

    # query command
    query_parser = subparsers.add_parser("query", help="Execute a SLayer query from JSON")
    query_parser.add_argument("query_json", help="JSON query string or @file.json")
    query_parser.add_argument("--models-dir", default=os.environ.get("SLAYER_MODELS_DIR", "./slayer_data"))
    query_parser.add_argument("--format", choices=["json", "table"], default="table")
    query_parser.add_argument("--dry-run", action="store_true", help="Generate SQL without executing")
    query_parser.add_argument("--explain", action="store_true", help="Run EXPLAIN ANALYZE on the query")

    # ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Auto-ingest models from a datasource")
    ingest_parser.add_argument("--datasource", required=True)
    ingest_parser.add_argument("--schema", default=None)
    ingest_parser.add_argument("--models-dir", default=os.environ.get("SLAYER_MODELS_DIR", "./slayer_data"))

    # models command
    models_parser = subparsers.add_parser("models", help="Manage models")
    models_parser.add_argument("--models-dir", default=os.environ.get("SLAYER_MODELS_DIR", "./slayer_data"))
    models_subparsers = models_parser.add_subparsers(dest="models_command")

    models_subparsers.add_parser("list", help="List all models")

    models_show_parser = models_subparsers.add_parser("show", help="Show a model definition")
    models_show_parser.add_argument("name", help="Model name")

    models_create_parser = models_subparsers.add_parser("create", help="Create a model from a YAML file")
    models_create_parser.add_argument("file", help="Path to YAML file")

    models_delete_parser = models_subparsers.add_parser("delete", help="Delete a model")
    models_delete_parser.add_argument("name", help="Model name")

    # datasources command
    datasources_parser = subparsers.add_parser("datasources", help="Manage datasources")
    datasources_parser.add_argument("--models-dir", default=os.environ.get("SLAYER_MODELS_DIR", "./slayer_data"))
    datasources_subparsers = datasources_parser.add_subparsers(dest="datasources_command")

    datasources_subparsers.add_parser("list", help="List all datasources")

    datasources_show_parser = datasources_subparsers.add_parser("show", help="Show a datasource definition")
    datasources_show_parser.add_argument("name", help="Datasource name")

    args = parser.parse_args()

    if args.command == "serve":
        _run_serve(args)
    elif args.command == "mcp":
        _run_mcp(args)
    elif args.command == "query":
        _run_query(args)
    elif args.command == "ingest":
        _run_ingest(args)
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
    from slayer.storage.yaml_storage import YAMLStorage

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

    storage = YAMLStorage(base_dir=args.models_dir)
    engine = SlayerQueryEngine(storage=storage)
    result = engine.execute(query=slayer_query)

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
    from slayer.storage.yaml_storage import YAMLStorage

    storage = YAMLStorage(base_dir=args.models_dir)
    app = create_app(storage=storage)

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)


def _run_mcp(args):
    from slayer.mcp.server import create_mcp_server
    from slayer.storage.yaml_storage import YAMLStorage

    storage = YAMLStorage(base_dir=args.models_dir)
    mcp = create_mcp_server(storage=storage)
    mcp.run()


def _run_ingest(args):
    from slayer.engine.ingestion import ingest_datasource
    from slayer.storage.yaml_storage import YAMLStorage

    storage = YAMLStorage(base_dir=args.models_dir)
    ds = storage.get_datasource(args.datasource)
    if ds is None:
        print(f"Datasource '{args.datasource}' not found in {args.models_dir}")
        sys.exit(1)

    models = ingest_datasource(datasource=ds, schema=args.schema)
    for model in models:
        storage.save_model(model)
        print(f"Ingested: {model.name} ({len(model.dimensions)} dims, {len(model.measures)} measures)")


def _run_models(args):
    import yaml

    from slayer.core.models import SlayerModel
    from slayer.storage.yaml_storage import YAMLStorage

    storage = YAMLStorage(base_dir=args.models_dir)

    if args.models_command == "list":
        names = storage.list_models()
        if not names:
            print("No models found.")
            return
        for name in names:
            model = storage.get_model(name)
            if model and model.hidden:
                continue
            desc = f"  — {model.description}" if model and model.description else ""
            print(f"{name}{desc}")

    elif args.models_command == "show":
        model = storage.get_model(args.name)
        if model is None:
            print(f"Model '{args.name}' not found.")
            sys.exit(1)
        data = model.model_dump(mode="json", exclude_none=True)
        print(yaml.dump(data, sort_keys=False, default_flow_style=False).rstrip())

    elif args.models_command == "create":
        with open(args.file) as f:
            data = yaml.safe_load(f)
        model = SlayerModel.model_validate(data)
        storage.save_model(model)
        print(f"Created model '{model.name}'.")

    elif args.models_command == "delete":
        deleted = storage.delete_model(args.name)
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

    from slayer.storage.yaml_storage import YAMLStorage

    storage = YAMLStorage(base_dir=args.models_dir)

    if args.datasources_command == "list":
        names = storage.list_datasources()
        if not names:
            print("No datasources found.")
            return
        for name in names:
            ds = storage.get_datasource(name)
            ds_type = ds.type if ds and ds.type else "unknown"
            print(f"{name}  ({ds_type})")

    elif args.datasources_command == "show":
        ds = storage.get_datasource(args.name)
        if ds is None:
            print(f"Datasource '{args.name}' not found.")
            sys.exit(1)
        data = ds.model_dump(mode="json", exclude_none=True)
        if "password" in data:
            data["password"] = "********"
        if "connection_string" in data:
            data["connection_string"] = "********"
        print(yaml.dump(data, sort_keys=False, default_flow_style=False).rstrip())

    else:
        print("Usage: slayer datasources {list,show}")
        sys.exit(1)


if __name__ == "__main__":
    main()
