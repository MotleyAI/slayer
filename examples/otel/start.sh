#!/bin/sh
# Wait for the OTLP endpoint and register the demo datasource (prepare.py), ingest it through
# the CLI (one traced ingest), then serve.
set -e

python /examples/prepare.py
slayer ingest --datasource demo --schema public --storage /data
exec slayer serve --host 0.0.0.0 --port 5143 --storage /data
