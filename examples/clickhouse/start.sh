#!/bin/sh
# Ingest models from ClickHouse and start the SLayer API server.
set -e

python -c "
from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir='/data')
engine = SlayerQueryEngine(storage=storage)
ds = DatasourceConfig(
    name='demo', type='clickhouse',
    host='clickhouse', port=8123,
    database='slayer_demo', username='slayer', password='slayer',
)
run_sync(storage.save_datasource(ds))
models = engine.run_sync(ingest_datasource(ds, client=engine.client_for(ds)))
for m in models:
    run_sync(storage.save_model(m))
print(f'Ingested {len(models)} models')
"

exec slayer serve --host 0.0.0.0 --port 5143 --models-dir /data
