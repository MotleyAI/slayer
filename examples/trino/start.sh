#!/bin/sh
# Ingest datasets from Trino and start the SLayer API server.
set -e

python -c "
from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir='/data')
engine = SlayerQueryEngine(storage=storage)
ds = DatasourceConfig(
    name='demo', type='trino',
    host='trino', port=8080,
    database='memory/default', username='slayer',
)
run_sync(storage.save_datasource(ds))
datasets = engine.ingest_sync(ds)
for d in datasets:
    run_sync(storage.save_dataset(d))
print(f'Ingested {len(datasets)} datasets')
"

exec slayer serve --host 0.0.0.0 --port 5143 --storage /data
