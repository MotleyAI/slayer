#!/bin/sh
# Ingest models from SQL Server and start the SLayer API server.

python -c "
from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir='/data')
ds = DatasourceConfig(
    name='demo', type='mssql',
    host='sqlserver', port=1433,
    database='slayer_demo', username='sa', password='YourStrong@Passw0rd',
)
run_sync(storage.save_datasource(ds))
result = run_sync(ingest_datasource_idempotent(datasource=ds, storage=storage))
print(f'Ingested {len(result.additions)} models')
"

exec slayer serve --host 0.0.0.0 --port 5143 --storage /data
