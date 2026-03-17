#!/bin/sh
# Ingest models from MySQL and start the SLayer API server.

python -c "
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir='/data')
ds = DatasourceConfig(
    name='demo', type='mysql',
    host='mysql', port=3306,
    database='slayer_demo', username='slayer', password='slayer',
)
storage.save_datasource(ds)
models = ingest_datasource(datasource=ds)
for m in models:
    storage.save_model(m)
print(f'Ingested {len(models)} models')
"

exec slayer serve --host 0.0.0.0 --port 5143 --models-dir /data
