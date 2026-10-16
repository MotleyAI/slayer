"""Print SLayer's REST OpenAPI schema as JSON to stdout."""

import json
import sys
import tempfile

from slayer.api.server import create_app
from slayer.storage.yaml_storage import YAMLStorage

with tempfile.TemporaryDirectory() as storage_dir:
    schema = create_app(YAMLStorage(base_dir=storage_dir)).openapi()
json.dump(schema, sys.stdout, indent=1)
