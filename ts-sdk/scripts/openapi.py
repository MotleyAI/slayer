"""Write SLayer's REST OpenAPI schema to the path given as the first argument."""

import json
import sys
import tempfile

from slayer.api.server import create_app
from slayer.storage.yaml_storage import YAMLStorage

with tempfile.TemporaryDirectory() as storage_dir:
    schema = create_app(YAMLStorage(base_dir=storage_dir)).openapi()
with open(sys.argv[1], "w") as out:
    json.dump(schema, out, indent=1)
