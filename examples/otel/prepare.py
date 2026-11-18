"""Prepare the SLayer container: wait for the OTLP endpoint, then register the demo datasource.

start.sh runs this before `slayer ingest` and `slayer serve`.
"""

import os
import socket
import time
from urllib.parse import urlsplit

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.storage.yaml_storage import YAMLStorage

STORAGE = "/data"
WAIT_SECONDS = 120


def wait_for_otlp_endpoint() -> None:
    """Wait for the host and port in OTEL_EXPORTER_OTLP_ENDPOINT, so the startup ingest is traced."""
    try:
        url = urlsplit(os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
        hostname = url.hostname
        port = url.port or (443 if url.scheme == "https" else 80)
    except ValueError as error:
        print(f"OTEL_EXPORTER_OTLP_ENDPOINT is not a valid URL ({error}); not waiting for it")
        return
    if not hostname:
        return
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        try:
            socket.create_connection((hostname, port), timeout=2).close()
            print(f"OTLP endpoint {hostname}:{port} is reachable")
            return
        except OSError:
            time.sleep(2)
    print(f"OTLP endpoint {hostname}:{port} not reachable; continuing without it")


def register_datasource() -> None:
    run_sync(YAMLStorage(base_dir=STORAGE).save_datasource(DatasourceConfig(
        name="demo", type="postgres", host="postgres", port=5432,
        database="slayer_demo", username="slayer", password=os.environ["POSTGRES_PASSWORD"],
    )))


if __name__ == "__main__":
    wait_for_otlp_endpoint()
    register_datasource()
