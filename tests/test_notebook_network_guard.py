"""The DuckDB notebook transient-network classifier skips on any external-host
outage (DEV-1866: a shared CI-runner IP gets 403/404-blocked by the CDN, which
must skip — not fail — the httpfs demo), while real slayer bugs still fail loudly.
Network-free: the classifier is fed captured error strings directly."""

import nbformat
import pytest

from tests.integration.test_notebooks import (
    _duckdb_failure_text,
    _duckdb_network_error_is_transient,
)

_HOST = "cdn.jsdelivr.net"
_URL = f"https://{_HOST}/npm/vega-datasets@2/data/seattle-weather.csv"


@pytest.mark.parametrize(
    "text",
    [
        f"HTTP Error: HTTP GET error on '{_URL}' (HTTP 403 Forbidden)",
        f"HTTP Error: HTTP GET error on '{_URL}' (HTTP 408 Request Timeout)",
        f"HTTP Error: HTTP GET error on '{_URL}' (HTTP 429 Too Many Requests)",
        f"HTTP Error: HTTP GET error on '{_URL}' (HTTP 503 Service Unavailable)",
        f"IO Error: Could not resolve host: {_HOST}",
        "curl: (22) The requested URL returned error: 403 install.duckdb.org",
    ],
)
def test_external_host_outage_is_transient(text):
    assert _duckdb_network_error_is_transient(text) is True


@pytest.mark.parametrize(
    "text",
    [
        # DuckDB's httpfs range/download mismatch names no host but is remote-only.
        "HTTP Error: Server sent back more data than expected, `SET force_download=true` might help in this case",
        "(_duckdb.HTTPException) HTTP Error: Server sent back more data than expected",
    ],
)
def test_hostless_httpfs_transport_error_is_transient(text):
    assert _duckdb_network_error_is_transient(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "",
        "BinderException: Referenced column 'temp_max' not found",
        "NotImplementedError: dialect does not support this aggregation",
        # A 404/400 on the fixed URL = the resource is genuinely wrong/gone: a real
        # failure that must stay loud, not a CDN outage — including via curl's -f.
        f"HTTP Error: HTTP GET error on '{_URL}' (HTTP 404 Not Found)",
        f"HTTP Error: HTTP GET error on '{_URL}' (HTTP 400 Bad Request)",
        "curl: (22) The requested URL returned error: 404 install.duckdb.org",
        # A transient status but no external host named — not a classifiable outage.
        "HTTP Error: HTTP GET error on 'https://example.internal/x' (HTTP 503)",
    ],
)
def test_real_bugs_are_not_transient(text):
    assert _duckdb_network_error_is_transient(text) is False


def _nb_with_error_streams(*chunks: str):
    nb = nbformat.v4.new_notebook()
    cell = nbformat.v4.new_code_cell("x")
    cell["outputs"] = [
        nbformat.v4.new_output("stream", name="stderr", text=c) for c in chunks
    ]
    cell["outputs"].append(
        nbformat.v4.new_output("error", ename="CalledProcessError", evalue="", traceback=[])
    )
    nb.cells = [cell]
    return nb


def test_failure_text_rejoins_chunked_stderr():
    # nbclient splits one stderr write into several stream outputs; the host must
    # survive a split across a chunk boundary so the host gate still matches.
    nb = _nb_with_error_streams("HTTP GET error on 'https://cdn.jsd", "elivr.net/x' (HTTP 429)")
    text = _duckdb_failure_text(nb)
    assert _HOST in text
    assert _duckdb_network_error_is_transient(text) is True


def test_failure_text_ignores_cells_without_error_output():
    nb = nbformat.v4.new_notebook()
    ok = nbformat.v4.new_code_cell("x")
    ok["outputs"] = [nbformat.v4.new_output("stream", name="stdout", text=_HOST)]
    nb.cells = [ok]
    assert _duckdb_failure_text(nb) == ""
