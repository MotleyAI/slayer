"""Regression tests for the DuckDB notebook transient-network classifier.

The classifier must skip only genuine transient network failures and never a
deterministic notebook bug — even one whose error text happens to contain a
transient-looking phrase like "timed out" next to a remote URL.
"""

from tests.integration.test_notebooks import (
    _duckdb_failure_text,
    _duckdb_network_error_is_transient,
)


def _notebook(cells):
    return type("_NB", (), {"cells": cells})()


def _is_transient(cells) -> bool:
    return _duckdb_network_error_is_transient(_duckdb_failure_text(_notebook(cells)))


def test_bash_source_embedding_traceback_is_not_transient():
    """A %%bash CalledProcessError embeds the cell source (with the CDN URL); a
    deterministic 'timed out' there must NOT be read as a network hiccup."""
    cells = [{"outputs": [
        {"output_type": "stream", "name": "stderr",
         "text": "AssertionError: expected 48 rows, timed out waiting\n"},
        {"output_type": "error", "ename": "CalledProcessError",
         "evalue": ("Command 'b\"slayer query @hero.json  # "
                    "read_csv_auto('https://cdn.jsdelivr.net/...') timed out\"' "
                    "returned non-zero exit status 1."),
         "traceback": ["...cdn.jsdelivr.net... timed out ..."]},
    ]}]
    assert _is_transient(cells) is False


def test_bash_stderr_network_error_is_transient():
    """A real network failure surfaces on the %%bash stderr stream (host +
    signature) and IS transient."""
    cells = [{"outputs": [
        {"output_type": "stream", "name": "stderr",
         "text": "curl: (6) Could not resolve host: cdn.jsdelivr.net\n"},
        {"output_type": "error", "ename": "CalledProcessError",
         "evalue": "Command 'b\"...\"' returned non-zero exit status 6.",
         "traceback": ["..."]},
    ]}]
    assert _is_transient(cells) is True


def test_python_exception_network_error_is_transient():
    """A Python cell carries the real error in evalue/traceback (no source
    embedding), so an httpfs 5xx over the CDN IS transient."""
    cells = [{"outputs": [
        {"output_type": "error", "ename": "IOException",
         "evalue": "HTTP 503 error for HTTP HEAD to 'https://cdn.jsdelivr.net/...'",
         "traceback": ["duckdb.IOException: HTTP 503 ... cdn.jsdelivr.net"]},
    ]}]
    assert _is_transient(cells) is True


def test_deterministic_python_bug_is_not_transient():
    """A genuine NotImplementedError with no host/signature fails loudly."""
    cells = [{"outputs": [
        {"output_type": "error", "ename": "NotImplementedError",
         "evalue": "regroup attach combined with a transform measure is not supported",
         "traceback": ["NotImplementedError: ..."]},
    ]}]
    assert _is_transient(cells) is False
