"""Session-scoped fixture stack for the live-Metabase e2e suite (DEV-1562).

Boots one Metabase Docker container (Metabase v0.62.1.5) plus two
token-protected pg-serve instances (both bound to ``0.0.0.0`` with
per-session random tokens so the Metabase container can reach them via
``host.docker.internal``). The primary instance backs Metabase + most
tests; the second is used by the auth-error tests (L.2 / L.3) where the
test deliberately presents a wrong password. Yields a small typed
``MetabaseE2EEnv`` to every test.

Skips cleanly when Docker is unavailable or the container never reaches a
healthy state within the boot budget; never fails the suite for environmental
reasons.
"""

from __future__ import annotations

import json
import logging
import secrets
import shutil
import subprocess
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest
import requests
from pydantic import BaseModel, ConfigDict

from tests.integration._pg_serve_helpers import start_pg_demo_server

METABASE_IMAGE = "metabase/metabase:v0.62.1.5"
METABASE_INTERNAL_PORT = 3000

ADMIN_EMAIL = "admin@slayer.test"
ADMIN_PASSWORD = "slayer-pg-e2e-pw"  # NOSONAR(S2068) — fixture credential
ADMIN_FIRST = "SLayer"
ADMIN_LAST = "Tester"
ADMIN_SITE_NAME = "SLayer PG E2E"

HEALTH_TIMEOUT_S = 180
METADATA_TIMEOUT_S = 90
DOCKER_INFO_TIMEOUT_S = 5
DOCKER_RUN_TIMEOUT_S = 240  # generous: covers first-time image pull
DOCKER_STOP_TIMEOUT_S = 15

# Per-session random tokens. Module-level so they're picked up by every
# helper without threading args, but evaluated once per pytest process,
# never persisted to disk. Both pg-serves are bound to 0.0.0.0 (so a
# Metabase container can reach them via host.docker.internal), and the
# `validate_bind_address` guard in `_pg_serve_helpers.start_pg_demo_server`
# refuses to start either server unless a real token is configured —
# i.e. defence-in-depth so the helper can't accidentally expose
# unauthenticated query access on a network-facing interface.
PRIMARY_TOKEN_VALUE = secrets.token_urlsafe(32)
AUTH_TEST_TOKEN_VALUE = secrets.token_urlsafe(32)


class MetabaseClient:
    """Thin synchronous wrapper around the Metabase REST API."""

    def __init__(self, *, base_url: str, session_token: str, db_id: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.session_token = session_token
        self.db_id = db_id
        self._session = requests.Session()
        self._session.headers.update({"X-Metabase-Session": session_token})

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    @staticmethod
    def _raise_with_body(resp: "requests.Response", path: str) -> None:
        """``raise_for_status`` but with the response body in the message.

        ``requests.exceptions.HTTPError`` defaults to status + URL only; for
        Metabase 4xx debugging we need the JSON body (the ``message`` /
        ``error`` / ``via`` fields name the actual MBQL or SQL rejection
        reason), otherwise CI failures look like opaque ``400 Client Error``.
        """
        if resp.ok:
            return
        body_preview = (resp.text or "").strip()[:2000]
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason} from {path}: {body_preview}",
            response=resp,
        )

    def post(self, path: str, json_body: Optional[dict] = None, *, timeout: int = 30) -> dict:
        resp = self._session.post(self._url(path), json=json_body or {}, timeout=timeout)
        self._raise_with_body(resp, path)
        return resp.json() if resp.content else {}

    def get(self, path: str, params: Optional[dict] = None, *, timeout: int = 30) -> Any:
        resp = self._session.get(self._url(path), params=params, timeout=timeout)
        self._raise_with_body(resp, path)
        return resp.json() if resp.content else {}

    def put(self, path: str, json_body: Optional[dict] = None, *, timeout: int = 30) -> dict:
        resp = self._session.put(self._url(path), json=json_body or {}, timeout=timeout)
        self._raise_with_body(resp, path)
        return resp.json() if resp.content else {}

    def post_raw(self, path: str, json_body: Optional[dict] = None, *, timeout: int = 30):
        """POST without raise_for_status — for error-envelope tests."""
        return self._session.post(self._url(path), json=json_body or {}, timeout=timeout)

    # Convenience helpers -----------------------------------------------------

    def dataset(self, query: dict, *, timeout: int = 60) -> dict:
        return self.post("/api/dataset", {"database": self.db_id, **query}, timeout=timeout)

    def database_metadata(self) -> dict:
        return self.get(f"/api/database/{self.db_id}/metadata")

    def sync_schema(self) -> dict:
        return self.post(f"/api/database/{self.db_id}/sync_schema")

    def table_metadata(self, table_id: int) -> dict:
        return self.get(f"/api/table/{table_id}/query_metadata")

    def field_values(self, field_id: int) -> dict:
        return self.get(f"/api/field/{field_id}/values")

    def table_id_by_name(self, table_name: str) -> int:
        for tbl in self.database_metadata().get("tables", []):
            if tbl.get("name") == table_name:
                return int(tbl["id"])
        raise LookupError(f"table {table_name!r} not present in Metabase metadata")

    def field_id_by_name(self, table_name: str, field_name: str) -> int:
        tid = self.table_id_by_name(table_name)
        for f in self.table_metadata(tid).get("fields", []):
            if f.get("name") == field_name:
                return int(f["id"])
        raise LookupError(f"field {table_name}.{field_name!r} not present")


class MetabaseE2EEnv(BaseModel):
    """Shared state yielded by the ``metabase_e2e_env`` fixture.

    ``arbitrary_types_allowed`` because the fields carry runtime resources
    (``MetabaseClient`` wraps a ``requests.Session``; ``log_records`` is a
    live ``LogRecord`` list mutated by the in-process pg-serve handler;
    ``pg_primary_storage`` is the SLayer ``StorageBackend`` handle for
    B.3 / B.4 mutation tests). The model is yielded once per session and
    never serialised — Pydantic gives us the field declarations + repr +
    consistency with the rest of the codebase, nothing more.

    Both pg-serves are token-protected and bound to ``0.0.0.0`` (so the
    Metabase container reaches them via ``host.docker.internal``). The
    primary server backs Metabase + most tests; the auth-test server
    backs L.2 / L.3 (bad-password / bogus-database scenarios) where the
    test deliberately presents a wrong credential.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_url: str
    session_token: str
    client: MetabaseClient
    token_db_id: int
    pg_primary: Tuple[str, int]
    pg_primary_password: str
    pg_auth: Tuple[str, int, str]
    log_records: List[logging.LogRecord]
    pg_primary_storage: Any

    def make_client(self, db_id: int) -> MetabaseClient:
        """Return a MetabaseClient bound to a different db_id (same session)."""
        return MetabaseClient(
            base_url=self.base_url, session_token=self.session_token, db_id=db_id
        )


# ---------------------------------------------------------------------------
# Docker probes / Metabase bootstrap
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            timeout=DOCKER_INFO_TIMEOUT_S,
            text=True,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and result.stdout.strip() != ""


def _run_metabase_container() -> Tuple[str, int]:
    """Start the Metabase container; return ``(container_id, host_port)``."""
    container_name = f"slayer-mb-e2e-{uuid.uuid4().hex[:8]}"
    cmd = [
        "docker", "run",
        "-d", "--rm",
        "--name", container_name,
        "--add-host", "host.docker.internal:host-gateway",
        "-p", f"127.0.0.1::{METABASE_INTERNAL_PORT}",
        METABASE_IMAGE,
    ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=DOCKER_RUN_TIMEOUT_S
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"docker run failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    container_id = result.stdout.strip()

    # If anything fails between ``docker run`` returning and us returning
    # ``container_id`` to the caller, the fixture's ``finally`` block never
    # sees the id and can't call _stop_container — leaking the container.
    # Wrap the inspect call so any failure cleans up before propagating.
    try:
        inspect = subprocess.run(
            [
                "docker", "inspect",
                "--format",
                '{{(index (index .NetworkSettings.Ports "' + str(METABASE_INTERNAL_PORT) + '/tcp") 0).HostPort}}',
                container_id,
            ],
            capture_output=True, text=True, timeout=10,
        )
        if inspect.returncode != 0 or not inspect.stdout.strip().isdigit():
            raise RuntimeError(
                f"docker inspect failed to surface host port: {inspect.stderr.strip()}"
            )
        host_port = int(inspect.stdout.strip())
    except Exception:
        _stop_container(container_id)
        raise
    return container_id, host_port


def _stop_container(container_id: Optional[str]) -> None:
    if not container_id:
        return
    try:
        subprocess.run(
            ["docker", "stop", container_id],
            capture_output=True, timeout=DOCKER_STOP_TIMEOUT_S,
        )
    except Exception:
        pass


def _dump_container_logs(container_id: str, tail: int = 200) -> str:
    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", str(tail), container_id],
            capture_output=True, text=True, timeout=10,
        )
        return (result.stdout or "") + (result.stderr or "")
    except Exception as exc:
        return f"<log dump failed: {exc}>"


# Where the fixture parks the Metabase container's tail logs on teardown so
# the CI workflow's ``if: failure()`` step can surface them. The container
# is started with ``--rm`` and stopped during fixture teardown, so by the
# time the workflow's log-dump step runs the container is already gone —
# we dump the logs to a file ahead of ``docker stop``.
CONTAINER_LOG_DUMP_PATH = "/tmp/slayer-metabase-e2e-container.log"  # NOSONAR(S5443) — fixed path for CI workflow to read


def _wait_for_health(base_url: str, timeout_s: int) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{base_url}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def _fetch_setup_token(base_url: str) -> Optional[str]:
    r = requests.get(f"{base_url}/api/session/properties", timeout=10)
    r.raise_for_status()
    props = r.json()
    token = props.get("setup-token")
    return token if token else None


def _run_setup(base_url: str, setup_token: str) -> str:
    """Walk Metabase's first-boot setup; return the admin session id."""
    body = {
        "token": setup_token,
        "user": {
            "first_name": ADMIN_FIRST,
            "last_name": ADMIN_LAST,
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
            "password_confirm": ADMIN_PASSWORD,
            "site_name": ADMIN_SITE_NAME,
        },
        "prefs": {
            "site_name": ADMIN_SITE_NAME,
            "allow_tracking": False,
        },
    }
    r = requests.post(f"{base_url}/api/setup", json=body, timeout=60)
    r.raise_for_status()
    payload = r.json()
    sid = payload.get("id") or payload.get("session_id")
    if not sid:
        raise RuntimeError(f"Metabase /api/setup returned no session id: {payload}")
    return sid


def _login(base_url: str) -> str:
    """Fallback login when the instance is already set up."""
    r = requests.post(
        f"{base_url}/api/session",
        json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
        timeout=30,
    )
    r.raise_for_status()
    sid = r.json().get("id")
    if not sid:
        raise RuntimeError("Metabase /api/session returned no session id")
    return sid


def _register_database(
    base_url: str,
    session_token: str,
    *,
    name: str,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> int:
    body = {
        "engine": "postgres",
        "name": name,
        "details": {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "ssl": False,
            "tunnel-enabled": False,
            "advanced-options": False,
        },
        "is_full_sync": True,
        "is_on_demand": False,
    }
    r = requests.post(
        f"{base_url}/api/database",
        json=body,
        headers={"X-Metabase-Session": session_token},
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(
            f"POST /api/database failed (status {r.status_code}): {r.text}"
        )
    payload = r.json()
    db_id = payload.get("id")
    if not isinstance(db_id, int):
        raise RuntimeError(f"POST /api/database returned no integer id: {payload}")
    return db_id


def _wait_for_metadata(
    base_url: str, session_token: str, db_id: int, *, min_tables: int, timeout_s: int
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    headers = {"X-Metabase-Session": session_token}
    last_payload: Dict[str, Any] = {}
    while time.monotonic() < deadline:
        r = requests.get(
            f"{base_url}/api/database/{db_id}/metadata",
            headers=headers,
            timeout=30,
        )
        if r.status_code == 200:
            last_payload = r.json()
            tables = last_payload.get("tables") or []
            if len(tables) >= min_tables:
                return last_payload
        time.sleep(2)
    raise RuntimeError(
        f"Metabase metadata never reached {min_tables}+ tables on db {db_id}. "
        f"Last payload table count: {len(last_payload.get('tables') or [])}"
    )


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def _bootstrap_metabase_session(
    *, container_id: str, host_port: int, port_a: int, port_b: int,
) -> Tuple[str, str, int, int]:
    """Boot Metabase, run first-time setup, register both pg-serve databases.

    Returns ``(base_url, session_token, db_id_no_token, db_id_token)``.
    Extracted out of ``metabase_e2e_env`` to keep the fixture's Cognitive
    Complexity below Sonar's S3776 threshold (15).
    """
    base_url = f"http://127.0.0.1:{host_port}"

    if not _wait_for_health(base_url, HEALTH_TIMEOUT_S):
        logs = _dump_container_logs(container_id)
        pytest.skip(
            f"Metabase container never reached healthy state within {HEALTH_TIMEOUT_S}s. "
            f"Last logs:\n{logs}"
        )

    setup_token = _fetch_setup_token(base_url)
    if setup_token:
        session_token = _run_setup(base_url, setup_token)
    else:
        session_token = _login(base_url)

    db_id_no_token = _register_database(
        base_url, session_token,
        name="slayer-jaffle",
        host="host.docker.internal", port=port_a, dbname="jaffle_shop",
        user="tester", password=PRIMARY_TOKEN_VALUE,
    )
    _wait_for_metadata(
        base_url, session_token, db_id_no_token,
        min_tables=7, timeout_s=METADATA_TIMEOUT_S,
    )

    db_id_token = _register_database(
        base_url, session_token,
        name="slayer-jaffle-token",
        host="host.docker.internal", port=port_b, dbname="jaffle_shop",
        user="tester", password=AUTH_TEST_TOKEN_VALUE,
    )
    _wait_for_metadata(
        base_url, session_token, db_id_token,
        min_tables=7, timeout_s=METADATA_TIMEOUT_S,
    )

    return base_url, session_token, db_id_no_token, db_id_token


@pytest.fixture(scope="session")
def metabase_e2e_env() -> Iterator[MetabaseE2EEnv]:
    """Session-scoped Metabase + dual pg-serve bootstrap."""
    pytest.importorskip("asyncpg")

    if not _docker_available():
        pytest.skip("Docker is unavailable; the metabase_e2e suite needs a working Docker daemon")

    log_records: List[logging.LogRecord] = []
    storage_sink: list = []
    loop_a = thread_a = None
    loop_b = thread_b = None
    container_id: Optional[str] = None

    try:
        loop_a, thread_a, host_a, port_a = start_pg_demo_server(
            token=PRIMARY_TOKEN_VALUE,
            log_records=log_records,
            storage_sink=storage_sink,
            bind_host="0.0.0.0",  # NOSONAR(S104) — required so Metabase-in-container reaches pg-serve via host.docker.internal; token-protected per validate_bind_address
        )
        loop_b, thread_b, host_b, port_b = start_pg_demo_server(
            token=AUTH_TEST_TOKEN_VALUE,
            bind_host="0.0.0.0",  # NOSONAR(S104) — same; Metabase auth path test (A.5) drives it via host.docker.internal too
        )

        container_id, host_port = _run_metabase_container()
        base_url, session_token, db_id_no_token, db_id_token = _bootstrap_metabase_session(
            container_id=container_id, host_port=host_port, port_a=port_a, port_b=port_b,
        )

        client = MetabaseClient(
            base_url=base_url, session_token=session_token, db_id=db_id_no_token,
        )

        env = MetabaseE2EEnv(
            base_url=base_url,
            session_token=session_token,
            client=client,
            token_db_id=db_id_token,
            pg_primary=(host_a, port_a),
            pg_primary_password=PRIMARY_TOKEN_VALUE,
            pg_auth=(host_b, port_b, AUTH_TEST_TOKEN_VALUE),
            log_records=log_records,
            pg_primary_storage=storage_sink[0] if storage_sink else None,
        )
        yield env
    finally:
        if container_id:
            try:
                with open(CONTAINER_LOG_DUMP_PATH, "w") as f:
                    f.write(_dump_container_logs(container_id, tail=400))
            except Exception:
                pass
            _stop_container(container_id)
        for loop, thread in ((loop_b, thread_b), (loop_a, thread_a)):
            if loop is None or thread is None:
                continue
            try:
                loop.call_soon_threadsafe(loop.stop)
            except Exception:
                pass
            try:
                thread.join(timeout=5)
            except Exception:
                pass


# Used by tests that re-emit the env in a per-test mutation context.
def encode_native_query(sql: str) -> Dict[str, Any]:
    """Convenience helper used by native-SQL tests (E.7, G.4, etc.)."""
    return {"type": "native", "native": {"query": sql, "template-tags": {}}}


def encode_mbql_query(*, source_table: int, **extras: Any) -> Dict[str, Any]:
    """Build an MBQL ``query`` body for ``/api/dataset``."""
    inner: Dict[str, Any] = {"source-table": source_table}
    inner.update(extras)
    return {"type": "query", "query": inner}


# Re-export the JSON dump shim used by tests so they don't need to import json themselves.
__all__ = [
    "MetabaseClient",
    "MetabaseE2EEnv",
    "encode_mbql_query",
    "encode_native_query",
    "metabase_e2e_env",
    "json",
]
