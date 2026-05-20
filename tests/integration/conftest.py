"""Shared integration-test fixtures (DEV-1390).

Currently hosts the Flight SQL demo-server fixture used by both
``test_integration_flight.py`` (JayDeBeAPI) and
``test_integration_flight_pyarrow_client.py``.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Tuple

import pytest

JDBC_DRIVER_VERSION = "18.3.0"
JDBC_DRIVER_URL = (
    "https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/"
    f"{JDBC_DRIVER_VERSION}/flight-sql-jdbc-driver-{JDBC_DRIVER_VERSION}.jar"
)
JDBC_DRIVER_CLASS = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
_CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache"


def _java_on_path() -> bool:
    return shutil.which("java") is not None


@pytest.fixture(scope="session")
def jdbc_jar() -> Path:
    """Download (once) and return the path to the Apache flight-sql-jdbc-driver JAR.

    Mirrors the same fixture in ``tests/flight/conftest.py`` so the JAR is shared
    via ``tests/.cache/`` between the Phase 1.0 capture harness and the live
    integration suite.
    """
    if not _java_on_path():
        pytest.skip("Java >= 11 required on PATH for Flight SQL JDBC tests")

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    jar_path = _CACHE_DIR / f"flight-sql-jdbc-driver-{JDBC_DRIVER_VERSION}.jar"

    if not jar_path.exists():
        try:
            urllib.request.urlretrieve(JDBC_DRIVER_URL, jar_path)
        except Exception as exc:
            pytest.skip(f"Could not download flight-sql-jdbc-driver: {exc}")

    if jar_path.stat().st_size < 1_000_000:
        jar_path.unlink(missing_ok=True)
        pytest.skip("Cached flight-sql-jdbc-driver JAR looks corrupted")

    return jar_path


def _format_flight_jdbc_url(
    *,
    host: str,
    port: int,
    use_encryption: bool = False,
    token: Optional[str] = None,
    environment_id: Optional[str] = None,
) -> str:
    params = [f"useEncryption={'true' if use_encryption else 'false'}"]
    if token is not None:
        params.append(f"token={token}")
    if environment_id is not None:
        params.append(f"environmentId={environment_id}")
    return f"jdbc:arrow-flight-sql://{host}:{port}/?{'&'.join(params)}"


@pytest.fixture
def flight_jdbc_url() -> Callable[..., str]:
    return _format_flight_jdbc_url


_ARROW_JVM_OPENS = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    # SecureRandom uses /dev/random by default; on fresh CI containers it
    # can block for many minutes waiting for entropy. Point it at urandom
    # so the JVM bootstrap doesn't stall before the first gRPC call.
    "-Djava.security.egd=file:/dev/./urandom",
)


def _ensure_jvm_started_for_arrow(jar_path: Path) -> None:
    """Pre-start the JVM with the ``--add-opens`` flags Arrow needs on Java 17+.

    Arrow's MemoryUtil reflectively pokes at ``java.nio.Buffer.address``;
    Java 17+ strict module access blocks this unless ``java.nio`` is opened
    to the unnamed module. We start JPype's JVM eagerly (with the JDBC JAR
    on the classpath, so JayDeBeAPI's lazy ``startJVM()`` becomes a no-op
    and ``jpype.JClass(...)`` can resolve the driver class).
    """
    import jpype

    if jpype.isJVMStarted():
        return
    jpype.startJVM(
        jpype.getDefaultJVMPath(),
        *_ARROW_JVM_OPENS,
        classpath=[str(jar_path)],
        convertStrings=True,
    )


def pytest_sessionfinish(session, exitstatus) -> None:  # noqa: ARG001
    """Shut down the JVM after the integration suite finishes.

    JPype's ``startJVM`` spins up non-daemon Java threads (Reference
    Handler, Finalizer, Common-Cleaner) that keep the Python process
    alive after pytest's test session ends. On a local shell that's
    invisible (the next prompt kills the process); on GitHub Actions
    the pytest worker turns into a 16-minute orphan that ultimately
    hits the 20-minute step ceiling. Explicit shutdown is the
    documented fix.
    """
    try:
        import jpype
    except ImportError:
        return
    if jpype.isJVMStarted():
        jpype.shutdownJVM()


@pytest.fixture
def jaydebeapi_connect(jdbc_jar: Path) -> Callable[..., Any]:
    """Return a factory that opens a JayDeBeAPI connection to a Flight SQL URL."""
    import jaydebeapi

    _ensure_jvm_started_for_arrow(jdbc_jar)

    def _connect(url: str, driver_args: list[str] | None = None):
        return jaydebeapi.connect(
            JDBC_DRIVER_CLASS,
            url,
            driver_args if driver_args is not None else [],
            str(jdbc_jar),
        )

    return _connect


def _start_flight_demo_server(*, token: Optional[str]):
    """Boot a Flight SQL server backed by the bundled Jaffle Shop demo.

    Returns ``(server, host, port)``. The caller is responsible for
    ``server.shutdown()`` + ``.wait()``.
    """
    from slayer.cli import _prepare_demo, _resolve_storage
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.flight.handlers import FlightHandlers
    from slayer.flight.server import build_server

    args = argparse.Namespace(
        storage=tempfile.mkdtemp(prefix="slayer-flight-it-"),
        models_dir=None,
        datasource=None,
        force=False,
    )
    storage = _resolve_storage(args)
    _prepare_demo(args, storage)
    engine = SlayerQueryEngine(storage=storage)
    handlers = FlightHandlers(engine=engine, storage=storage)
    server = build_server(
        host="127.0.0.1", port=0, handlers=handlers, token=token,
    )
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    # Tiny grace period so the gRPC listener is ready before clients race in.
    time.sleep(0.3)
    return server, "127.0.0.1", server.port


@pytest.fixture(scope="module")
def flight_demo_server() -> Iterator[Tuple[str, int]]:
    """Yield ``(host, port)`` of a no-auth Flight SQL server backed by the Jaffle Shop demo."""
    server, host, port = _start_flight_demo_server(token=None)
    try:
        yield host, port
    finally:
        server.shutdown()
        server.wait()


@pytest.fixture(scope="module")
def flight_demo_server_with_token() -> Iterator[Tuple[str, int, str]]:
    """Same as ``flight_demo_server`` but with a bearer token enforced."""
    token = "s3cret"
    server, host, port = _start_flight_demo_server(token=token)
    try:
        yield host, port, token
    finally:
        server.shutdown()
        server.wait()
