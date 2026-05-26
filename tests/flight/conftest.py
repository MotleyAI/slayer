"""Pytest fixtures for the Flight SQL facade test suite (DEV-1390).

Provides:

* ``jdbc_jar`` — session-scoped fixture that downloads (and caches) the
  upstream Apache ``flight-sql-jdbc-driver`` JAR into ``tests/.cache/``
  on first run. Skips the calling test if Java is not on PATH.
* ``flight_jdbc_url`` — helper that formats a JDBC URL given a Flight
  endpoint location and optional auth/encryption flags.
* ``jaydebeapi_connect`` — factory that returns a JayDeBeAPI connection
  to a given Flight SQL endpoint URL.
* ``capture_stub`` — spins up a ``CaptureFlightServer`` on an ephemeral
  port in a background thread and yields ``(grpc_location, log_path)``.
"""

import shutil
import threading
import time
import urllib.request
from pathlib import Path
from typing import Callable, Iterator, Tuple

import pytest

from slayer.flight._capture_stub import CaptureFlightServer

JDBC_DRIVER_VERSION = "18.3.0"
JDBC_DRIVER_URL = (
    "https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/"
    f"{JDBC_DRIVER_VERSION}/flight-sql-jdbc-driver-{JDBC_DRIVER_VERSION}.jar"
)
JDBC_DRIVER_CLASS = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache"


def _java_on_path() -> bool:
    return shutil.which("java") is not None


@pytest.fixture(scope="session")
def jdbc_jar() -> Path:
    """Download (once) and return the path to the Apache flight-sql-jdbc-driver JAR."""
    if not _java_on_path():
        pytest.skip("Java >= 11 required on PATH for Flight SQL JDBC tests")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    jar_path = CACHE_DIR / f"flight-sql-jdbc-driver-{JDBC_DRIVER_VERSION}.jar"

    if not jar_path.exists():
        try:
            urllib.request.urlretrieve(JDBC_DRIVER_URL, jar_path)
        except Exception as exc:
            pytest.skip(f"Could not download flight-sql-jdbc-driver: {exc}")

    if jar_path.stat().st_size < 1_000_000:
        # Partial download — drop the stub so the next run re-fetches.
        jar_path.unlink(missing_ok=True)
        pytest.skip("Cached flight-sql-jdbc-driver JAR looks corrupted")

    return jar_path


def _format_flight_jdbc_url(
    *,
    host: str,
    port: int,
    use_encryption: bool = False,
    token: str | None = None,
    environment_id: str | None = None,
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
)


def _ensure_jvm_started_for_arrow(jar_path: Path) -> None:
    """Pre-start JPype's JVM with the ``--add-opens`` flags Arrow needs on Java 17+."""
    import jpype

    if jpype.isJVMStarted():
        return
    jpype.startJVM(
        jpype.getDefaultJVMPath(),
        *_ARROW_JVM_OPENS,
        classpath=[str(jar_path)],
        convertStrings=True,
    )


@pytest.fixture
def jaydebeapi_connect(jdbc_jar: Path) -> Callable[..., object]:
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


@pytest.fixture
def capture_stub(tmp_path: Path) -> Iterator[Tuple[str, Path]]:
    """Spin up a CaptureFlightServer on an ephemeral port.

    Yields ``(grpc_location, log_path)`` where ``log_path`` accumulates one
    JSON record per RPC. Cleans up on teardown.
    """
    log_path = tmp_path / "capture.jsonl"
    server = CaptureFlightServer("grpc://127.0.0.1:0", log_path)
    actual_location = f"grpc://127.0.0.1:{server.port}"

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    # Tiny grace period so the server is ready to accept before tests race in.
    time.sleep(0.1)

    try:
        yield actual_location, log_path
    finally:
        server.shutdown()
        server.wait()
        thread.join(timeout=2)
