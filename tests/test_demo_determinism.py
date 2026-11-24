"""The Jaffle Shop demo is deterministic: same generator, same data (specs/demo/jaffle-shop)."""

from __future__ import annotations

import hashlib
import io
import re
import shlex
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from slayer.demo import jaffle_shop
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_GUIDE = REPO_ROOT / "docs" / "getting-started" / "cli.md"
COUNT_QUERY = '{"source_model": "orders", "measures": ["count(*)"]}'

_SEED_PROBE = """
import random, uuid
import numpy as np
from slayer.demo.jaffle_shop import seed_jafgen_generators as seed
seed(); plain = [random.random() for _ in range(5)]
seed(); mixed = []
for _ in range(5):
    uuid.uuid4()
    mixed.append(random.random())
seed(); first = [uuid.uuid4() for _ in range(3)]
seed(); again = [uuid.uuid4() for _ in range(3)]
seed(); np_first = np.random.rand(3).tolist()
seed(); np_again = np.random.rand(3).tolist()
print(plain == mixed, first == again, all(u.version == 4 for u in first), np_first == np_again)
"""


def _digests(data_dir: str) -> dict[str, str]:
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(data_dir).glob("*.csv"))}


def _rows(db: Path) -> dict[str, list[tuple]]:
    with duckdb.connect(str(db), read_only=True) as conn:
        return {t: conn.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall() for t in jaffle_shop.TABLE_NAMES}


class TestSeededGeneration:
    def test_runs_are_byte_identical_across_directories_and_output_branches(self, tmp_path: Path, jafgen_dir: str) -> None:
        # jafgen_dir ran elsewhere through the pumped (StringIO) branch; this run inherits a real fd.
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        with (tmp_path / "jafgen.log").open("w") as stream:
            data_dir = jaffle_shop.generate_data(output_dir=str(run_dir), years=1, stream=stream)
        digests = _digests(data_dir)
        assert digests
        assert digests == _digests(jafgen_dir)

    def test_two_builds_hold_the_same_rows(self, tmp_path: Path, jafgen_dir: str) -> None:
        built = tmp_path / "built.duckdb"
        jaffle_shop.build_jaffle_shop(db_path=str(built), years=1, stream=io.StringIO())
        loaded = tmp_path / "loaded.duckdb"
        with duckdb.connect(str(loaded)) as conn:
            jaffle_shop.create_schema(conn)
            jaffle_shop.load_data(conn=conn, data_dir=jafgen_dir)
        assert _rows(built) == _rows(loaded)

    def test_uuid_draws_use_their_own_rng_and_every_generator_is_seeded(self) -> None:
        result = subprocess.run([sys.executable, "-c", _SEED_PROBE], capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, result.stderr
        assert result.stdout.split() == ["True", "True", "True", "True"]


class TestUnseedableGenerator:
    def test_setup_fails_naming_jafgen_and_generates_nothing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(jaffle_shop, "find_spec", lambda name: None)
        db = tmp_path / "demo" / "jaffle_shop.duckdb"
        db.parent.mkdir()
        with pytest.raises(RuntimeError, match="jafgen"):
            jaffle_shop.build_jaffle_shop(db_path=str(db), stream=io.StringIO())
        assert not db.exists()
        assert list(tmp_path.rglob("*.csv")) == []


def _documented_count() -> tuple[str, str]:
    """The CLI guide's "Verify it works" query and its expected-output block."""
    section = CLI_GUIDE.read_text(encoding="utf-8").split("## Verify it works", 1)[1].split("\n## ", 1)[0]
    command = re.search(r"```bash\n(slayer query .+?)\n```", section)
    expected = re.search(r"Expected output:\s*\n```[^\n]*\n(.*?)```", section, re.S)
    assert command and expected, section
    return command.group(1), expected.group(1)


def test_cli_guide_shows_the_real_count(doc_demo_storage: YAMLStorage) -> None:
    command, expected = _documented_count()
    argv = shlex.split(command)
    assert argv[:2] == ["slayer", "query"]
    assert argv[2] == COUNT_QUERY
    result = run_cli_in_process([*argv[1:], "--storage", doc_demo_storage.base_dir])
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected.strip()
