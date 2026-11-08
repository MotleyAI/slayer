"""Exact fingerprints of a file tree: per-file content hashes, reused only while a file's stat proves it unchanged."""

import hashlib
import os
import time

from pydantic import BaseModel

#: A file changed this close to (or after) its hashing may hide a same-tick rewrite, so it is re-hashed (git's "racily clean").
RACY_NS = 2_000_000_000

StatKey = tuple[int, int, int, int]


class _Hashed(BaseModel):
    stat: StatKey
    hashed_at_ns: int
    digest: str


_HASHES: dict[str, dict[str, _Hashed]] = {}


def tree_fingerprint(base_dir: str, *, suffixes: tuple[str, ...]) -> str:
    """Digest of every ``suffixes`` file under ``base_dir``, by relative path and content; a racing delete raises ``OSError``."""
    root = os.path.abspath(base_dir)
    known = _HASHES.get(root, {})
    seen: dict[str, _Hashed] = {}
    for dirpath, _dirs, files in os.walk(root):
        for fname in files:
            if fname.endswith(suffixes):
                path = os.path.join(dirpath, fname)
                rel = os.path.relpath(path, root)
                seen[rel] = _current(path, cached=known.get(rel))
    _HASHES[root] = seen
    total = hashlib.sha256()
    for rel in sorted(seen):
        total.update(f"{rel}\0{seen[rel].digest}\n".encode())
    return total.hexdigest()


def stat_key(path: str) -> StatKey:
    """(inode, size, mtime, ctime): every content write bumps ctime, which no portable API can set back."""
    st = os.stat(path)  # NOSONAR(S6549) — callers pass sanitized store paths
    return st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


def file_digest(path: str) -> str:
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def _current(path: str, *, cached: _Hashed | None) -> _Hashed:
    stat = stat_key(path)
    if cached is not None and cached.stat == stat and stat[3] < cached.hashed_at_ns - RACY_NS:
        return cached
    hashed_at_ns = time.time_ns()
    return _Hashed(stat=stat, hashed_at_ns=hashed_at_ns, digest=file_digest(path))
