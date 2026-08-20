"""Pure comparison logic for the engine A/B audit.

Slayer-free and pandas-free: imported by runner.py under BOTH interpreters
(pinned-PyPI venv and the branch poetry venv).
"""

import datetime as dt
import math
import statistics
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from pydantic import BaseModel, Field

REL_TOL = 1e-9
ABS_TOL = 1e-12
PERF_RATIO_THRESHOLD = 1.3
PERF_FLOOR_SECONDS = 0.020


# ---------------------------------------------------------------------------
# Typed-tagged cell encoding (lossless, strict-JSON-safe)
# ---------------------------------------------------------------------------

def encode_cell(value: Any) -> Any:
    if isinstance(value, bool) or value is None or isinstance(value, (int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return {"__f__": "nan"}
        if math.isinf(value):
            return {"__f__": "inf" if value > 0 else "-inf"}
        return value
    if isinstance(value, Decimal):
        return {"__dec__": str(value)}
    if isinstance(value, dt.datetime):
        return {"__dt__": value.isoformat()}
    if isinstance(value, dt.date):
        return {"__d__": value.isoformat()}
    return {"__repr__": repr(value)}


def decode_cell(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    try:
        if "__f__" in value:
            return float(value["__f__"])
        if "__dec__" in value:
            return Decimal(value["__dec__"])
        if "__dt__" in value:
            return dt.datetime.fromisoformat(value["__dt__"])
        if "__d__" in value:
            return dt.date.fromisoformat(value["__d__"])
        if "__repr__" in value:
            return value["__repr__"]
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"malformed tagged cell: {value!r}") from exc
    raise ValueError(f"unrecognized tagged cell: {value!r}")


# ---------------------------------------------------------------------------
# Cell equality with tolerance + cross-representation coercion
# ---------------------------------------------------------------------------

def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal))  # bool included: bool is int


def _as_float(value: Any) -> float:
    return float(value)


def _parse_temporal(s: str) -> "dt.date | dt.datetime | None":
    for parser in (dt.datetime.fromisoformat, dt.date.fromisoformat):
        try:
            parsed = parser(s)
        except (ValueError, TypeError):
            continue
        return parsed
    return None


def _numbers_equal(a: float, b: float, rel_tol: float, abs_tol: float) -> bool:
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)


def _to_datetime(value: "dt.date | dt.datetime") -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value
    return dt.datetime(value.year, value.month, value.day)


def cells_equal(
    a: Any, b: Any, rel_tol: float = REL_TOL, abs_tol: float = ABS_TOL
) -> tuple[bool, Optional[str]]:
    """Return (equal, drift): drift names the coercion that was needed, if any."""
    a, b = decode_cell(a), decode_cell(b)
    if a is None or b is None:
        return (a is None and b is None, None)

    a_num, b_num = _is_number(a), _is_number(b)
    if a_num and b_num:
        equal = _numbers_equal(_as_float(a), _as_float(b), rel_tol, abs_tol)
        drift = "bool_int" if equal and (isinstance(a, bool) != isinstance(b, bool)) else None
        return (equal, drift)

    a_temp = isinstance(a, (dt.date, dt.datetime))
    b_temp = isinstance(b, (dt.date, dt.datetime))
    if a_temp and b_temp:
        drift = "date_datetime" if type(a) is not type(b) else None
        return (_to_datetime(a) == _to_datetime(b), drift)

    if isinstance(a, str) and isinstance(b, str):
        return (a == b, None)

    # exactly one side is a string: try coercions, noting the drift
    if isinstance(a, str) != isinstance(b, str):
        s, other = (a, b) if isinstance(a, str) else (b, a)
        if _is_number(other):
            try:
                parsed = float(s)
            except ValueError:
                return (False, None)
            return (_numbers_equal(parsed, _as_float(other), rel_tol, abs_tol), "numeric_repr")
        if isinstance(other, (dt.date, dt.datetime)):
            parsed_temporal = _parse_temporal(s)
            if parsed_temporal is None:
                return (False, None)
            return (_to_datetime(parsed_temporal) == _to_datetime(other), "datetime_repr")
    return (False, None)


# ---------------------------------------------------------------------------
# Canonicalization / sorting
# ---------------------------------------------------------------------------

def _cell_sort_key(value: Any) -> tuple:
    value = decode_cell(value)
    if value is None:
        return (0, 0.0, "")
    if _is_number(value):
        f = _as_float(value)
        if math.isnan(f):
            return (1, math.inf, "nan")
        return (1, f, str(value))
    if isinstance(value, (dt.date, dt.datetime)):
        return (3, _to_datetime(value).isoformat(), "")
    if isinstance(value, str):
        try:
            return (1, float(value), value)
        except ValueError:
            pass
        parsed = _parse_temporal(value)
        if parsed is not None:
            return (3, _to_datetime(parsed).isoformat(), value)
        return (2, value, "")
    return (4, repr(value), "")


def row_sort_key(row: list) -> tuple:
    return tuple(_cell_sort_key(cell) for cell in row)


def canonical_rows(rows: list, ordered: bool) -> list:
    decoded = [[decode_cell(cell) for cell in row] for row in rows]
    if ordered:
        return decoded
    return sorted(decoded, key=row_sort_key)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

class Verdict(BaseModel):
    status: str
    detail: str = ""
    error_type_drift: bool = False
    type_drift: bool = False
    expected_error: bool = False
    error_match_failed: list[str] = Field(default_factory=list)


def _error_match_failures(entry: dict, pypi: dict, branch: dict) -> list[str]:
    needle = entry.get("expect_error_match")
    if not needle:
        return []
    failed = []
    for side_name, side in (("pypi", pypi), ("branch", branch)):
        if side["status"] != "error":
            continue
        haystack = f"{side.get('error_type') or ''} {side.get('error_msg') or ''}".lower()
        if needle.lower() not in haystack:
            failed.append(side_name)
    return failed


def _compare_rows(
    rows_a: list, rows_b: list, rel_tol: float, abs_tol: float
) -> tuple[bool, bool, str]:
    """Pairwise compare equal-length row lists: (equal, type_drift, first_diff)."""
    drift = False
    for i, (row_a, row_b) in enumerate(zip(rows_a, rows_b)):
        for j, (cell_a, cell_b) in enumerate(zip(row_a, row_b)):
            equal, cell_drift = cells_equal(cell_a, cell_b, rel_tol, abs_tol)
            if not equal:
                return (False, drift, f"row {i} col {j}: {cell_a!r} != {cell_b!r}")
            drift = drift or cell_drift is not None
    return (True, drift, "")


def classify_entry(
    entry: dict, pypi: dict, branch: dict,
    rel_tol: float = REL_TOL, abs_tol: float = ABS_TOL,
) -> Verdict:
    expect_error = bool(entry.get("expect_error"))
    p_err = pypi["status"] == "error"
    b_err = branch["status"] == "error"

    if p_err or b_err:
        match_failed = _error_match_failures(entry, pypi, branch)
        if p_err and b_err:
            return Verdict(
                status="BOTH_ERROR" if expect_error else "BOTH_ERROR_UNEXPECTED",
                detail=f"pypi: {pypi.get('error_type')}; branch: {branch.get('error_type')}",
                error_type_drift=pypi.get("error_type") != branch.get("error_type"),
                expected_error=expect_error,
                error_match_failed=match_failed,
            )
        side = "PYPI_ONLY_ERROR" if p_err else "BRANCH_ONLY_ERROR"
        erring = pypi if p_err else branch
        return Verdict(
            status=side,
            detail=f"{erring.get('error_type')}: {erring.get('error_msg')}",
            expected_error=expect_error,
            error_match_failed=match_failed,
        )

    cols_p, cols_b = pypi["columns"], branch["columns"]
    if len(cols_p) != len(cols_b):
        return Verdict(status="SHAPE_MISMATCH", detail=f"columns {cols_p} vs {cols_b}")

    ordered = bool(entry.get("ordered"))
    rows_p = canonical_rows(pypi["rows"], ordered=ordered)
    rows_b = canonical_rows(branch["rows"], ordered=ordered)
    if len(rows_p) != len(rows_b):
        return Verdict(status="VALUE_MISMATCH",
                       detail=f"row count {len(rows_p)} vs {len(rows_b)}")

    equal, drift, diff = _compare_rows(rows_p, rows_b, rel_tol, abs_tol)
    if not equal and ordered:
        eq_unordered, drift, _ = _compare_rows(
            canonical_rows(pypi["rows"], ordered=False),
            canonical_rows(branch["rows"], ordered=False),
            rel_tol, abs_tol,
        )
        if eq_unordered:
            return Verdict(status="ORDER_MISMATCH", detail=diff, type_drift=drift)
    if not equal:
        return Verdict(status="VALUE_MISMATCH", detail=diff, type_drift=drift)

    if cols_p != cols_b:
        renamed = [f"{p} -> {b}" for p, b in zip(cols_p, cols_b) if p != b]
        return Verdict(status="NAME_DRIFT", detail="; ".join(renamed), type_drift=drift)
    return Verdict(status="MATCH", type_drift=drift)


# ---------------------------------------------------------------------------
# Warnings (informational)
# ---------------------------------------------------------------------------

def warning_drift(pypi_warnings: list, branch_warnings: list) -> Optional[str]:
    import json

    def counter(warnings: list) -> dict:
        counts: dict = {}
        for w in warnings:
            key = json.dumps(w, sort_keys=True, default=str)
            counts[key] = counts.get(key, 0) + 1
        return counts

    if counter(pypi_warnings) == counter(branch_warnings):
        return None
    kinds_p = sorted(str(w.get("kind")) for w in pypi_warnings)
    kinds_b = sorted(str(w.get("kind")) for w in branch_warnings)
    if kinds_p != kinds_b:
        return f"warning kinds differ: pypi={kinds_p} branch={kinds_b}"
    return f"warning payloads differ for kinds {sorted(set(kinds_p))}"


# ---------------------------------------------------------------------------
# Performance flags
# ---------------------------------------------------------------------------

class PerfFlag(BaseModel):
    entry_id: str
    metric: str
    pypi_median: float
    branch_median: float
    ratio: float
    delta: float
    flagged: bool


def _validated_median(times: list, label: str) -> float:
    if not times:
        raise ValueError(f"{label}: empty timing sample")
    if any(not math.isfinite(t) for t in times):
        raise ValueError(f"{label}: non-finite timing value")
    return statistics.median(times)


def flag_perf(
    entry_id: str, metric: str, *, pypi_times: list, branch_times: list,
    ratio_threshold: float = PERF_RATIO_THRESHOLD,
    floor_seconds: float = PERF_FLOOR_SECONDS,
) -> PerfFlag:
    pypi_median = _validated_median(pypi_times, "pypi_times")
    branch_median = _validated_median(branch_times, "branch_times")
    delta = branch_median - pypi_median
    ratio = branch_median / pypi_median if pypi_median > 0 else math.inf
    eps = 1e-9  # strict > without float-dust false positives at the boundary
    flagged = delta > floor_seconds * (1 + eps) and (
        pypi_median == 0 or branch_median > ratio_threshold * pypi_median * (1 + eps)
    )
    return PerfFlag(
        entry_id=entry_id, metric=metric,
        pypi_median=pypi_median, branch_median=branch_median,
        ratio=ratio, delta=delta, flagged=flagged,
    )


def pool_abba(run1: dict, run2: dict) -> dict:
    pooled: dict = {}
    for run in (run1, run2):
        for query_id, metrics in run.items():
            slot = pooled.setdefault(query_id, {})
            for metric, times in metrics.items():
                slot.setdefault(metric, []).extend(times)
    return pooled
