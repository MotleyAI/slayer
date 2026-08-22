"""Render the RESULTS.md charts from the audit's out/ artifacts.

Reads out/correctness.json, out/correctness-external.json and out/timings-*.json
(the current run's files, newest after the main correctness pass), computes the
branch-vs-pinned-PyPI stats, and writes self-contained SVG charts into
report_assets/. Dependency-free (stdlib only) so it runs in the plain venv.

    poetry run python tests/perf/compare/render_report.py
"""

import json
import math
import os
import statistics
from pathlib import Path

_DIR = Path(__file__).resolve().parent
OUT = _DIR / "out"
ASSETS = _DIR / "report_assets"
BACKENDS = ["sqlite", "duckdb", "postgres"]
SCALES = ["10k", "40k", "100k", "1m", "10m"]

# dataviz reference palette (light surface) — backends use categorical slots 1-3
SURFACE, INK, SEC, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#898781"
GRID, AXIS = "#e1e0d9", "#c3c2b7"
SERIES = {"sqlite": "#2a78d6", "duckdb": "#eb6834", "postgres": "#1baf7a"}
FONT = 'font-family="system-ui,-apple-system,Segoe UI,sans-serif"'

def _cutoff() -> float:
    """Boundary that keeps this audit's timing files and drops stale prior-run ones.
    Prefer correctness.json (the sqlite/duckdb pass, written first, so its mtime also
    predates the later postgres timing files); fall back to correctness-external.json
    for an external-only run. 0.0 (keep everything) if neither exists."""
    for name in ("correctness.json", "correctness-external.json"):
        p = OUT / name
        if p.exists():
            return os.path.getmtime(p) - 60
    return 0.0


_CUTOFF = _cutoff()


def _fresh(p: Path) -> bool:
    return p.exists() and os.path.getmtime(p) >= _CUTOFF


def _timings(side: str, backend: str, scale: str) -> dict:
    out = {}
    for n in (1, 2):
        p = OUT / f"timings-{side}{n}-{backend}-{scale}.json"
        if not _fresh(p):
            continue
        for eid, metrics in json.loads(p.read_text()).get("timings", {}).items():
            slot = out.setdefault(eid, {})
            for k, ts in metrics.items():
                if k in ("exec", "gen"):
                    slot.setdefault(k, []).extend(ts)
    return out


def _pairs(backend: str, scale: str):
    """(entry, pypi_median_s, branch_median_s) for entries timed on both sides."""
    pp, pb = _timings("pypi", backend, scale), _timings("branch", backend, scale)
    rows = []
    for e in sorted(set(pp) & set(pb)):  # sorted → deterministic SVG output across runs
        if "exec" in pp[e] and "exec" in pb[e]:
            rows.append((e, statistics.median(pp[e]["exec"]), statistics.median(pb[e]["exec"])))
    return rows


# ---------------------------------------------------------------------------
# SVG helpers
# ---------------------------------------------------------------------------

def _svg(w, h, body):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
            f'viewBox="0 0 {w} {h}" {FONT}>'
            f'<rect width="{w}" height="{h}" rx="10" fill="{SURFACE}"/>{body}</svg>')


def _text(x, y, s, size: float = 13, fill=INK, anchor="start", weight="400", extra=""):
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{fill}" '
            f'text-anchor="{anchor}" font-weight="{weight}" {extra}>{s}</text>')


def _legend(x, y, items):
    out = []
    for label, color in items:
        out.append(f'<rect x="{x}" y="{y-9}" width="11" height="11" rx="2.5" fill="{color}"/>')
        out.append(_text(x + 16, y, label, size=12.5, fill=SEC))
        x += 20 + 8.5 * len(label) + 14
    return "".join(out)


# ---------------------------------------------------------------------------
# Chart A — median branch/pypi exec ratio vs scale, per backend (line)
# ---------------------------------------------------------------------------

def chart_ratio_by_scale(ratios):
    W, H = 720, 380
    L, R, T, B = 58, 96, 58, 46
    pw, ph = W - L - R, H - T - B
    ymin, ymax = 0.8, 1.6
    xs = [L + pw * i / (len(SCALES) - 1) for i in range(len(SCALES))]

    def yp(v):
        return T + ph * (ymax - v) / (ymax - ymin)

    b = [_text(L, 30, "Median execution time: branch ÷ pinned 0.9.12", size=17, weight="600"),
         _text(L, 48, "Lower is better · 1.00× = parity · by dataset size, per backend",
               size=12.5, fill=MUTED)]
    for gv in (0.8, 1.0, 1.2, 1.4, 1.6):
        col, dash = (AXIS, ' stroke-dasharray="5 4"') if gv == 1.0 else (GRID, "")
        b.append(f'<line x1="{L}" y1="{yp(gv):.1f}" x2="{L+pw}" y2="{yp(gv):.1f}" stroke="{col}"{dash}/>')
        b.append(_text(L - 10, yp(gv) + 4, f"{gv:.1f}×", size=11.5, fill=MUTED, anchor="end",
                       extra='font-variant-numeric="tabular-nums"'))
    b.append(_text(L + pw + 8, yp(1.0) + 4, "parity", size=11, fill=MUTED))
    for i, sc in enumerate(SCALES):
        b.append(_text(xs[i], T + ph + 22, sc, size=12, fill=SEC, anchor="middle",
                       extra='font-variant-numeric="tabular-nums"'))
    for backend in BACKENDS:
        col = SERIES[backend]
        pts = [(xs[i], yp(ratios[backend][sc])) for i, sc in enumerate(SCALES)
               if math.isfinite(ratios[backend].get(sc, float("nan")))]
        if not pts:
            continue  # backend absent from this run's artifacts
        b.append(f'<polyline fill="none" stroke="{col}" stroke-width="2.5" '
                 f'points="{" ".join(f"{x:.1f},{y:.1f}" for x, y in pts)}"/>')
        for x, y in pts:
            b.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.2" fill="{col}" stroke="{SURFACE}" stroke-width="1.5"/>')
    b.append(_legend(L, T + ph + 40, [(k, SERIES[k]) for k in BACKENDS]))
    return _svg(W, H, "".join(b))


# ---------------------------------------------------------------------------
# Chart B — per-query pypi vs branch exec at 1m (scatter, log-log)
# ---------------------------------------------------------------------------

def chart_scatter_1m(points):
    W, H = 720, 470
    L, R, T, B = 62, 30, 64, 58
    pw, ph = W - L - R, H - T - B
    lo, hi = 3.0, 1200.0  # ms

    def X(ms):
        return L + pw * (math.log10(max(ms, lo)) - math.log10(lo)) / (math.log10(hi) - math.log10(lo))

    def Y(ms):
        return T + ph * (1 - (math.log10(max(ms, lo)) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)))

    b = [_text(L, 30, "Per-query execution at 1M rows — branch vs 0.9.12", size=17, weight="600"),
         _text(L, 48, "each dot is one query · above the diagonal = branch slower", size=12.5, fill=MUTED)]
    for gv in (10, 100, 1000):
        b.append(f'<line x1="{X(gv):.1f}" y1="{T}" x2="{X(gv):.1f}" y2="{T+ph}" stroke="{GRID}"/>')
        b.append(f'<line x1="{L}" y1="{Y(gv):.1f}" x2="{L+pw}" y2="{Y(gv):.1f}" stroke="{GRID}"/>')
        b.append(_text(X(gv), T + ph + 20, f"{gv}", size=11, fill=MUTED, anchor="middle",
                       extra='font-variant-numeric="tabular-nums"'))
        b.append(_text(L - 8, Y(gv) + 4, f"{gv}", size=11, fill=MUTED, anchor="end",
                       extra='font-variant-numeric="tabular-nums"'))
    b.append(_text(L + pw / 2, H - 10, "0.9.12 exec (ms, log)", size=12, fill=SEC, anchor="middle"))
    b.append(f'<text x="16" y="{T+ph/2}" font-size="12" fill="{SEC}" text-anchor="middle" '
             f'transform="rotate(-90 16 {T+ph/2:.0f})" {FONT}>branch exec (ms, log)</text>')
    # y=x parity diagonal
    b.append(f'<line x1="{X(lo):.1f}" y1="{Y(lo):.1f}" x2="{X(hi):.1f}" y2="{Y(hi):.1f}" '
             f'stroke="{AXIS}" stroke-width="1.5" stroke-dasharray="5 4"/>')
    b.append(_text(X(hi) - 6, Y(hi) - 8, "parity", size=11, fill=MUTED, anchor="end"))
    for backend in BACKENDS:  # postgres/aqua drawn last would hide; order by density
        col = SERIES[backend]
        for _, pm, bm in points[backend]:
            b.append(f'<circle cx="{X(pm*1000):.1f}" cy="{Y(bm*1000):.1f}" r="3.2" fill="{col}" '
                     f'fill-opacity="0.72" stroke="{SURFACE}" stroke-width="0.6"/>')
    b.append(_legend(L, H - 34, [(k, SERIES[k]) for k in BACKENDS]))
    return _svg(W, H, "".join(b))


# ---------------------------------------------------------------------------
# Chart C — added latency of bench_time_shift_date_range on sqlite (bars)
# ---------------------------------------------------------------------------

def chart_timeshift_bars(deltas, ratios):
    scales = [s for s in SCALES if s in deltas]
    if not scales:
        return None  # no bench_time_shift_date_range sqlite data in this run
    W, H = 720, 380
    L, R, T, B = 68, 24, 62, 48
    pw, ph = W - L - R, H - T - B
    vmax = max(max(deltas[s] for s in scales) * 1.15, 1.0)  # positive floor: all-equal deltas → vmax>0
    bw = pw / len(scales) * 0.52
    col = SERIES["sqlite"]
    b = [_text(L, 30, "Cost of the time_shift boundary-fix at scale (SQLite)", size=17, weight="600"),
         _text(L, 48, "added latency of bench_time_shift_date_range: branch − 0.9.12", size=12.5, fill=MUTED)]
    for frac in (0, 0.25, 0.5, 0.75, 1.0):
        gy = T + ph * (1 - frac)
        b.append(f'<line x1="{L}" y1="{gy:.1f}" x2="{L+pw}" y2="{gy:.1f}" stroke="{GRID}"/>')
        b.append(_text(L - 8, gy + 4, f"{frac*vmax/1000:.1f}s", size=11, fill=MUTED, anchor="end",
                       extra='font-variant-numeric="tabular-nums"'))
    for i, sc in enumerate(scales):
        cx = L + pw * (i + 0.5) / len(scales)
        d = deltas[sc]
        bh = ph * d / vmax
        y = T + ph - bh
        b.append(f'<rect x="{cx-bw/2:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bh:.1f}" rx="4" fill="{col}"/>')
        lab = f"+{d:.0f}ms" if d < 1000 else f"+{d/1000:.1f}s"
        b.append(_text(cx, y - 16, lab, size=12, fill=INK, anchor="middle", weight="600",
                       extra='font-variant-numeric="tabular-nums"'))
        b.append(_text(cx, y - 3, f"{ratios[sc]:.2f}×", size=10.5, fill=MUTED, anchor="middle"))
        b.append(_text(cx, T + ph + 20, sc, size=12, fill=SEC, anchor="middle",
                       extra='font-variant-numeric="tabular-nums"'))
    b.append(_text(L + pw / 2, H - 8, "rows in fact table", size=11.5, fill=MUTED, anchor="middle"))
    return _svg(W, H, "".join(b))


# ---------------------------------------------------------------------------
def main():
    ASSETS.mkdir(exist_ok=True)
    ratios = {b: {} for b in BACKENDS}
    for backend in BACKENDS:
        for sc in SCALES:
            rs = [bm / pm for _, pm, bm in _pairs(backend, sc) if pm > 0]
            if rs:  # leave scale absent (not NaN) when this run has no data for it
                ratios[backend][sc] = statistics.median(rs)
    points = {b: _pairs(b, "1m") for b in BACKENDS}
    TS = "bench_time_shift_date_range"
    deltas, ts_ratio = {}, {}
    for sc in SCALES:
        row = next((r for r in _pairs("sqlite", sc) if r[0] == TS), None)
        if row:
            _, pm, bm = row
            deltas[sc] = (bm - pm) * 1000
            ts_ratio[sc] = bm / pm

    (ASSETS / "ratio_by_scale.svg").write_text(chart_ratio_by_scale(ratios))
    (ASSETS / "scatter_1m.svg").write_text(chart_scatter_1m(points))
    ts_svg = chart_timeshift_bars(deltas=deltas, ratios=ts_ratio)
    if ts_svg:
        (ASSETS / "timeshift_sqlite.svg").write_text(ts_svg)
    else:
        (ASSETS / "timeshift_sqlite.svg").unlink(missing_ok=True)  # drop stale prior-run asset
        print("skipped timeshift chart: no bench_time_shift_date_range sqlite data")

    print("charts written to", ASSETS)
    print("\nmedian ratios (branch/pypi exec):")
    for b in BACKENDS:
        print(" ", b, {s: round(ratios[b][s], 2) for s in SCALES if s in ratios[b]})
    print("\ntime_shift_date_range sqlite added latency:")
    for s in SCALES:
        if s in deltas:
            print(f"  {s}: +{deltas[s]:.0f}ms ({ts_ratio[s]:.2f}x)")


if __name__ == "__main__":
    main()
