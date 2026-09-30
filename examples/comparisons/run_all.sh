#!/usr/bin/env bash
# Run every engine's probe runner; extra arguments (--row Q4, --id X, --verbose) go to all of them.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$HERE/../.."
status=0

echo "=== SLayer ==="
(cd "$REPO" && poetry run python examples/comparisons/slayer/run_slayer.py "$@") || status=1

echo
echo "=== Malloy ==="
(cd "$HERE/malloy" && { [ -d node_modules ] || npm ci; } && node run_malloy.mjs "$@") || status=1

echo
echo "=== Cube ==="
(cd "$HERE/cube" && { [ -d node_modules ] || npm ci; } && cd "$REPO" &&
  poetry run python examples/comparisons/cube/run_cube.py "$@") || status=1

echo
echo "=== MetricFlow ==="
(cd "$HERE/metricflow" && {
  [ -x .venv/bin/python ] || python3 -m venv .venv 2>/dev/null || uv venv --clear --python 3.12 .venv
} && { .venv/bin/python -m pip install -q -r requirements.txt 2>/dev/null ||
  uv pip install -q --python .venv/bin/python -r requirements.txt; } &&
  .venv/bin/python run_metricflow.py "$@") || status=1

exit $status
