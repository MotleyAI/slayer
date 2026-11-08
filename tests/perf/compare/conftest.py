"""Make sibling script-style modules (classify, oracle, corpus) importable in tests."""

import sys
from pathlib import Path

_DIR = str(Path(__file__).parent)
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)
