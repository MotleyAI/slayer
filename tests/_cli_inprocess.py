"""In-process CLI invocation for tests (DEV-1815).

Replaces ``subprocess.run([sys.executable, "-m", "slayer.cli", ...])`` for
argparse-plumbing tests: spawning a Python process just to parse args costs
~1s each (interpreter start + importing slayer). Calling ``slayer.cli.main()``
in-process keeps the same argparse → dispatch path at a fraction of the cost.

One end-to-end subprocess smoke (interactive + state-writing) is deliberately
retained elsewhere to cover the console-script/process boundary.
"""

from __future__ import annotations

import io
import os
import sys
from contextlib import redirect_stderr, redirect_stdout

from pydantic import BaseModel


class CliResult(BaseModel):
    returncode: int
    stdout: str
    stderr: str


def run_cli_in_process(
    args: list[str], *, stdin: str = "", env: dict[str, str] | None = None
) -> CliResult:
    """Invoke ``slayer.cli.main()`` in-process and capture its result.

    ``sys.argv`` becomes ``["slayer", *args]``; ``sys.stdin`` is fed ``stdin``.
    A normal return maps to exit code 0; ``SystemExit`` maps to its code
    (``None`` → 0, non-int → 1). argv/stdin/stdout/stderr/environ are restored.
    """
    from slayer.cli import main

    old_argv, old_stdin = sys.argv, sys.stdin
    old_environ = dict(os.environ)
    out, err = io.StringIO(), io.StringIO()
    sys.argv = ["slayer", *args]
    sys.stdin = io.StringIO(stdin)
    if env is not None:
        os.environ.clear()
        os.environ.update(env)
    code = 0
    try:
        with redirect_stdout(out), redirect_stderr(err):
            main()
    except SystemExit as exc:
        code = 0 if exc.code is None else exc.code if isinstance(exc.code, int) else 1
    finally:
        sys.argv, sys.stdin = old_argv, old_stdin
        os.environ.clear()
        os.environ.update(old_environ)
    return CliResult(returncode=code, stdout=out.getvalue(), stderr=err.getvalue())
