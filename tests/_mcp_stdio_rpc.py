"""A raw newline-delimited JSON-RPC client for an MCP server subprocess over stdio."""

from __future__ import annotations

import itertools
import json
import os
import queue
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from mcp.types import LATEST_PROTOCOL_VERSION

REPO_ROOT = Path(__file__).resolve().parent.parent


def server_env() -> dict[str, str]:
    """The test env minus live-embedding credentials."""
    return {k: v for k, v in os.environ.items() if k not in ("OPENAI_API_KEY", "SLAYER_EMBEDDING_MODEL")}


class StdioRpc:
    """Drives one server process; every stdout line must be a JSON-RPC message."""

    def __init__(self, argv: list[str], *, env: dict[str, str] | None = None) -> None:
        self.started_at = time.monotonic()
        self.proc = subprocess.Popen(
            argv, cwd=REPO_ROOT, env=env if env is not None else server_env(),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", bufsize=1,
        )
        self._ids = itertools.count(1)
        self._inbox: queue.Queue[dict[str, Any]] = queue.Queue()
        self.messages: list[dict[str, Any]] = []
        self.non_protocol_stdout: list[str] = []
        self._stderr: list[str] = []
        self._lock = threading.Lock()
        threading.Thread(target=self._read_stdout, daemon=True).start()
        threading.Thread(target=self._read_stderr, daemon=True).start()

    def _read_stdout(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                message = None
            if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                with self._lock:
                    self.non_protocol_stdout.append(line)
                continue
            with self._lock:
                self.messages.append(message)
            self._inbox.put(message)

    def _read_stderr(self) -> None:
        assert self.proc.stderr is not None
        for line in self.proc.stderr:
            with self._lock:
                self._stderr.append(line)

    @property
    def stderr(self) -> str:
        with self._lock:
            return "".join(self._stderr)

    def _send(self, payload: dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()

    def request(self, method: str, params: dict[str, Any] | None = None) -> int:
        request_id = next(self._ids)
        self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}})
        return request_id

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params or {}})

    def call_tool(self, name: str, arguments: dict[str, Any], *, progress_token: str | None = None) -> int:
        params: dict[str, Any] = {"name": name, "arguments": arguments}
        if progress_token is not None:
            params["_meta"] = {"progressToken": progress_token}
        return self.request("tools/call", params)

    def cancel(self, request_id: int) -> None:
        self.notify("notifications/cancelled", {"requestId": request_id, "reason": "test"})

    def response(self, request_id: int, *, timeout: float) -> dict[str, Any]:
        """The response to ``request_id``; raises ``TimeoutError`` after ``timeout`` seconds."""
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                found = next((m for m in self.messages if m.get("id") == request_id and "method" not in m), None)
            if found is not None:
                return found
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"no response to request {request_id} within {timeout}s; stderr:\n{self.stderr}")
            try:
                self._inbox.get(timeout=min(remaining, 0.2))
            except queue.Empty:
                pass

    def has_response(self, request_id: int) -> bool:
        with self._lock:
            return any(m.get("id") == request_id and "method" not in m for m in self.messages)

    def progress(self, token: str | None = None) -> list[dict[str, Any]]:
        """Progress notifications received so far, for ``token`` or for any token."""
        with self._lock:
            found = [m for m in self.messages if m.get("method") == "notifications/progress"]
        return [m for m in found if token is None or m["params"].get("progressToken") == token]

    def wait_for_progress(self, token: str, *, timeout: float) -> list[dict[str, Any]]:
        deadline = time.monotonic() + timeout
        while not self.progress(token):
            if time.monotonic() > deadline:
                raise TimeoutError(f"no progress for {token!r} within {timeout}s; stderr:\n{self.stderr}")
            time.sleep(0.05)
        return self.progress(token)

    def wait_for_stderr(self, text: str, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while text not in self.stderr:
            if time.monotonic() > deadline:
                raise TimeoutError(f"{text!r} not on stderr within {timeout}s; stderr:\n{self.stderr}")
            time.sleep(0.05)

    def handshake(self, *, timeout: float) -> list[str]:
        """``initialize`` + ``initialized`` + ``tools/list``; returns the tool names."""
        init = self.request("initialize", {
            "protocolVersion": LATEST_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "slayer-tests", "version": "0"},
        })
        deadline = time.monotonic() + timeout
        reply = self.response(init, timeout=timeout)
        assert "result" in reply, reply
        self.notify("notifications/initialized")
        listed = self.response(self.request("tools/list"), timeout=max(0.1, deadline - time.monotonic()))
        return [t["name"] for t in listed["result"]["tools"]]

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=30)


def tool_text(reply: dict[str, Any]) -> tuple[bool, str]:
    """``(is_error, text)`` of a ``tools/call`` response."""
    result = reply["result"]
    texts = [c["text"] for c in result.get("content", []) if c.get("type") == "text"]
    return bool(result.get("isError")), "\n".join(texts)
