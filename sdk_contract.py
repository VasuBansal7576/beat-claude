#!/usr/bin/env python3
"""
Real SDK retry contract gate.

This harness does not contain or invent an SDK. It starts a raw TCP HTTP server
and runs the SDK command provided by the caller. The command must send one
logical event to SDK_ENDPOINT and let the SDK handle retry behavior.

Usage:
  SDK_COMMAND='node path/to/real-sdk-smoke-test.js' python sdk_contract.py

The SDK smoke command receives:
  SDK_ENDPOINT=http://127.0.0.1:<port>/collect
  SDK_EVENT_NAME=<503_probe|lost_ack_probe>
  SDK_CUSTOMER_ID=contract_customer

Pass criteria:
  1. the raw server receives exactly two HTTP requests for each probe
  2. both requests contain an event_id
  3. the retry request reuses the first event_id

If SDK_COMMAND is missing, this exits 2. That is not a pass and not a fail of a
real SDK; it means the production SDK was not supplied, so Path A remains
unverified.
"""

from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import threading
from dataclasses import dataclass, field


@dataclass
class RawRetryServer:
    mode: str
    event_ids: list[str] = field(default_factory=list)
    request_bodies: list[dict] = field(default_factory=list)
    _requests_seen: int = 0

    def __post_init__(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    def _serve(self) -> None:
        while self._requests_seen < 2:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            with conn:
                body = self._read_http_body(conn)
                event = json.loads(body.decode("utf-8"))
                self.request_bodies.append(event)
                self.event_ids.append(str(event.get("event_id", "")))
                self._requests_seen += 1

                if self.mode == "503" and self._requests_seen == 1:
                    conn.sendall(
                        b"HTTP/1.1 503 Service Unavailable\r\n"
                        b"Content-Length: 0\r\n"
                        b"Connection: close\r\n\r\n"
                    )
                    continue

                if self.mode == "lost_ack" and self._requests_seen == 1:
                    continue

                conn.sendall(
                    b"HTTP/1.1 202 Accepted\r\n"
                    b"Content-Length: 2\r\n"
                    b"Connection: close\r\n\r\nok"
                )

    @staticmethod
    def _read_http_body(conn: socket.socket) -> bytes:
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk

        headers, _, remainder = data.partition(b"\r\n\r\n")
        content_length = 0
        for line in headers.split(b"\r\n"):
            if line.lower().startswith(b"content-length:"):
                content_length = int(line.split(b":", 1)[1].strip())
                break

        body = remainder
        while len(body) < content_length:
            chunk = conn.recv(content_length - len(body))
            if not chunk:
                break
            body += chunk
        return body


def run_sdk_command(command: str, server: RawRetryServer) -> None:
    env = os.environ.copy()
    env.update(
        {
            "SDK_ENDPOINT": f"http://127.0.0.1:{server.port}/collect",
            "SDK_EVENT_NAME": f"{server.mode}_probe",
            "SDK_CUSTOMER_ID": "contract_customer",
        }
    )
    completed = subprocess.run(
        shlex.split(command),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=15,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"SDK command failed for {server.mode}:\n{completed.stdout}")


def assert_retry_reuses_event_id(command: str, mode: str) -> None:
    server = RawRetryServer(mode)
    try:
        run_sdk_command(command, server)
        if len(server.event_ids) != 2:
            raise AssertionError(f"{mode}: expected 2 SDK sends, observed {len(server.event_ids)}")
        if not server.event_ids[0] or not server.event_ids[1]:
            raise AssertionError(f"{mode}: SDK payload missing event_id; observed={server.request_bodies}")
        if server.event_ids[0] != server.event_ids[1]:
            raise AssertionError(f"{mode}: retry changed event_id; observed={server.event_ids}")
        print(f"[PASS] {mode}: real SDK reused event_id across retry: {server.event_ids[0]}")
    finally:
        server.close()


def main() -> int:
    command = os.environ.get("SDK_COMMAND", "").strip()
    if not command:
        print("[UNVERIFIED] SDK_COMMAND is not set; real production SDK was not supplied.")
        print("[UNVERIFIED] Path A remains blocked until this harness passes against the real SDK.")
        return 2

    assert_retry_reuses_event_id(command, "503")
    assert_retry_reuses_event_id(command, "lost_ack")
    print("[PASS] Real SDK contract verified: query-time count(DISTINCT event_id) can collapse SDK retries.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
