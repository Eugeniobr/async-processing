"""Deterministic HTTP provider, deliberately confined to test support."""

from __future__ import annotations

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


def run(delay: float, port: int) -> None:
    lock = threading.Lock()
    gate = threading.Event()
    gate.set()
    state = {"brl_available": False, "started": 0, "active": 0}

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status: int, payload: dict) -> None:
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self) -> None:
            if self.path != "/control":
                self.reply(404, {})
                return
            data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            with lock:
                if "brl_available" in data:
                    state["brl_available"] = bool(data["brl_available"])
                if "block" in data:
                    gate.clear() if data["block"] else gate.set()
            self.reply(200, {"updated": True})

        def do_GET(self) -> None:
            url = urlsplit(self.path)
            if url.path in {"/healthz", "/state"}:
                with lock:
                    snapshot = dict(state)
                self.reply(200, snapshot)
                return
            currency = url.path.split("/")[-2].upper()
            with lock:
                state["started"] += 1
                state["active"] += 1
                available = state["brl_available"]
            try:
                gate.wait(timeout=15)
                time.sleep(delay)  # Test-only controlled service latency.
                if currency == "GBP":
                    self.reply(422, {"error": "deterministic permanent failure"})
                elif currency == "BRL" and not available:
                    self.reply(503, {"error": "deterministic temporary outage"})
                else:
                    self.reply(
                        200,
                        {
                            "base": currency,
                            "quote": "USD",
                            "rate": "1.250000000000",
                            "date": parse_qs(url.query)["date"][0],
                        },
                    )
            finally:
                with lock:
                    state["active"] -= 1

        def log_message(self, *_args: object) -> None:
            pass

    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--port", type=int, default=8080)
    arguments = parser.parse_args()
    run(arguments.delay, arguments.port)
