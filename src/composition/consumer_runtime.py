"""Small delivery lifecycle and instance HTTP status surface."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from infrastructure.metrics import MetricsRecorder
from infrastructure.rabbitmq_consumer import Delivery, TransactionConsumer


class DrainTimeout(RuntimeError):
    """The AMQP channel is closed but synchronous work may still be running."""


class ConsumerRuntime:
    """Track deliveries so intake can stop before active work is drained."""

    def __init__(
        self,
        consumer: TransactionConsumer,
        close_channel: Callable[[], Awaitable[object]],
        *,
        shutdown_seconds: float,
    ) -> None:
        self.consumer = consumer
        self.close_channel = close_channel
        self.shutdown_seconds = shutdown_seconds
        self.ready = True
        self.alive = True
        self.tasks: set[asyncio.Task] = set()

    async def handle(self, delivery: Delivery) -> None:
        if not self.ready:
            return  # Never ACK a late delivery; channel close recovers it.
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        try:
            await self.consumer.handle(delivery)
        finally:
            self.tasks.discard(task)

    async def drain(self) -> None:
        self.ready = False
        if not self.tasks:
            return
        _, pending = await asyncio.wait(self.tasks, timeout=self.shutdown_seconds)
        if pending:
            await self.close_channel()
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise DrainTimeout(
                "consumer drain deadline expired; unfinished work remains recoverable"
            )


class ConsumerStatusServer:
    """Serve this process's metrics and lifecycle; never probe the rate provider."""

    def __init__(self, metrics: MetricsRecorder, port: int) -> None:
        self.runtime: ConsumerRuntime | None = None
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/metrics":
                    status, payload, content_type = (
                        200,
                        metrics.render(),
                        "text/plain; version=0.0.4; charset=utf-8",
                    )
                elif self.path in {"/healthz", "/readyz"}:
                    runtime = owner.runtime
                    healthy = runtime is not None and runtime.alive
                    if self.path == "/readyz":
                        healthy = healthy and runtime.ready
                    status, payload, content_type = (
                        200 if healthy else 503,
                        b"ok\n" if healthy else b"not ready\n",
                        "text/plain",
                    )
                else:
                    status, payload, content_type = 404, b"not found\n", "text/plain"
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)
