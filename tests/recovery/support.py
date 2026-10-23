"""Isolated real-infrastructure fixtures for acknowledgement-boundary recovery tests."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from multiprocessing.connection import Connection, Listener
from pathlib import Path
from urllib.parse import quote, urlsplit

import aio_pika
import httpx
import pytest
from sqlalchemy import create_engine, select, text

from domain.transaction import TransactionEvent
from infrastructure.postgres_schema import transactions_table
from infrastructure.rabbitmq_publisher import TransactionPublisher

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://async_processing:async_processing@localhost:5433/async_processing",
)
MANAGEMENT_URL = os.getenv("RABBITMQ_MANAGEMENT_URL", "http://localhost:15672")
USERNAME = "async_processing"
PASSWORD = "async_processing"

pytestmark = pytest.mark.skipif(not RABBITMQ_URL, reason="RABBITMQ_URL is not configured")


def _migration_module():
    path = Path("migrations/versions/001_transactions.py")
    spec = importlib.util.spec_from_file_location("recovery_migration", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RecoveryEnvironment:
    """Own one vhost and the test-only database schema lifecycle."""

    def __init__(self, tmp_path: Path) -> None:
        self.vhost = f"t022-{uuid.uuid4().hex}"
        self._encoded_vhost = quote(self.vhost, safe="")
        self.rabbitmq_url = _vhost_url(self.vhost)
        self.database_url = DATABASE_URL
        self._client = httpx.Client()
        self._tmp_path = tmp_path
        self._engine = create_engine(self.database_url, pool_pre_ping=True)

    def start(self) -> None:
        response = self._client.put(
            f"{MANAGEMENT_URL}/api/vhosts/{self._encoded_vhost}", auth=(USERNAME, PASSWORD)
        )
        response.raise_for_status()
        response = self._client.put(
            f"{MANAGEMENT_URL}/api/permissions/{self._encoded_vhost}/{USERNAME}",
            auth=(USERNAME, PASSWORD),
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )
        response.raise_for_status()
        with self._engine.begin() as connection:
            connection.execute(text("DROP SCHEMA IF EXISTS transactions CASCADE"))
            _migration_module().upgrade(connection)
        asyncio.run(self._declare_topology())

    def close(self) -> None:
        try:
            self._client.delete(
                f"{MANAGEMENT_URL}/api/vhosts/{self._encoded_vhost}", auth=(USERNAME, PASSWORD)
            )
        finally:
            with self._engine.begin() as connection:
                connection.execute(text("DROP SCHEMA IF EXISTS transactions CASCADE"))
            self._engine.dispose()
            self._client.close()

    async def _declare_topology(self) -> None:
        connection = await aio_pika.connect(self.rabbitmq_url)
        try:
            channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
            work = await channel.declare_exchange(
                "transactions", aio_pika.ExchangeType.DIRECT, durable=True
            )
            retry = await channel.declare_exchange(
                "transactions.retry", aio_pika.ExchangeType.DIRECT, durable=True
            )
            queue_arguments = {"x-queue-type": "quorum", "x-overflow": "reject-publish"}
            work_queue = await channel.declare_queue(
                "transactions.work", durable=True, arguments=queue_arguments
            )
            failure_queue = await channel.declare_queue(
                "transactions.failure", durable=True, arguments=queue_arguments
            )
            await work_queue.bind(work, routing_key="work")
            await failure_queue.bind(work, routing_key="failure")
            for delay in (1, 2, 4, 8, 16, 32, 60):
                retry_queue = await channel.declare_queue(
                    f"transactions.retry.{delay}s",
                    durable=True,
                    arguments={
                        **queue_arguments,
                        "x-message-ttl": delay * 1000,
                        "x-dead-letter-exchange": "transactions",
                        "x-dead-letter-routing-key": "work",
                    },
                )
                await retry_queue.bind(retry, routing_key=f"retry.{delay}s")
        finally:
            await connection.close()

    def publish_event(self, transaction_id: str) -> None:
        asyncio.run(self._publish_event(transaction_id))

    async def _publish_event(self, transaction_id: str) -> None:
        connection = await aio_pika.connect(self.rabbitmq_url)
        try:
            channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
            publisher = TransactionPublisher(
                await channel.get_exchange("transactions"),
                await channel.get_exchange("transactions.retry"),
                confirm_timeout_seconds=5,
            )
            event = TransactionEvent.from_accepted_input(
                supported_currencies=frozenset({"EUR", "USD"}),
                currency_allowlist_version="currencies-v1",
                transaction_id=transaction_id,
                user_id="recovery-user",
                original_amount=Decimal("12.34567890"),
                currency="EUR",
                event_timestamp=datetime(2026, 10, 22, 10, 30, tzinfo=UTC),
            )
            await publisher.publish_work(event)
        finally:
            await connection.close()

    def row_count(self) -> int:
        with self._engine.connect() as connection:
            return len(connection.execute(select(transactions_table.c.transaction_id)).all())

    def queue_states(self) -> dict[str, dict[str, int]]:
        result: dict[str, dict[str, int]] = {}
        for name in (
            "transactions.work",
            "transactions.failure",
            *(f"transactions.retry.{delay}s" for delay in (1, 2, 4, 8, 16, 32, 60)),
        ):
            response = self._client.get(
                f"{MANAGEMENT_URL}/api/queues/{self._encoded_vhost}/{quote(name, safe='')}",
                auth=(USERNAME, PASSWORD),
            )
            response.raise_for_status()
            payload = response.json()
            result[name] = {
                "ready": payload.get("messages_ready", 0),
                "unacknowledged": payload.get("messages_unacknowledged", 0),
            }
        return result

    def wait_for(self, predicate: object, description: str, timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():  # type: ignore[operator]
                return
            time.sleep(0.05)
        raise AssertionError(
            f"timed out waiting for {description}; queues={json.dumps(self.queue_states())}"
        )

    def spawn(
        self, *, mode: str, checkpoint: str, max_messages: int = 1
    ) -> tuple[subprocess.Popen[str], Connection]:
        # Linux limits Unix-domain paths to roughly 108 bytes.  Pytest's nested
        # temporary directory can exceed that length, so IPC uses a short,
        # controller-owned path and never production configuration.
        socket_path = Path("/tmp") / f"t022-{uuid.uuid4().hex[:12]}.sock"
        listener = Listener(str(socket_path), family="AF_UNIX")
        command = [
            sys.executable,
            "tests/recovery/consumer_helper.py",
            "--rabbitmq-url",
            self.rabbitmq_url,
            "--database-url",
            self.database_url,
            "--ipc-socket",
            str(socket_path),
            "--mode",
            mode,
            "--checkpoint",
            checkpoint,
            "--max-messages",
            str(max_messages),
            "--deadline-seconds",
            "15",
        ]
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        listener._listener._socket.settimeout(15)  # type: ignore[attr-defined]
        try:
            ipc = listener.accept()
        except TimeoutError as error:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                f"consumer helper did not connect: stdout={stdout!r} stderr={stderr!r}"
            ) from error
        finally:
            listener.close()
            socket_path.unlink(missing_ok=True)
        return process, ipc


@pytest.fixture()
def recovery_environment(tmp_path: Path) -> RecoveryEnvironment:
    environment = RecoveryEnvironment(tmp_path)
    environment.start()
    try:
        yield environment
    finally:
        environment.close()


def checkpoint(ipc: Connection, expected: str) -> None:
    assert ipc.poll(15), f"timed out waiting for {expected} checkpoint"
    assert ipc.recv() == {"event": "checkpoint", "name": expected}


def completed(process: subprocess.Popen[str], ipc: Connection) -> dict[str, object]:
    assert ipc.poll(20), "consumer helper did not report completion"
    message = ipc.recv()
    stdout, stderr = process.communicate(timeout=5)
    assert process.returncode == 0, f"helper failed: stdout={stdout!r} stderr={stderr!r}"
    assert message["event"] == "completed", f"helper did not complete: {message!r}"
    return message


def kill_at_checkpoint(process: subprocess.Popen[str], ipc: Connection) -> None:
    process.kill()
    stdout, stderr = process.communicate(timeout=5)
    ipc.close()
    assert process.returncode == -9, f"expected SIGKILL: stdout={stdout!r} stderr={stderr!r}"


def assert_queues_empty(environment: RecoveryEnvironment) -> None:
    environment.wait_for(
        lambda: all(
            state["ready"] == 0 and state["unacknowledged"] == 0
            for state in environment.queue_states().values()
        ),
        "all recovery queues to resolve",
    )


def _vhost_url(vhost: str) -> str:
    parsed = urlsplit(RABBITMQ_URL)
    return f"{parsed.scheme}://{parsed.netloc}/{quote(vhost, safe='')}"
