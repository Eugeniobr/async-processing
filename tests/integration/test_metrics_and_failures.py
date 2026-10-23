"""Compose-backed proof of failure forwarding, multiprocess metrics, and retention."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

import aio_pika
import pytest

from domain.transaction import TransactionEvent
from infrastructure.rabbitmq_publisher import TransactionPublisher
from tests.recovery.support import RecoveryEnvironment

pytestmark = pytest.mark.skipif(
    not os.getenv("RABBITMQ_URL"), reason="RABBITMQ_URL is not configured"
)


def _event(transaction_id: str, currency: str) -> TransactionEvent:
    return TransactionEvent.from_accepted_input(
        supported_currencies=frozenset({"EUR", "GBP", "USD"}),
        currency_allowlist_version="currencies-v1",
        transaction_id=transaction_id,
        user_id="metrics-user",
        original_amount=Decimal("12.34567890"),
        currency=currency,
        event_timestamp=datetime(2026, 10, 22, 10, 30, tzinfo=UTC),
    )


async def _publish(environment: RecoveryEnvironment, *events: TransactionEvent) -> None:
    connection = await aio_pika.connect(environment.rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        publisher = TransactionPublisher(
            await channel.get_exchange("transactions"),
            await channel.get_exchange("transactions.retry"),
            confirm_timeout_seconds=5,
        )
        for event in events:
            await publisher.publish_work(event)
    finally:
        await connection.close()


def _environment(environment: RecoveryEnvironment, metric_directory: Path) -> dict[str, str]:
    return {
        **os.environ,
        "RABBITMQ_URL": environment.rabbitmq_url,
        "DATABASE_URL": environment.database_url,
        "CURRENCY_ALLOWLIST_PATH": "config/currencies-v1.json",
        "PROMETHEUS_MULTIPROC_DIR": str(metric_directory),
    }


def _run(code: str, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path.cwd(),
        env=environment,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )


_FAILURE_WORKER = """
import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
import aio_pika
from sqlalchemy import create_engine
from application.money_converter import MoneyConverter
from application.ports import PermanentRateError, RetryableRateError
from application.transaction_processor import TransactionProcessor
from domain.rates import RateQuote
from infrastructure.metrics import MetricsRecorder
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.rabbitmq_consumer import TransactionConsumer
from infrastructure.rabbitmq_publisher import TransactionPublisher

class Provider:
    def get_usd_rate(self, currency, requested_date):
        if currency == 'EUR':
            raise RetryableRateError('provider unavailable')
        raise PermanentRateError('no eligible rate')

async def main():
    engine = create_engine(__import__('os').environ['DATABASE_URL'])
    connection = await aio_pika.connect(__import__('os').environ['RABBITMQ_URL'])
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        publisher = TransactionPublisher(await channel.get_exchange('transactions'),
            await channel.get_exchange('transactions.retry'), confirm_timeout_seconds=5)
        processor = TransactionProcessor(Provider(), MoneyConverter(),
            PostgresTransactionRepository(engine))
        consumer = TransactionConsumer(processor, publisher, metrics=MetricsRecorder())
        queue = await channel.get_queue('transactions.work')
        for _ in range(2):
            await consumer.handle(await queue.get(fail=True, no_ack=False))
        print('forwarded=2')
    finally:
        await connection.close()
        engine.dispose()

asyncio.run(main())
"""


_SCRAPE_WORKER = """
from fastapi.testclient import TestClient
from api.app import create_app
from composition.container import build_api_container
from composition.settings import Settings

container = build_api_container(Settings())
try:
    with TestClient(create_app(container)) as client:
        response = client.get('/metrics')
        print(response.status_code)
        print(response.text)
finally:
    container.close()
"""


_RESTART_WORKER = """
import asyncio
import os
import time
import httpx
from composition.consumer_main import run_with_processor
from composition.settings import Settings
from domain.outcomes import RetryableFailure
from infrastructure.metrics import MetricsRecorder

class IdleProcessor:
    def process(self, event):
        return RetryableFailure('test restart must not acknowledge unknown work')

async def main():
    task = asyncio.create_task(
        run_with_processor(Settings(), IdleProcessor(), metrics=MetricsRecorder())
    )
    deadline = time.monotonic() + 10
    try:
        async with httpx.AsyncClient() as client:
            while time.monotonic() < deadline:
                response = await client.get(
                    os.environ.get('RABBITMQ_MANAGEMENT_URL', 'http://localhost:15672')
                    + '/api/queues/' + os.environ['RABBITMQ_VHOST_ENCODED']
                    + '/transactions.work',
                    auth=('async_processing', 'async_processing'),
                )
                if response.status_code == 200 and response.json().get('consumers', 0) >= 1:
                    print('consumer_restarted')
                    return
                await asyncio.sleep(0.02)
        raise RuntimeError('restarted consumer did not register before deadline')
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

asyncio.run(main())
"""


def test_compose_metrics_include_confirmed_failure_kinds_and_retain_permanent_failure(
    tmp_path: Path,
) -> None:
    """Scrape real consumer shards and prove a durable failure survives a consumer restart."""

    environment = RecoveryEnvironment(tmp_path)
    environment.start()
    try:
        metrics_directory = tmp_path / "prometheus"
        metrics_directory.mkdir()
        child_environment = _environment(environment, metrics_directory)
        child_environment["RABBITMQ_VHOST_ENCODED"] = quote(environment.vhost, safe="")

        retry_event = _event("metrics-retry", "EUR")
        permanent_event = _event("metrics-permanent", "GBP")
        asyncio.run(_publish(environment, retry_event, permanent_event))

        worker = _run(_FAILURE_WORKER, child_environment)
        assert worker.returncode == 0, worker.stderr
        assert worker.stdout == "forwarded=2\n"

        scrape = _run(_SCRAPE_WORKER, child_environment)
        assert scrape.returncode == 0, scrape.stderr
        assert "200" in scrape.stdout
        assert 'transaction_processing_failures_total{kind="retryable"} 1.0' in scrape.stdout
        assert 'transaction_processing_failures_total{kind="permanent"} 1.0' in scrape.stdout

        environment.wait_for(
            lambda: environment.queue_states()["transactions.failure"]["ready"] >= 1,
            "the confirmed permanent forwarding",
        )
        restart = _run(_RESTART_WORKER, child_environment)
        assert restart.returncode == 0, restart.stderr
        assert restart.stdout == "consumer_restarted\n"

        # A restarted work consumer neither drains nor deletes the operator queue.
        assert environment.queue_states()["transactions.failure"]["ready"] >= 1
        assert _failure_transaction_id(environment) == permanent_event.transaction_id
    finally:
        environment.close()


def _failure_transaction_id(environment: RecoveryEnvironment) -> str:
    async def read() -> str:
        connection = await aio_pika.connect(environment.rabbitmq_url)
        try:
            channel = await connection.channel()
            message = await (await channel.get_queue("transactions.failure")).get(
                fail=True, no_ack=True
            )
            return str(json.loads(message.body)["event"]["transaction_id"])
        finally:
            await connection.close()

    return asyncio.run(read())
