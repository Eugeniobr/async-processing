from __future__ import annotations

import asyncio
import importlib.util
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

import aio_pika
import httpx
from sqlalchemy import create_engine, select, text

from application.money_converter import MoneyConverter
from application.transaction_processor import TransactionProcessor
from domain.rates import RateQuote
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.postgres_schema import transactions_table
from infrastructure.rabbitmq_consumer import TransactionConsumer

DATABASE_URL = (
    "postgresql+psycopg://async_processing:async_processing@localhost:5433/async_processing"
)
AMQP = "amqp://async_processing:async_processing@localhost:5672/"


def _migration() -> object:
    spec = importlib.util.spec_from_file_location(
        "migration_001", Path("migrations/versions/001_transactions.py")
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _body(amount: str = "10.00000000") -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event": {
                "transaction_id": "concurrent-id",
                "user_id": "first-user",
                "original_amount": amount,
                "currency": "EUR",
                "event_timestamp": "2026-10-20T10:00:00+00:00",
                "currency_allowlist_version": "currencies-v1",
            },
            "attempt": 0,
            "accepted_at": "2026-10-20T10:01:00+00:00",
            "currency_allowlist_version": "currencies-v1",
            "last_failure_kind": None,
        }
    ).encode()


class BarrierProvider:
    def __init__(self, barrier: threading.Barrier) -> None:
        self.barrier = barrier

    def get_usd_rate(self, currency: str, requested_date: date) -> RateQuote:
        self.barrier.wait(timeout=10)
        return RateQuote(
            currency,
            "USD",
            Decimal("1.250000000000"),
            requested_date,
            requested_date,
            "test",
            datetime.now(UTC),
        )


class NoForward:
    async def publish_retry(self, *_: object) -> None:
        raise AssertionError("unexpected retry")

    async def publish_failure(self, *_: object) -> None:
        raise AssertionError("unexpected failure")


async def _publish(vhost_url: str) -> None:
    c = await aio_pika.connect(vhost_url)
    try:
        ch = await c.channel()
        ex = await ch.declare_exchange("work", auto_delete=True)
        q = await ch.declare_queue("work", durable=True, arguments={"x-queue-type": "quorum"})
        await q.bind(ex, "work")
        for _ in range(4):
            await ex.publish(aio_pika.Message(_body()), "work")
    finally:
        await c.close()


async def _consume(vhost_url: str, barrier: threading.Barrier) -> None:
    c = await aio_pika.connect(vhost_url)
    try:
        ch = await c.channel()
        q = await ch.get_queue("work")
        delivery = await q.get(fail=True, no_ack=False)
        engine = create_engine(DATABASE_URL)
        try:
            processor = TransactionProcessor(
                BarrierProvider(barrier), MoneyConverter(), PostgresTransactionRepository(engine)
            )
            await TransactionConsumer(processor, NoForward()).handle(delivery)
        finally:
            engine.dispose()
    finally:
        await c.close()


async def _publish_conflict(vhost_url: str) -> None:
    connection = await aio_pika.connect(vhost_url)
    try:
        channel = await connection.channel()
        await (await channel.get_exchange("work")).publish(
            aio_pika.Message(_body("99.00000000")), "work"
        )
    finally:
        await connection.close()


def test_concurrent_rabbitmq_deliveries_persist_one_first_record() -> None:
    engine = create_engine(DATABASE_URL)
    client = httpx.Client()
    vhost = f"t020-{uuid.uuid4().hex}"
    encoded = quote(vhost, safe="")
    auth = ("async_processing", "async_processing")
    try:
        with engine.begin() as c:
            c.execute(text("DROP SCHEMA IF EXISTS transactions CASCADE"))
            _migration().upgrade(c)
        client.put(f"http://localhost:15672/api/vhosts/{encoded}", auth=auth).raise_for_status()
        client.put(
            f"http://localhost:15672/api/permissions/{encoded}/async_processing",
            auth=auth,
            json={"configure": ".*", "write": ".*", "read": ".*"},
        ).raise_for_status()
        url = AMQP.rstrip("/") + f"/{encoded}"
        asyncio.run(_publish(url))
        barrier = threading.Barrier(4)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: asyncio.run(_consume(url, barrier)), range(4)))
        asyncio.run(_publish_conflict(url))
        asyncio.run(_consume(url, threading.Barrier(1)))
        with engine.connect() as c:
            rows = c.execute(select(transactions_table)).mappings().all()
        assert len(rows) == 1
        assert rows[0]["transaction_id"] == "concurrent-id"
        assert rows[0]["original_amount"] == Decimal("10.00000000")
        assert rows[0]["converted_usd_amount"] == Decimal("12.50000000")
    finally:
        client.delete(f"http://localhost:15672/api/vhosts/{encoded}", auth=auth)
        client.close()
        with engine.begin() as c:
            c.execute(text("DROP SCHEMA IF EXISTS transactions CASCADE"))
        engine.dispose()
