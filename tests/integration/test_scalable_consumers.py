"""Real competing deliveries and real shutdown boundaries; use a disposable DB."""

from __future__ import annotations

import asyncio
import os
import threading
from datetime import UTC, datetime
from decimal import Decimal

import aio_pika
import pytest
from sqlalchemy import create_engine, select

from application.money_converter import MoneyConverter
from application.transaction_processor import TransactionProcessor
from composition.consumer_main import run_with_processor
from composition.consumer_runtime import DrainTimeout
from composition.settings import Settings
from domain.rates import RateQuote
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.postgres_schema import transactions_table
from tests.recovery.support import RecoveryEnvironment, assert_queues_empty

pytestmark = pytest.mark.skipif(not os.getenv("RABBITMQ_URL"), reason="real broker required")


@pytest.fixture()
def environment(tmp_path):
    value = RecoveryEnvironment(tmp_path)
    value.start()
    try:
        yield value
    finally:
        value.close()


class Provider:
    def get_usd_rate(self, currency, requested_date):
        return RateQuote(
            base_currency=currency,
            quote_currency="USD",
            rate=Decimal("1.25"),
            requested_date=requested_date,
            rate_date=requested_date,
            provider="deterministic-test",
            fetched_at=datetime.now(UTC),
        )


def test_three_competing_consumers_overlap_unique_inserts(environment) -> None:
    engine = create_engine(environment.database_url)
    barrier = threading.Barrier(3, timeout=10)
    reached = []
    outcomes = []
    acks = []

    class Repository(PostgresTransactionRepository):
        def record_first(self, transaction):
            reached.append(transaction.event.transaction_id)
            barrier.wait()  # All three reached the actual SQL write boundary.
            return super().record_first(transaction)

    class Processor:
        def __init__(self):
            self.real = TransactionProcessor(Provider(), MoneyConverter(), Repository(engine))

        def process(self, event):
            result = self.real.process(event)
            outcomes.append(type(result).__name__)
            return result

    async def scenario():
        connections = []
        try:

            async def handle_on_independent_connection():
                connection = await aio_pika.connect(environment.rabbitmq_url)
                connections.append(connection)
                channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
                await channel.set_qos(prefetch_count=1)
                queue = await channel.get_queue("transactions.work")
                delivery = await queue.get(no_ack=False)
                from infrastructure.rabbitmq_consumer import TransactionConsumer
                from infrastructure.rabbitmq_publisher import TransactionPublisher

                consumer = TransactionConsumer(
                    Processor(),
                    TransactionPublisher(
                        await channel.get_exchange("transactions"),
                        await channel.get_exchange("transactions.retry"),
                        confirm_timeout_seconds=5,
                    ),
                )
                await consumer.handle(delivery)
                acks.append(delivery.processed)

            await asyncio.wait_for(
                asyncio.gather(*(handle_on_independent_connection() for _ in range(3))), 15
            )
        finally:
            for connection in connections:
                await connection.close()

    try:
        for _ in range(3):
            environment.publish_event("concurrent-identical")
        asyncio.run(scenario())
        assert reached == ["concurrent-identical"] * 3
        assert sorted(outcomes) == ["Duplicate", "Duplicate", "Processed"]
        assert acks == [True] * 3
        with engine.connect() as connection:
            rows = connection.execute(select(transactions_table)).mappings().all()
        assert len(rows) == 1
        assert rows[0]["original_amount"] == Decimal("12.34567890")
        assert rows[0]["converted_usd_amount"] == Decimal("15.43209862")
        assert rows[0]["rate_provider"] == "deterministic-test"
        assert_queues_empty(environment)
    finally:
        engine.dispose()


@pytest.mark.parametrize("expires", [False, True], ids=["graceful", "deadline"])
def test_shutdown_during_real_processing_preserves_work(environment, expires) -> None:
    engine = create_engine(environment.database_url)
    release = threading.Event()

    async def scenario():
        started = asyncio.Event()
        loop = asyncio.get_running_loop()

        class BlockingProvider(Provider):
            def get_usd_rate(self, currency, requested_date):
                loop.call_soon_threadsafe(started.set)
                assert release.wait(10)
                return super().get_usd_rate(currency, requested_date)

        stop = asyncio.Event()
        settings = Settings(
            rabbitmq_url=environment.rabbitmq_url,
            database_url=environment.database_url,
            consumer_prefetch=1,
            consumer_concurrency=1,
            consumer_shutdown_seconds=0.1 if expires else 3,
        )
        processor = TransactionProcessor(
            BlockingProvider(), MoneyConverter(), PostgresTransactionRepository(engine)
        )
        task = asyncio.create_task(run_with_processor(settings, processor, stop_event=stop))
        try:
            await asyncio.wait_for(started.wait(), 5)
            stop.set()
            if expires:
                with pytest.raises(DrainTimeout):
                    await asyncio.wait_for(task, 5)
            else:
                release.set()
                await asyncio.wait_for(task, 5)
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    try:
        environment.publish_event("shutdown-delivery")
        asyncio.run(scenario())  # Also waits for any uncancellable thread to finish.
        if expires:
            # The original is still on the broker even if its thread committed.
            async def recover():
                connection = await aio_pika.connect(environment.rabbitmq_url)
                try:
                    channel = await connection.channel(
                        publisher_confirms=True, on_return_raises=True
                    )
                    queue = await channel.get_queue("transactions.work")
                    delivery = await queue.get(no_ack=False)
                    assert delivery.redelivered
                    from infrastructure.rabbitmq_consumer import TransactionConsumer
                    from infrastructure.rabbitmq_publisher import TransactionPublisher

                    processor = TransactionProcessor(
                        Provider(), MoneyConverter(), PostgresTransactionRepository(engine)
                    )
                    publisher = TransactionPublisher(
                        await channel.get_exchange("transactions"),
                        await channel.get_exchange("transactions.retry"),
                        confirm_timeout_seconds=5,
                    )
                    await TransactionConsumer(processor, publisher).handle(delivery)
                finally:
                    await connection.close()

            asyncio.run(recover())
        assert environment.row_count() == 1
        assert_queues_empty(environment)
    finally:
        release.set()
        engine.dispose()
