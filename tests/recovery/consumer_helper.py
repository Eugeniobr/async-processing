"""Separate-process real consumer used only by recovery integration tests."""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from multiprocessing.connection import Client, Connection

import aio_pika
from sqlalchemy import create_engine

from application.money_converter import MoneyConverter
from application.ports import RetryableRateError
from application.transaction_processor import ProcessingOutcome, TransactionProcessor
from domain.rates import RateQuote
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.rabbitmq_consumer import TransactionConsumer, decode_envelope
from infrastructure.rabbitmq_publisher import TransactionPublisher


class DeterministicProvider:
    """Return one stable historical quote or a deliberate temporary failure."""

    def __init__(self, *, temporarily_unavailable: bool) -> None:
        self._temporarily_unavailable = temporarily_unavailable

    def get_usd_rate(self, currency: str, requested_date: date) -> RateQuote:
        if self._temporarily_unavailable:
            raise RetryableRateError("test provider temporarily unavailable")
        return RateQuote(
            base_currency=currency,
            quote_currency="USD",
            rate=Decimal("1.250000000000"),
            requested_date=requested_date,
            rate_date=requested_date,
            provider="recovery-test",
            fetched_at=datetime.now(UTC),
        )


class RecordingProcessor:
    """Retain outcomes for the parent without changing delivery control."""

    def __init__(self, processor: TransactionProcessor) -> None:
        self._processor = processor
        self.outcomes: list[str] = []

    def process(self, event: object) -> ProcessingOutcome:
        outcome = self._processor.process(event)  # type: ignore[arg-type]
        self.outcomes.append(type(outcome).__name__)
        return outcome


async def _checkpoint(connection: Connection, name: str) -> None:
    connection.send({"event": "checkpoint", "name": name})
    await asyncio.to_thread(connection.recv)


async def run_consumer(arguments: argparse.Namespace) -> None:
    ipc = Client(arguments.ipc_socket, family="AF_UNIX")
    engine = create_engine(arguments.database_url, pool_pre_ping=True)
    connection: aio_pika.abc.AbstractRobustConnection | None = None
    try:
        processor = RecordingProcessor(
            TransactionProcessor(
                DeterministicProvider(temporarily_unavailable=arguments.mode == "retry"),
                MoneyConverter(),
                PostgresTransactionRepository(engine),
            )
        )
        connection = await aio_pika.connect(arguments.rabbitmq_url)
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        await channel.set_qos(prefetch_count=1)
        publisher = TransactionPublisher(
            await channel.get_exchange("transactions"),
            await channel.get_exchange("transactions.retry"),
            confirm_timeout_seconds=5,
        )

        after_processing = None
        after_retry_publish = None
        if arguments.checkpoint == "after_processing":

            async def after_processing() -> None:
                await _checkpoint(ipc, "after_processing")
        elif arguments.checkpoint == "after_retry_publish":

            async def after_retry_publish() -> None:
                await _checkpoint(ipc, "after_retry_publish")

        consumer = TransactionConsumer(
            processor,
            publisher,
            after_processing=after_processing,
            after_retry_publish=after_retry_publish,
        )
        queue = await channel.get_queue("transactions.work")
        completed = 0
        envelopes: list[dict[str, object]] = []
        deadline = asyncio.get_running_loop().time() + arguments.deadline_seconds
        while completed < arguments.max_messages:
            delivery = await queue.get(fail=False, no_ack=False)
            if delivery is None:
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("deadline while waiting for recovery delivery")
                await asyncio.sleep(0.05)
                continue
            envelope = decode_envelope(delivery.body)
            await consumer.handle(delivery)
            envelopes.append(
                {
                    "transaction_id": envelope.event.transaction_id,
                    "attempt": envelope.attempt,
                    "currency_allowlist_version": envelope.currency_allowlist_version,
                }
            )
            completed += 1
        ipc.send({"event": "completed", "outcomes": processor.outcomes, "envelopes": envelopes})
    except Exception as error:
        ipc.send({"event": "error", "type": type(error).__name__, "message": str(error)})
        raise
    finally:
        if connection is not None:
            await connection.close()
        engine.dispose()
        ipc.close()


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rabbitmq-url", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--ipc-socket", required=True)
    parser.add_argument("--mode", choices=("normal", "retry"), required=True)
    parser.add_argument(
        "--checkpoint", choices=("none", "after_processing", "after_retry_publish"), default="none"
    )
    parser.add_argument("--max-messages", type=int, default=1)
    parser.add_argument("--deadline-seconds", type=float, default=15)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_consumer(_arguments()))
