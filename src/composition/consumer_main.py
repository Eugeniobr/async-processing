"""Run one bounded-prefetch RabbitMQ consumer process."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import aio_pika

from composition.container import build_container
from composition.settings import Settings
from infrastructure.metrics import MetricsRecorder
from infrastructure.rabbitmq_consumer import (
    ProcessingFailureMetrics,
    Processor,
    TransactionConsumer,
)
from infrastructure.rabbitmq_publisher import TransactionPublisher
from infrastructure.tracing import TransactionTracer


async def run() -> None:
    """Start the planned consumer and close all owned resources on shutdown."""

    settings = Settings()
    MetricsRecorder.require_multiprocess_directory()
    container = build_container(settings)
    try:
        await run_with_processor(
            settings,
            container.processor,
            metrics=container.metrics,
            tracer=container.tracer,
        )
    finally:
        container.close()


async def run_with_processor(
    settings: Settings,
    processor: Processor,
    *,
    metrics: ProcessingFailureMetrics | None = None,
    after_processing: Callable[[], Awaitable[None]] | None = None,
    after_retry_publish: Callable[[], Awaitable[None]] | None = None,
    tracer: TransactionTracer | None = None,
) -> None:
    """Run the standard consumer with explicitly supplied processing dependencies.

    Production calls this through :func:`run`; tests can supply a deterministic
    provider through a fully assembled processor without changing configuration.
    """

    connection = await aio_pika.connect(settings.rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        await channel.set_qos(prefetch_count=settings.consumer_prefetch)
        publisher = TransactionPublisher(
            await channel.get_exchange("transactions"),
            await channel.get_exchange("transactions.retry"),
            confirm_timeout_seconds=5,
            tracer=tracer,
        )
        consumer = TransactionConsumer(
            processor,
            publisher,
            metrics=metrics,
            after_processing=after_processing,
            after_retry_publish=after_retry_publish,
            tracer=tracer,
        )
        queue = await channel.get_queue("transactions.work")
        tag = await queue.consume(consumer.handle, no_ack=False)
        try:
            await asyncio.Future()
        finally:
            await queue.cancel(tag)
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(run())
