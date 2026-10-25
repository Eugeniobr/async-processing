"""Run a bounded competing consumer with explicit signal and resource lifecycles."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections.abc import Awaitable, Callable

import aio_pika

from composition.consumer_runtime import ConsumerRuntime, ConsumerStatusServer, DrainTimeout
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

logger = logging.getLogger(__name__)


async def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()
    if settings.metrics_mode == "multiprocess":
        MetricsRecorder.require_multiprocess_directory()
    container = build_container(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    status = (
        ConsumerStatusServer(container.metrics, settings.consumer_metrics_port)
        if settings.metrics_mode == "instance"
        else None
    )
    try:
        await run_with_processor(
            settings,
            container.processor,
            metrics=container.metrics,
            tracer=container.tracer,
            stop_event=stop,
            status_server=status,
        )
    except DrainTimeout:
        # Cancelling to_thread cannot stop a running synchronous call. AMQP has
        # already closed: end this process without closing clients under workers.
        # The OS releases sockets and threads; unacked originals are redelivered.
        logger.exception("consumer drain timed out; exiting for supervisor restart")
        os._exit(1)
    finally:
        if status:
            await asyncio.to_thread(status.close)
        container.close()


async def run_with_processor(
    settings: Settings,
    processor: Processor,
    *,
    metrics: ProcessingFailureMetrics | None = None,
    after_processing: Callable[[], Awaitable[None]] | None = None,
    after_retry_publish: Callable[[], Awaitable[None]] | None = None,
    tracer: TransactionTracer | None = None,
    stop_event: asyncio.Event | None = None,
    status_server: ConsumerStatusServer | None = None,
) -> None:
    """Allow deterministic dependency injection without production test switches."""

    stop = stop_event or asyncio.Event()
    connection = await aio_pika.connect(settings.rabbitmq_url)
    runtime = None
    cancelled = False
    channel_failed = False
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        channel_closed = asyncio.Event()
        channel.close_callbacks.add(lambda *_: channel_closed.set())
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
            recover_unconfirmed=channel.close,
            tracer=tracer,
            concurrency=min(settings.consumer_concurrency, settings.consumer_prefetch),
        )
        runtime = ConsumerRuntime(
            consumer, channel.close, shutdown_seconds=settings.consumer_shutdown_seconds
        )
        if status_server:
            status_server.runtime = runtime
        queue = await channel.get_queue("transactions.work")
        tag = await queue.consume(runtime.handle, no_ack=False)
        logger.info(
            "consumer ready prefetch=%s concurrency=%s",
            settings.consumer_prefetch,
            settings.consumer_concurrency,
        )
        waiters = [asyncio.create_task(stop.wait()), asyncio.create_task(channel_closed.wait())]
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            channel_failed = channel_closed.is_set() and not stop.is_set()
        except asyncio.CancelledError:
            cancelled = True
        finally:
            runtime.ready = False
            logger.info("consumer draining active=%s", len(runtime.tasks))
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            if not channel.is_closed:
                await queue.cancel(tag)
                logger.info("consumer intake cancelled; draining existing deliveries")
            await runtime.drain()
    finally:
        if runtime:
            runtime.ready = False
            runtime.alive = False
        await connection.close()
    if channel_failed:
        raise RuntimeError("consumer channel closed unexpectedly; supervisor must restart")
    if cancelled:
        raise asyncio.CancelledError


if __name__ == "__main__":
    asyncio.run(run())
