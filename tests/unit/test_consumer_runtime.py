"""Deterministic bounds and cancellation checks for the scalable consumer."""

from __future__ import annotations

import asyncio
import threading

import pytest
from prometheus_client import CollectorRegistry

from domain.outcomes import Processed
from infrastructure.metrics import MetricsRecorder
from infrastructure.rabbitmq_consumer import TransactionConsumer
from tests.unit.test_rabbitmq_consumer import Delivery, Publisher, body


def test_processing_does_not_block_loop_and_respects_concurrency() -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        two_started = asyncio.Event()
        release = threading.Event()
        lock = threading.Lock()
        active = 0
        maximum = 0

        class Processor:
            def process(self, event: object) -> Processed:
                nonlocal active, maximum
                with lock:
                    active += 1
                    maximum = max(maximum, active)
                    if active == 2:
                        loop.call_soon_threadsafe(two_started.set)
                try:
                    assert release.wait(3), "test controller did not release processing"
                    return Processed("done")
                finally:
                    with lock:
                        active -= 1

        consumer = TransactionConsumer(Processor(), Publisher([]), concurrency=2)
        deliveries = [Delivery(body()) for _ in range(3)]
        tasks = [asyncio.create_task(consumer.handle(item)) for item in deliveries]
        try:
            await asyncio.wait_for(two_started.wait(), 2)
            assert maximum == 2
            assert all(item.ack_count == 0 for item in deliveries)
        finally:
            release.set()
            await asyncio.gather(*tasks)
        assert maximum == 2
        assert all(item.ack_count == 1 for item in deliveries)

    asyncio.run(scenario())


def test_cancelling_thread_await_does_not_ack_later_commit() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        loop = asyncio.get_running_loop()
        release = threading.Event()

        class Processor:
            def process(self, event: object) -> Processed:
                loop.call_soon_threadsafe(started.set)
                assert release.wait(3)
                return Processed("committed-after-cancellation")

        delivery = Delivery(body())
        consumer = TransactionConsumer(Processor(), Publisher([]))
        task = asyncio.create_task(consumer.handle(delivery))
        try:
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
        assert delivery.ack_count == 0

    asyncio.run(scenario())


def test_instance_metrics_expose_bounded_outcomes_and_processing_time() -> None:
    metrics = MetricsRecorder(registry=CollectorRegistry())
    metrics.record_delivery_outcome("processed")
    metrics.record_delivery_outcome("duplicate")
    metrics.record_processing_duration(0.5)
    rendered = metrics.render().decode()
    assert 'transaction_deliveries_total{outcome="processed"} 1.0' in rendered
    assert 'transaction_deliveries_total{outcome="duplicate"} 1.0' in rendered
    assert "transaction_processing_seconds_count 1.0" in rendered
    assert "transaction_id" not in rendered
    assert "user_id" not in rendered
    with pytest.raises(ValueError):
        metrics.record_delivery_outcome("arbitrary-id")


def test_drain_timeout_closes_channel_before_cancelling_without_ack() -> None:
    from composition.consumer_runtime import ConsumerRuntime, DrainTimeout

    async def scenario() -> None:
        started = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        calls = []

        class Processor:
            def process(self, event: object) -> Processed:
                loop.call_soon_threadsafe(started.set)
                assert release.wait(3)
                return Processed("committed")

        async def close() -> None:
            calls.append("closed")

        delivery = Delivery(body())
        runtime = ConsumerRuntime(
            TransactionConsumer(Processor(), Publisher([])), close, shutdown_seconds=0.02
        )
        task = asyncio.create_task(runtime.handle(delivery))
        try:
            await asyncio.wait_for(started.wait(), 2)
            with pytest.raises(DrainTimeout):
                await runtime.drain()
            assert calls == ["closed"]
            assert delivery.ack_count == 0
            assert task.cancelled()
            assert not runtime.ready
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_drain_waits_for_completion_before_ack() -> None:
    from composition.consumer_runtime import ConsumerRuntime

    async def scenario() -> None:
        started = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()

        class Processor:
            def process(self, event: object) -> Processed:
                loop.call_soon_threadsafe(started.set)
                assert release.wait(3)
                return Processed("committed")

        async def close() -> None:
            raise AssertionError("successful drain must not force-close")

        delivery = Delivery(body())
        runtime = ConsumerRuntime(
            TransactionConsumer(Processor(), Publisher([])), close, shutdown_seconds=2
        )
        task = asyncio.create_task(runtime.handle(delivery))
        try:
            await asyncio.wait_for(started.wait(), 2)
            drain = asyncio.create_task(runtime.drain())
            await asyncio.sleep(0)  # Give drain one loop turn, not a timing assertion.
            assert not drain.done()
            assert delivery.ack_count == 0
            release.set()
            await drain
            await task
            assert delivery.ack_count == 1
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_consumer_http_metrics_are_instance_local_and_probes_follow_runtime() -> None:
    from types import SimpleNamespace

    import httpx

    from composition.consumer_runtime import ConsumerStatusServer

    first = MetricsRecorder(registry=CollectorRegistry())
    second = MetricsRecorder(registry=CollectorRegistry())
    first.record_processing_failure("permanent")
    servers = [ConsumerStatusServer(first, 0), ConsumerStatusServer(second, 0)]
    try:
        first_url = f"http://127.0.0.1:{servers[0]._server.server_address[1]}"
        second_url = f"http://127.0.0.1:{servers[1]._server.server_address[1]}"
        with httpx.Client() as client:
            assert client.get(first_url + "/readyz").status_code == 503
            state = SimpleNamespace(ready=True, alive=True)
            servers[0].runtime = state
            assert client.get(first_url + "/readyz").status_code == 200
            assert 'kind="permanent"} 1.0' in client.get(first_url + "/metrics").text
            assert 'kind="permanent"}' not in client.get(second_url + "/metrics").text
            state.ready = False
            assert client.get(first_url + "/readyz").status_code == 503
            assert client.get(first_url + "/healthz").status_code == 200
            state.alive = False
            assert client.get(first_url + "/healthz").status_code == 503
    finally:
        for server in servers:
            server.close()


def test_offload_propagates_trace_parent_and_errors_are_recorded_before_span_end(caplog) -> None:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from infrastructure.tracing import OpenTelemetryTransactionTracer

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = OpenTelemetryTransactionTracer(provider.get_tracer("unit"), provider)

    class Processor:
        def process(self, event: object) -> Processed:
            with tracer.span("thread.operation", stage="unit"):
                return Processed("done")

    async def scenario():
        consumer = TransactionConsumer(Processor(), Publisher([]), tracer=tracer)
        with tracer.span("root", stage="test", transaction_id="correlation-only"):
            await consumer.handle(Delivery(body()))
        with pytest.raises(ValueError), tracer.span("error", stage="test"):
            raise ValueError("controlled failure")

    try:
        asyncio.run(scenario())
        spans = {span.name: span for span in exporter.get_finished_spans()}
        assert (
            spans["thread.operation"].parent.span_id
            == spans["transaction.conversion"].context.span_id
        )
        assert spans["thread.operation"].attributes["transaction_id"] == "tx-consumer-1"
        assert spans["thread.operation"].context.trace_id == spans["root"].context.trace_id
        assert spans["error"].status.status_code == trace.StatusCode.ERROR
        assert len(spans["error"].events) == 1
        assert "ended span" not in caplog.text
    finally:
        tracer.close()
