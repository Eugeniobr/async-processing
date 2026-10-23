from __future__ import annotations

import asyncio
import json
import os
import uuid
from urllib.parse import quote

import aio_pika
import httpx
import pytest

from domain.outcomes import Processed, RetryableFailure
from infrastructure.rabbitmq_consumer import TransactionConsumer
from infrastructure.rabbitmq_publisher import PublicationIndeterminate

RABBITMQ_URL = os.environ.get("RABBITMQ_URL")
USERNAME = "async_processing"
PASSWORD = "async_processing"
MANAGEMENT_URL = "http://localhost:15672"

pytestmark = pytest.mark.skipif(not RABBITMQ_URL, reason="RABBITMQ_URL is not configured")


class Processor:
    def process(self, _event: object) -> Processed:
        return Processed("tx-real-consumer")


class Publisher:
    async def publish_retry(self, _envelope: object, _delay: int) -> None:
        raise AssertionError("successful processing must not retry")

    async def publish_failure(self, _envelope: object, _reason: str) -> None:
        raise AssertionError("successful processing must not fail")


class FailingProcessor:
    def __init__(self, outcome: object) -> None:
        self._outcome = outcome

    def process(self, _event: object) -> object:
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


class FailingPublisher:
    async def publish_retry(self, _envelope: object, _delay: int) -> None:
        raise PublicationIndeterminate("test publication connection loss")

    async def publish_failure(self, _envelope: object, _reason: str) -> None:
        raise AssertionError("retryable processing must not use permanent failure routing")

    async def publish_invalid_envelope(self, _body: bytes, _reason: str) -> None:
        raise AssertionError("valid delivery must not use invalid-envelope routing")


def _body() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "event": {
                "transaction_id": "tx-real-consumer",
                "user_id": "user-1",
                "original_amount": "1.00000000",
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


async def _exercise(vhost_url: str) -> None:
    connection = await aio_pika.connect(vhost_url)
    try:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("consumer-test", auto_delete=True)
        queue = await channel.declare_queue("consumer-test", exclusive=True, auto_delete=True)
        await queue.bind(exchange, routing_key="work")
        await exchange.publish(aio_pika.Message(_body()), routing_key="work")
        delivery = await queue.get(fail=True, no_ack=False)
        await TransactionConsumer(Processor(), Publisher()).handle(delivery)
        assert await queue.get(fail=False, no_ack=True) is None
    finally:
        await connection.close()


def test_manual_acknowledgement_against_real_rabbitmq() -> None:
    vhost = f"t019-{uuid.uuid4().hex}"
    encoded = quote(vhost, safe="")
    client = httpx.Client()
    auth = (USERNAME, PASSWORD)
    try:
        client.put(f"{MANAGEMENT_URL}/api/vhosts/{encoded}", auth=auth).raise_for_status()
        client.put(
            f"{MANAGEMENT_URL}/api/permissions/{encoded}/{USERNAME}",
            auth=auth,
            json={"configure": ".*", "write": ".*", "read": ".*"},
        ).raise_for_status()
        asyncio.run(_exercise(f"amqp://{USERNAME}:{PASSWORD}@localhost:5672/{encoded}"))
    finally:
        client.delete(f"{MANAGEMENT_URL}/api/vhosts/{encoded}", auth=auth)
        client.close()


@pytest.mark.parametrize(
    "outcome",
    [RuntimeError("unexpected processor failure"), RetryableFailure("database offline")],
)
def test_unconfirmed_delivery_closes_its_channel_and_is_redelivered(outcome: object) -> None:
    """A real broker requeues uncertain work after the bounded consumer closes its channel."""

    vhost = f"t019-recovery-{uuid.uuid4().hex}"
    encoded = quote(vhost, safe="")
    client = httpx.Client()
    auth = (USERNAME, PASSWORD)

    async def exercise() -> None:
        connection = await aio_pika.connect(
            f"amqp://{USERNAME}:{PASSWORD}@localhost:5672/{encoded}"
        )
        try:
            channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
            exchange = await channel.declare_exchange(
                "consumer-recovery", durable=True, auto_delete=False
            )
            queue = await channel.declare_queue(
                f"consumer-recovery-{uuid.uuid4().hex}",
                durable=True,
                auto_delete=False,
                arguments={"x-queue-type": "quorum"},
            )
            await queue.bind(exchange, routing_key="work")
            await exchange.publish(aio_pika.Message(_body()), routing_key="work")
            delivery = await queue.get(fail=True, no_ack=False)
            publisher = FailingPublisher() if isinstance(outcome, RetryableFailure) else Publisher()

            await TransactionConsumer(FailingProcessor(outcome), publisher).handle(delivery)

            assert channel.is_closed
            recovery_channel = await connection.channel()
            redelivered = await (await recovery_channel.get_queue(queue.name)).get(
                fail=True, no_ack=False
            )
            assert redelivered.redelivered is True
            await redelivered.ack()
        finally:
            await connection.close()

    try:
        client.put(f"{MANAGEMENT_URL}/api/vhosts/{encoded}", auth=auth).raise_for_status()
        client.put(
            f"{MANAGEMENT_URL}/api/permissions/{encoded}/{USERNAME}",
            auth=auth,
            json={"configure": ".*", "write": ".*", "read": ".*"},
        ).raise_for_status()
        asyncio.run(exercise())
    finally:
        client.delete(f"{MANAGEMENT_URL}/api/vhosts/{encoded}", auth=auth)
        client.close()
