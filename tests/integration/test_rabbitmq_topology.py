"""RabbitMQ 4.3.6 checks using a disposable vhost, never local application queues."""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import quote

import aio_pika
import httpx
import pytest

from domain.transaction import RetryEnvelope, TransactionEvent
from infrastructure.rabbitmq_publisher import TransactionPublisher, UnroutablePublication
from infrastructure.rabbitmq_topology import (
    RabbitMqTopology,
    RabbitMqTopologyValidator,
    TopologyValidationError,
)

RABBITMQ_URL = os.environ.get("RABBITMQ_URL")
MANAGEMENT_URL = os.environ.get("RABBITMQ_MANAGEMENT_URL", "http://localhost:15672")
USERNAME = os.environ.get("RABBITMQ_USERNAME", "async_processing")
PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "async_processing")

pytestmark = pytest.mark.skipif(not RABBITMQ_URL, reason="RABBITMQ_URL is not configured")


def _event() -> TransactionEvent:
    return TransactionEvent(
        transaction_id=f"topology-{uuid.uuid4()}",
        user_id="integration-user",
        original_amount=Decimal("1.00000000"),
        currency="EUR",
        event_timestamp=datetime(2026, 10, 22, tzinfo=UTC),
        currency_allowlist_version="v1",
    )


def _configure_vhost(client: httpx.Client, vhost: str) -> None:
    encoded = quote(vhost, safe="")
    auth = (USERNAME, PASSWORD)
    for response in (
        client.put(f"{MANAGEMENT_URL}/api/vhosts/{encoded}", auth=auth),
        client.put(
            f"{MANAGEMENT_URL}/api/permissions/{encoded}/{USERNAME}",
            auth=auth,
            json={"configure": ".*", "write": ".*", "read": ".*"},
        ),
        client.put(
            f"{MANAGEMENT_URL}/api/policies/{encoded}/core-safety",
            auth=auth,
            json={
                "pattern": "^transactions\\.(work|failure)$",
                "apply-to": "quorum_queues",
                "definition": {
                    "overflow": "reject-publish",
                    "max-length": 100000,
                    "delivery-limit": -1,
                },
                "priority": 0,
            },
        ),
        client.put(
            f"{MANAGEMENT_URL}/api/policies/{encoded}/retry-safety",
            auth=auth,
            json={
                "pattern": "^transactions\\.retry\\.(1s|2s|4s|8s|16s|32s|60s)$",
                "apply-to": "quorum_queues",
                "definition": {
                    "overflow": "reject-publish",
                    "max-length": 100000,
                    "delivery-limit": -1,
                    "dead-letter-strategy": "at-least-once",
                    "dead-letter-exchange": "transactions",
                    "dead-letter-routing-key": "work",
                },
                "priority": 0,
            },
        ),
    ):
        response.raise_for_status()


async def _declare_topology(vhost_url: str) -> None:
    connection = await aio_pika.connect(vhost_url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        work = await channel.declare_exchange(
            "transactions", aio_pika.ExchangeType.DIRECT, durable=True
        )
        retry = await channel.declare_exchange(
            "transactions.retry", aio_pika.ExchangeType.DIRECT, durable=True
        )
        await channel.declare_queue(
            "transactions.work", durable=True, arguments={"x-queue-type": "quorum"}
        )
        await channel.declare_queue(
            "transactions.failure", durable=True, arguments={"x-queue-type": "quorum"}
        )
        work_queue = await channel.get_queue("transactions.work")
        failure_queue = await channel.get_queue("transactions.failure")
        await work_queue.bind(work, routing_key="work")
        await failure_queue.bind(work, routing_key="failure")
        for delay in RabbitMqTopology().retry_delays_seconds:
            queue = await channel.declare_queue(
                f"transactions.retry.{delay}s",
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-message-ttl": delay * 1000,
                    "x-dead-letter-exchange": "transactions",
                    "x-dead-letter-routing-key": "work",
                },
            )
            await queue.bind(retry, routing_key=f"retry.{delay}s")
    finally:
        await connection.close()


async def _verify_publisher_and_retry_routing(vhost_url: str) -> None:
    connection = await aio_pika.connect(vhost_url)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        work = await channel.get_exchange("transactions")
        retry = await channel.get_exchange("transactions.retry")
        publisher = TransactionPublisher(work, retry, confirm_timeout_seconds=5)
        event = _event()
        await publisher.publish_work(event)

        envelope = RetryEnvelope(
            schema_version=1,
            event=event,
            attempt=1,
            accepted_at=datetime.now(UTC),
            currency_allowlist_version="v1",
            last_failure_kind="provider_unavailable",
        )
        for delay in RabbitMqTopology().retry_delays_seconds:
            await publisher.publish_retry(envelope, delay)

        unroutable = await channel.declare_exchange(
            f"unroutable-{uuid.uuid4()}", aio_pika.ExchangeType.DIRECT, auto_delete=True
        )
        with pytest.raises(UnroutablePublication):
            await TransactionPublisher(unroutable, retry, confirm_timeout_seconds=5).publish_work(
                _event()
            )

        work_queue = await channel.get_queue("transactions.work")
        received_count = 0
        received_ids: set[str] = set()
        deadline = time.monotonic() + 70
        while received_count < 8 and time.monotonic() < deadline:
            message = await work_queue.get(fail=False, no_ack=True)
            if message is None:
                await asyncio.sleep(0.2)
                continue
            received_count += 1
            received_ids.add(message.message_id or "")
        assert event.transaction_id in received_ids
        assert received_count == 8  # one work publication plus seven retry deliveries
    finally:
        await connection.close()


def test_real_broker_validates_topology_confirms_and_routes_every_retry_bucket() -> None:
    vhost = f"t011-{uuid.uuid4().hex}"
    encoded = quote(vhost, safe="")
    client = httpx.Client()
    try:
        # Validate the Compose-managed topology itself. The disposable vhost below
        # is only a routing sandbox and never substitutes for this configuration check.
        RabbitMqTopologyValidator(
            client,
            management_url=MANAGEMENT_URL,
            username=USERNAME,
            password=PASSWORD,
        ).validate()
        with pytest.raises(TopologyValidationError, match="required queue"):
            RabbitMqTopologyValidator(
                client,
                management_url=MANAGEMENT_URL,
                username=USERNAME,
                password=PASSWORD,
                topology=RabbitMqTopology(failure_queue="incompatible.failure"),
            ).validate()

        _configure_vhost(client, vhost)
        vhost_url = f"amqp://{USERNAME}:{PASSWORD}@localhost:5672/{encoded}"
        asyncio.run(_declare_topology(vhost_url))
        asyncio.run(_verify_publisher_and_retry_routing(vhost_url))
    finally:
        response = client.delete(
            f"{MANAGEMENT_URL}/api/vhosts/{encoded}", auth=(USERNAME, PASSWORD)
        )
        if response.status_code not in {204, 404}:
            response.raise_for_status()
        client.close()
