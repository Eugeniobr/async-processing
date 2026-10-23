from __future__ import annotations

import httpx
import pytest

from infrastructure.rabbitmq_topology import RabbitMqTopologyValidator, TopologyValidationError


def _queue(name: str, *, ttl: int | None = None) -> dict[str, object]:
    arguments: dict[str, object] = {"x-queue-type": "quorum"}
    policy: dict[str, object] = {
        "overflow": "reject-publish",
        "max-length": 100000,
        "delivery-limit": -1,
    }
    if ttl is not None:
        arguments["x-message-ttl"] = ttl
        policy.update(
            {
                "dead-letter-strategy": "at-least-once",
                "dead-letter-exchange": "transactions",
                "dead-letter-routing-key": "work",
            }
        )
    return {
        "name": name,
        "type": "quorum",
        "durable": True,
        "exclusive": False,
        "auto_delete": False,
        "arguments": arguments,
        "effective_policy_definition": policy,
    }


def _payloads() -> dict[str, object]:
    retry_delays = (1, 2, 4, 8, 16, 32, 60)
    return {
        "/api/overview": {"rabbitmq_version": "4.3.6"},
        "/api/feature-flags": [{"name": "stream_queue", "state": "enabled"}],
        "/api/exchanges/%2F": [
            {
                "name": "transactions",
                "type": "direct",
                "durable": True,
                "auto_delete": False,
                "internal": False,
            },
            {
                "name": "transactions.retry",
                "type": "direct",
                "durable": True,
                "auto_delete": False,
                "internal": False,
            },
        ],
        "/api/queues/%2F": [
            _queue("transactions.work"),
            _queue("transactions.failure"),
            *[_queue(f"transactions.retry.{delay}s", ttl=delay * 1000) for delay in retry_delays],
        ],
        "/api/bindings/%2F": [
            {
                "source": "transactions",
                "destination": "transactions.work",
                "destination_type": "queue",
                "routing_key": "work",
            },
            {
                "source": "transactions",
                "destination": "transactions.failure",
                "destination_type": "queue",
                "routing_key": "failure",
            },
            *[
                {
                    "source": "transactions.retry",
                    "destination": f"transactions.retry.{delay}s",
                    "destination_type": "queue",
                    "routing_key": f"retry.{delay}s",
                }
                for delay in retry_delays
            ],
        ],
    }


def validator(payloads: dict[str, object]) -> RabbitMqTopologyValidator:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payloads[request.url.raw_path.decode()])

    return RabbitMqTopologyValidator(
        httpx.Client(transport=httpx.MockTransport(handler)),
        management_url="http://rabbitmq:15672",
        username="user",
        password="password",
    )


def test_accepts_the_approved_rabbitmq_topology() -> None:
    validator(_payloads()).validate()


@pytest.mark.parametrize(
    ("path", "mutation", "expected"),
    [
        ("/api/overview", lambda data: data.update(rabbitmq_version="4.3.5"), "version"),
        ("/api/feature-flags", lambda data: data.clear(), "stream_queue"),
        (
            "/api/queues/%2F",
            lambda data: data[2]["effective_policy_definition"].pop("dead-letter-strategy"),
            "retry queue",
        ),
        ("/api/bindings/%2F", lambda data: data.pop(), "bindings"),
    ],
)
def test_rejects_each_missing_required_topology_setting(
    path: str, mutation: object, expected: str
) -> None:
    payloads = _payloads()
    mutation(payloads[path])

    with pytest.raises(TopologyValidationError, match=expected):
        validator(payloads).validate()
