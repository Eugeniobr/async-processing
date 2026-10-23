"""Validate the broker topology required by the durable processing design."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

import httpx


class TopologyValidationError(RuntimeError):
    """The broker cannot provide the topology this service was planned to use."""


@dataclass(frozen=True)
class RabbitMqTopology:
    """Names and immutable values of the approved RabbitMQ 4.3.6 topology."""

    vhost: str = "/"
    work_exchange: str = "transactions"
    retry_exchange: str = "transactions.retry"
    work_queue: str = "transactions.work"
    failure_queue: str = "transactions.failure"
    retry_delays_seconds: tuple[int, ...] = (1, 2, 4, 8, 16, 32, 60)

    @property
    def retry_queues(self) -> dict[str, int]:
        return {f"transactions.retry.{delay}s": delay * 1000 for delay in self.retry_delays_seconds}


class RabbitMqTopologyValidator:
    """Fail fast by checking the effective broker configuration through management API."""

    def __init__(
        self,
        client: httpx.Client,
        *,
        management_url: str,
        username: str,
        password: str,
        topology: RabbitMqTopology | None = None,
    ) -> None:
        self._client = client
        self._management_url = management_url.rstrip("/")
        self._auth = (username, password)
        self._topology = topology or RabbitMqTopology()

    def validate(self) -> None:
        """Raise when RabbitMQ 4.3.6 or any required topology setting is absent."""

        overview = self._get("/api/overview")
        if overview.get("rabbitmq_version") != "4.3.6":
            raise TopologyValidationError("RabbitMQ version must be 4.3.6")

        feature_flags = self._get("/api/feature-flags")
        if not any(
            flag.get("name") == "stream_queue" and flag.get("state") == "enabled"
            for flag in self._require_list(feature_flags, "feature flags")
            if isinstance(flag, dict)
        ):
            raise TopologyValidationError("RabbitMQ feature flag stream_queue must be enabled")

        vhost = quote(self._topology.vhost, safe="")
        exchanges = self._require_list(self._get(f"/api/exchanges/{vhost}"), "exchanges")
        queues = self._require_list(self._get(f"/api/queues/{vhost}"), "queues")
        bindings = self._require_list(self._get(f"/api/bindings/{vhost}"), "bindings")

        self._validate_exchanges(exchanges)
        self._validate_queues(queues)
        self._validate_bindings(bindings)

    def _get(self, path: str) -> object:
        try:
            response = self._client.get(
                f"{self._management_url}{path}", auth=self._auth, timeout=5.0
            )
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise TopologyValidationError(f"unable to inspect RabbitMQ topology: {path}") from error

    @staticmethod
    def _require_list(value: object, description: str) -> list[object]:
        if not isinstance(value, list):
            raise TopologyValidationError(f"RabbitMQ {description} response must be a list")
        return value

    def _validate_exchanges(self, exchanges: list[object]) -> None:
        expected = {self._topology.work_exchange, self._topology.retry_exchange}
        found = {item.get("name"): item for item in exchanges if isinstance(item, dict)}
        for name in expected:
            exchange = found.get(name)
            if not isinstance(exchange, dict) or any(
                (
                    exchange.get("type") != "direct",
                    exchange.get("durable") is not True,
                    exchange.get("auto_delete") is not False,
                    exchange.get("internal") is not False,
                )
            ):
                raise TopologyValidationError(
                    f"exchange {name} is not the required durable direct exchange"
                )

    def _validate_queues(self, queues: list[object]) -> None:
        found = {item.get("name"): item for item in queues if isinstance(item, dict)}
        for queue_name in (self._topology.work_queue, self._topology.failure_queue):
            self._validate_queue(found.get(queue_name), queue_name, ttl_milliseconds=None)
        for queue_name, ttl in self._topology.retry_queues.items():
            self._validate_queue(found.get(queue_name), queue_name, ttl_milliseconds=ttl)

    def _validate_queue(
        self, queue: object, queue_name: str, *, ttl_milliseconds: int | None
    ) -> None:
        if not isinstance(queue, dict):
            raise TopologyValidationError(f"required queue {queue_name} is missing")
        arguments = queue.get("arguments")
        policy = queue.get("effective_policy_definition")
        if not isinstance(arguments, dict) or not isinstance(policy, dict):
            raise TopologyValidationError(f"queue {queue_name} does not expose required settings")
        if any(
            (
                queue.get("type") != "quorum",
                queue.get("durable") is not True,
                queue.get("exclusive") is not False,
                queue.get("auto_delete") is not False,
                arguments.get("x-queue-type") != "quorum",
                policy.get("overflow") != "reject-publish",
                policy.get("max-length") != 100000,
                policy.get("delivery-limit") != -1,
            )
        ):
            raise TopologyValidationError(
                f"queue {queue_name} is not configured for durable safety"
            )
        if ttl_milliseconds is None:
            return
        if arguments.get("x-message-ttl") != ttl_milliseconds or any(
            (
                policy.get("dead-letter-strategy") != "at-least-once",
                policy.get("dead-letter-exchange") != self._topology.work_exchange,
                policy.get("dead-letter-routing-key") != "work",
            )
        ):
            raise TopologyValidationError(
                f"retry queue {queue_name} has incompatible delay or DLX settings"
            )

    def _validate_bindings(self, bindings: list[object]) -> None:
        actual = {
            (item.get("source"), item.get("destination"), item.get("routing_key"))
            for item in bindings
            if isinstance(item, dict) and item.get("destination_type") == "queue"
        }
        expected = {
            (self._topology.work_exchange, self._topology.work_queue, "work"),
            (self._topology.work_exchange, self._topology.failure_queue, "failure"),
            *(
                (self._topology.retry_exchange, queue_name, f"retry.{ttl // 1000}s")
                for queue_name, ttl in self._topology.retry_queues.items()
            ),
        }
        missing = expected - actual
        if missing:
            raise TopologyValidationError(
                f"required RabbitMQ bindings are missing: {sorted(missing)!r}"
            )
