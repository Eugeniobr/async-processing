"""Prove supervisor restart after actual broker connection loss."""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx


async def run() -> None:
    evidence = Path(".runtime/scaling-evidence/compose")
    names = ["scale-validation-api-1"] + [f"scale-validation-consumer-{i}" for i in (1, 2, 3)]

    def counts():
        return {
            name: int(
                subprocess.check_output(
                    ["docker", "inspect", "--format", "{{.RestartCount}}", name], text=True
                )
            )
            for name in names
        }

    before = await asyncio.to_thread(counts)
    async with httpx.AsyncClient(timeout=3) as client:
        response = await client.get(
            "http://localhost:15674/api/queues/%2F/transactions.failure",
            auth=("async_processing", "async_processing"),
        )
        response.raise_for_status()
        retained = response.json()["messages_ready"]
        await asyncio.to_thread(
            subprocess.run,
            ["docker", "restart", "scale-validation-rabbitmq-1"],
            check=True,
            capture_output=True,
        )
        deadline = time.monotonic() + 90
        while True:
            after = await asyncio.to_thread(counts)
            try:
                api = await client.get("http://localhost:18000/healthz")
                broker = await client.get(
                    "http://localhost:15674/api/queues/%2F/transactions.work",
                    auth=("async_processing", "async_processing"),
                )
                healthy = (
                    api.status_code == broker.status_code == 200
                    and broker.json().get("consumers") == 3
                )
            except httpx.HTTPError:
                healthy = False
            if healthy and all(after[name] > before[name] for name in names):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"supervisor restart deadline: before={before}, latest={after}")
            await asyncio.sleep(0.2)
        event_id = "restart-" + uuid.uuid4().hex[:12]
        response = await client.post(
            "http://localhost:18000/transactions",
            json={
                "id": event_id,
                "user_id": event_id,
                "amount": "1.00000000",
                "currency": "USD",
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )
        assert response.status_code == 202
        while True:
            response = await client.get(f"http://localhost:18000/users/{event_id}/summary")
            response.raise_for_status()
            if response.json()["transaction_count"] == 1:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("post-restart accepted event did not persist")
            await asyncio.sleep(0.1)
        response = await client.get(
            "http://localhost:15674/api/queues/%2F/transactions.failure",
            auth=("async_processing", "async_processing"),
        )
        response.raise_for_status()
        assert response.json()["messages_ready"] == retained
        result = {
            "restart_counts_before": before,
            "restart_counts_after": after,
            "post_restart_event": event_id,
            "persisted": 1,
            "permanent_failures_retained_across_broker_restart": retained,
        }
        (evidence / "restart-summary.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(run())
