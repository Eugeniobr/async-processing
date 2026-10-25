"""Real three-container routing, duplicate, and graceful shutdown proof."""

from __future__ import annotations

import asyncio
import json
import re
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx


async def run() -> None:
    evidence = Path(".runtime/scaling-evidence/compose")
    evidence.mkdir(parents=True, exist_ok=True)
    api = "http://127.0.0.1:18000"
    broker = "http://127.0.0.1:15674"
    provider = "http://127.0.0.1:18082"
    auth = ("async_processing", "async_processing")
    user = "compose-scale-" + uuid.uuid4().hex[:10]
    timestamp = datetime.now(UTC).replace(microsecond=0).isoformat()
    expected = set()
    processes = []
    containers = subprocess.check_output(
        [
            "docker",
            "ps",
            "--filter",
            "label=com.docker.compose.project=scale-validation",
            "--filter",
            "label=com.docker.compose.service=consumer",
            "--format",
            "{{.ID}}",
        ],
        text=True,
    ).split()
    assert len(containers) == 3

    async def read_metrics(container):
        return await asyncio.to_thread(
            subprocess.check_output,
            [
                "docker",
                "exec",
                container,
                "python",
                "-c",
                "import urllib.request; print(urllib.request.urlopen('http://localhost:8001/metrics').read().decode())",
            ],
            text=True,
        )

    def failures(rendered, kind):
        match = re.search(
            rf'transaction_processing_failures_total{{kind="{kind}"}} ([0-9.]+)', rendered
        )
        return float(match[1]) if match else 0

    # Stages include gates longer than Uvicorn's idle keep-alive. Use fresh HTTP
    # connections rather than retrying an ambiguously completed POST on a stale socket.
    async with httpx.AsyncClient(
        timeout=10, limits=httpx.Limits(max_keepalive_connections=0)
    ) as client:
        response = await client.post(
            f"{provider}/control", json={"brl_available": False, "block": False}
        )
        response.raise_for_status()

        async def submit(transaction_id, currency="EUR"):
            response = await client.post(
                f"{api}/transactions",
                json={
                    "id": transaction_id,
                    "user_id": user,
                    "amount": "2.50000000",
                    "currency": currency,
                    "timestamp": timestamp,
                },
            )
            assert response.status_code == 202, response.text

        async def queues():
            response = await client.get(f"{broker}/api/queues/%2F", auth=auth)
            response.raise_for_status()
            return {row["name"]: row for row in response.json()}

        async def wait_for(predicate, description, seconds=30):
            deadline = time.monotonic() + seconds
            while True:
                value = await predicate()
                if value:
                    return value
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"{description}; queues={await queues()}")
                await asyncio.sleep(0.1)

        async def history():
            response = await client.get(
                f"{api}/users/{user}/transactions",
                params={
                    "from": timestamp,
                    "to": "2099-01-01T00:00:00Z",
                    "limit": 100,
                },
            )
            response.raise_for_status()
            return response.json()["items"]

        async def registered():
            return (await queues())["transactions.work"].get("consumers", 0) == 3

        await wait_for(registered, "all three consumers registered before publishing")
        baseline_processed = {}
        baseline_failures = {"retryable": 0, "permanent": 0}
        for container in containers:
            rendered = await read_metrics(container)
            for kind in baseline_failures:
                baseline_failures[kind] += failures(rendered, kind)
            match = re.search(
                r'transaction_deliveries_total{outcome="processed"} ([0-9.]+)', rendered
            )
            baseline_processed[container] = float(match[1]) if match else 0
        prior_failure_messages = (await queues())["transactions.failure"].get("messages_ready", 0)
        first = [f"{user}-{number}" for number in range(60)]
        await asyncio.gather(*(submit(transaction_id) for transaction_id in [*first, *first[:20]]))
        expected.update(first)

        async def all_initial():
            return len(await history()) == 60

        await wait_for(all_initial, "initial persistence")

        metrics = {}
        for container in containers:
            rendered = await read_metrics(container)
            match = re.search(
                r'transaction_deliveries_total{outcome="processed"} ([0-9.]+)', rendered
            )
            assert match and float(match[1]) > baseline_processed[container], (
                "every container must process new work in this run"
            )
            metrics[container] = rendered
        (evidence / "per-instance-metrics.json").write_text(json.dumps(metrics))

        # Retry and permanent destinations, while all three replicas are live.
        retry_id, failure_id = f"{user}-retry", f"{user}-permanent"
        await submit(retry_id, "BRL")
        await submit(failure_id, "GBP")
        expected.add(retry_id)

        async def forwarded():
            state = await queues()
            return state["transactions.failure"].get(
                "messages_ready", 0
            ) == prior_failure_messages + 1 and any(
                row.get("messages", 0) > 0
                for name, row in state.items()
                if name.startswith("transactions.retry.")
            )

        await wait_for(forwarded, "confirmed retry and permanent forwarding")
        forwarding = await queues()
        (evidence / "routing-queues.json").write_text(json.dumps(forwarding, indent=2))
        forwarded_metrics = {container: await read_metrics(container) for container in containers}
        for kind, previous in baseline_failures.items():
            assert sum(failures(value, kind) for value in forwarded_metrics.values()) > previous
        (evidence / "forwarding-instance-metrics.json").write_text(json.dumps(forwarded_metrics))
        response = await client.post(f"{provider}/control", json={"brl_available": True})
        response.raise_for_status()

        async def retry_persisted():
            return any(row["id"] == retry_id for row in await history())

        await wait_for(retry_persisted, "provider recovery and retried persistence")

        # Hold real provider calls, stop all consumers, then release only after
        # each process observes Basic.CancelOk and work remains unacknowledged.
        response = await client.post(f"{provider}/control", json={"block": True})
        response.raise_for_status()
        blocked = [f"{user}-shutdown-{number}" for number in range(12)]
        expected.update(blocked)
        await asyncio.gather(*(submit(transaction_id) for transaction_id in blocked))

        async def active_calls():
            response = await client.get(f"{provider}/state")
            return response.json()["active"] >= 3

        await wait_for(active_calls, "blocked real provider requests")
        stop_checkpoint = datetime.now(UTC).isoformat()
        try:
            for container in containers:
                processes.append(
                    subprocess.Popen(
                        ["docker", "stop", "--time", "40", container],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                    )
                )

            async def intake_stopped():
                logs = await asyncio.gather(
                    *(
                        asyncio.to_thread(
                            subprocess.check_output,
                            ["docker", "logs", "--since", stop_checkpoint, container],
                            text=True,
                            stderr=subprocess.STDOUT,
                        )
                        for container in containers
                    )
                )
                # This log is emitted after the real Basic.CancelOk response.
                return all("consumer intake cancelled" in output for output in logs)

            await wait_for(intake_stopped, "SIGTERM stops intake before active work is ACKed", 10)
            assert not any(row["id"] in blocked for row in await history())

            async def observed_unacked():
                state = await queues()
                return state if state["transactions.work"]["messages_unacknowledged"] >= 3 else None

            # Management queue counters are sampled asynchronously, too.
            checkpoint_queues = await wait_for(observed_unacked, "blocked unacked deliveries", 8)
            assert not any(row["id"] in blocked for row in await history())
            (evidence / "shutdown-checkpoint.json").write_text(
                json.dumps(
                    {
                        "checkpoint": "all containers observed Basic.CancelOk",
                        "provider_calls_still_blocked": True,
                        "blocked_ids_persisted": 0,
                        "work_ready": checkpoint_queues["transactions.work"]["messages_ready"],
                        "work_unacked": checkpoint_queues["transactions.work"][
                            "messages_unacknowledged"
                        ],
                    },
                    indent=2,
                )
            )
        finally:
            await client.post(f"{provider}/control", json={"block": False})
            for process in processes:
                assert await asyncio.to_thread(process.wait, timeout=45) == 0
            await asyncio.to_thread(
                subprocess.run, ["docker", "start", *containers], check=True, capture_output=True
            )

        async def completed():
            return {row["id"] for row in await history()} == expected

        await wait_for(completed, "final accepted/persisted reconciliation")
        rows = await history()
        actual = [row["id"] for row in rows]
        assert len(actual) == len(set(actual)) == len(expected)
        assert failure_id not in actual
        assert all(
            row["rate"] == "1.250000000000" and row["converted_usd_amount"] == "3.12500000"
            for row in rows
        )

        async def finished():
            state = await queues()
            return all(
                row.get("messages_ready", 0) == row.get("messages_unacknowledged", 0) == 0
                for name, row in state.items()
                if name != "transactions.failure"
            )

        await wait_for(finished, "work and delay queues drained")
        final = await queues()
        assert final["transactions.failure"]["messages_ready"] == prior_failure_messages + 1
        summary = {
            "competing_containers": len(containers),
            "persisted_distinct_ids": len(actual),
            "expected_success_ids": len(expected),
            "intentional_permanent_failures": [failure_id],
            "prior_failure_messages_preserved": prior_failure_messages,
            "missing": sorted(expected - set(actual)),
            "unexpected": sorted(set(actual) - expected),
            "duplicate_rows": len(actual) - len(set(actual)),
            "graceful_shutdown": ("intake stopped during blocked provider calls; no premature ACK"),
            "final_queues": {
                name: {
                    "ready": row.get("messages_ready", 0),
                    "unacked": row.get("messages_unacknowledged", 0),
                }
                for name, row in final.items()
            },
        }
        (evidence / "summary.json").write_text(json.dumps(summary, indent=2))
        (evidence / "expected-ids.json").write_text(json.dumps(sorted(expected)))
        (evidence / "persisted-ids.json").write_text(json.dumps(actual))
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(run())
