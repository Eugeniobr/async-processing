"""Actual kind/KEDA scale-out and scale-in evidence; no manual replica changes."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import socket
import subprocess
import time
import uuid
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", default="kind-async-processing-scale")
    parser.add_argument("--namespace", default="async-processing")
    parser.add_argument("--kubectl", default=".runtime/tools/kubectl")
    parser.add_argument("--count", type=int, default=1200)
    parser.add_argument("--deadline", type=int, default=300)
    parser.add_argument("--evidence", default=".runtime/scaling-evidence/autoscaling")
    args = parser.parse_args()
    Path(".runtime").mkdir(exist_ok=True)
    lock = open(".runtime/scaling-demo.lock", "a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("another local scaling demo is still running") from None
    if not args.context.startswith("kind-") or args.namespace in {"default", "kube-system"}:
        raise SystemExit("demo is restricted to an explicitly named local kind namespace")
    evidence = Path(args.evidence)
    evidence.mkdir(parents=True, exist_ok=True)
    root = [args.kubectl, "--context", args.context, "-n", args.namespace]

    def kubectl(*command: str) -> str:
        result = subprocess.run(
            [*root, *command], check=True, text=True, capture_output=True, timeout=210
        )
        return result.stdout

    def resource(kind: str) -> dict:
        return json.loads(kubectl("get", kind, "-o", "json"))

    def diagnostic() -> None:
        for name, command in {
            "pods": ["get", "pods", "-o", "wide"],
            "events": ["get", "events", "--sort-by=.lastTimestamp"],
            "hpa": ["describe", "hpa", "keda-hpa-consumer"],
            "scaler": ["get", "scaledobject", "consumer", "-o", "yaml"],
            "consumer": ["logs", "deployment/consumer", "--tail=100"],
        }.items():
            try:
                (evidence / f"{name}.log").write_text(kubectl(*command))
            except Exception as error:
                (evidence / f"{name}.log").write_text(str(error))

    with ExitStack() as stack:

        def forward(service: str, port: int) -> str:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                local = sock.getsockname()[1]
            log = stack.enter_context((evidence / f"forward-{service}.log").open("w"))
            process = subprocess.Popen(
                [*root, "port-forward", f"service/{service}", f"{local}:{port}"],
                stdout=log,
                stderr=subprocess.STDOUT,
            )

            def stop_forward() -> None:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)

            stack.callback(stop_forward)
            return f"http://127.0.0.1:{local}"

        try:
            kubectl("apply", "-f", "tests/scaling/provider.yaml")
            kubectl("rollout", "status", "deployment/test-rates", "--timeout=120s")
            kubectl(
                "patch",
                "deployment",
                "consumer",
                "--type=strategic",
                "--patch-file",
                "tests/scaling/consumer-patch.yaml",
            )
            kubectl("rollout", "status", "deployment/consumer", "--timeout=120s")
            api = forward("api", 8000)
            broker = forward("rabbitmq", 15672)
            provider = forward("test-rates", 8080)
            prometheus = forward("prometheus", 9090)
            with httpx.Client(timeout=2) as client:
                for base, path in [
                    (api, "/healthz"),
                    (broker, "/api/overview"),
                    (provider, "/healthz"),
                    (prometheus, "/-/ready"),
                ]:
                    ready_deadline = time.monotonic() + 30
                    while True:
                        try:
                            response = client.get(
                                base + path, auth=("async_processing", "async_processing")
                            )
                            response.raise_for_status()
                            break
                        except httpx.HTTPError:
                            if time.monotonic() >= ready_deadline:
                                raise
                            time.sleep(0.1)
            result = asyncio.run(run(args, api, broker, prometheus, resource, evidence))
            diagnostic()
            result["versions"] = {
                "kind": "0.33.0",
                "kubernetes": "1.34.11",
                "keda": "2.21.0",
                "rabbitmq": "4.3.6",
                "postgresql": "16.9",
                "rate_condition": "test-only EUR/USD=1.25, 250ms",
                "concurrency": 4,
                "prefetch": 8,
                "target_messages": 20,
            }
            (evidence / "summary.json").write_text(json.dumps(result, indent=2))
            print(json.dumps(result, indent=2))
            kubectl("set", "env", "deployment/consumer", "FRANKFURTER_BASE_URL-")
            kubectl("rollout", "status", "deployment/consumer", "--timeout=120s")
            kubectl("delete", "-f", "tests/scaling/provider.yaml")
        except BaseException:
            diagnostic()
            print(f"Failure: state and test overlay preserved. Diagnostics: {evidence}")
            raise


async def run(args, api, broker, prometheus, resource, evidence):
    auth = ("async_processing", "async_processing")
    user = "scale-" + uuid.uuid4().hex[:12]
    timestamp = "2024-05-15T10:30:00Z"
    accepted = []
    timeline = []
    metric_peaks = {}
    deadline = time.monotonic() + args.deadline
    started = time.monotonic()
    params = {"from": "2024-05-15T00:00:00Z", "to": "2024-05-16T00:00:00Z", "limit": 100}

    async with httpx.AsyncClient(timeout=15) as client:
        empty = await client.get(f"{api}/users/{user}/summary")
        empty.raise_for_status()
        assert empty.json()["transaction_count"] == 0

        async def snapshot():
            response = await client.get(f"{broker}/api/queues/%2F", auth=auth)
            response.raise_for_status()
            queues = {
                row["name"]: {
                    "ready": row.get("messages_ready", 0),
                    "unacked": row.get("messages_unacknowledged", 0),
                }
                for row in response.json()
            }
            deployment = await asyncio.to_thread(resource, "deployment/consumer")
            hpa = await asyncio.to_thread(resource, "hpa/keda-hpa-consumer")
            metrics = await client.get(
                f"{prometheus}/api/v1/query",
                params={"query": "sum by (outcome) (transaction_deliveries_total)"},
            )
            metrics.raise_for_status()
            for value in metrics.json()["data"]["result"]:
                outcome = value["metric"]["outcome"]
                metric_peaks[outcome] = max(metric_peaks.get(outcome, 0), float(value["value"][1]))
            sample = {
                "utc": datetime.now(UTC).isoformat(),
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "desired": deployment["spec"]["replicas"],
                "current": deployment.get("status", {}).get("replicas", 0),
                "ready": deployment.get("status", {}).get("readyReplicas", 0),
                "hpa_desired": hpa.get("status", {}).get("desiredReplicas"),
                "queues": queues,
            }
            timeline.append(sample)
            with (evidence / "timeline.jsonl").open("a") as output:
                output.write(json.dumps(sample) + "\n")
            print(json.dumps(sample), flush=True)
            return sample

        while True:
            initial = await snapshot()
            if initial["ready"] == initial["current"] == initial["desired"] == 1:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("baseline did not return to one actual consumer")
            await asyncio.sleep(1)

        stop = asyncio.Event()

        async def monitor():
            while not stop.is_set():
                await snapshot()
                try:
                    await asyncio.wait_for(stop.wait(), 2)
                except TimeoutError:
                    pass

        monitor_task = asyncio.create_task(monitor())
        try:
            offered = [f"{user}-{index:05}" for index in range(args.count)]
            offered += offered[:100]  # Concurrent identical duplicate submissions.
            slots = asyncio.Semaphore(50)

            async def submit(event_id):
                async with slots:
                    response = await client.post(
                        f"{api}/transactions",
                        json={
                            "id": event_id,
                            "user_id": user,
                            "amount": "2.50000000",
                            "currency": "EUR",
                            "timestamp": timestamp,
                        },
                    )
                    assert response.status_code == 202, (response.status_code, response.text)
                    accepted.append(event_id)

            await asyncio.gather(*(submit(event_id) for event_id in offered))
            (evidence / "accepted-ids.json").write_text(json.dumps(accepted))
            drain_seconds = None
            while True:
                sample = await snapshot()
                outstanding = sum(
                    row["ready"] + row["unacked"] for row in sample["queues"].values()
                )
                peak = max(row["ready"] for row in timeline)
                completion = await client.get(f"{api}/users/{user}/summary")
                completion.raise_for_status()
                if outstanding == 0 and completion.json()["transaction_count"] == args.count:
                    drain_seconds = drain_seconds or time.monotonic() - started
                if (
                    peak >= 2
                    and drain_seconds is not None
                    and sample["ready"] == sample["current"] == sample["desired"] == 1
                ):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"scale/drain deadline; latest={sample}; observed_peak_ready={peak}"
                    )
                await asyncio.sleep(2)

            rows = []
            for offset in range(0, args.count + 100, 100):
                response = await client.get(
                    f"{api}/users/{user}/transactions", params={**params, "offset": offset}
                )
                response.raise_for_status()
                page = response.json()["items"]
                rows.extend(page)
                if len(page) < 100:
                    break
            expected = set(accepted)
            actual = [row["id"] for row in rows]
            missing = sorted(expected - set(actual))
            unexpected = sorted(set(actual) - expected)
            assert not missing and not unexpected
            assert len(actual) == len(set(actual)) == args.count
            assert all(
                row["rate"] == "1.250000000000"
                and row["converted_usd_amount"] == "3.12500000"
                and row["rate_date"] == "2024-05-15"
                for row in rows
            )
            summary = (await client.get(f"{api}/users/{user}/summary")).json()
            from decimal import Decimal

            assert summary["transaction_count"] == args.count
            assert Decimal(summary["total_usd"]) == Decimal("3.125") * args.count
            assert metric_peaks.get("processed", 0) > 0
            assert metric_peaks.get("duplicate", 0) > 0
            (evidence / "persisted-ids.json").write_text(json.dumps(actual))
            return {
                "user": user,
                "accepted_submissions": len(accepted),
                "distinct_accepted": len(expected),
                "persisted": len(actual),
                "missing": missing,
                "unexpected": unexpected,
                "duplicate_rows": 0,
                "peak_ready_replicas": max(row["ready"] for row in timeline),
                "final_ready_replicas": sample["ready"],
                "drain_seconds": round(drain_seconds, 3),
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "final_queues": sample["queues"],
                "observed_instance_metric_peaks": metric_peaks,
            }
        finally:
            stop.set()
            await monitor_task


if __name__ == "__main__":
    main()
