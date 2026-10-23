"""Process-boundary verification for the Prometheus multiprocess scrape design."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _run(code: str, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path.cwd(),
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def test_api_scrape_process_aggregates_confirmed_consumer_failures_from_shared_directory(
    tmp_path: Path,
) -> None:
    """An API registry sees consumer data through files, never a shared Python object."""

    metric_directory = tmp_path / "prometheus"
    metric_directory.mkdir()
    environment = {
        **os.environ,
        "PROMETHEUS_MULTIPROC_DIR": str(metric_directory),
        "DATABASE_URL": "postgresql+psycopg://async_processing:async_processing@localhost:5433/async_processing",
        "RABBITMQ_URL": "amqp://async_processing:async_processing@localhost:5672/",
        "CURRENCY_ALLOWLIST_PATH": "config/currencies-v1.json",
    }
    worker = _run(
        "import asyncio\n"
        "import json\n"
        "from domain.outcomes import PermanentFailure, RetryableFailure\n"
        "from infrastructure.metrics import MetricsRecorder\n"
        "from infrastructure.rabbitmq_consumer import TransactionConsumer\n"
        "body = json.dumps({'schema_version': 1, 'event': {'transaction_id': 'metric-id', "
        "'user_id': 'metric-user', 'original_amount': '1.00000000', 'currency': 'EUR', "
        "'event_timestamp': '2026-10-22T10:30:00+00:00', "
        "'currency_allowlist_version': 'currencies-v1'}, 'attempt': 0, "
        "'accepted_at': '2026-10-22T10:31:00+00:00', "
        "'currency_allowlist_version': 'currencies-v1', 'last_failure_kind': None}).encode()\n"
        "class Delivery:\n"
        "    def __init__(self): self.body = body\n"
        "    async def ack(self): pass\n"
        "class Processor:\n"
        "    def __init__(self, outcome): self.outcome = outcome\n"
        "    def process(self, event): return self.outcome\n"
        "class Publisher:\n"
        "    async def publish_retry(self, envelope, delay): pass\n"
        "    async def publish_failure(self, envelope, reason): pass\n"
        "    async def publish_invalid_envelope(self, raw, reason): pass\n"
        "metrics = MetricsRecorder()\n"
        "asyncio.run(TransactionConsumer(Processor(RetryableFailure('offline')), Publisher(), "
        "metrics=metrics).handle(Delivery()))\n"
        "asyncio.run(TransactionConsumer(Processor(PermanentFailure('invalid')), Publisher(), "
        "metrics=metrics).handle(Delivery()))\n",
        environment,
    )
    assert worker.returncode == 0, worker.stderr

    scrape = _run(
        "from fastapi.testclient import TestClient\n"
        "from api.app import create_app\n"
        "from composition.container import build_api_container\n"
        "from composition.settings import Settings\n"
        "container = build_api_container(Settings())\n"
        "with TestClient(create_app(container)) as client:\n"
        "    response = client.get('/metrics')\n"
        "    print(response.status_code)\n"
        "    print(response.text)\n",
        environment,
    )
    assert scrape.returncode == 0, scrape.stderr
    assert "200" in scrape.stdout
    assert 'transaction_processing_failures_total{kind="retryable"} 1.0' in scrape.stdout
    assert 'transaction_processing_failures_total{kind="permanent"} 1.0' in scrape.stdout
