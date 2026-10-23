"""Prometheus metrics with explicit processing-only semantics."""

from __future__ import annotations

import os
from pathlib import Path

from prometheus_client import REGISTRY, CollectorRegistry, Counter, generate_latest, multiprocess

MULTIPROC_DIRECTORY_ENV = "PROMETHEUS_MULTIPROC_DIR"


class MetricsRecorder:
    """Count classified processing failures, never publication or acknowledgement outcomes."""

    def __init__(
        self, *, registry: CollectorRegistry = REGISTRY, register_counter: bool = True
    ) -> None:
        self._registry = registry
        self._processing_failures = (
            Counter(
                "transaction_processing_failures",
                "Classified transaction processing failures after processing has been attempted.",
                ("kind",),
                registry=registry,
            )
            if register_counter
            else None
        )

    @classmethod
    def for_scrape(cls) -> MetricsRecorder:
        """Create the API-side registry that aggregates consumer process metric files."""

        directory = cls.require_multiprocess_directory()
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry, path=str(directory))
        return cls(registry=registry, register_counter=False)

    @staticmethod
    def require_multiprocess_directory() -> Path:
        """Validate the directory shared by the API and consumer before process startup."""

        raw_directory = os.getenv(MULTIPROC_DIRECTORY_ENV)
        if not raw_directory:
            raise RuntimeError(f"{MULTIPROC_DIRECTORY_ENV} must be configured before startup")
        directory = Path(raw_directory)
        if not directory.is_dir() or not os.access(directory, os.W_OK):
            raise RuntimeError(f"{MULTIPROC_DIRECTORY_ENV} must name a writable directory")
        return directory

    def record_processing_failure(self, kind: str) -> None:
        """Record a classified retryable or permanent processing failure."""

        if kind not in {"retryable", "permanent"}:
            raise ValueError("kind must be retryable or permanent")
        if self._processing_failures is None:
            raise RuntimeError("the scrape registry cannot record processing failures")
        self._processing_failures.labels(kind=kind).inc()

    def render(self) -> bytes:
        """Return the Prometheus text exposition for the owned registry."""

        return generate_latest(self._registry)
