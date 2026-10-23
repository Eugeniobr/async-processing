"""Small, optional OpenTelemetry boundary for local transaction tracing.

Tracing is deliberately observational: callers never receive exporter failures and
business delivery semantics must not depend on a span being recorded.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol

from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.trace import Span

logger = logging.getLogger(__name__)

_transaction_id: ContextVar[str | None] = ContextVar("transaction_id", default=None)


class TraceSpan(Protocol):
    """A deliberately narrow span surface for infrastructure adapters."""

    def set_outcome(self, outcome: str) -> None: ...


class TransactionTracer(Protocol):
    """Carry only the approved transaction correlation metadata."""

    @contextmanager
    def span(
        self,
        name: str,
        *,
        stage: str,
        transaction_id: str | None = None,
        attempt: int | None = None,
        parent_context: Context | None = None,
    ) -> Iterator[TraceSpan]: ...

    def inject(self, carrier: MutableMapping[str, str]) -> None: ...

    def extract(self, carrier: Mapping[str, object] | None) -> Context | None: ...

    def close(self) -> None: ...

    def flush(self) -> None: ...


@dataclass(frozen=True)
class _NoopTraceSpan:
    """Keep tracing optional without conditionals throughout business adapters."""

    def set_outcome(self, outcome: str) -> None:
        del outcome


class NoopTransactionTracer:
    """Default tracer used when local tracing is not configured."""

    @contextmanager
    def span(
        self,
        name: str,
        *,
        stage: str,
        transaction_id: str | None = None,
        attempt: int | None = None,
        parent_context: Context | None = None,
    ) -> Iterator[TraceSpan]:
        del name, stage, transaction_id, attempt, parent_context
        yield _NoopTraceSpan()

    def inject(self, carrier: MutableMapping[str, str]) -> None:
        del carrier

    def extract(self, carrier: Mapping[str, object] | None) -> Context | None:
        del carrier
        return None

    def close(self) -> None:
        """No process-local exporter exists for the no-op implementation."""

    def flush(self) -> None:
        """No process-local exporter exists for the no-op implementation."""


class OpenTelemetryTransactionTracer:
    """Create scoped local spans and propagate their W3C parent context via AMQP."""

    def __init__(self, tracer: trace.Tracer, provider: object) -> None:
        self._tracer = tracer
        self._provider = provider

    @contextmanager
    def span(
        self,
        name: str,
        *,
        stage: str,
        transaction_id: str | None = None,
        attempt: int | None = None,
        parent_context: Context | None = None,
    ) -> Iterator[TraceSpan]:
        correlated_id = transaction_id or _transaction_id.get()
        attributes: dict[str, str | int] = {"stage": stage}
        if correlated_id is not None:
            attributes["transaction_id"] = correlated_id
        if attempt is not None:
            attributes["attempt"] = attempt
        span = self._tracer.start_span(name, context=parent_context, attributes=attributes)
        token = _transaction_id.set(correlated_id)
        try:
            with trace.use_span(span, end_on_exit=True):
                yield _OpenTelemetryTraceSpan(span)
        except Exception as error:
            span.record_exception(error)
            span.set_status(trace.Status(trace.StatusCode.ERROR))
            raise
        finally:
            _transaction_id.reset(token)

    def inject(self, carrier: MutableMapping[str, str]) -> None:
        propagate.inject(carrier)

    def extract(self, carrier: Mapping[str, object] | None) -> Context | None:
        if carrier is None:
            return None
        normalized = {str(key): str(value) for key, value in carrier.items()}
        return propagate.extract(normalized)

    def close(self) -> None:
        """Flush local observations without affecting transaction completion."""

        try:
            self._provider.shutdown()  # type: ignore[union-attr]
        except Exception:
            logger.exception("local_transaction_tracing_shutdown_failed")

    def flush(self) -> None:
        """Make test and development inspection deterministic without changing delivery."""

        try:
            self._provider.force_flush()  # type: ignore[union-attr]
        except Exception:
            logger.exception("local_transaction_tracing_flush_failed")


@dataclass(frozen=True)
class _OpenTelemetryTraceSpan:
    """Expose only outcome mutation; attributes remain deliberately low-cardinality."""

    span: Span

    def set_outcome(self, outcome: str) -> None:
        self.span.set_attribute("outcome", outcome)


def build_local_tracer(endpoint: str | None, *, service_name: str) -> TransactionTracer:
    """Build OTLP tracing only when explicitly enabled for local development.

    Configuration/setup failures degrade to a no-op tracer.  The exporter runs in
    a batch worker, so a later collector outage cannot affect caller control flow.
    """

    if not endpoint:
        return NoopTransactionTracer()
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
        return OpenTelemetryTransactionTracer(provider.get_tracer("async-processing"), provider)
    except Exception:
        logger.exception("local_transaction_tracing_disabled")
        return NoopTransactionTracer()


__all__ = [
    "NoopTransactionTracer",
    "OpenTelemetryTransactionTracer",
    "TraceSpan",
    "TransactionTracer",
    "build_local_tracer",
]
