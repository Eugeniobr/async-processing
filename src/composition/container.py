"""Explicit application assembly and ownership of process-local I/O resources."""

from __future__ import annotations

from dataclasses import dataclass

import aio_pika
import httpx
from prometheus_client import CollectorRegistry
from sqlalchemy import Engine, create_engine

from application.money_converter import MoneyConverter
from application.transaction_processor import TransactionProcessor
from composition.settings import CurrencyAllowlist, Settings, load_currency_allowlist
from infrastructure.frankfurter_ecb import FrankfurterEcbRateProvider
from infrastructure.metrics import MetricsRecorder
from infrastructure.postgres_repository import PostgresTransactionRepository
from infrastructure.rabbitmq_publisher import TransactionPublisher
from infrastructure.tracing import TransactionTracer, build_local_tracer


@dataclass
class ApplicationContainer:
    """Own the shared clients assembled for the HTTP process lifetime."""

    settings: Settings
    currency_allowlist: CurrencyAllowlist
    rate_client: httpx.Client
    database_engine: Engine
    repository: PostgresTransactionRepository
    processor: TransactionProcessor
    metrics: MetricsRecorder
    tracer: TransactionTracer

    def close(self) -> None:
        """Close resources this container created once application shutdown begins."""

        self.rate_client.close()
        self.database_engine.dispose()
        self.tracer.close()


@dataclass
class HttpApplicationContainer:
    """HTTP resources including the connection that owns confirmed publication."""

    application: ApplicationContainer
    connection: aio_pika.abc.AbstractConnection
    publisher: TransactionPublisher

    @property
    def metrics(self) -> MetricsRecorder:
        return self.application.metrics

    @property
    def repository(self) -> PostgresTransactionRepository:
        return self.application.repository

    @property
    def currency_allowlist(self) -> CurrencyAllowlist:
        return self.application.currency_allowlist

    @property
    def tracer(self) -> TransactionTracer:
        return self.application.tracer

    async def close(self) -> None:
        """Close the broker connection before disposing process-local I/O."""

        await self.connection.close()
        self.application.close()


def build_container(settings: Settings) -> ApplicationContainer:
    """Build the consumer's constructor-injected components and local recorder."""

    return _build_container(
        settings,
        MetricsRecorder(registry=CollectorRegistry()),
        trace_service_name="async-processing-consumer",
    )


def build_api_container(settings: Settings) -> ApplicationContainer:
    """Build the HTTP process with the registry that aggregates consumer observations."""

    return _build_container(
        settings, MetricsRecorder.for_scrape(), trace_service_name="async-processing-api"
    )


async def build_http_container(settings: Settings) -> HttpApplicationContainer:
    """Assemble the API's query and confirmed-publish dependencies explicitly."""

    application = build_api_container(settings)
    try:
        connection = await aio_pika.connect(settings.rabbitmq_url)
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        publisher = TransactionPublisher(
            await channel.get_exchange("transactions"),
            await channel.get_exchange("transactions.retry"),
            confirm_timeout_seconds=5,
            tracer=application.tracer,
        )
        return HttpApplicationContainer(application, connection, publisher)
    except Exception:
        application.close()
        raise


def _build_container(
    settings: Settings, metrics: MetricsRecorder, *, trace_service_name: str
) -> ApplicationContainer:
    """Build shared dependencies while the caller explicitly chooses metric lifecycle."""

    tracer = build_local_tracer(settings.otlp_traces_endpoint, service_name=trace_service_name)
    rate_client = httpx.Client()
    database_engine = create_engine(settings.database_url)
    try:
        currency_allowlist = load_currency_allowlist(settings.currency_allowlist_path)
        provider = FrankfurterEcbRateProvider(
            rate_client,
            base_url=str(settings.frankfurter_base_url),
            tracer=tracer,
        )
        repository = PostgresTransactionRepository(database_engine, tracer=tracer)
        processor = TransactionProcessor(provider, MoneyConverter(), repository)
        return ApplicationContainer(
            settings=settings,
            currency_allowlist=currency_allowlist,
            rate_client=rate_client,
            database_engine=database_engine,
            repository=repository,
            processor=processor,
            metrics=metrics,
            tracer=tracer,
        )
    except Exception:
        rate_client.close()
        database_engine.dispose()
        tracer.close()
        raise
