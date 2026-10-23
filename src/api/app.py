"""FastAPI application assembly with explicit optional acceptance dependencies."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from inspect import isawaitable
from typing import Protocol

from fastapi import FastAPI

from api.errors import register_error_handlers
from api.routes import (
    UserQueryRepository,
    register_metrics_route,
    register_transaction_routes,
    register_user_routes,
)
from composition.settings import CurrencyAllowlist
from infrastructure.metrics import MetricsRecorder

logger = logging.getLogger(__name__)


class ApplicationResources(Protocol):
    """The lifecycle surface required by the HTTP application."""

    metrics: MetricsRecorder
    repository: UserQueryRepository

    def close(self) -> None: ...


def create_app(resources: ApplicationResources) -> FastAPI:
    """Create an application with explicit resource lifetime and no business routes."""

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        logger.info("application_started", extra={"event": "application_started"})
        try:
            yield
        finally:
            close_result = resources.close()
            if isawaitable(close_result):
                await close_result
            logger.info("application_stopped", extra={"event": "application_stopped"})

    app = FastAPI(lifespan=lifespan)
    register_error_handlers(app)
    register_user_routes(app, resources.repository)
    publisher = getattr(resources, "publisher", None)
    allowlist = getattr(resources, "currency_allowlist", None)
    if publisher is not None and isinstance(allowlist, CurrencyAllowlist):
        register_transaction_routes(
            app, publisher, allowlist, tracer=getattr(resources, "tracer", None)
        )

    register_metrics_route(app, resources.metrics)

    return app
