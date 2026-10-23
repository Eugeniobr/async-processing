"""Translate established domain and availability errors at the HTTP boundary."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from application.ports import PermanentRateError, RetryableRateError
from infrastructure.rabbitmq_publisher import PublicationError


def register_error_handlers(app: FastAPI) -> None:
    """Install the contract's compact ``{detail: string}`` error responses."""

    async def validation_error(_: Request, error: Exception) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(error)})

    async def temporary_error(_: Request, error: Exception) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(error)})

    app.add_exception_handler(ValueError, validation_error)
    app.add_exception_handler(PermanentRateError, validation_error)
    app.add_exception_handler(RetryableRateError, temporary_error)
    app.add_exception_handler(PublicationError, temporary_error)
