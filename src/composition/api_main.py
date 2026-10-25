"""Run the HTTP composition root with its explicit broker lifecycle."""

from __future__ import annotations

import asyncio

import uvicorn

from api.app import create_app
from composition.container import build_http_container
from composition.settings import Settings


async def run() -> None:
    """Create the broker-backed application and serve it until shutdown."""

    container = await build_http_container(Settings())
    application = create_app(container)
    server = uvicorn.Server(uvicorn.Config(application, host="0.0.0.0", port=8000))
    broker_failed = False

    def broker_closed(*_args: object) -> None:
        nonlocal broker_failed
        if not server.should_exit:
            broker_failed = True
            server.should_exit = True

    container.connection.close_callbacks.add(broker_closed)
    try:
        await server.serve()
        if broker_failed:
            raise RuntimeError("API broker connection closed; supervisor must restart")
    finally:
        await container.close()


if __name__ == "__main__":
    asyncio.run(run())
