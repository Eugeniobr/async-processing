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
    try:
        await uvicorn.Server(
            uvicorn.Config(create_app(container), host="0.0.0.0", port=8000)
        ).serve()
    finally:
        await container.close()


if __name__ == "__main__":
    asyncio.run(run())
