"""``run_bridge`` — синхронный контракт моста (как email/tg/mx); внутри поднимает uvicorn.

``deliver`` (= ``run_fdm`` через ingress-письмо) замыкается в app-state; HTTP-обработчики
вызывают его через ``anyio.to_thread.run_sync``. Graceful-shutdown — ``timeout_graceful_shutdown``.
"""
from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from email.message import EmailMessage

import uvicorn

from threlium.logutil import logger
from threlium.settings import ThreliumSettings

from .server import build_app

log = logger.bind(component="isomorph")


def run_bridge(deliver: Callable[[EmailMessage], None], *, settings: ThreliumSettings) -> None:
    iso = settings.bridges.isomorph
    if not iso.api_key.strip():
        log.error("api_key_required")
        sys.exit(1)

    verbose = str(getattr(settings, "log_level", "")).upper() == "DEBUG"
    app = build_app(deliver, settings=settings, verbose=verbose)

    config = uvicorn.Config(
        app,
        host=iso.listen_host,
        port=iso.listen_port,
        timeout_graceful_shutdown=iso.graceful_shutdown_sec,
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config)
    log.info("serving", host=iso.listen_host, port=iso.listen_port,
             surfaces=list(iso.enabled_surfaces), verbose=verbose)
    asyncio.run(server.serve())
