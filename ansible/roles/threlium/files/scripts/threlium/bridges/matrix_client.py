"""Shared Matrix ``AsyncClient`` lifecycle (bridge ingress + egress)."""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from nio import AsyncClient, AsyncClientConfig

from threlium.settings import ThreliumSettings
from threlium.types import matrix_homeserver_url


@asynccontextmanager
async def matrix_client(
    settings: ThreliumSettings,
    *,
    correlation_headers: dict[str, str] | None = None,
) -> AsyncIterator[AsyncClient]:
    """Token-auth ``AsyncClient``; закрывает сессию в ``finally``."""
    matrix_cfg = settings.bridges.matrix
    hs_raw = matrix_cfg.homeserver
    tok = matrix_cfg.token
    mxid = matrix_cfg.user
    if not hs_raw or not tok or not mxid:
        raise RuntimeError("matrix_client: homeserver, token and user are required")
    base = matrix_homeserver_url(hs_raw)
    cfg = AsyncClientConfig(custom_headers=correlation_headers) if correlation_headers else None
    client = AsyncClient(base, user="", config=cfg) if cfg else AsyncClient(base, user="")
    client.access_token = tok
    client.user_id = mxid
    try:
        yield client
    finally:
        await client.close()
