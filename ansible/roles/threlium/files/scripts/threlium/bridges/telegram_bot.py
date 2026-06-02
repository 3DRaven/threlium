"""Shared Telegram Bot API client kwargs / context (bridge poll + egress)."""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from telegram import Bot as TelegramBot  # type: ignore[import-untyped]
from telegram.request import HTTPXRequest  # type: ignore[import-untyped]

from threlium.bridges.telegram import telegram_token
from threlium.settings import ThreliumSettings


def telegram_bot_kwargs(
    settings: ThreliumSettings,
    *,
    correlation_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """``TelegramBot(token, **kwargs)`` для PTB v21+."""
    token_val = telegram_token(settings)
    kw: dict[str, Any] = {}
    api_base = settings.bridges.telegram.bot_api_base
    if api_base:
        kw["base_url"] = api_base
    if correlation_headers:
        kw["request"] = HTTPXRequest(httpx_kwargs={"headers": correlation_headers})
    return {"token": token_val, **kw}


@asynccontextmanager
async def telegram_bot(
    settings: ThreliumSettings,
    *,
    correlation_headers: dict[str, str] | None = None,
) -> AsyncIterator[TelegramBot]:
    """Async context manager вокруг ``TelegramBot``."""
    kw = telegram_bot_kwargs(settings, correlation_headers=correlation_headers)
    async with TelegramBot(**kw) as bot:
        yield bot
