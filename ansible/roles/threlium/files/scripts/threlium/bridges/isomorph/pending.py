"""``IsomorphPendingRegistry`` — long-hold ожидание push, без коллизий идентичных запросов.

Коррелятор треда — контент-адресуемый ``ingress_mid`` (``Message-ID`` ингресса = hash хвоста). Но один
``ingress_mid`` может соответствовать НЕСКОЛЬКИМ одновременным коннектам (идентичные тела двух клиентов →
один и тот же MID → один FSM-пайплайн (notmuch дедупит по Message-ID) → один ответ). Поэтому бакет —
СПИСОК ожидающих, у каждого свой ``request_id`` (per-connection identity); регистрация НЕ затирает соседа
(как было при ``dict[mid] = pending``), а резолв из ``/internal/v1/push`` раздаёт ОДИН ответ broadcast'ом
ВСЕМ ожидающим этого ``ingress_mid`` (идемпотентно: идентичный запрос → идентичный ответ). Снятие — по
конкретному future (disconnect/timeout своего коннекта), не задевая других ждущих тот же MID.
"""
from __future__ import annotations

import asyncio
import uuid

from threlium.logutil import logger
from .push_types import IsomorphBridgePushPayload

_log = logger.bind(stage="isomorph_bridge")


class _Pending:
    __slots__ = ("future", "api_surface", "stream", "request_id")

    def __init__(
        self,
        future: "asyncio.Future[IsomorphBridgePushPayload]",
        api_surface: str,
        stream: bool,
        request_id: str,
    ) -> None:
        self.future = future
        self.api_surface = api_surface
        self.stream = stream
        self.request_id = request_id


class IsomorphPendingRegistry:
    def __init__(self) -> None:
        self._by_id: dict[str, list[_Pending]] = {}

    def register(
        self, ingress_mid: str, *, api_surface: str, stream: bool
    ) -> "asyncio.Future[IsomorphBridgePushPayload]":
        loop = asyncio.get_running_loop()
        fut: "asyncio.Future[IsomorphBridgePushPayload]" = loop.create_future()
        rid = uuid.uuid4().hex[:12]
        bucket = self._by_id.setdefault(ingress_mid, [])
        bucket.append(_Pending(fut, api_surface, stream, rid))
        _log.debug(
            "isomorph_hold_register",
            mid_tail=ingress_mid[-20:],
            request_id=rid,
            waiters=len(bucket),
        )
        return fut

    def resolve(self, payload: IsomorphBridgePushPayload) -> bool:
        """Broadcast ответа ВСЕМ ожидающим ``payload.ingress_mid``. ``True`` если доставлен хоть одному."""
        entries = self._by_id.pop(payload.ingress_mid, None)
        if not entries:
            _log.debug("isomorph_hold_resolve_miss", push_mid_tail=payload.ingress_mid[-20:])
            return False
        delivered = 0
        for entry in entries:
            if not entry.future.done():
                entry.future.set_result(payload)
                delivered += 1
        _log.debug(
            "isomorph_hold_resolve",
            push_mid_tail=payload.ingress_mid[-20:],
            delivered=delivered,
            waiters=len(entries),
        )
        return delivered > 0

    def discard(self, ingress_mid: str, fut: "asyncio.Future[IsomorphBridgePushPayload]") -> None:
        """Снять СВОЙ pending (disconnect/timeout); поздний push станет no-op для него, не задев соседей."""
        bucket = self._by_id.get(ingress_mid)
        if not bucket:
            return
        for entry in list(bucket):
            if entry.future is fut:
                bucket.remove(entry)
                if not entry.future.done():
                    entry.future.cancel()
        if not bucket:
            self._by_id.pop(ingress_mid, None)

    def forget(self, ingress_mid: str, fut: "asyncio.Future[IsomorphBridgePushPayload]") -> None:
        """Убрать СВОЮ запись без отмены (после успешной отдачи), оставив прочих ждущих тот же MID."""
        bucket = self._by_id.get(ingress_mid)
        if not bucket:
            return
        kept = [e for e in bucket if e.future is not fut]
        if kept:
            self._by_id[ingress_mid] = kept
        else:
            self._by_id.pop(ingress_mid, None)
