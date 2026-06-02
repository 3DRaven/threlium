"""Парсинг ``X-Threlium-Route`` → типизированный :class:`~threlium.types.IngressRoute`."""
from __future__ import annotations

from typing import TypeVar

from threlium.types import (
    EmailIngressRoute,
    IngressRoute,
    IngressRouteB62Wire,
    MatrixIngressRoute,
    TelegramIngressRoute,
)

T = TypeVar("T", EmailIngressRoute, TelegramIngressRoute, MatrixIngressRoute)


def require_ingress_route(
    route_wire: IngressRouteB62Wire | None,
    route_type: type[T],
) -> T:
    """Wire ``X-Threlium-Route`` → ``route_type``; нет wire / неверный channel → ``RuntimeError``."""
    r = IngressRouteB62Wire.parse_route_from_optional_header(route_wire)
    if r is None:
        raise RuntimeError(
            "FSM-инвариант: письмо из выборки notmuch с X-Threlium-Route "
            "не содержит непустого заголовка X-Threlium-Route"
        )
    if not isinstance(r, route_type):
        raise RuntimeError(
            f"FSM-инвариант: ожидался {route_type.__name__}, "
            f"получен {type(r).__name__} (channel={r.channel!r})"
        )
    return r


__all__ = ["require_ingress_route"]
