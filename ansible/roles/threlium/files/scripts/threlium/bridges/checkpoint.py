"""Checkpoint-поля ingress-маршрута из newest ``tag:route from:<bridge>`` в notmuch."""
from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

import notmuch2  # pyright: ignore[reportMissingImports]

import threlium.nm as nm
from threlium.bridges.routing import require_ingress_route
from threlium.types import (
    IngressRoute,
    IngressRouteB62Wire,
    MailHeaderName,
    NotmuchBridgeFromLocalhost,
    NotmuchQueryConnective,
    NotmuchTag,
)

T = TypeVar("T")
R = TypeVar("R", bound=IngressRoute)


def latest_route_checkpoint(
    bridge: NotmuchBridgeFromLocalhost,
    route_type: type[R],
    pick: Callable[[R], T | None],
) -> T | None:
    """``pick(route)`` для самого нового ``tag:route from:<bridge>`` с маршрутом ``route_type``."""
    q = NotmuchQueryConnective.join_and(
        NotmuchTag.ROUTE.as_tag_query_term(),
        bridge.as_from_query_term(),
    )
    with nm.notmuch_database(write=False) as db:
        for nm_msg in db.messages(q, sort=notmuch2.Database.SORT.NEWEST_FIRST):
            route_w = IngressRouteB62Wire.parse_present_from_nm_message(
                nm_msg, MailHeaderName.ROUTE.value
            )
            if route_w is None:
                continue
            route = require_ingress_route(route_w, route_type)
            picked = pick(route)
            if picked is not None:
                return picked
    return None
