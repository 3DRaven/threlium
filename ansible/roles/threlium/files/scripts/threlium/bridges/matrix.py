#!/usr/bin/env python3
"""Matrix /sync мост → ingress@localhost через fdm (matrix-nio).

Курсор инкрементального ``/sync`` (токен ``next_batch`` от homeserver) сохраняется
в поле ``MatrixIngressRoute.sync_batch`` последнего matrix-сообщения в union-notmuch
(`docs/INDEX.md` §1, root = ``stages/``). Сетевой доступ к homeserver — только
``matrix-nio`` (:class:`nio.AsyncClient`).

Сетевые ошибки, ``SyncError``, ответ без непустого ``next_batch`` → исключение;
systemd перезапускает сервис.

---------------------------------------------------------------------------
Этап 0 (конспект API matrix-nio, зафиксированный в коде):

- ``AsyncClient(homeserver, user="")``: ``homeserver`` — полный URL (``https://…``).
  Сессия по токену: ``client.access_token``, ``client.user_id`` (полный MXID), без
  ``login()`` / без записи сессии на диск.
- ``await client.sync(timeout=60000)``: ``timeout`` в миллисекундах; при успехе —
  ``SyncResponse`` с ``next_batch`` и ``rooms.join[room_id].timeline.events``.
  Продолжение: до вызова выставить ``client.next_batch`` из notmuch (поле
  ``sync_batch`` в маршруте) либо полагаться на внутреннее состояние клиента после
  первого успешного sync.
- Состояние ``m.room.name`` в ``rooms.join[*].state`` → заголовок ``Subject:`` bridge→ingress
  (аналог темы треда; wire :class:`~threlium.types.bridges.MatrixRoomNameWire` →
  :class:`~threlium.types.rfc.RfcSubjectWire`).
- События ``m.room.message`` парсятся в подклассы ``nio.events.room_events.RoomMessage``
  (напр. ``RoomMessageText``). У этих dataclass **нет** отдельных полей под
  ``m.relates_to`` / reply — matrix-nio кладёт полный JSON в ``Event.source``;
  предок для reply читается оттуда (см. :func:`reply_parent_event_id_from_room_message`).
- Ошибка sync: ``SyncError`` (подтип ``ErrorResponse``), не успешный ``SyncResponse``.
---------------------------------------------------------------------------
"""
from __future__ import annotations

import asyncio
import msgspec
import notmuch2  # pyright: ignore[reportMissingImports]
import sys
import time
from collections.abc import Callable, Mapping
from email.message import EmailMessage
from pathlib import Path

import threlium.nm as nm
from nio import AsyncClient
from nio.events.room_events import RoomMessage, RoomNameEvent
from nio.responses import SyncError, SyncResponse

from threlium.bridges import (
    BridgeInReplyTo,
    build_bridge_ingress_email,
    matrix_room_name_to_ingress_subject_wire,
)
from threlium.bridges.notmuch_space_anchor import resolve_bridge_tail_mid_for_space
from threlium.logutil import logger
from threlium.settings import ThreliumSettings
from threlium.types import (
    IngressRouteB62Wire,
    MailHeaderName,
    MatrixIngressRoute,
    MatrixNativeId,
    MatrixRoomEventId,
    MatrixRoomId,
    MatrixRoomNameWire,
    MatrixSyncBatchCursor,
    NotmuchBridgeFromLocalhost,
    NotmuchMessageIdInner,
    NotmuchQueryConnective,
    NotmuchTag,
    RfcMessageIdWire,
    ThreliumSpaceB62Wire,
    matrix_homeserver_url,
    matrix_space_from_room_id,
)
from threlium.systemd_notify import notify_status
from threlium.types.systemd_status import SystemdStatusBody

log = logger.bind(stage="bridge_matrix")


def reply_parent_event_id_from_room_message(ev: RoomMessage) -> MatrixRoomEventId | None:
    """``event_id`` предка для Matrix-reply из события matrix-nio.

    В matrix-nio у :class:`~nio.events.room_events.RoomMessage` и подклассов
    (``RoomMessageText`` и др.) **нет** полей dataclass для ``m.relates_to`` /
    ``m.in_reply_to`` — парсер заполняет только ``body``/``msgtype``/медиа-поля,
    остальное остаётся в :attr:`~nio.events.room_events.Event.source`.
    Читаем стандартный путь Matrix Client-Server в ``source`` после того, как
    событие уже прошло ``RoomMessage.parse_event`` / ``from_dict``.
    """
    return _reply_parent_event_id_from_message_source(ev.source)


def _reply_parent_event_id_from_message_source(
    source: Mapping[str, object],
) -> MatrixRoomEventId | None:
    content = source.get("content")
    if not isinstance(content, dict):
        return None
    rel = content.get("m.relates_to")
    if not isinstance(rel, dict):
        return None
    irt = rel.get("m.in_reply_to")
    if not isinstance(irt, dict):
        return None
    eid = irt.get("event_id")
    if isinstance(eid, str) and eid.strip():
        return MatrixRoomEventId(eid.strip())
    return None


def matrix_room_message_bridge_in_reply_to(
    *,
    room_id: MatrixRoomId,
    parent_event_id: MatrixRoomEventId | None,
    db: notmuch2.Database,
) -> BridgeInReplyTo:
    """IRT для события комнаты: явный родитель (reply) или fallback по якорю Space."""
    if parent_event_id is not None:
        parent_native = MatrixNativeId(
            v=1, room_id=room_id, event_id=parent_event_id
        )
        return RfcMessageIdWire.from_native(parent_native)
    sw = ThreliumSpaceB62Wire.from_threlium_space(matrix_space_from_room_id(room_id))
    return resolve_bridge_tail_mid_for_space(
        db, bridge=NotmuchBridgeFromLocalhost.MATRIX, space_wire=sw,
    )


def _parse_matrix_routing(route_wire: IngressRouteB62Wire | None) -> MatrixIngressRoute:
    """Wire ``X-Threlium-Route`` → ``MatrixIngressRoute`` (строго: нет wire / не matrix → ошибка FSM)."""
    r = IngressRouteB62Wire.parse_route_from_optional_header(route_wire)
    if r is None:
        raise RuntimeError(
            "FSM-инвариант: письмо из выборки notmuch с X-Threlium-Route не содержит "
            "непустого заголовка X-Threlium-Route"
        )
    if not isinstance(r, MatrixIngressRoute):
        raise RuntimeError(
            f"FSM-инвариант: ожидался MatrixIngressRoute, получен {type(r).__name__} (channel={r.channel!r})"
        )
    return r


def _sync_since_from_index(thome: Path) -> MatrixSyncBatchCursor | None:
    """Токен ``since`` для ``/sync``: ``next_batch`` из самого нового ``tag:route from:matrix`` письма."""
    q = NotmuchQueryConnective.join_and(
        NotmuchTag.ROUTE.as_tag_query_term(),
        NotmuchBridgeFromLocalhost.MATRIX.as_from_query_term(),
    )
    with nm.notmuch_database(write=False) as db:
        for nm_msg in db.messages(q, sort=notmuch2.Database.SORT.NEWEST_FIRST):
            route_w = IngressRouteB62Wire.parse_present_from_nm_message(
                nm_msg, MailHeaderName.ROUTE.value
            )
            if route_w is None:
                continue
            info = _parse_matrix_routing(route_w)
            if info.sync_batch and str(info.sync_batch).strip():
                return MatrixSyncBatchCursor(str(info.sync_batch).strip())
    return None


def _require_next_batch_from_sync(resp: SyncResponse) -> str:
    nb = resp.next_batch
    if not isinstance(nb, str) or not nb.strip():
        raise RuntimeError(
            "FSM-инвариант: ответ /sync без непустого строкового next_batch "
            f"(получено {nb!r})"
        )
    return nb.strip()


def matrix_room_name_wire_from_sync_state_events(
    state_events: list,
) -> MatrixRoomNameWire | None:
    """Последнее непустое ``m.room.name`` в списке state из ответа ``/sync`` (matrix-nio)."""
    last: MatrixRoomNameWire | None = None
    for ev in state_events:
        if not isinstance(ev, RoomNameEvent):
            continue
        w = MatrixRoomNameWire.parse_present_optional(ev.name)
        if w is not None:
            last = w
    return last


def _room_message_plain_body(ev: RoomMessage) -> str:
    content = ev.source.get("content")
    if isinstance(content, dict):
        b = content.get("body")
        if isinstance(b, str):
            return b
    return ""


async def _matrix_ingress_loop(
    deliver: Callable[[EmailMessage], None],
    thome_p: Path,
    homeserver: str,
    access_token: str,
    user_id: str,
) -> None:
    client = AsyncClient(homeserver, user="")
    client.access_token = access_token
    client.user_id = user_id
    since = _sync_since_from_index(thome_p)
    if since:
        client.next_batch = since
    sync_ok_logged = False
    try:
        while True:
            resp = await client.sync(timeout=60_000)
            if isinstance(resp, SyncError):
                raise RuntimeError(f"FSM-инвариант: Matrix sync error: {resp!s}")
            if not isinstance(resp, SyncResponse):
                raise RuntimeError(f"FSM-инвариант: неожиданный тип ответа sync: {type(resp).__name__}")
            checkpoint = _require_next_batch_from_sync(resp)
            if not sync_ok_logged:
                notify_status(SystemdStatusBody.bridge_matrix_connected_idle())
                sync_ok_logged = True
            if resp.rooms.join:
                candidate_mids: set[NotmuchMessageIdInner] = set()
                for room_id_raw, room_info in resp.rooms.join.items():
                    room_id = MatrixRoomId(room_id_raw)
                    for ev in room_info.timeline.events:
                        if not isinstance(ev, RoomMessage):
                            continue
                        ev_id = MatrixRoomEventId(ev.event_id)
                        native = MatrixNativeId(v=1, room_id=room_id, event_id=ev_id)
                        candidate_mids.add(
                            NotmuchMessageIdInner.from_present_wire(
                                RfcMessageIdWire.from_native(native)
                            )
                        )

                known_mids: set[NotmuchMessageIdInner] = set()
                with nm.notmuch_database(write=False) as db:
                    for mid_nm in candidate_mids:
                        if nm.notmuch_index_has_message_id_in_db(db, mid_nm):
                            known_mids.add(mid_nm)

                for room_id_raw, room_info in resp.rooms.join.items():
                    room_id = MatrixRoomId(room_id_raw)
                    subj_w = matrix_room_name_to_ingress_subject_wire(
                        matrix_room_name_wire_from_sync_state_events(room_info.state)
                    )
                    for ev in room_info.timeline.events:
                        if not isinstance(ev, RoomMessage):
                            continue
                        body = _room_message_plain_body(ev)
                        if not body.strip():
                            continue
                        ev_id = MatrixRoomEventId(ev.event_id)
                        parent_eid = reply_parent_event_id_from_room_message(ev)
                        native = MatrixNativeId(v=1, room_id=room_id, event_id=ev_id)
                        mid_wire = RfcMessageIdWire.from_native(native)
                        mid_nm = NotmuchMessageIdInner.from_present_wire(mid_wire)
                        if mid_nm in known_mids:
                            log.info("duplicate_skip", room_id=room_id, event_id=ev_id)
                            continue
                        mid = mid_wire.value
                        with nm.notmuch_database(write=False) as db:
                            irt = matrix_room_message_bridge_in_reply_to(
                                room_id=room_id,
                                parent_event_id=parent_eid,
                                db=db,
                            )
                        route = MatrixIngressRoute(
                            channel="matrix",
                            v=1,
                            room_id=room_id,
                            event_id=ev_id,
                            sync_batch=MatrixSyncBatchCursor(checkpoint),
                            reply_to_event_id=parent_eid,
                        )
                        raw_obj: dict[str, object] = {
                            "route": msgspec.to_builtins(route),
                            "body": body,
                            "room_id": room_id,
                            "event_id": ev_id,
                        }
                        if parent_eid is not None:
                            raw_obj["reply_to_event_id"] = str(parent_eid)
                        raw_capture = msgspec.json.encode(raw_obj).decode("utf-8")
                        sw = ThreliumSpaceB62Wire.from_threlium_space(
                            matrix_space_from_room_id(room_id)
                        )
                        msg = build_bridge_ingress_email(
                            channel="matrix",
                            body=body,
                            route=route,
                            message_id=mid,
                            in_reply_to=irt,
                            subject=subj_w,
                            raw_capture=raw_capture,
                            space_wire=sw,
                        )
                        notify_status(
                            SystemdStatusBody.bridge_matrix_delivering_room(room_id=room_id)
                        )
                        deliver(msg)
            notify_status(SystemdStatusBody.bridge_matrix_connected_idle())
            time.sleep(0)
    finally:
        await client.close()


def run_bridge(deliver: Callable[[EmailMessage], None], *, settings: ThreliumSettings) -> None:
    thome = str(settings.home)
    matrix_cfg = settings.bridges.matrix
    hs_raw = matrix_cfg.homeserver
    tok = matrix_cfg.token
    mxid = matrix_cfg.user
    if not thome or not hs_raw or not tok or not mxid:
        log.error("required_settings_missing")
        sys.exit(1)
    homeserver = matrix_homeserver_url(hs_raw)
    thome_p = Path(thome)
    asyncio.run(_matrix_ingress_loop(deliver, thome_p, homeserver, tok, mxid))
