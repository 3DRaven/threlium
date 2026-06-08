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
from collections.abc import Callable
from email.message import EmailMessage

import threlium.nm as nm
from nio import AsyncClient
from nio.events.room_events import RoomMessage, RoomNameEvent
from nio.responses import SyncError, SyncResponse

from threlium.bridges import (
    BridgeInReplyTo,
    build_bridge_ingress_email,
    matrix_room_name_to_ingress_subject_wire,
)
from threlium.bridges.checkpoint import latest_route_checkpoint
from threlium.bridges.dedup import filter_known_message_ids_in_db
from threlium.bridges.notmuch_space_anchor import (
    SPACE_SETTLE_TIMEOUT_SEC,
    resolve_bridge_tail_mid_for_space,
    space_thread_settled_read,
)
from threlium.logutil import logger
from threlium.settings import ThreliumSettings
from threlium.types import (
    BridgeIngressChannel,
    MatrixIngressRoute,
    MatrixNativeId,
    MatrixRoomEventId,
    MatrixRoomId,
    MatrixRoomNameWire,
    MatrixSyncBatchCursor,
    NotmuchBridgeFromLocalhost,
    NotmuchMessageIdInner,
    RfcMessageIdWire,
    ThreliumSpaceB62Wire,
    matrix_homeserver_url,
    matrix_space_from_room_id,
    reply_parent_event_id_from_message_source,
)
from threlium.types.matrix_client_room_message import MatrixInboundRoomMessageSourceContent
from threlium.systemd_notify import notify_status
from threlium.types.systemd_status import SystemdStatusBody

log = logger.bind(stage="bridge_matrix")


def reply_parent_event_id_from_room_message(ev: RoomMessage) -> MatrixRoomEventId | None:
    """``event_id`` предка для Matrix-reply из события matrix-nio."""
    return reply_parent_event_id_from_message_source(ev.source)


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


@nm.read_retry
def _filter_known_message_ids(
    candidate_mids: set[NotmuchMessageIdInner],
) -> set[NotmuchMessageIdInner]:
    """dedup-проверка в коротком READ-сеансе (``@nm.read_retry``, reopen-on-modified)."""
    with nm.notmuch_database(write=False) as db:
        return filter_known_message_ids_in_db(db, candidate_mids)


@nm.read_retry
def _bridge_in_reply_to_for_room_event(
    *, room_id: MatrixRoomId, parent_event_id: MatrixRoomEventId | None
) -> BridgeInReplyTo:
    """Материализовать ``BridgeInReplyTo`` (VO) в коротком READ-сеансе (``@nm.read_retry``)."""
    with nm.notmuch_database(write=False) as db:
        return matrix_room_message_bridge_in_reply_to(
            room_id=room_id, parent_event_id=parent_event_id, db=db
        )


def _matrix_space_key(room_id: MatrixRoomId) -> str:
    """Ключ пространства комнаты — для per-space сериализации ingest (FIFO + serial-per-thread)."""
    return ThreliumSpaceB62Wire.from_threlium_space(matrix_space_from_room_id(room_id)).value


def _matrix_space_settled(room_id: MatrixRoomId, parent_eid: MatrixRoomEventId | None) -> bool:
    """Можно ли ingest'ить событие СЕЙЧАС: явный reply (``parent_eid``) → к названному родителю (да);
    иначе fallback → предыдущий ход комнаты должен быть архивирован (:func:`space_thread_settled_read`,
    один READ — без блокировки; поллинг делает цикл, не блокируя соседние комнаты, THREAD_MODEL §3)."""
    if parent_eid is not None:
        return True
    return space_thread_settled_read(
        bridge=NotmuchBridgeFromLocalhost.MATRIX,
        space_wire=ThreliumSpaceB62Wire.from_threlium_space(matrix_space_from_room_id(room_id)),
    )


def _sync_since_from_index() -> MatrixSyncBatchCursor | None:
    """Токен ``since`` для ``/sync``: ``next_batch`` из newest ``from:matrix``."""
    def _pick(route: MatrixIngressRoute) -> MatrixSyncBatchCursor | None:
        if route.sync_batch and str(route.sync_batch).strip():
            return MatrixSyncBatchCursor(str(route.sync_batch).strip())
        return None

    return latest_route_checkpoint(
        NotmuchBridgeFromLocalhost.MATRIX,
        MatrixIngressRoute,
        _pick,
    )


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
        try:
            parsed = msgspec.convert(content, type=MatrixInboundRoomMessageSourceContent)
        except msgspec.ValidationError:
            parsed = None
        if parsed is not None and parsed.body:
            return parsed.body
        b = content.get("body")
        if isinstance(b, str):
            return b
    return ""


async def _matrix_ingress_loop(
    deliver: Callable[[EmailMessage], None],
    homeserver: str,
    access_token: str,
    user_id: str,
) -> None:
    # Long-lived /sync: держим один AsyncClient на цикл (не matrix_client() — тот закрывает
    # сессию после каждой egress-операции; ingress и egress разный lifecycle).
    client = AsyncClient(homeserver, user="")
    client.access_token = access_token
    client.user_id = user_id
    since = _sync_since_from_index()
    if since:
        client.next_batch = since
    sync_ok_logged = False

    def _emit_event(
        room_id: MatrixRoomId,
        ev_id: MatrixRoomEventId,
        mid_wire: RfcMessageIdWire,
        parent_eid: MatrixRoomEventId | None,
        body: str,
        subj_w: Any,
        checkpoint: str,
    ) -> None:
        irt = _bridge_in_reply_to_for_room_event(room_id=room_id, parent_event_id=parent_eid)
        route = MatrixIngressRoute(
            channel=BridgeIngressChannel.MATRIX, v=1, room_id=room_id, event_id=ev_id,
            sync_batch=MatrixSyncBatchCursor(checkpoint), reply_to_event_id=parent_eid,
        )
        raw_obj: dict[str, object] = {
            "route": msgspec.to_builtins(route), "body": body,
            "room_id": room_id, "event_id": ev_id,
        }
        if parent_eid is not None:
            raw_obj["reply_to_event_id"] = str(parent_eid)
        raw_capture = msgspec.json.encode(raw_obj).decode("utf-8")
        sw = ThreliumSpaceB62Wire.from_threlium_space(matrix_space_from_room_id(room_id))
        msg = build_bridge_ingress_email(
            channel=BridgeIngressChannel.MATRIX, body=body, route=route,
            message_id=mid_wire, in_reply_to=irt, subject=subj_w,
            raw_capture=raw_capture, space_wire=sw,
        )
        notify_status(SystemdStatusBody.bridge_matrix_delivering_room(room_id=room_id))
        deliver(msg)

    # Pending: события (fallback), чья комната ещё НЕ отстоялась (предыдущий ход не архивирован). sync уже
    # потреблён (checkpoint сохранён в элементе) → держим в памяти, перепроверяем каждый тик БЕЗ блокировки;
    # события ДРУГИХ комнат идут параллельно. (deadline, room_id, ev_id, mid_wire, parent_eid, body, subj_w, checkpoint).
    pending: list[tuple[float, MatrixRoomId, MatrixRoomEventId, RfcMessageIdWire, MatrixRoomEventId | None, str, Any, str]] = []
    try:
        while True:
            blocked: set[str] = set()  # комнаты, уже обработанные в этом тике (FIFO + serial-per-thread)

            # 1) Перепроверить pending: комната отстоялась → emit; иначе оставить (соседи независимы).
            carried: list[tuple[float, MatrixRoomId, MatrixRoomEventId, RfcMessageIdWire, MatrixRoomEventId | None, str, Any, str]] = []
            for item in pending:
                deadline, p_room, p_ev, p_mid, p_parent, p_body, p_subj, p_ckpt = item
                sk = _matrix_space_key(p_room)
                if sk in blocked:
                    carried.append(item)
                    continue
                blocked.add(sk)
                if time.monotonic() >= deadline:
                    log.warning("space_settle_deadline_force_ingest", room_id=p_room, event_id=p_ev)
                    _emit_event(p_room, p_ev, p_mid, p_parent, p_body, p_subj, p_ckpt)
                elif _matrix_space_settled(p_room, p_parent):
                    _emit_event(p_room, p_ev, p_mid, p_parent, p_body, p_subj, p_ckpt)
                else:
                    carried.append(item)
            pending = carried

            # 2) sync (короткий, если есть pending — быстрее доперепроверить).
            resp = await client.sync(timeout=2_000 if pending else 60_000)
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

                known_mids = _filter_known_message_ids(candidate_mids)

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
                        if parent_eid is not None:
                            # Явный reply → IRT к названному родителю; от хода комнаты не зависит.
                            _emit_event(room_id, ev_id, mid_wire, parent_eid, body, subj_w, checkpoint)
                            continue
                        sk = _matrix_space_key(room_id)
                        if sk in blocked:
                            pending.append((time.monotonic() + SPACE_SETTLE_TIMEOUT_SEC, room_id, ev_id, mid_wire, parent_eid, body, subj_w, checkpoint))
                            continue
                        blocked.add(sk)
                        if _matrix_space_settled(room_id, parent_eid):
                            _emit_event(room_id, ev_id, mid_wire, parent_eid, body, subj_w, checkpoint)
                        else:
                            pending.append((time.monotonic() + SPACE_SETTLE_TIMEOUT_SEC, room_id, ev_id, mid_wire, parent_eid, body, subj_w, checkpoint))
            notify_status(SystemdStatusBody.bridge_matrix_connected_idle())
            time.sleep(0)
    finally:
        await client.close()


def run_bridge(deliver: Callable[[EmailMessage], None], *, settings: ThreliumSettings) -> None:
    matrix_cfg = settings.bridges.matrix
    hs_raw = matrix_cfg.homeserver
    tok = matrix_cfg.token
    mxid = matrix_cfg.user
    if not settings.home or not hs_raw or not tok or not mxid:
        log.error("required_settings_missing")
        sys.exit(1)
    homeserver = matrix_homeserver_url(hs_raw)
    asyncio.run(_matrix_ingress_loop(deliver, homeserver, tok, mxid))
