#!/usr/bin/env python3
"""egress_matrix@localhost: Matrix Client-Server API, затем запись отправленного в ``archive``."""
from __future__ import annotations

import asyncio
import hashlib
import json
from email.message import EmailMessage

from nio import AsyncClient, AsyncClientConfig
from nio.responses import RoomSendError, RoomSendResponse

from threlium.delivery import run_fdm
from threlium.egress_self_archive import (
    build_egress_sent_record_to_archive,
    find_existing_egress_archive,
)
from threlium.ingress_route_resolve import (
    resolve_egress_task_route_ancestor,
    resolve_egress_task_route_ancestor_with_thread_correlation,
)
from threlium.logutil import logger
from threlium.mime_reform import RFC822_FOR_INSERT, system_part_text
from threlium.settings import ThreliumSettings
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader
from threlium.types import (
    FsmStage,
    IngressRoute,
    MatrixIngressRoute,
    MatrixNativeId,
    MatrixOutboundPlainBodyWire,
    MatrixRoomEventId,
    MatrixRoomId,
    MatrixRoomSendTxnId,
    RfcMessageIdWire,
    MailHeaderName,
    build_matrix_client_room_message_m_text_content,
    matrix_client_room_message_m_text_content_as_dict_for_nio,
    matrix_homeserver_url,
)

_HDR = MailHeaderName

log = logger.bind(stage="egress_matrix")


def _txn_id(msg: EmailMessage) -> MatrixRoomSendTxnId:
    """Детерминированный ``txnId`` из ``Message-ID`` задания (server-side idempotency)."""
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, _HDR.MESSAGE_ID)
    if mid_w is None:
        raise RuntimeError("egress_matrix: task has no Message-ID for txnId")
    raw = mid_w.value.encode("utf-8", errors="replace")
    return MatrixRoomSendTxnId(hashlib.sha256(raw).hexdigest()[:24])


async def _send_matrix_message(
    *,
    stage: FsmStage,
    homeserver: str,
    token: str,
    user: str,
    room_id: MatrixRoomId,
    body: MatrixOutboundPlainBodyWire,
    reply_to_event_id: MatrixRoomEventId | None,
    txn: MatrixRoomSendTxnId,
    correlation_headers: dict[str, str] | None = None,
) -> RoomSendResponse:
    """Отправка в Matrix; возвращает ``RoomSendResponse`` с API-присвоенным ``event_id``."""
    hs = homeserver or ""
    tok = token or ""
    uid = user or ""
    base = matrix_homeserver_url(hs)
    cfg = AsyncClientConfig(custom_headers=correlation_headers) if correlation_headers else None
    client = AsyncClient(base, user="", config=cfg) if cfg else AsyncClient(base, user="")
    client.access_token = tok
    client.user_id = uid
    try:
        payload = build_matrix_client_room_message_m_text_content(body, reply_to_event_id)
        content = matrix_client_room_message_m_text_content_as_dict_for_nio(payload)
        resp = await client.room_send(room_id, "m.room.message", content, tx_id=txn)
        if isinstance(resp, RoomSendError):
            raise RuntimeError(f"egress_matrix: send error {resp!s}")
        if not isinstance(resp, RoomSendResponse):
            raise RuntimeError(
                f"egress_matrix: unexpected response type {type(resp).__name__}"
            )
        log.info("send_ok")
        return resp
    finally:
        await client.close()


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    creds = config.bridges.matrix
    homeserver_vo = creds.homeserver
    token_vo = creds.token
    user_vo = creds.user
    if not token_vo:
        raise RuntimeError("egress_matrix: THRELIUM_MATRIX_TOKEN unset or empty")
    if not homeserver_vo:
        raise RuntimeError("egress_matrix: THRELIUM_MATRIX_HOMESERVER unset or empty")
    if not user_vo:
        raise RuntimeError("egress_matrix: THRELIUM_MATRIX_USER unset or empty")

    def _wrong_type(r: IngressRoute) -> str:
        return (
            "egress_matrix: ожидался MatrixIngressRoute, получен "
            f"{type(r).__name__} (channel={r.channel!r})"
        )

    correlation_headers: dict[str, str] | None = None
    if config.e2e.litellm_route_correlation:
        routing, _snap, thread_resolved = (
            resolve_egress_task_route_ancestor_with_thread_correlation(
                msg,
                MatrixIngressRoute,
                wrong_route_type_message=_wrong_type,
            )
        )
        correlation_headers = {
            LitellmCorrelationHeader.THREAD_ROOT_MID.value:
                thread_resolved.message_id_inner.as_angle_bracket_header(),
        }
    else:
        routing, _snap = resolve_egress_task_route_ancestor(
            msg,
            MatrixIngressRoute,
            wrong_route_type_message=_wrong_type,
        )

    room_id = routing.room_id
    event_id = routing.event_id

    log.info("routing", room_id=room_id, event_ref=event_id)

    body_wire = MatrixOutboundPlainBodyWire.parse_present_optional(
        system_part_text(msg)
    )
    if body_wire is None:
        raise RuntimeError("egress_matrix: plain body is empty after strip")

    txn = _txn_id(msg)

    send_resp = asyncio.run(
        _send_matrix_message(
            stage=stage,
            homeserver=homeserver_vo,
            token=token_vo,
            user=user_vo,
            room_id=routing.room_id,
            body=body_wire,
            reply_to_event_id=routing.reply_to_event_id,
            txn=txn,
            correlation_headers=correlation_headers,
        )
    )

    if find_existing_egress_archive(msg) is not None:
        log.info("archive_exists_skip_write")
        return None

    glue_native = MatrixNativeId(
        v=1,
        room_id=routing.room_id,
        event_id=MatrixRoomEventId(send_resp.event_id),
    )
    glue_mid = RfcMessageIdWire.from_native(glue_native)

    sent_raw = json.dumps(
        {
            "channel": "matrix",
            "room_id": routing.room_id,
            "event_id": routing.event_id,
            "sent_event_id": send_resp.event_id,
            "reply_to_event_id": routing.reply_to_event_id,
            "body": body_wire.value,
        },
        ensure_ascii=False,
        indent=2,
    )
    archive_email = build_egress_sent_record_to_archive(
        msg, stage=stage, sent_raw=sent_raw, glue_message_id_wire=glue_mid,
        settings=config,
    )
    run_fdm(archive_email.as_bytes(policy=RFC822_FOR_INSERT))
    log.info("archive_written")
    return None
