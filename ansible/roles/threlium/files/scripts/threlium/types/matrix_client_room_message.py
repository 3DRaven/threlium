"""Тело события Matrix Client-Server ``m.room.message`` (egress → nio ``room_send``)."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec

from .bridges import MatrixOutboundPlainBodyWire
from .identity import MatrixRoomEventId


class MatrixClientRoomMessageInReplyTo(msgspec.Struct, frozen=True):
    """Вложенный объект ``m.in_reply_to`` (только ``event_id``)."""

    event_id: MatrixRoomEventId


class MatrixClientRoomMessageRelatesTo(msgspec.Struct, frozen=True):
    """Блок ``m.relates_to`` для ответа в тред."""

    m_in_reply_to: MatrixClientRoomMessageInReplyTo = msgspec.field(name="m.in_reply_to")


class MatrixClientRoomMessageMTextContent(msgspec.Struct, frozen=True):
    """Контент ``m.room.message`` с ``msgtype`` ``m.text`` (CS API).

    Поле ``body`` — wire-строка из :class:`~threlium.types.bridges.MatrixOutboundPlainBodyWire`
    (уже нормализована на границе VO).
    """

    body: str
    msgtype: Literal["m.text"] = "m.text"
    m_relates_to: MatrixClientRoomMessageRelatesTo | None = msgspec.field(
        name="m.relates_to",
        default=None,
    )


def build_matrix_client_room_message_m_text_content(
    body: MatrixOutboundPlainBodyWire,
    reply_to_event_id: MatrixRoomEventId | None,
) -> MatrixClientRoomMessageMTextContent:
    """Сборка контента из VO моста и опционального ``reply_to`` из маршрута."""
    rel: MatrixClientRoomMessageRelatesTo | None = None
    if reply_to_event_id is not None:
        rel = MatrixClientRoomMessageRelatesTo(
            m_in_reply_to=MatrixClientRoomMessageInReplyTo(event_id=reply_to_event_id),
        )
    return MatrixClientRoomMessageMTextContent(body=body.value, m_relates_to=rel)


def matrix_client_room_message_m_text_content_as_dict_for_nio(
    content: MatrixClientRoomMessageMTextContent,
) -> dict[str, object]:
    """JSON-совместимый ``dict`` для ``nio.AsyncClient.room_send`` / ``Api.room_send``."""
    raw = msgspec.json.encode(content)
    out = msgspec.json.decode(raw)
    if not isinstance(out, dict):
        raise TypeError(f"expected dict from matrix room message JSON, got {type(out).__name__}")
    if out.get("m.relates_to") is None:
        out.pop("m.relates_to", None)
    return out


class MatrixInboundRoomMessageSourceContent(msgspec.Struct, frozen=True):
    """``content`` из ``Event.source`` matrix-nio (ingress bridge)."""

    body: str | None = None
    m_relates_to: MatrixClientRoomMessageRelatesTo | None = msgspec.field(
        name="m.relates_to",
        default=None,
    )


class MatrixInboundRoomMessageSource(msgspec.Struct, frozen=True):
    """Корень ``Event.source`` для ``m.room.message`` (ingress)."""

    content: MatrixInboundRoomMessageSourceContent | None = None


def reply_parent_event_id_from_message_source(
    source: Mapping[str, object],
) -> MatrixRoomEventId | None:
    """``event_id`` предка reply из ``Event.source`` (matrix-nio ingress)."""
    content_raw = source.get("content")
    if not isinstance(content_raw, dict):
        return None
    try:
        parsed = msgspec.convert(source, type=MatrixInboundRoomMessageSource)
    except msgspec.ValidationError:
        return None
    rel = parsed.content.m_relates_to if parsed.content is not None else None
    if rel is None or rel.m_in_reply_to is None:
        return None
    return rel.m_in_reply_to.event_id
