"""Сборка ``dict`` HTTP-заголовков e2e-корреляции к LiteLLM из :class:`email.message.EmailMessage`."""
from __future__ import annotations

from email.message import EmailMessage

import notmuch2  # pyright: ignore[reportMissingImports]

from threlium import nm
from threlium.ingress_route_resolve import (
    resolve_route_from_thread_oldest_route_tag,
    resolve_route_from_thread_oldest_route_tag_under_db,
)
from threlium.types import (
    LitellmCallSite,
    MailHeaderName,
    NotmuchMessageIdInner,
    RfcMessageIdWire,
)
from threlium.litellm_route_context import get_litellm_http_correlation
from threlium.settings import ThreliumSettings
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader


def _header_line(msg: EmailMessage, name: str) -> str | None:
    raw = msg.get(name)
    if raw is None:
        return None
    s = str(raw).strip()
    return s if s else None


def _normalize_optional_header(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _assemble_litellm_correlation_dict(
    *,
    from_hdr: str | None,
    to_hdr: str | None,
    message_id_hdr: str | None,
    in_reply_to_hdr: str | None,
    route_wire_value: str,
    thread_root_mid: str | None,
    call_site: LitellmCallSite,
) -> dict[str, str]:
    """Единственное место белого списка ключей корреляции (кроме служебных seq в TLS)."""
    out: dict[str, str] = {}
    for hdr, raw in (
        (MailHeaderName.FROM, from_hdr),
        (MailHeaderName.TO, to_hdr),
        (MailHeaderName.MESSAGE_ID, message_id_hdr),
        (MailHeaderName.IN_REPLY_TO, in_reply_to_hdr),
    ):
        nv = _normalize_optional_header(raw)
        if nv:
            out[hdr.value] = nv
    out[MailHeaderName.ROUTE.value] = route_wire_value
    if thread_root_mid:
        out[LitellmCorrelationHeader.THREAD_ROOT_MID.value] = thread_root_mid
    out[LitellmCorrelationHeader.CALL_SITE.value] = call_site.value
    return out


def build_litellm_correlation_headers_from_notmuch(
    db: notmuch2.Database,
    nm_msg: notmuch2.Message,
    *,
    call_site: LitellmCallSite,
) -> dict[str, str]:
    """Сборка снимка корреляции из :class:`notmuch2.Message` под уже открытым READ ``db``.

    Заголовки envelope и резолв ``X-Threlium-Route`` по корню треда — без повторного открытия
    индекса и без разбора файла с диска (путь LightRAG при ``e2e_litellm_route_correlation``).
    ``X-Threlium-Thread-Root`` — уголковый ``Message-ID`` самого старого в треде письма с
    ``tag:route`` (тот же резолв, что и ``X-Threlium-Route``), один на весь notmuch-тред.
    """
    mid_inner = nm.require_inner_message_id_from_notmuch_message(nm_msg)
    from_hdr = nm.header_field_optional(nm_msg, MailHeaderName.FROM)
    to_hdr = nm.header_field_optional(nm_msg, MailHeaderName.TO)
    mid_hdr = mid_inner.as_angle_bracket_header()
    irt_hdr = nm.header_field_optional(nm_msg, MailHeaderName.IN_REPLY_TO)
    resolved = resolve_route_from_thread_oldest_route_tag_under_db(db, mid_inner)
    return _assemble_litellm_correlation_dict(
        from_hdr=from_hdr,
        to_hdr=to_hdr,
        message_id_hdr=mid_hdr,
        in_reply_to_hdr=irt_hdr,
        route_wire_value=resolved.route_wire.value,
        thread_root_mid=resolved.message_id_inner.as_angle_bracket_header(),
        call_site=call_site,
    )


def fsm_correlation_snap(
    msg: EmailMessage | None,
    settings: ThreliumSettings,
    call_site: LitellmCallSite | None = None,
) -> dict[str, str] | None:
    """E2e-снимок корреляции для FSM single-tool стадий.

    При ``settings.e2e.litellm_route_correlation`` — TLS snap или (если ``msg`` задан)
    сборка с конверта через :func:`build_litellm_correlation_headers`.
    ``call_site`` переопределяет ``X-Threlium-Call-Site`` (до override в
    :func:`~threlium.litellm_required_tool.invoke_required_tool` по ``function.name``).
    ``msg=None`` — только TLS snap без fallback на envelope (ingress distill, enrich).
    """
    if not settings.e2e.litellm_route_correlation:
        return None
    snap = get_litellm_http_correlation()
    if snap is not None:
        corr = dict(snap)
    elif msg is not None and call_site is not None:
        corr = build_litellm_correlation_headers(msg, call_site=call_site)
    else:
        return None
    if call_site is not None:
        corr[LitellmCorrelationHeader.CALL_SITE.value] = call_site.value
    return corr


def build_litellm_correlation_headers(
    msg: EmailMessage,
    *,
    call_site: LitellmCallSite,
) -> dict[str, str]:
    """Поля с конверта + ``X-Threlium-Call-Site``; ``X-Threlium-Route`` — всегда wire корня треда в notmuch.

    ``X-Threlium-Thread-Root`` — уголковый ``Message-ID`` **того же** письма в notmuch, что и
    резолв маршрута: самое старое в треде с ``tag:route`` (один корень треда для всех каналов
    и стадий FSM), см. :func:`~threlium.ingress_route_resolve.resolve_route_from_thread_oldest_route_tag`.

    Значение маршрута не берётся с MIME конверта: в многошаговых тредах там может быть wire
    не-корневого шага; для стабильной корреляции LiteLLM используется
    :func:`~threlium.ingress_route_resolve.resolve_route_from_thread_oldest_route_tag`.
    """

    mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID)
    mid_inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    if mid_inner is None or mid_w is None:
        raise RuntimeError(
            "FSM-инвариант: resolve_route_from_thread_oldest_route_tag требует непустой "
            f"{MailHeaderName.MESSAGE_ID.value} на конверте"
        )

    from_hdr = _header_line(msg, MailHeaderName.FROM.value)
    to_hdr = _header_line(msg, MailHeaderName.TO.value)
    mid_hdr = mid_w.value
    irt_hdr = _header_line(msg, MailHeaderName.IN_REPLY_TO.value)
    resolved = resolve_route_from_thread_oldest_route_tag(msg)
    return _assemble_litellm_correlation_dict(
        from_hdr=from_hdr,
        to_hdr=to_hdr,
        message_id_hdr=mid_hdr,
        in_reply_to_hdr=irt_hdr,
        route_wire_value=resolved.route_wire.value,
        thread_root_mid=resolved.message_id_inner.as_angle_bracket_header(),
        call_site=call_site,
    )
