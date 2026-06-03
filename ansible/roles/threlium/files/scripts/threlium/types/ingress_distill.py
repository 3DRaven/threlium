"""VO и envelope для ingress distill (tool → несколько ``<history>``)."""
from __future__ import annotations

from email.message import EmailMessage
from enum import StrEnum
from typing import Self

import msgspec

from threlium.mail_header_names import MailHeaderName
from threlium.types._core import _OptionalStripEmpty
from threlium.types.bridge_ingress_channel import BridgeIngressChannel
from threlium.types.fsm_strings import OrphanNoticePrefixLine
from threlium.types.ingress import IngressRouteB62Wire
from threlium.types.ingress_distill_tool_args import IngressDistillToolArgs
from threlium.types.rfc import (
    RfcDateWire,
    RfcFromWire,
    RfcInReplyToWire,
    RfcMessageIdWire,
    RfcSubjectWire,
    RfcToWire,
)

_HDR = MailHeaderName


class IngressDistillHistoryPartKind(StrEnum):
    """Порядок attach на ingress→enrich: metadata first, ``USER_INTENT`` перед request_echo."""

    USER_REPLY_LANGUAGE = "user_reply_language"
    STEP_BACK_NOTES = "step_back_notes"
    OPEN_GAPS = "open_gaps"
    USER_INTENT = "user_intent"


class IngressExternalBodyText(_OptionalStripEmpty):
    """Полное внешнее тело до LLM distill (``<system>`` user query)."""


class IngressDistillBriefText(_OptionalStripEmpty):
    """Текст distill brief ``USER_INTENT`` (``## User intent``); не canonical user query."""


class IngressDistillHistoryPart(msgspec.Struct, frozen=True, kw_only=True):
    """Одна ``<history>``-часть после Jinja-рендера поля tool."""

    kind: IngressDistillHistoryPartKind
    text: str


class IngressDistillEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """Переменные ``ingress/distill_user.j2``."""

    channel: BridgeIngressChannel
    from_hdr: RfcFromWire | None
    to_hdr: RfcToWire | None
    subject: RfcSubjectWire | None
    date: RfcDateWire | None
    message_id: RfcMessageIdWire | None
    in_reply_to: RfcInReplyToWire | None
    orphan_notice: OrphanNoticePrefixLine | None
    full_body: IngressExternalBodyText

    @classmethod
    def from_email(
        cls,
        msg: EmailMessage,
        *,
        channel: BridgeIngressChannel,
        full_body: IngressExternalBodyText,
        orphan_notice: OrphanNoticePrefixLine | None = None,
    ) -> Self:
        return cls(
            channel=channel,
            from_hdr=RfcFromWire.parse_present_from_email(msg, _HDR.FROM),
            to_hdr=RfcToWire.parse_present_from_email(msg, _HDR.TO),
            subject=RfcSubjectWire.parse_present_from_email(msg, _HDR.SUBJECT),
            date=RfcDateWire.parse_present_from_email(msg, _HDR.DATE),
            message_id=RfcMessageIdWire.parse_present_from_email(msg, _HDR.MESSAGE_ID),
            in_reply_to=RfcInReplyToWire.parse_present_from_email(msg, _HDR.IN_REPLY_TO),
            orphan_notice=orphan_notice,
            full_body=full_body,
        )


class IngressDistillResult(msgspec.Struct, frozen=True, kw_only=True):
    """Набор history-частей distill (metadata + user intent); user query — request_echo."""

    parts: tuple[IngressDistillHistoryPart, ...]

    def user_intent_brief(self) -> IngressDistillBriefText:
        for part in reversed(self.parts):
            if part.kind is IngressDistillHistoryPartKind.USER_INTENT:
                brief = IngressDistillBriefText.parse(part.text)
                if not brief.value:
                    raise ValueError("ingress_distill: empty user_intent history part")
                return brief
        raise ValueError("ingress_distill: no USER_INTENT history part")


def _render_history_part(
    prompt_path: object,
    *,
    kind: IngressDistillHistoryPartKind,
    **vars: object,
) -> IngressDistillHistoryPart | None:
    from threlium.prompts import render_prompt
    from threlium.types import PromptPath

    text = render_prompt(prompt_path, **vars).strip()
    if not text:
        return None
    return IngressDistillHistoryPart(kind=kind, text=text)


def ingress_distill_history_parts_from_tool_args(
    args: IngressDistillToolArgs,
) -> tuple[IngressDistillHistoryPart, ...]:
    """Jinja per field → отдельные ``<history>``; ``user_intent`` последний среди distill parts."""
    from threlium.types import PromptPath

    parts: list[IngressDistillHistoryPart] = []

    lang = args.user_reply_language.strip()
    if lang:
        p = _render_history_part(
            PromptPath.INGRESS_DISTILL_HISTORY_USER_REPLY_LANGUAGE,
            kind=IngressDistillHistoryPartKind.USER_REPLY_LANGUAGE,
            user_reply_language=lang,
        )
        if p is not None:
            parts.append(p)

    notes = [n.strip() for n in args.step_back_notes if n.strip()]
    if notes:
        p = _render_history_part(
            PromptPath.INGRESS_DISTILL_HISTORY_STEP_BACK_NOTES,
            kind=IngressDistillHistoryPartKind.STEP_BACK_NOTES,
            step_back_notes=notes,
        )
        if p is not None:
            parts.append(p)

    gaps = [g.strip() for g in args.open_gaps if g.strip()]
    if gaps:
        p = _render_history_part(
            PromptPath.INGRESS_DISTILL_HISTORY_OPEN_GAPS,
            kind=IngressDistillHistoryPartKind.OPEN_GAPS,
            open_gaps=gaps,
        )
        if p is not None:
            parts.append(p)

    intent = args.user_intent.strip()
    if not intent:
        raise ValueError("ingress_distill: empty user_intent from tool")
    p = _render_history_part(
        PromptPath.INGRESS_DISTILL_HISTORY_USER_QUERY,
        kind=IngressDistillHistoryPartKind.USER_INTENT,
        user_intent=intent,
    )
    if p is None:
        raise ValueError("ingress_distill: empty user_intent history after render")
    parts.append(p)

    return tuple(parts)


def ingress_distill_fallback_history_parts(
    full_body: str,
) -> tuple[IngressDistillHistoryPart, ...]:
    """Fail-safe: одна history = user intent из усечённого тела (без LLM)."""
    from threlium.types import PromptPath

    body = full_body.strip()
    if not body:
        raise ValueError("ingress_distill fallback: empty body")
    p = _render_history_part(
        PromptPath.INGRESS_DISTILL_HISTORY_USER_QUERY,
        kind=IngressDistillHistoryPartKind.USER_INTENT,
        user_intent=body,
    )
    if p is None:
        raise ValueError("ingress_distill fallback: empty history after render")
    return (p,)


def bridge_channel_from_email(msg: EmailMessage) -> BridgeIngressChannel:
    """Канал из ``X-Threlium-Route``; иначе email."""
    route_hdr = IngressRouteB62Wire.parse_present_from_email(msg, _HDR.ROUTE)
    if route_hdr is not None:
        ing = IngressRouteB62Wire.parse_route_from_optional_header(route_hdr)
        if ing is not None:
            slug = str(ing.channel).strip().lower()
            try:
                return BridgeIngressChannel(slug)
            except ValueError:
                pass
    return BridgeIngressChannel.EMAIL


__all__ = [
    "IngressDistillBriefText",
    "IngressDistillEnvelope",
    "IngressDistillHistoryPart",
    "IngressDistillHistoryPartKind",
    "IngressDistillResult",
    "IngressExternalBodyText",
    "bridge_channel_from_email",
    "ingress_distill_fallback_history_parts",
    "ingress_distill_history_parts_from_tool_args",
]
