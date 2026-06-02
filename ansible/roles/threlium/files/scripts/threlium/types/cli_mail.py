"""Сценарные заголовки для CLI-стадий (без ``X-Threlium-*`` в этих входах)."""
from __future__ import annotations

from email.message import EmailMessage
from typing import Self

import msgspec

from threlium.mail_header_names import MailHeaderName
from .rfc import RfcMessageIdWire

_HDR = MailHeaderName


class CliIntentPayload(msgspec.Struct, frozen=True):
    """Нормализованный JSON ``{"cli": {"argv": [...], "cwd"?, "privileged"?}}`` после границы парсинга."""

    argv: list[str]
    cwd: str | None = None
    privileged: bool = False


class CliIntentEnvelope(msgspec.Struct, frozen=True):
    """Обёртка ``{"cli": {...}}`` для строгого ``msgspec.json.decode`` тела ``<system>``.

    Каноничный JSON рендерит ``prompts/reasoning/cli_intent/email_body.j2`` (``| tojson``),
    поэтому salvage-regex (``parse_json_loose``) больше не нужен — см. ``docs/CONTEXT_CONTRACT.md``.
    """

    cli: CliIntentPayload


class CliResumeMessageIdHeader(msgspec.Struct, frozen=True, kw_only=True):
    """``Message-ID`` входа ``cli_resume`` для нормализации через notmuch."""

    message_id: RfcMessageIdWire | None

    @classmethod
    def from_email(cls, msg: EmailMessage) -> Self:
        return cls(message_id=RfcMessageIdWire.parse_present_from_email(msg, _HDR.MESSAGE_ID))

    @classmethod
    def from_message(cls, msg: EmailMessage) -> Self:
        return cls.from_email(msg)
