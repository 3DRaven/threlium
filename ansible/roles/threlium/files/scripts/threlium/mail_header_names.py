"""Канонические имена полей заголовков почты: RFC 5322 и ``X-Threlium-*`` (wire = ``value``).

Вынесено из ``threlium.types``, чтобы :mod:`threlium.logutil` и :mod:`threlium.mime_reform`
не создавали циклический импорт через ``types/__init__.py``.
"""
from __future__ import annotations

from enum import StrEnum


class MailHeaderName(StrEnum):
    """Перечень имён заголовков для wire/RFC822 и кэша заголовков notmuch.

    Синхронизировать с ``threlium.fsm_emit.HDR_*`` и использованием в кодовой базе.
    """

    # --- RFC 5322 (распространённые поля тела конверта) ---
    FROM = "From"
    TO = "To"
    SUBJECT = "Subject"
    DATE = "Date"
    MESSAGE_ID = "Message-ID"
    IN_REPLY_TO = "In-Reply-To"
    REFERENCES = "References"
    REPLY_TO = "Reply-To"
    SENDER = "Sender"
    CC = "Cc"
    BCC = "Bcc"
    RETURN_PATH = "Return-Path"
    DELIVERED_TO = "Delivered-To"
    CONTENT_TYPE = "Content-Type"
    MIME_VERSION = "MIME-Version"
    CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding"
    CONTENT_DISPOSITION = "Content-Disposition"

    # --- Threlium extension (только реальные заголовки FSM-писем) ---
    ROUTE = "X-Threlium-Route"
    HOP_BUDGET = "X-Threlium-Hop-Budget"
    CAPABILITIES = "X-Threlium-Capabilities"
    SPACE_HASH = "X-Threlium-Space-Hash"
    IRT_HASH = "X-Threlium-Irt-Hash"
    # Part-level (на отдельных <history>-MIME-частях, не на конверте письма):
    # CONTENT_SCORE ставит источник из настроек; ORIGIN штампует enrich_fast при сплайсе.
    CONTENT_SCORE = "X-Threlium-Content-Score"
    ORIGIN = "X-Threlium-Origin"

    @classmethod
    def propagate_from_incoming(cls) -> tuple[MailHeaderName, ...]:
        """Заголовки, которые :func:`emit_transition_preserving_payload` копирует как есть из входа.

        Whitelist-подход: только перечисленные заголовки переносятся;
        всё остальное (envelope, X-Threlium-*, Received, …) либо пересобирается
        явно, либо управляется через ``managed_headers`` у низкоуровневого билдера.
        """
        return (cls.SUBJECT,)
