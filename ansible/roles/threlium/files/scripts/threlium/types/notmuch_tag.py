"""Имена notmuch-тегов на wire: единый StrEnum (синхрон с fdm.conf / notmuch2).

Стадии FSM не входят: идентификация ящика стадии в запросах — через ``to:``/``from:``
и :class:`~threlium.types.fsm_stage.FsmStage.rfc822_mailbox`, не через ``tag:<stage>``.
"""
from __future__ import annotations

from enum import StrEnum


class NotmuchTag(StrEnum):
    """Теги, которыми проект оперирует в индексе (без ведущего ``+`` в notmuch2 API)."""

    UNREAD = "unread"
    ROUTE = "route"
    LIGHTRAG_INDEXED = "lightrag_indexed"
    LIGHTRAG_SKIPPED = "lightrag_skipped"
    CONTEXT_SUMMARIZED = "context_summarized"
    ERROR = "error"

    def as_tag_query_term(self) -> str:
        """Термин notmuch search ``tag:<value>``."""
        return f"tag:{self.value}"
