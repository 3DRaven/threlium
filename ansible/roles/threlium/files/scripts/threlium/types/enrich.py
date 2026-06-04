"""Проекции контекста стадии ``enrich`` для Jinja (``docs/TYPES.md`` уровень 3).

Граница: ``EmailMessage`` → :class:`EnrichTaskHypothesesPromptContext`; в
``render_prompt`` уходят только развёрнутые ``str`` (как
:class:`~threlium.types.reasoning.ReasoningIncomingEnvelope` в ``states/reasoning.py``).
"""
from __future__ import annotations

import msgspec

from threlium.types._core import _OptionalStripEmpty
from threlium.types.fsm_strings import (
    EnrichGlobalMemoryText,
    EnrichGraphAnswerText,
    EnrichThreadMemoryText,
    EnrichUnifiedMailContextText,
)
from threlium.types.reasoning import ReasoningUserMessageText


def optional_enrich_part_for_jinja(vo: _OptionalStripEmpty | None) -> str | None:
    """Present-or-None VO → kwargs Jinja (``TYPES.md`` § граница ``.value``)."""
    return vo.value if vo is not None else None


class EnrichTaskHypothesesPromptContext(msgspec.Struct, frozen=True, kw_only=True):
    """Контекст ``lightrag/enrich_task_hypotheses.j2`` (уровень 3, после ``EnrichResult``)."""

    incoming_user_message: ReasoningUserMessageText
    graph_answer: EnrichGraphAnswerText | None
    unified_mail_context: EnrichUnifiedMailContextText | None
    thread_memory: EnrichThreadMemoryText | None
    global_memory: EnrichGlobalMemoryText | None

    def for_jinja(self) -> dict[str, object]:
        return {
            "incoming_user_message": self.incoming_user_message.value,
            "graph_answer": optional_enrich_part_for_jinja(self.graph_answer),
            "unified_mail_context": optional_enrich_part_for_jinja(
                self.unified_mail_context
            ),
            "thread_memory": optional_enrich_part_for_jinja(self.thread_memory),
            "global_memory": optional_enrich_part_for_jinja(self.global_memory),
        }


__all__ = [
    "EnrichTaskHypothesesPromptContext",
    "optional_enrich_part_for_jinja",
]
