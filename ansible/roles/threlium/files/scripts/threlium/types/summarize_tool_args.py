"""Аргументы tool-вызовов суммаризации (thread context / response buffer).

Отдельные VO на каждый tool (DDD, ``docs/TYPES.md`` § VO). См. ``summarize_tool_bridge``.
"""
from __future__ import annotations

import msgspec


class SummarizeThreadContextToolArgs(msgspec.Struct, frozen=True):
    """Сжатая сводка батча писем треда (стадия ``summarize_context``)."""

    summary: str


class SummarizeResponseBufferToolArgs(msgspec.Struct, frozen=True):
    """Структурированное наблюдение по буферу ответа (стадия ``response_observe``)."""

    observation: str


class SummarizeHistoryUnit(msgspec.Struct, frozen=True):
    """Одна ``<history>``-часть для overflow-сжатия (контент-адресный CID + тело).

    ``source_mid`` — notmuch inner mid письма-носителя, для ``tag:context_summarized``
    после валидной сводки (несколько единиц могут ссылаться на один mid).
    """

    cid: str
    text: str
    source_mid: str


class SummarizeContextBatch(msgspec.Struct, frozen=True):
    """Батч ``<history>``-частей для overflow-сжатия (гранулярные units, не письма)."""

    units: list[SummarizeHistoryUnit]


class SummarizeContextStagePayload(msgspec.Struct, frozen=True):
    """Wire-форма ``<system>`` для перехода ``enrich → summarize_context`` (CONTEXT §5 overflow).

    ``user_query`` — wire plain ``str`` (``<user-query>`` CID, не distill ``user_intent``);
    decode boundary → ``EnrichUserQueryText.require``; неизменен по циклу summarize_memory → enrich.

    TYPES (``docs/TYPES.md`` § stage payload): сериализация/разбор строго через ``msgspec`` (не
    ``json.dumps`` / ``json.loads`` + ручной ``dict``).
    """

    summarize: SummarizeContextBatch
    user_query: str  # wire; validated via validated_user_query()


def validated_user_query(payload: SummarizeContextStagePayload) -> "EnrichUserQueryText":
    """Decode boundary: wire ``str`` → ``EnrichUserQueryText``."""
    from threlium.types.fsm_strings import EnrichUserQueryText

    return EnrichUserQueryText.require(name="summarize user_query", raw=payload.user_query)


__all__ = [
    "SummarizeContextBatch",
    "SummarizeContextStagePayload",
    "SummarizeHistoryUnit",
    "SummarizeResponseBufferToolArgs",
    "SummarizeThreadContextToolArgs",
    "validated_user_query",
]
