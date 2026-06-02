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


class SummarizeContextBatch(msgspec.Struct, frozen=True):
    """Батч писем для overflow-сжатия: параллельные ``mids`` / ``bodies``."""

    mids: list[str]
    bodies: list[str]


class SummarizeContextStagePayload(msgspec.Struct, frozen=True):
    """Wire-форма ``<system>`` для перехода ``enrich → summarize_context`` (CONTEXT §5 overflow).

    ``user_query`` — канонический ход пользователя (последняя ``<history>`` входящего enrich,
    distill ``user_query``). Едет неизменным по циклу ``enrich → summarize_context →
    summarize_memory → enrich``: суммаризация не меняет сообщения пользователя, поэтому
    re-trigger enrich обязан повторить тот же user message (читается из ``<history>``).

    TYPES (``docs/TYPES.md`` § stage payload): сериализация/разбор строго через ``msgspec`` (не
    ``json.dumps`` / ``json.loads`` + ручной ``dict``).
    """

    summarize: SummarizeContextBatch
    user_query: str


__all__ = [
    "SummarizeContextBatch",
    "SummarizeContextStagePayload",
    "SummarizeResponseBufferToolArgs",
    "SummarizeThreadContextToolArgs",
]
