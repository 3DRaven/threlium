"""Аргументы tool-вызовов стадии ``enrich`` (task plan / hypotheses).

После :class:`~threlium.types.litellm_tool_call.LiteLlmToolCallArgumentsWire`
и ``validate_tool_args_json`` — см. ``enrich_tool_bridge``. Отдельные VO на каждый
tool (DDD, ``docs/TYPES.md`` § VO), даже при схожей форме.
"""
from __future__ import annotations

import msgspec


class EnrichTaskPlanToolArgs(msgspec.Struct, frozen=True):
    """Seed-подзадачи (``<task-init>``): список коротких формулировок."""

    subtasks: list[str]


class EnrichTaskHypothesesToolArgs(msgspec.Struct, frozen=True):
    """Late-проход: проверяемые гипотезы после RAG (тот же ``<task-init>`` ledger).

    Отдельный VO от :class:`EnrichTaskPlanToolArgs` (DDD, ``docs/TYPES.md`` § VO):
    другой tool-name / call-site / промпт, хотя форма аргументов совпадает.
    """

    subtasks: list[str]


__all__ = [
    "EnrichTaskHypothesesToolArgs",
    "EnrichTaskPlanToolArgs",
]
