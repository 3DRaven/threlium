"""Имена OpenAI tools стадии ``enrich`` (task plan / query plan)."""
from __future__ import annotations

from enum import nonmember

from ._core import ToolFunctionNameBase


class EnrichToolBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → args для стадии enrich."""


class EnrichToolFunctionName(ToolFunctionNameBase):
    ENRICH_TASK_PLAN = "enrich_task_plan"
    ENRICH_TASK_HYPOTHESES = "enrich_task_hypotheses"
    ENRICH_QUERY_PLAN = "enrich_query_plan"

    _bridge_error = nonmember(EnrichToolBridgeError)
    _label = nonmember("enrich")


__all__ = ["EnrichToolBridgeError", "EnrichToolFunctionName"]
