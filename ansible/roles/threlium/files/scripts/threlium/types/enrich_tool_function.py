"""Имена OpenAI tools стадии ``enrich`` (task plan / query plan)."""
from __future__ import annotations

from enum import StrEnum
from typing import Self

from litellm.types.utils import ChatCompletionMessageToolCall


class EnrichToolBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → args для стадии enrich."""


class EnrichToolFunctionName(StrEnum):
    ENRICH_TASK_PLAN = "enrich_task_plan"
    ENRICH_TASK_HYPOTHESES = "enrich_task_hypotheses"
    ENRICH_QUERY_PLAN = "enrich_query_plan"

    @classmethod
    def parse_tool_call(cls, tc: ChatCompletionMessageToolCall) -> Self:
        func = tc.function
        if func is None or not func.name:
            raise EnrichToolBridgeError("tool_call without function.name")
        raw = func.name.strip()
        try:
            return cls(raw)
        except ValueError as exc:
            raise EnrichToolBridgeError(
                f"unknown enrich tool function.name={raw!r}"
            ) from exc

    def assert_matches(self, expected: EnrichToolFunctionName) -> None:
        if self != expected:
            raise EnrichToolBridgeError(
                f"expected tool {expected.value!r}, got {self.value!r}"
            )


__all__ = ["EnrichToolBridgeError", "EnrichToolFunctionName"]
