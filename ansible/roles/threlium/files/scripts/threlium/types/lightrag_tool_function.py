"""Имена OpenAI tools для LLM-фаз LightRAG."""
from __future__ import annotations

from enum import StrEnum
from typing import Self

from litellm.types.utils import ChatCompletionMessageToolCall


class LightragToolBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → wire для LightRAG."""


class LightragToolFunctionName(StrEnum):
    EXTRACT_KNOWLEDGE_GRAPH = "extract_knowledge_graph"
    SUMMARIZE_DESCRIPTIONS = "summarize_descriptions"
    EXTRACT_QUERY_KEYWORDS = "extract_query_keywords"
    GENERATE_RAG_ANSWER = "generate_rag_answer"

    @classmethod
    def parse_tool_call(cls, tc: ChatCompletionMessageToolCall) -> Self:
        func = tc.function
        if func is None or not func.name:
            raise LightragToolBridgeError("tool_call without function.name")
        raw = func.name.strip()
        try:
            return cls(raw)
        except ValueError as exc:
            raise LightragToolBridgeError(
                f"unknown LightRAG tool function.name={raw!r}"
            ) from exc

    def assert_matches(self, expected: LightragToolFunctionName) -> None:
        if self != expected:
            raise LightragToolBridgeError(
                f"expected tool {expected.value!r}, got {self.value!r}"
            )


__all__ = ["LightragToolBridgeError", "LightragToolFunctionName"]
