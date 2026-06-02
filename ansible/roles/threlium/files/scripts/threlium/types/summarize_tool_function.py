"""Имена OpenAI tools суммаризации (thread context / response buffer)."""
from __future__ import annotations

from enum import nonmember

from ._core import ToolFunctionNameBase


class SummarizeToolBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → args для суммаризации."""


class SummarizeToolFunctionName(ToolFunctionNameBase):
    SUMMARIZE_THREAD_CONTEXT = "summarize_thread_context"
    SUMMARIZE_RESPONSE_BUFFER = "summarize_response_buffer"

    _bridge_error = nonmember(SummarizeToolBridgeError)
    _label = nonmember("summarize")


__all__ = ["SummarizeToolBridgeError", "SummarizeToolFunctionName"]
