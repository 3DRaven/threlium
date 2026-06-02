"""Parse tool_calls → args суммаризации (thread context / response buffer).

Тонкие обёртки над :func:`~threlium.litellm_tool_bridge.parse_single_tool`
(общий каркас ``docs/TYPES.md`` § tool bridge).
"""
from __future__ import annotations

from litellm.types.utils import Message

from threlium.litellm_tool_bridge import parse_single_tool
from threlium.types import PromptPath
from threlium.types.summarize_tool_args import (
    SummarizeResponseBufferToolArgs,
    SummarizeThreadContextToolArgs,
)
from threlium.types.summarize_tool_function import (
    SummarizeToolBridgeError,
    SummarizeToolFunctionName,
)


def parse_summarize_thread_context_assistant(
    assistant: Message,
) -> SummarizeThreadContextToolArgs:
    return parse_single_tool(
        assistant,
        expected=SummarizeToolFunctionName.SUMMARIZE_THREAD_CONTEXT,
        tool_spec_path=PromptPath.SUMMARIZE_CONTEXT_TOOL_SPEC,
        args_type=SummarizeThreadContextToolArgs,
        bridge_error=SummarizeToolBridgeError,
        context="summarize_thread_context",
    )


def parse_summarize_response_buffer_assistant(
    assistant: Message,
) -> SummarizeResponseBufferToolArgs:
    return parse_single_tool(
        assistant,
        expected=SummarizeToolFunctionName.SUMMARIZE_RESPONSE_BUFFER,
        tool_spec_path=PromptPath.RESPONSE_OBSERVE_TOOL_SPEC,
        args_type=SummarizeResponseBufferToolArgs,
        bridge_error=SummarizeToolBridgeError,
        context="summarize_response_buffer",
    )


__all__ = [
    "parse_summarize_response_buffer_assistant",
    "parse_summarize_thread_context_assistant",
]
