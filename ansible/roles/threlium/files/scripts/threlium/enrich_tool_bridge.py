"""Parse tool_calls → args стадии ``enrich`` (task plan / query plan).

Тонкие обёртки над :func:`~threlium.litellm_tool_bridge.parse_single_tool`
(общий каркас ``docs/TYPES.md`` § tool bridge).
"""
from __future__ import annotations

from litellm.types.utils import Message

from threlium.litellm_tool_bridge import parse_single_tool
from threlium.types import PromptPath
from threlium.types.enrich_tool_args import (
    EnrichQueryPlanToolArgs,
    EnrichTaskHypothesesToolArgs,
    EnrichTaskPlanToolArgs,
)
from threlium.types.enrich_tool_function import (
    EnrichToolBridgeError,
    EnrichToolFunctionName,
)


def parse_enrich_task_plan_assistant(assistant: Message) -> EnrichTaskPlanToolArgs:
    return parse_single_tool(
        assistant,
        expected=EnrichToolFunctionName.ENRICH_TASK_PLAN,
        tool_spec_path=PromptPath.LIGHTRAG_ENRICH_TASK_PLAN_TOOL_SPEC,
        args_type=EnrichTaskPlanToolArgs,
        bridge_error=EnrichToolBridgeError,
        context="enrich_task_plan",
    )


def parse_enrich_task_hypotheses_assistant(
    assistant: Message,
) -> EnrichTaskHypothesesToolArgs:
    return parse_single_tool(
        assistant,
        expected=EnrichToolFunctionName.ENRICH_TASK_HYPOTHESES,
        tool_spec_path=PromptPath.LIGHTRAG_ENRICH_TASK_HYPOTHESES_TOOL_SPEC,
        args_type=EnrichTaskHypothesesToolArgs,
        bridge_error=EnrichToolBridgeError,
        context="enrich_task_hypotheses",
    )


def parse_enrich_query_plan_assistant(assistant: Message) -> EnrichQueryPlanToolArgs:
    return parse_single_tool(
        assistant,
        expected=EnrichToolFunctionName.ENRICH_QUERY_PLAN,
        tool_spec_path=PromptPath.LIGHTRAG_ENRICH_QUERY_PLAN_TOOL_SPEC,
        args_type=EnrichQueryPlanToolArgs,
        bridge_error=EnrichToolBridgeError,
        context="enrich_query_plan",
    )


__all__ = [
    "parse_enrich_query_plan_assistant",
    "parse_enrich_task_hypotheses_assistant",
    "parse_enrich_task_plan_assistant",
]
