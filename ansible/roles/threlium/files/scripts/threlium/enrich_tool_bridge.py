"""Parse tool_calls → args стадии ``enrich`` (task plan / query plan).

Контракт ``docs/TYPES.md`` § tool bridge: ``require_single_tool_call`` →
``*ToolFunctionName.parse_tool_call`` → ``validate_tool_args_json`` → ``msgspec.convert``.
"""
from __future__ import annotations

import jsonschema
import msgspec
from litellm.types.utils import Message

from threlium.litellm_tool_response import require_single_tool_call
from threlium.litellm_tool_spec import (
    load_tool_spec,
    tool_spec_parameters,
    validate_tool_args_json,
)
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
from threlium.types.litellm_tool_call import LiteLlmToolCallArgumentsWire

_TASK_CONTEXT = "enrich_task_plan"
_HYPOTHESES_CONTEXT = "enrich_task_hypotheses"
_QUERY_CONTEXT = "enrich_query_plan"


def parse_enrich_task_plan_assistant(assistant: Message) -> EnrichTaskPlanToolArgs:
    tc = require_single_tool_call(assistant, context=_TASK_CONTEXT)
    name = EnrichToolFunctionName.parse_tool_call(tc)
    name.assert_matches(EnrichToolFunctionName.ENRICH_TASK_PLAN)
    spec = load_tool_spec(PromptPath.LIGHTRAG_ENRICH_TASK_PLAN_TOOL_SPEC)
    schema = tool_spec_parameters(spec)
    wire = LiteLlmToolCallArgumentsWire.from_tool_call(tc)
    try:
        args_dict = validate_tool_args_json(schema, wire)
    except jsonschema.ValidationError as exc:
        raise EnrichToolBridgeError(
            f"{_TASK_CONTEXT}: arguments failed jsonschema"
        ) from exc
    try:
        return msgspec.convert(args_dict, type=EnrichTaskPlanToolArgs)
    except (RuntimeError, ValueError, msgspec.ValidationError) as exc:
        raise EnrichToolBridgeError(
            f"{_TASK_CONTEXT}: invalid arguments"
        ) from exc


def parse_enrich_task_hypotheses_assistant(
    assistant: Message,
) -> EnrichTaskHypothesesToolArgs:
    tc = require_single_tool_call(assistant, context=_HYPOTHESES_CONTEXT)
    name = EnrichToolFunctionName.parse_tool_call(tc)
    name.assert_matches(EnrichToolFunctionName.ENRICH_TASK_HYPOTHESES)
    spec = load_tool_spec(PromptPath.LIGHTRAG_ENRICH_TASK_HYPOTHESES_TOOL_SPEC)
    schema = tool_spec_parameters(spec)
    wire = LiteLlmToolCallArgumentsWire.from_tool_call(tc)
    try:
        args_dict = validate_tool_args_json(schema, wire)
    except jsonschema.ValidationError as exc:
        raise EnrichToolBridgeError(
            f"{_HYPOTHESES_CONTEXT}: arguments failed jsonschema"
        ) from exc
    try:
        return msgspec.convert(args_dict, type=EnrichTaskHypothesesToolArgs)
    except (RuntimeError, ValueError, msgspec.ValidationError) as exc:
        raise EnrichToolBridgeError(
            f"{_HYPOTHESES_CONTEXT}: invalid arguments"
        ) from exc


def parse_enrich_query_plan_assistant(assistant: Message) -> EnrichQueryPlanToolArgs:
    tc = require_single_tool_call(assistant, context=_QUERY_CONTEXT)
    name = EnrichToolFunctionName.parse_tool_call(tc)
    name.assert_matches(EnrichToolFunctionName.ENRICH_QUERY_PLAN)
    spec = load_tool_spec(PromptPath.LIGHTRAG_ENRICH_QUERY_PLAN_TOOL_SPEC)
    schema = tool_spec_parameters(spec)
    wire = LiteLlmToolCallArgumentsWire.from_tool_call(tc)
    try:
        args_dict = validate_tool_args_json(schema, wire)
    except jsonschema.ValidationError as exc:
        raise EnrichToolBridgeError(
            f"{_QUERY_CONTEXT}: arguments failed jsonschema"
        ) from exc
    try:
        return msgspec.convert(args_dict, type=EnrichQueryPlanToolArgs)
    except (RuntimeError, ValueError, msgspec.ValidationError) as exc:
        raise EnrichToolBridgeError(
            f"{_QUERY_CONTEXT}: invalid arguments"
        ) from exc


__all__ = [
    "parse_enrich_query_plan_assistant",
    "parse_enrich_task_hypotheses_assistant",
    "parse_enrich_task_plan_assistant",
]
