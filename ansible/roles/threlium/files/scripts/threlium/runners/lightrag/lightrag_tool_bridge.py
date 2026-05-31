"""Parse tool_calls → msgspec Struct → wire VO → ``str`` для LightRAG."""
from __future__ import annotations

import msgspec
from litellm.types.utils import Message

from threlium.litellm_tool_response import require_single_tool_call
from threlium.litellm_tool_spec import (
    load_tool_spec,
    tool_call_arguments_wire_from_tool_call,
    tool_spec_parameters,
    validate_tool_args_json,
)
from threlium.types.lightrag_tool_args import (
    ExtractKnowledgeGraphToolArgs,
    ExtractQueryKeywordsToolArgs,
    GenerateRagAnswerToolArgs,
    SummarizeDescriptionsToolArgs,
)
from threlium.types.lightrag_tool_function import (
    LightragToolBridgeError,
    LightragToolFunctionName,
)
from threlium.types.lightrag_tool_phase import LightragToolPhaseSpec
from threlium.types.lightrag_tool_wire import (
    LightragEntitySummaryText,
    LightragExtractionDelimiterText,
    LightragKeywordsJsonText,
    LightragRagAnswerText,
)

from .lightrag_tool_serialize import (
    lightrag_extraction_delimiter_from_args,
    lightrag_keywords_json_from_args,
    lightrag_rag_answer_from_args,
    lightrag_summary_from_args,
)


def parse_tool_call_for_phase(
    msg: Message,
    phase: LightragToolPhaseSpec,
) -> msgspec.Struct:
    ctx = f"LightRAG phase {phase.call_site.value}"
    tc = require_single_tool_call(msg, context=ctx)
    name = LightragToolFunctionName.parse_tool_call(tc)
    name.assert_matches(phase.tool_name)
    spec = load_tool_spec(phase.tool_spec_path)
    schema = tool_spec_parameters(spec)
    wire = tool_call_arguments_wire_from_tool_call(tc)
    args_dict = validate_tool_args_json(schema, wire)
    return msgspec.convert(args_dict, type=phase.args_type)


def to_lightrag_return_value(
    wire: LightragExtractionDelimiterText
    | LightragKeywordsJsonText
    | LightragEntitySummaryText
    | LightragRagAnswerText,
) -> str:
    return wire.value


def struct_to_lightrag_wire(
    phase: LightragToolPhaseSpec,
    args: msgspec.Struct,
) -> (
    LightragExtractionDelimiterText
    | LightragKeywordsJsonText
    | LightragEntitySummaryText
    | LightragRagAnswerText
):
    if isinstance(args, ExtractKnowledgeGraphToolArgs):
        return lightrag_extraction_delimiter_from_args(args)
    if isinstance(args, SummarizeDescriptionsToolArgs):
        return lightrag_summary_from_args(args)
    if isinstance(args, ExtractQueryKeywordsToolArgs):
        return lightrag_keywords_json_from_args(args)
    if isinstance(args, GenerateRagAnswerToolArgs):
        return lightrag_rag_answer_from_args(args)
    raise LightragToolBridgeError(
        f"unsupported args type {type(args).__name__} for phase {phase.call_site.value}"
    )


__all__ = [
    "parse_tool_call_for_phase",
    "struct_to_lightrag_wire",
    "to_lightrag_return_value",
]
