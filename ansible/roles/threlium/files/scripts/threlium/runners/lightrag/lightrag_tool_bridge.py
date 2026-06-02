"""Parse tool_calls → msgspec Struct → wire VO → ``str`` для LightRAG."""
from __future__ import annotations

import msgspec
from litellm.types.utils import Message

from threlium.litellm_tool_bridge import parse_single_tool
from threlium.types.lightrag_tool_args import (
    ExtractKnowledgeGraphEntityToolArgs,
    ExtractKnowledgeGraphGleaningToolArgs,
    ExtractQueryKeywordsToolArgs,
    GenerateRagAnswerToolArgs,
    SummarizeDescriptionsToolArgs,
)
from threlium.types.lightrag_tool_function import LightragToolBridgeError
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
    return parse_single_tool(
        msg,
        expected=phase.tool_name,
        tool_spec_path=phase.tool_spec_path,
        args_type=phase.args_type,
        bridge_error=LightragToolBridgeError,
        context=f"LightRAG phase {phase.call_site.value}",
    )


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
    if isinstance(
        args,
        (ExtractKnowledgeGraphEntityToolArgs, ExtractKnowledgeGraphGleaningToolArgs),
    ):
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
