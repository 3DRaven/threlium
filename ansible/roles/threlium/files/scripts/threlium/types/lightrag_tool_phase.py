"""Реестр фаз LightRAG LLM → tool spec / Struct / call-site."""
from __future__ import annotations

from dataclasses import dataclass
import msgspec

from threlium.types.litellm_call_site import LitellmCallSite
from threlium.types.lightrag_tool_args import (
    ExtractKnowledgeGraphToolArgs,
    ExtractQueryKeywordsToolArgs,
    GenerateRagAnswerToolArgs,
    SummarizeDescriptionsToolArgs,
)
from threlium.types.lightrag_tool_function import LightragToolFunctionName
from threlium.types.prompt_path import PromptPath


@dataclass(frozen=True, slots=True)
class LightragToolPhaseSpec:
    call_site: LitellmCallSite
    tool_name: LightragToolFunctionName
    tool_spec_path: PromptPath
    args_type: type[msgspec.Struct]


_PHASES: tuple[LightragToolPhaseSpec, ...] = (
    LightragToolPhaseSpec(
        call_site=LitellmCallSite.LIGHTRAG_INDEX_ENTITY,
        tool_name=LightragToolFunctionName.EXTRACT_KNOWLEDGE_GRAPH,
        tool_spec_path=PromptPath.LIGHTRAG_EXTRACT_KNOWLEDGE_GRAPH_TOOL_SPEC,
        args_type=ExtractKnowledgeGraphToolArgs,
    ),
    LightragToolPhaseSpec(
        call_site=LitellmCallSite.LIGHTRAG_INDEX_GLEANING,
        tool_name=LightragToolFunctionName.EXTRACT_KNOWLEDGE_GRAPH,
        tool_spec_path=PromptPath.LIGHTRAG_EXTRACT_KNOWLEDGE_GRAPH_TOOL_SPEC,
        args_type=ExtractKnowledgeGraphToolArgs,
    ),
    LightragToolPhaseSpec(
        call_site=LitellmCallSite.LIGHTRAG_INDEX_SUMMARIZE,
        tool_name=LightragToolFunctionName.SUMMARIZE_DESCRIPTIONS,
        tool_spec_path=PromptPath.LIGHTRAG_SUMMARIZE_DESCRIPTIONS_TOOL_SPEC,
        args_type=SummarizeDescriptionsToolArgs,
    ),
    LightragToolPhaseSpec(
        call_site=LitellmCallSite.LIGHTRAG_QUERY_KEYWORDS,
        tool_name=LightragToolFunctionName.EXTRACT_QUERY_KEYWORDS,
        tool_spec_path=PromptPath.LIGHTRAG_EXTRACT_QUERY_KEYWORDS_TOOL_SPEC,
        args_type=ExtractQueryKeywordsToolArgs,
    ),
    LightragToolPhaseSpec(
        call_site=LitellmCallSite.LIGHTRAG_QUERY_RESPONSE,
        tool_name=LightragToolFunctionName.GENERATE_RAG_ANSWER,
        tool_spec_path=PromptPath.LIGHTRAG_GENERATE_RAG_ANSWER_TOOL_SPEC,
        args_type=GenerateRagAnswerToolArgs,
    ),
)

_BY_CALL_SITE: dict[str, LightragToolPhaseSpec] = {
    p.call_site.value: p for p in _PHASES
}


def lightrag_tool_phase_for_call_site(call_site_wire: str) -> LightragToolPhaseSpec:
    spec = _BY_CALL_SITE.get(call_site_wire)
    if spec is None:
        raise RuntimeError(f"lightrag: no tool phase for call_site={call_site_wire!r}")
    return spec


def detect_lightrag_call_site_wire(
    base_call_site: str | None,
    *,
    keyword_extraction: bool,
    has_history: bool,
    has_system_prompt: bool,
) -> str:
    """Гранулярный ``X-Threlium-Call-Site`` (перенос из ``_detect_lightrag_phase``)."""
    if base_call_site == LitellmCallSite.LIGHTRAG_QUERY.value:
        if keyword_extraction:
            return LitellmCallSite.LIGHTRAG_QUERY_KEYWORDS.value
        return LitellmCallSite.LIGHTRAG_QUERY_RESPONSE.value

    if keyword_extraction:
        return LitellmCallSite.LIGHTRAG_QUERY_KEYWORDS.value

    if has_history:
        return LitellmCallSite.LIGHTRAG_INDEX_GLEANING.value
    if not has_system_prompt:
        return LitellmCallSite.LIGHTRAG_INDEX_SUMMARIZE.value
    return LitellmCallSite.LIGHTRAG_INDEX_ENTITY.value


__all__ = [
    "LightragToolPhaseSpec",
    "detect_lightrag_call_site_wire",
    "lightrag_tool_phase_for_call_site",
]
