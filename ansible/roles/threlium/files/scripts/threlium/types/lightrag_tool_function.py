"""Имена OpenAI tools для LLM-фаз LightRAG."""
from __future__ import annotations

from enum import nonmember

from ._core import ToolFunctionNameBase


class LightragToolBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → wire для LightRAG."""


class LightragToolFunctionName(ToolFunctionNameBase):
    EXTRACT_KNOWLEDGE_GRAPH = "extract_knowledge_graph"
    EXTRACT_KNOWLEDGE_GRAPH_GLEANING = "extract_knowledge_graph_gleaning"
    SUMMARIZE_DESCRIPTIONS = "summarize_descriptions"
    EXTRACT_QUERY_KEYWORDS = "extract_query_keywords"
    GENERATE_RAG_ANSWER = "generate_rag_answer"

    _bridge_error = nonmember(LightragToolBridgeError)
    _label = nonmember("LightRAG")


__all__ = ["LightragToolBridgeError", "LightragToolFunctionName"]
