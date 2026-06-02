"""Источник вызова LiteLLM для e2e HTTP-заголовков (граница ``.value`` → wire).

Контракт (``docs/TYPES.md`` § уровень доменных строк):

* для **chat completion с одним tool** значение ``X-Threlium-Call-Site`` **строго**
  равно ``tools[0].function.name`` — гранулярная идентификация места вызова без
  инспекции тела (``bodyPatterns``); инвариант проверяется в
  :func:`~threlium.litellm_client.merge_litellm_call_kwargs_and_log`;
* не-tool вызовы (LightRAG embedding / rerank) и TLS-fallback используют отдельные
  «pipeline» значения (``lightrag_index`` / ``lightrag_query`` / ``lightrag_query_rerank`` /
  ``fsm``), которые **никогда** не появляются на ``/chat/completions`` с tool;
* reasoning multi-tool — единственный chat-вызов с ``len(tools) > 1``; его call_site =
  :attr:`~LitellmCallSite.REASONING` (а при единственном разрешённом route — ``function.name`` этого tool).

Фаза LightRAG (entity / gleaning / summarize / keywords / response) определяется в
рантайме по сигналам ``llm_func`` —
:func:`~threlium.types.lightrag_tool_phase.detect_lightrag_call_site_wire` — и тоже
маппится на ``function.name`` соответствующего tool.
"""
from __future__ import annotations

from enum import StrEnum


class LitellmCallSite(StrEnum):
    """Замкнутый набор wire-значений ``X-Threlium-Call-Site``.

    Для tool-вызовов значение совпадает с ``function.name`` единственного tool.
    """

    # --- Не-tool / pipeline-маркеры (значение != function.name) ---
    FSM = "fsm"
    LIGHTRAG_INDEX = "lightrag_index"
    LIGHTRAG_QUERY = "lightrag_query"
    LIGHTRAG_QUERY_RERANK = "lightrag_query_rerank"

    # --- Chat tool calls: значение == function.name единственного tool ---
    INGRESS_DISTILL = "ingress_distill"
    CONFIRM_CLI_HITL = "confirm_cli_hitl"
    ENRICH_TASK_PLAN = "enrich_task_plan"
    ENRICH_TASK_HYPOTHESES = "enrich_task_hypotheses"
    ENRICH_QUERY_PLAN = "enrich_query_plan"
    SUMMARIZE_THREAD_CONTEXT = "summarize_thread_context"
    SUMMARIZE_RESPONSE_BUFFER = "summarize_response_buffer"

    EXTRACT_KNOWLEDGE_GRAPH = "extract_knowledge_graph"
    EXTRACT_KNOWLEDGE_GRAPH_GLEANING = "extract_knowledge_graph_gleaning"
    SUMMARIZE_DESCRIPTIONS = "summarize_descriptions"
    EXTRACT_QUERY_KEYWORDS = "extract_query_keywords"
    GENERATE_RAG_ANSWER = "generate_rag_answer"

    # --- Reasoning umbrella (multi-tool) ---
    REASONING = "reasoning"


# Pipeline-маркеры и tool-имена LightRAG индексации: общий счётчик
# ``X-Threlium-Litellm-Req-Seq`` (отдельная ось от FSM-конвейера).
LIGHTRAG_INDEX_CALL_SITES: frozenset[str] = frozenset(
    {
        LitellmCallSite.LIGHTRAG_INDEX.value,
        LitellmCallSite.EXTRACT_KNOWLEDGE_GRAPH.value,
        LitellmCallSite.EXTRACT_KNOWLEDGE_GRAPH_GLEANING.value,
        LitellmCallSite.SUMMARIZE_DESCRIPTIONS.value,
    }
)


__all__ = ["LIGHTRAG_INDEX_CALL_SITES", "LitellmCallSite"]
