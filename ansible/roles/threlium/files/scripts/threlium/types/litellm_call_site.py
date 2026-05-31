"""Источник вызова LiteLLM для e2e HTTP-заголовков (граница ``.value`` → wire).

Гранулярные значения ``lightrag_index_*`` / ``lightrag_query_*`` позволяют
WireMock-стабам различать фазы LightRAG **без** инспекции тела запроса
(``bodyPatterns``). Детекция фазы — в ``types.lightrag_tool_phase.detect_lightrag_call_site_wire``.
"""
from __future__ import annotations

from enum import StrEnum


class LitellmCallSite(StrEnum):
    """Узкий замкнутый набор: см. ``docs/TYPES.md`` § уровень доменных строк.

    Значения ``lightrag_index`` / ``lightrag_query`` — базовые; гранулярные
    подфазы (``*_entity``, ``*_gleaning``, …) определяются в рантайме
    по сигналам ``llm_func`` (``keyword_extraction``, ``history_messages``,
    ``system_prompt``), не по содержимому промптов — см.
    :func:`~threlium.types.lightrag_tool_phase.detect_lightrag_call_site_wire`.
    """

    FSM = "fsm"
    CLI_HITL_RESUME = "cli_hitl_resume"
    SUMMARIZE_CONTEXT = "summarize_context"
    INGRESS_DISTILL = "ingress_distill"

    LIGHTRAG_INDEX = "lightrag_index"
    LIGHTRAG_INDEX_ENTITY = "lightrag_index_entity"
    LIGHTRAG_INDEX_GLEANING = "lightrag_index_gleaning"
    LIGHTRAG_INDEX_SUMMARIZE = "lightrag_index_summarize"

    LIGHTRAG_QUERY = "lightrag_query"
    LIGHTRAG_QUERY_KEYWORDS = "lightrag_query_keywords"
    LIGHTRAG_QUERY_RESPONSE = "lightrag_query_response"
    LIGHTRAG_QUERY_RERANK = "lightrag_query_rerank"
