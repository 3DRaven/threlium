"""Сайты резолва LiteLLM routing (отдельно от :class:`~threlium.types.litellm_call_site.LitellmCallSite` e2e)."""
from __future__ import annotations

from enum import StrEnum


class LitellmRoutingSite(StrEnum):
    """Ключи ``targets`` в JSON маршрутизации и точки вызова в коде."""

    CLI_HITL_RESUME = "cli_hitl_resume"
    REASONING = "reasoning"
    ENRICH_PLAN = "enrich_plan"
    RESPONSE_OBSERVE = "response_observe"
    LIGHTRAG_LLM = "lightrag_llm"
    LIGHTRAG_EMBEDDING = "lightrag_embedding"
    LIGHTRAG_RERANK = "lightrag_rerank"
    SUMMARIZE_CONTEXT = "summarize_context"
