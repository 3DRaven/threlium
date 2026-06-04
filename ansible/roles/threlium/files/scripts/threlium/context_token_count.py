"""Подсчёт токенов и обрезка по токенам для enrich/reasoning (токенайзер LightRAG).

Единый контракт токенайзера (``docs/CONTEXT_CONTRACT.md``): та же tiktoken-модель, что
LightRAG использует для чанкинга и усечения retrieval-контекста, считает enrich-бюджеты —
cap графового запроса (шаг 4), cap промпта гипотез (шаг 7), token-ledger reasoning (шаг 9)
и pack суммаризации. Так «токены, которые мы считаем» == «токены, которые видит модель».

Бюджет одного hop к LLM-сайту:

    budget = model_context_tokens − max_tokens(site) − overhead − safety_margin

где ``max_tokens(site)`` — лимит ответа выбранного эндпоинта (резерв под генерацию),
``overhead`` — фиксированная обвязка промпта/tool-spec, ``safety_margin`` — запас.
"""
from __future__ import annotations

from functools import lru_cache

from lightrag.utils import TiktokenTokenizer, Tokenizer

from threlium.settings import ThreliumSettings, resolve_llm_endpoint
from threlium.types import LitellmRoutingSite

_DEFAULT_TIKTOKEN_MODEL = "gpt-4o-mini"


@lru_cache(maxsize=8)
def _tokenizer_for_model(model_name: str) -> Tokenizer:
    return TiktokenTokenizer(model_name or _DEFAULT_TIKTOKEN_MODEL)


def build_tokenizer(settings: ThreliumSettings) -> Tokenizer:
    """Токенайзер LightRAG из ``lightrag.tiktoken_model_name`` (кэшируется по имени модели)."""
    return _tokenizer_for_model(settings.lightrag.tiktoken_model_name)


def count_tokens(tokenizer: Tokenizer, text: str) -> int:
    if not text:
        return 0
    return len(tokenizer.encode(text))


def trim_from_end_tokens(tokenizer: Tokenizer, text: str, max_tokens: int) -> str:
    """Оставить **начало** строки, отбросить токены **с конца** до ``max_tokens``.

    Порядок секций промпта (user → hints/subtasks → unified newest-first) даёт физический
    хвост = старые письма, поэтому обрезка с конца снимает их первыми, не задевая intent.
    """
    if max_tokens <= 0:
        return ""
    ids = tokenizer.encode(text)
    if len(ids) <= max_tokens:
        return text
    return tokenizer.decode(ids[:max_tokens])


def site_max_tokens(settings: ThreliumSettings, site: LitellmRoutingSite) -> int:
    """Лимит ответа выбранного эндпоинта сайта (резерв под генерацию); ``None`` → 0."""
    ep = resolve_llm_endpoint(settings.litellm, site)
    return ep.max_tokens or 0


def _budget(
    settings: ThreliumSettings, site: LitellmRoutingSite, overhead_tokens: int
) -> int:
    e = settings.enrich
    return max(
        0,
        e.model_context_tokens
        - site_max_tokens(settings, site)
        - overhead_tokens
        - e.context_safety_margin_tokens,
    )


def lightrag_query_budget(settings: ThreliumSettings) -> int:
    """Шаг 4: бюджет токенов строки запроса к LightRAG (keyword + rag_response shell)."""
    return _budget(
        settings,
        LitellmRoutingSite.LIGHTRAG_LLM,
        settings.enrich.lightrag_query_overhead_tokens,
    )


def hypotheses_prompt_budget(settings: ThreliumSettings) -> int:
    """Шаг 7: бюджет токенов промпта ``enrich_task_hypotheses`` (tool spec + preamble)."""
    return _budget(
        settings,
        LitellmRoutingSite.ENRICH_TASK_HYPOTHESES,
        settings.enrich.enrich_task_hypotheses_overhead_tokens,
    )


def reasoning_effective_budget(settings: ThreliumSettings) -> int:
    """Шаг 9: бюджет токенов контекста reasoning (reasoning/user.j2 + system shell)."""
    return _budget(
        settings,
        LitellmRoutingSite.REASONING,
        settings.enrich.reasoning_overhead_tokens,
    )


def summarize_content_budget(settings: ThreliumSettings) -> int:
    """summarize_context: бюджет токенов под new-history-блок (system+user shell)."""
    return _budget(
        settings,
        LitellmRoutingSite.SUMMARIZE_CONTEXT,
        settings.enrich.summarize_overhead_tokens,
    )


__all__ = [
    "build_tokenizer",
    "count_tokens",
    "trim_from_end_tokens",
    "site_max_tokens",
    "lightrag_query_budget",
    "hypotheses_prompt_budget",
    "reasoning_effective_budget",
    "summarize_content_budget",
]
