"""RAG instance construction and e2e correlation bridge installation."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from lightrag import LightRAG
from lightrag.llm_roles import RoleLLMConfig
from lightrag.utils import EmbeddingFunc

from threlium.lightrag_chunking import threlium_email_chunking_func
from threlium.lightrag_prompts import install_overlay
from threlium.settings import (
    ThreliumSettings,
    resolve_llm_endpoint,
    resolve_embedding_endpoint,
    resolve_rerank_endpoint,
)
from threlium.types import (
    LitellmCallSite,
    LitellmRoutingSite,
)

from threlium.runners.lightrag._adapters import (
    CallSiteResolver,
    build_embedding_func,
    build_llm_func,
    build_rerank_func,
    extract_call_site,
    fixed_call_site,
)
from threlium.logutil import logger

log = logger.bind(stage="lightrag")


def _chunk_dims(settings: ThreliumSettings) -> tuple[int, int]:
    body_max = max(64, settings.lightrag.chunk_body_tokens)
    pct = max(0, min(99, settings.lightrag.chunk_body_overlap_pct))
    overlap = max(0, min(body_max - 1, int(body_max * pct / 100)))
    return body_max, overlap


def _working_dir(settings: ThreliumSettings) -> Path:
    raw = settings.lightrag.working_dir.strip()
    if not raw:
        raw = str(settings.home / "lightrag")
    p = Path(raw).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _embed_dim(settings: ThreliumSettings) -> int:
    raw = settings.lightrag.embed_dim.strip()
    return int(raw or "1536")


def _embed_max_tokens(settings: ThreliumSettings) -> int:
    raw = settings.lightrag.embed_max_tokens.strip()
    return int(raw or "8192")


_DEFAULT_ENTITY_TYPES = (
    "person,organization,location,concept,event,technology,document"
)


def _addon_params(settings: ThreliumSettings) -> dict[str, object]:
    language = settings.lightrag.language.strip() or "Russian"
    raw = settings.lightrag.entity_types.strip() or _DEFAULT_ENTITY_TYPES
    entity_types = [t.strip() for t in raw.split(",") if t.strip()]
    if not entity_types:
        entity_types = [t.strip() for t in _DEFAULT_ENTITY_TYPES.split(",")]
    return {"language": language, "entity_types": entity_types}


def build_rag(settings: ThreliumSettings) -> LightRAG:
    """Construct LightRAG instance from settings (not yet initialized)."""
    install_overlay(settings)

    body_max, overlap_toks = _chunk_dims(settings)
    llm_ep = resolve_llm_endpoint(settings.litellm, LitellmRoutingSite.LIGHTRAG_LLM)
    embed_ep = resolve_embedding_endpoint(settings.litellm)
    log.info(
        "litellm_routing",
        site=LitellmRoutingSite.LIGHTRAG_LLM.value,
        score=llm_ep.score,
        embedding_score=embed_ep.embedding_score,
    )
    rerank_ep = resolve_rerank_endpoint(settings.litellm)
    if rerank_ep is not None:
        log.info("litellm_routing_rerank", rerank_score=rerank_ep.rerank_score)

    # Отдельная llm-функция на каждую роль-точку вызова LightRAG (1.5 role_llm_configs). call-site и tool-spec
    # детерминированы РЕЗОЛВЕРОМ точки (без сниффинга формата): keyword/query → константа, extract → структурный
    # (одна роль LightRAG = entity/gleaning/summarize). База = extract. max_async/timeout наследуют базовые.
    def _role_llm(resolve_call_site: CallSiteResolver) -> Any:
        return build_llm_func(
            settings,
            llm_ep=llm_ep,
            default_max_retries=settings.litellm.max_retries,
            chat_template_kwargs=llm_ep.chat_template_kwargs or None,
            resolve_call_site=resolve_call_site,
        )

    extract_llm = _role_llm(extract_call_site)
    rag_kwargs: dict[str, Any] = {
        "working_dir": str(_working_dir(settings)),
        "llm_model_func": extract_llm,
        "role_llm_configs": {
            "extract": RoleLLMConfig(func=extract_llm),
            "keyword": RoleLLMConfig(func=_role_llm(fixed_call_site(LitellmCallSite.EXTRACT_QUERY_KEYWORDS))),
            "query": RoleLLMConfig(func=_role_llm(fixed_call_site(LitellmCallSite.GENERATE_RAG_ANSWER))),
        },
        "embedding_func": EmbeddingFunc(
            embedding_dim=_embed_dim(settings),
            max_token_size=_embed_max_tokens(settings),
            func=build_embedding_func(
                settings,
                embed_ep=embed_ep,
                default_max_retries=settings.litellm.max_retries,
            ),
        ),
        "addon_params": _addon_params(settings),
        "kv_storage": "JsonKVStorage",
        "vector_storage": "NanoVectorDBStorage",
        "graph_storage": "NetworkXStorage",
        "doc_status_storage": "JsonDocStatusStorage",
        "chunk_token_size": body_max,
        "chunk_overlap_token_size": overlap_toks,
        "chunking_func": threlium_email_chunking_func,
        "tiktoken_model_name": settings.lightrag.tiktoken_model_name,
        # JSON-режим извлечения сущностей (1.5): LightRAG шлёт entity_extraction_json_* промпт и
        # парсит ответ как нативный JSON {entities, relationships}. Наш tool-bridge форсит ровно эту
        # схему (tool spec = JSON LightRAG) → constrained decoding vLLM даёт валидный JSON.
        "entity_extraction_use_json": True,
    }
    if rerank_ep is not None:
        rag_kwargs["rerank_model_func"] = build_rerank_func(
            settings,
            rerank_ep=rerank_ep,
            default_max_retries=settings.litellm.max_retries,
        )
    if settings.e2e.litellm_route_correlation:
        # max_async НЕ обязан быть 1 для детерминизма: корреляция теперь per-call (ctxvar call-site +
        # thread-root, штампится на каждый запрос), а стабы матчатся по X-Threlium-Call-Site + hasContext
        # (thread-root) — БЕЗ зависимости от порядка вызовов (ни seq, ни phase-state в RAG-фазах; phase-state
        # только у FSM/reasoning-стабов, а это прямые litellm-вызовы вне RAG-loop). Параллельные LLM/embed —
        # ключевой разлок -n2: иначе ВСЕ вызовы (индексация+запросы) обоих тестов сериализуются на одном RAG-loop.
        # Внутритредовый порядок сохранён и так (последовательные await в aquery; per-thread-root lock для
        # ainsert↔aquery). max_parallel_insert=1 оставляем (drain — singleton-задача, insert_batch=1).
        rag_kwargs["llm_model_max_async"] = settings.lightrag.llm_model_max_async
        rag_kwargs["embedding_func_max_async"] = settings.lightrag.embedding_func_max_async
        rag_kwargs["max_parallel_insert"] = 1
    else:
        rag_kwargs["llm_model_max_async"] = settings.lightrag.llm_model_max_async
        rag_kwargs["embedding_func_max_async"] = settings.lightrag.embedding_func_max_async
        rag_kwargs["max_parallel_insert"] = settings.lightrag.max_parallel_insert
    return LightRAG(**rag_kwargs)
