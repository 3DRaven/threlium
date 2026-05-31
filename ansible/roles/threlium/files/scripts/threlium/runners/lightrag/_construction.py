"""RAG instance construction and e2e correlation bridge installation."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from lightrag import LightRAG
from lightrag.utils import EmbeddingFunc

from threlium.lightrag_chunking import threlium_email_chunking_func
from threlium.lightrag_prompts import install_overlay
from threlium.litellm_route_context import get_litellm_correlation_from_ctxvar
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
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader

from threlium.runners.lightrag._adapters import build_llm_func, build_embedding_func, build_rerank_func
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
    log.info("litellm_routing", site=LitellmRoutingSite.LIGHTRAG_LLM.value, score=llm_ep.score, embedding_score=embed_ep.embedding_score)
    rerank_ep = resolve_rerank_endpoint(settings.litellm)
    if rerank_ep is not None:
        log.info("litellm_routing_rerank", rerank_score=rerank_ep.rerank_score)

    rag_kwargs: dict[str, Any] = {
        "working_dir": str(_working_dir(settings)),
        "llm_model_func": build_llm_func(
            settings,
            llm_ep=llm_ep,
            default_max_retries=settings.litellm.max_retries,
            chat_template_kwargs=llm_ep.chat_template_kwargs or None,
        ),
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
    }
    if rerank_ep is not None:
        rag_kwargs["rerank_model_func"] = build_rerank_func(
            settings,
            rerank_ep=rerank_ep,
            default_max_retries=settings.litellm.max_retries,
        )
    if settings.e2e.litellm_route_correlation:
        rag_kwargs["llm_model_max_async"] = 1
        rag_kwargs["embedding_func_max_async"] = 1
        rag_kwargs["max_parallel_insert"] = 1
    else:
        rag_kwargs["llm_model_max_async"] = settings.lightrag.llm_model_max_async
        rag_kwargs["embedding_func_max_async"] = settings.lightrag.embedding_func_max_async
        rag_kwargs["max_parallel_insert"] = settings.lightrag.max_parallel_insert
    return LightRAG(**rag_kwargs)


def install_e2e_correlation_bridge(rag: LightRAG) -> None:
    """Wrap llm/embedding/rerank funcs with ContextVar → kwargs bridge.

    Called ONLY when e2e_litellm_route_correlation is enabled, AFTER initialize_storages
    (when LightRAG has already installed priority_limit_async_func_call wrappers).
    """
    original_llm = rag.llm_model_func

    async def _llm_bridge(*args: Any, **kwargs: Any) -> Any:
        corr = get_litellm_correlation_from_ctxvar()
        if corr is not None:
            kwargs["_threlium_e2e_correlation"] = corr
        return await original_llm(*args, **kwargs)

    rag.llm_model_func = _llm_bridge

    if rag.embedding_func is not None:
        original_embed = rag.embedding_func.func

        async def _embed_bridge(texts: list[str], **kwargs: Any) -> Any:
            corr = get_litellm_correlation_from_ctxvar()
            if corr is not None:
                kwargs["_threlium_e2e_correlation"] = corr
            return await original_embed(texts, **kwargs)

        rag.embedding_func.func = _embed_bridge

    if rag.rerank_model_func is not None:
        original_rerank = rag.rerank_model_func

        async def _rerank_bridge(query: str, documents: list[str], **kwargs: Any) -> Any:
            corr = get_litellm_correlation_from_ctxvar()
            if corr is not None:
                corr_copy = dict(corr)
                corr_copy[LitellmCorrelationHeader.CALL_SITE.value] = (
                    LitellmCallSite.LIGHTRAG_QUERY_RERANK.value
                )
                kwargs["_threlium_e2e_correlation"] = corr_copy
            return await original_rerank(query=query, documents=documents, **kwargs)

        rag.rerank_model_func = _rerank_bridge
