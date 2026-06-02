"""Shared LightRAG ``aquery`` / ``aquery_data`` / ``aquery_llm`` dispatch."""
from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any

from lightrag import QueryParam

from threlium.runners.lightrag._lifecycle import daemon_lightrag, run_rag_coroutine
from threlium.settings import ThreliumSettings

_ALLOWED_QUERY_MODES = frozenset(
    {"local", "global", "hybrid", "naive", "mix", "bypass"}
)


def build_lightrag_query_param(cfg: ThreliumSettings) -> QueryParam:
    raw = (cfg.lightrag.query_mode or "hybrid").strip().lower()
    mode = raw if raw in _ALLOWED_QUERY_MODES else "hybrid"
    base = QueryParam()
    return replace(
        base,
        mode=mode,  # type: ignore[arg-type]
        top_k=cfg.lightrag.query_top_k,
        chunk_top_k=cfg.lightrag.query_chunk_top_k,
        max_total_tokens=cfg.lightrag.query_max_total_tokens,
        max_entity_tokens=cfg.lightrag.query_max_entity_tokens,
        max_relation_tokens=cfg.lightrag.query_max_relation_tokens,
        response_type=cfg.lightrag.query_response_type,
        enable_rerank=cfg.lightrag.enable_rerank,
    )


async def lightrag_query_raw(
    rag: object,
    query: str,
    *,
    settings: ThreliumSettings,
    system_prompt: str | None = None,
    param: QueryParam | None = None,
    query_api: str | None = None,
) -> dict[str, Any] | str | None:
    """Call configured ``query_api`` on a LightRAG instance (async, same loop as daemon)."""
    qparam = param or build_lightrag_query_param(settings)
    api = query_api or settings.lightrag.query_api

    if api == "aquery":
        raw = await rag.aquery(  # type: ignore[attr-defined]
            query,
            param=qparam,
            system_prompt=system_prompt,
        )
        if isinstance(raw, AsyncIterator):
            raise RuntimeError("LightRAG aquery streaming is not supported")
        if raw is None:
            return None
        if not isinstance(raw, str):
            raise RuntimeError(
                f"LightRAG aquery unexpected return type {type(raw).__name__!r}"
            )
        return raw.strip() or None
    if api == "aquery_data":
        return await rag.aquery_data(query, param=qparam)  # type: ignore[attr-defined]
    if api == "aquery_llm":
        return await rag.aquery_llm(  # type: ignore[attr-defined]
            query,
            param=qparam,
            system_prompt=system_prompt,
        )
    raise RuntimeError(f"LightRAG unknown query_api {api!r}")


def run_lightrag_aquery(
    query: str,
    *,
    settings: ThreliumSettings,
    correlation: dict[str, str] | None = None,
    system_prompt: str | None = None,
    param: QueryParam | None = None,
    query_api: str | None = None,
) -> dict[str, Any] | str | None:
    """Sync entry: daemon LightRAG + ``run_rag_coroutine`` for FSM thread callers."""
    rag = daemon_lightrag()
    if rag is None:
        raise RuntimeError("LightRAG daemon is not running (start_rag_loop_thread)")
    return run_rag_coroutine(
        lightrag_query_raw(
            rag,
            query,
            settings=settings,
            system_prompt=system_prompt,
            param=param,
            query_api=query_api,
        ),
        settings=settings,
        correlation=correlation,
    )


__all__ = [
    "build_lightrag_query_param",
    "lightrag_query_raw",
    "run_lightrag_aquery",
]
