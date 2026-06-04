"""Prose ``<graph-answer>`` из LightRAG envelope (strict parse + Jinja)."""
from __future__ import annotations

import copy
from typing import Any

from pydantic import ValidationError

from threlium.prompts import render_prompt
from threlium.settings import EnrichSettings
from threlium.types.lightrag_query import GraphAnswerView, LightragQueryData
from threlium.types.prompt_path import PromptPath

try:
    from lightrag.api.routers.query_routes import QueryDataResponse
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("graph_answer_view requires lightrag-hku[api]") from exc


def _envelope_has_content(envelope: dict[str, Any]) -> bool:
    if not envelope.get("ok"):
        return False
    lr = envelope.get("lightrag")
    if not isinstance(lr, dict):
        return False
    llm_text = _llm_text(envelope)
    return bool(llm_text or (isinstance(lr, dict) and lr.get("raw")))


def _formulated_query(envelope: dict[str, Any]) -> str:
    th = envelope.get("threlium")
    if isinstance(th, dict):
        fq = th.get("formulated_query")
        if isinstance(fq, str) and fq.strip():
            return fq.strip()
    return ""


def _llm_text(envelope: dict[str, Any]) -> str | None:
    lr = envelope.get("lightrag")
    if not isinstance(lr, dict):
        return None
    llm_text = lr.get("llm_text")
    if not isinstance(llm_text, str):
        return None
    stripped = llm_text.strip()
    if not stripped or stripped == "(no graph context)":
        return None
    return stripped


def _parse_lightrag_query_data(raw: dict[str, Any]) -> LightragQueryData:
    payload = copy.deepcopy(raw)
    payload.pop("llm_response", None)
    try:
        resp = QueryDataResponse.model_validate(payload)
    except ValidationError as exc:
        raise RuntimeError(f"graph_answer: invalid QueryDataResponse: {exc}") from exc
    if resp.status != "success":
        raise RuntimeError(
            f"graph_answer: lightrag status={resp.status!r} message={resp.message!r}"
        )
    data = resp.data
    if not isinstance(data, dict):
        raise RuntimeError(
            f"graph_answer: expected data dict, got {type(data).__name__!r}"
        )
    return LightragQueryData.from_wire(data)


def build_graph_answer_view(
    envelope: dict[str, Any],
    *,
    limits: EnrichSettings,
) -> GraphAnswerView | None:
    """Strict parse → view for Jinja; ``None`` если нет контента (как пустой graph)."""
    if not _envelope_has_content(envelope):
        return None

    query_api = str(envelope.get("query_api") or "")
    formulated = _formulated_query(envelope)
    answer = _llm_text(envelope)
    lr = envelope.get("lightrag")
    raw = lr.get("raw") if isinstance(lr, dict) else None

    if query_api == "aquery":
        if not answer:
            return None
        return GraphAnswerView(
            formulated_query=formulated,
            answer=answer,
            entities=(),
            relations=(),
        )

    if not isinstance(raw, dict):
        raise RuntimeError(
            f"graph_answer: expected lightrag.raw dict for {query_api!r}, "
            f"got {type(raw).__name__!r}"
        )

    data = _parse_lightrag_query_data(raw)
    view = GraphAnswerView.from_query_data(
        formulated_query=formulated,
        answer=answer if query_api == "aquery_llm" else None,
        data=data,
        max_entities=limits.graph_answer_max_entities,
        max_relations=limits.graph_answer_max_relations,
        desc_max_chars=limits.graph_answer_desc_max_chars,
    )
    if not view.has_subgraph() and not view.answer:
        return None
    return view


def format_graph_answer_part(
    envelope: dict[str, Any],
    enrich_cfg: EnrichSettings,
) -> str | None:
    """Plain-text тело ``<graph-answer>`` (Jinja); ``None`` = часть не attach'ится (TYPES present-or-None)."""
    view = build_graph_answer_view(envelope, limits=enrich_cfg)
    if view is None:
        return None

    query_api = str(envelope.get("query_api") or "")
    kwargs = view.for_graph_answer_jinja()

    if query_api == "aquery":
        path = PromptPath.LIGHTRAG_GRAPH_ANSWER_ANSWER_ONLY
    elif query_api == "aquery_data":
        path = PromptPath.LIGHTRAG_GRAPH_ANSWER
    elif view.has_subgraph():
        path = PromptPath.LIGHTRAG_GRAPH_ANSWER
    else:
        path = PromptPath.LIGHTRAG_GRAPH_ANSWER_ANSWER_ONLY

    text = render_prompt(path, **kwargs).strip()
    return text or None


__all__ = [
    "build_graph_answer_view",
    "format_graph_answer_part",
]
