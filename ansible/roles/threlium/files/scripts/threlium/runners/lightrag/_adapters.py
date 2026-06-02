"""LiteLLM adapters: llm_func / embedding_func / rerank_func builders for LightRAG."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import jsonschema
import numpy as np
from litellm.types.utils import Embedding

from threlium.litellm_client import litellm_aembedding, litellm_arerank
from threlium.litellm_required_tool import ainvoke_with_bridge_retries, build_site_call
from threlium.litellm_tool_completion import acompletion_required_tool
from threlium.litellm_tool_response import LiteLlmToolResponseError
from threlium.litellm_tool_spec import load_tool_spec
from threlium.litellm_wire import require_embedding_response
from threlium.settings import (
    ThreliumSettings,
    LlmEndpoint,
    EmbeddingEndpoint,
    RerankEndpoint,
)
from threlium.types import (
    LitellmCallSite,
    LiteLlmArerankKwargs,
    LiteLlmAembeddingKwargs,
    LiteLlmChatMessage,
    lite_llm_aembedding_to_dict,
    lite_llm_arerank_to_dict,
)
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader
from threlium.types.lightrag_tool_function import LightragToolBridgeError
from threlium.types.lightrag_tool_phase import (
    detect_lightrag_call_site_wire,
    lightrag_tool_phase_for_call_site,
)

from threlium.logutil import logger

from .lightrag_tool_bridge import (
    parse_tool_call_for_phase,
    struct_to_lightrag_wire,
    to_lightrag_return_value,
)

log = logger.bind(stage="lightrag")

_MAX_LIGHTRAG_TOOL_BRIDGE_RETRIES = 2


def build_llm_func(
    settings: ThreliumSettings,
    *,
    llm_ep: LlmEndpoint,
    default_max_retries: int,
    chat_template_kwargs: dict[str, Any] | None = None,
) -> Callable[..., Awaitable[str]]:
    closure_max_tokens = llm_ep.max_tokens
    closure_ctk = chat_template_kwargs or llm_ep.chat_template_kwargs or None

    async def llm_func(
        prompt: str,
        system_prompt: str | None = None,
        history_messages: list[dict] | None = None,
        keyword_extraction: bool = False,
        max_tokens: int | None = None,
        hashing_kv: object | None = None,
        _priority: int = 10,
        enable_cot: bool = False,
        stream: bool | None = None,
        **kwargs: Any,
    ) -> str:
        correlation: dict[str, str] | None = kwargs.pop(
            "_threlium_e2e_correlation", None
        )
        if correlation is not None:
            base_cs = correlation.get(LitellmCorrelationHeader.CALL_SITE.value)
            granular_cs = detect_lightrag_call_site_wire(
                base_cs,
                keyword_extraction=keyword_extraction,
                has_history=bool(history_messages),
                has_system_prompt=bool(system_prompt),
            )
            correlation[LitellmCorrelationHeader.CALL_SITE.value] = granular_cs

        unsupported: list[str] = []
        if stream is True:
            unsupported.append("stream=True(ignored)")
        if enable_cot:
            unsupported.append("enable_cot=True(no-op)")
        if kwargs:
            unsupported.append(f"unknown_kwargs={sorted(kwargs.keys())}")
        if unsupported:
            log.debug("llm_func_unsupported_args", args=unsupported)

        effective_max = max_tokens if max_tokens is not None else closure_max_tokens

        messages: list[dict] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        for m in history_messages or []:
            messages.append(m)
        messages.append({"role": "user", "content": prompt})
        litellm_messages = [
            LiteLlmChatMessage(role=str(m["role"]), content=str(m["content"]))
            for m in messages
        ]

        if correlation is not None:
            call_site_wire = str(
                correlation[LitellmCorrelationHeader.CALL_SITE.value]
            )
        else:
            call_site_wire = detect_lightrag_call_site_wire(
                LitellmCallSite.LIGHTRAG_INDEX.value,
                keyword_extraction=keyword_extraction,
                has_history=bool(history_messages),
                has_system_prompt=bool(system_prompt),
            )
        phase = lightrag_tool_phase_for_call_site(call_site_wire)
        tool_spec = load_tool_spec(phase.tool_spec_path)
        tools = [tool_spec]

        call = build_site_call(
            settings,
            None,
            litellm_messages,
            endpoint=llm_ep,
            max_tokens=effective_max,
            chat_template_kwargs=closure_ctk,
        )

        async def _attempt() -> str:
            resp = await acompletion_required_tool(
                settings=settings,
                call=call,
                tools=tools,
                correlation_override=correlation,
            )
            msg_obj = resp.choices[0].message
            if msg_obj is None:
                raise RuntimeError("LightRAG LLM bridge: empty assistant message")
            args_struct = parse_tool_call_for_phase(msg_obj, phase)
            wire = struct_to_lightrag_wire(phase, args_struct)
            result = to_lightrag_return_value(wire)
            log.debug(
                "lightrag_tool_call",
                phase=phase.call_site.value,
                tool_name=phase.tool_name.value,
            )
            return result

        def _on_retry(attempt_no: int, exc: BaseException) -> None:
            log.warning(
                "lightrag_tool_bridge_retry",
                attempt=attempt_no,
                call_site=call_site_wire,
                error=str(exc),
            )

        return await ainvoke_with_bridge_retries(
            max_attempts=_MAX_LIGHTRAG_TOOL_BRIDGE_RETRIES + 1,
            attempt=_attempt,
            retry_errors=(
                LiteLlmToolResponseError,
                LightragToolBridgeError,
                jsonschema.ValidationError,
            ),
            on_retry=_on_retry,
        )

    return llm_func


def build_embedding_func(
    settings: ThreliumSettings,
    *,
    embed_ep: EmbeddingEndpoint,
    default_max_retries: int,
):
    mr_def = default_max_retries

    async def embed_func(texts: list[str], **_kwargs: Any):
        correlation: dict[str, str] | None = _kwargs.pop(
            "_threlium_e2e_correlation", None
        )
        mr = embed_ep.max_retries if embed_ep.max_retries is not None else mr_def
        call = LiteLlmAembeddingKwargs(
            model=embed_ep.model,
            embedding_input=texts,
            timeout=float(embed_ep.timeout),
            max_retries=mr,
            api_key=embed_ep.api_key,
            api_base=embed_ep.api_base,
            encoding_format=embed_ep.encoding_format,
        )
        call_kwargs = lite_llm_aembedding_to_dict(call)
        resp = require_embedding_response(
            await litellm_aembedding(settings=settings, **call_kwargs, correlation_override=correlation)
        )
        data: list[Embedding] = list(resp.data or [])
        return np.array([item.embedding for item in data], dtype=np.float32)

    return embed_func


def build_rerank_func(
    settings: ThreliumSettings,
    *,
    rerank_ep: RerankEndpoint,
    default_max_retries: int,
) -> Callable[..., Awaitable[list[dict[str, Any]]]]:
    mr_def = default_max_retries

    async def rerank_func(
        query: str,
        documents: list[str],
        top_n: int | None = None,
        **_kwargs: Any,
    ) -> list[dict[str, Any]]:
        correlation: dict[str, str] | None = _kwargs.pop(
            "_threlium_e2e_correlation", None
        )
        mr = rerank_ep.max_retries if rerank_ep.max_retries is not None else mr_def
        effective_top_n = top_n if top_n is not None else rerank_ep.top_n
        call = LiteLlmArerankKwargs(
            model=rerank_ep.model,
            query=query,
            documents=documents,
            timeout=float(rerank_ep.timeout),
            max_retries=mr,
            api_key=rerank_ep.api_key,
            api_base=rerank_ep.api_base,
            top_n=effective_top_n,
            custom_llm_provider="hosted_vllm",
        )
        call_kwargs = lite_llm_arerank_to_dict(call)
        resp = await litellm_arerank(
            settings=settings,
            **call_kwargs,
            correlation_override=correlation,
        )
        return [
            {"index": r["index"], "relevance_score": r["relevance_score"]}
            for r in (resp.results or [])
        ]

    return rerank_func
