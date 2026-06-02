"""Единая точка исходящего chat completion с ``tool_choice=required`` (один tool).

Контракт (см. ``docs/TYPES.md`` § tool bridge):

* ровно **один** tool на HTTP-вызов;
* ``X-Threlium-Call-Site`` корреляции = ``tools[0].function.name`` (гранулярная
  e2e-идентификация места вызова без инспекции тела);
* доменный разбор ответа — в ``*_tool_bridge`` модулях через
  :func:`~threlium.litellm_tool_response.require_single_tool_call`.

Сам ``litellm_completion`` / ``litellm_site_*`` из продуктового кода не вызывается —
только через этот модуль (reasoning multi-tool использует
:func:`correlation_with_call_site` + :func:`~threlium.litellm_tool_completion.completion_required_tool_sync`).
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from litellm.types.utils import Message

from threlium.litellm_tool_completion import (
    acompletion_required_tool,
    completion_required_tool_sync,
)
from threlium.litellm_tool_response import require_tool_calls_response
from threlium.settings import LlmEndpoint, ThreliumSettings, resolve_llm_endpoint
from threlium.types import (
    LiteLlmAcompletionKwargs,
    LiteLlmChatMessage,
    LitellmCallSite,
    LitellmRoutingSite,
)
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader

_RetryResultT = TypeVar("_RetryResultT")


def invoke_with_bridge_retries(
    *,
    max_attempts: int,
    attempt: Callable[[], _RetryResultT],
    retry_errors: tuple[type[BaseException], ...],
    on_retry: Callable[[int, BaseException], None] | None = None,
    on_exhausted: Callable[[BaseException], _RetryResultT] | None = None,
) -> _RetryResultT:
    """Повторять sync ``attempt`` до ``max_attempts`` раз при ``retry_errors``.

    Единый bridge-retry: ``attempt`` инкапсулирует invoke+parse одной попытки.
    ``on_retry(n_1based, exc)`` — лог между попытками (не на последней).
    ``on_exhausted(last_exc)`` — fallback после исчерпания; ``None`` → re-raise.
    ``max_attempts`` = число ретраев + 1 (``>= 1``).
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    last_error: BaseException | None = None
    for i in range(max_attempts):
        try:
            return attempt()
        except retry_errors as exc:
            last_error = exc
            if i + 1 < max_attempts and on_retry is not None:
                on_retry(i + 1, exc)
    assert last_error is not None
    if on_exhausted is not None:
        return on_exhausted(last_error)
    raise last_error


async def ainvoke_with_bridge_retries(
    *,
    max_attempts: int,
    attempt: Callable[[], Awaitable[_RetryResultT]],
    retry_errors: tuple[type[BaseException], ...],
    on_retry: Callable[[int, BaseException], None] | None = None,
    on_exhausted: Callable[[BaseException], _RetryResultT] | None = None,
) -> _RetryResultT:
    """Async-вариант :func:`invoke_with_bridge_retries` (``attempt`` — coroutine)."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    last_error: BaseException | None = None
    for i in range(max_attempts):
        try:
            return await attempt()
        except retry_errors as exc:
            last_error = exc
            if i + 1 < max_attempts and on_retry is not None:
                on_retry(i + 1, exc)
    assert last_error is not None
    if on_exhausted is not None:
        return on_exhausted(last_error)
    raise last_error


def tool_function_name(spec: dict[str, object]) -> str:
    """``function.name`` загруженного tool spec (валиден после ``load_tool_spec``)."""
    func = spec["function"]
    if not isinstance(func, dict):
        raise RuntimeError("tool spec: function must be an object")
    name = func.get("name")
    if not isinstance(name, str) or not name.strip():
        raise RuntimeError("tool spec: function.name must be a non-empty string")
    return name


def correlation_with_call_site(
    snap: dict[str, str] | None, call_site: LitellmCallSite | str
) -> dict[str, str] | None:
    """Копия снимка корреляции с переопределённым ``X-Threlium-Call-Site``.

    ``None`` (e2e-корреляция выключена) проходит насквозь — override не нужен.
    """
    if snap is None:
        return None
    wire = call_site.value if isinstance(call_site, LitellmCallSite) else call_site
    out = dict(snap)
    out[LitellmCorrelationHeader.CALL_SITE.value] = wire
    return out


def build_site_call(
    settings: ThreliumSettings,
    site: LitellmRoutingSite | None,
    messages: list[LiteLlmChatMessage],
    *,
    endpoint: LlmEndpoint | None = None,
    thinking_token_budget: int | None = None,
    max_tokens: int | None = None,
    chat_template_kwargs: dict[str, Any] | None = None,
) -> LiteLlmAcompletionKwargs:
    """``LiteLlmAcompletionKwargs`` из каталога ``settings.litellm`` для *site* или явного *endpoint*.

    ``endpoint`` — уже резолвленный профиль (LightRAG closure, reasoning loop).
    ``thinking_token_budget`` / ``max_tokens`` / ``chat_template_kwargs`` переопределяют endpoint.
    """
    if endpoint is None:
        if site is None:
            raise ValueError("build_site_call: site or endpoint required")
        ep = resolve_llm_endpoint(settings.litellm, site)
    else:
        ep = endpoint
    mr = ep.max_retries if ep.max_retries is not None else settings.litellm.max_retries
    tb = thinking_token_budget if thinking_token_budget is not None else ep.thinking_token_budget
    mt = max_tokens if max_tokens is not None else ep.max_tokens
    ctk = (
        chat_template_kwargs
        if chat_template_kwargs is not None
        else (ep.chat_template_kwargs or None)
    )
    return LiteLlmAcompletionKwargs(
        model=ep.model,
        messages=list(messages),
        timeout=float(ep.timeout),
        max_retries=mr,
        api_key=ep.api_key,
        api_base=ep.api_base,
        max_tokens=mt,
        thinking_token_budget=tb,
        chat_template_kwargs=ctk,
    )


def invoke_required_tool(
    *,
    settings: ThreliumSettings,
    call: LiteLlmAcompletionKwargs,
    tool_spec: dict[str, object],
    correlation_snap: dict[str, str] | None,
    context: str,
) -> Message:
    """Sync: один tool, ``call_site=function.name``; вернуть assistant с tool_call."""
    call_site = tool_function_name(tool_spec)
    corr = correlation_with_call_site(correlation_snap, call_site)
    resp = completion_required_tool_sync(
        settings=settings,
        call=call,
        tools=[tool_spec],
        correlation_override=corr,
    )
    return require_tool_calls_response(resp, context=context)


async def ainvoke_required_tool(
    *,
    settings: ThreliumSettings,
    call: LiteLlmAcompletionKwargs,
    tool_spec: dict[str, object],
    correlation_snap: dict[str, str] | None,
    context: str,
) -> Message:
    """Async: один tool, ``call_site=function.name``; вернуть assistant с tool_call."""
    call_site = tool_function_name(tool_spec)
    corr = correlation_with_call_site(correlation_snap, call_site)
    resp = await acompletion_required_tool(
        settings=settings,
        call=call,
        tools=[tool_spec],
        correlation_override=corr,
    )
    msg = resp.choices[0].message
    if msg is None:
        raise RuntimeError(f"{context}: empty assistant message")
    return cast(Message, msg)


__all__ = [
    "ainvoke_required_tool",
    "ainvoke_with_bridge_retries",
    "build_site_call",
    "correlation_with_call_site",
    "invoke_required_tool",
    "invoke_with_bridge_retries",
    "tool_function_name",
]
