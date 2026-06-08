"""Starlette-приложение isomorph-моста: long-hold + keep-alive + disconnect-aware ожидание push.

Маршруты тонкие; общий ``inference_handler`` для ``/v1/messages`` и ``/v1/chat/completions``.
Любой sync I/O (``deliver``=run_fdm) — через ``anyio.to_thread.run_sync`` (не морозить event loop).
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from email.message import EmailMessage

import msgspec
from anyio.to_thread import run_sync as _to_thread_run_sync
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from threlium.bridges import build_bridge_ingress_email
from threlium.logutil import logger
from threlium.settings import ThreliumSettings
from threlium.types import (
    BridgeIngressChannel,
    IsomorphApiSurface,
    IsomorphIngressRoute,
    NotmuchMessageIdInner,
)

from . import encoders
from .auth import is_authorized
from .history import ingress_message_id, parse_history
from .models_list import models_list_payload
from .snowflake_mid import extract_e2e_explicit_mid
from threlium.e2e_directives import extract_e2e_int_directive
from .pending import IsomorphPendingRegistry
from .push import handle_push
from .push_types import IsomorphBridgePushPayload
from .sse import SseFrame

log = logger.bind(bridge="isomorph")

DeliverFn = Callable[[EmailMessage], None]


class _AppState:
    def __init__(self, deliver: DeliverFn, settings: ThreliumSettings, *, verbose: bool) -> None:
        self.deliver = deliver
        self.settings = settings
        self.registry = IsomorphPendingRegistry()
        self.verbose = verbose


def _headers_lower(request: Request) -> dict[str, str]:
    return {k.lower(): v for k, v in request.headers.items()}


def _surface_error(surface: IsomorphApiSurface, message: str, *, err_type: str, status: int) -> Response:
    if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES:
        return Response(encoders.anthropic_error_json(message, err_type=err_type),
                        status_code=status, media_type="application/json")
    return Response(encoders.openai_error_json(message, err_type=err_type),
                    status_code=status, media_type="application/json")


async def inference_handler(request: Request, surface: IsomorphApiSurface) -> Response:
    state: _AppState = request.app.state.iso
    iso = state.settings.bridges.isomorph

    headers = _headers_lower(request)
    if not is_authorized(headers, api_key=iso.api_key):
        return _surface_error(surface, "invalid api key", err_type="authentication_error", status=401)

    if surface.value not in iso.enabled_surfaces:
        return _surface_error(surface, f"surface {surface.value} disabled", err_type="not_found_error", status=404)

    raw = await request.body()
    try:
        body = msgspec.json.decode(raw)
        if not isinstance(body, dict):
            raise ValueError("body must be a JSON object")
    except (msgspec.DecodeError, ValueError) as e:
        return _surface_error(surface, f"bad request body: {e}", err_type="invalid_request_error", status=400)

    stream = bool(body.get("stream", surface is IsomorphApiSurface.OPENAI_CHAT_COMPLETIONS))
    model = str(body.get("model", "")).strip() or "threlium"

    try:
        parsed = parse_history(surface, body)
    except ValueError as e:
        return _surface_error(surface, f"bad messages: {e}", err_type="invalid_request_error", status=400)

    # In-Reply-To: декодируется из водяного знака last-assistant в parse_history (БЕЗ notmuch/голосования).
    # Знака нет / первый ход → None (orphan, новый тред). Знак = ТОЧНЫЙ glue-MID хвоста = IRT нового хода.
    in_reply_to = parsed.in_reply_to
    tail_body = parsed.tail_body

    # E2E-ONLY: тест может прислать ГОТОВЫЙ thread-root в теле как `E2E_MID:<...@localhost>` (генерит тем же
    # `snowflake_to_mid`, что egress). Тогда thread-root = он, БЕЗ content-hash → нет зависимости от
    # реконструкции тела Cline/даты (устраняет date-drift сидирования). В проде (флаг off) — обычный путь.
    message_id = None
    # E2E-ONLY: per-request override request_timeout_sec из тела (E2E_REQUEST_TIMEOUT_SEC:<int>) — чтобы
    # тест проверил 504-таймаут БЕЗ понижения глобального конфига моста + рестарта (был serial-only; обобщение
    # E2E_MID:, см. threlium.e2e_directives). Только за флагом e2e; токен вырезается. Прод → iso.request_timeout_sec.
    request_timeout_override: int | None = None
    if state.settings.e2e.litellm_route_correlation:
        e2e_mid, tail_body = extract_e2e_explicit_mid(tail_body)
        if e2e_mid is not None:
            message_id = e2e_mid
        request_timeout_override, tail_body = extract_e2e_int_directive(tail_body, "REQUEST_TIMEOUT_SEC")
    if message_id is None:
        message_id = ingress_message_id(
            parent_value=in_reply_to.value if in_reply_to else "", tail_body=tail_body)
    # Коррелятор pending↔push = inner-форма ingress Message-ID (та же, что egress прочитает как
    # ancestor_mid ближайшего tag:route предка). Контент-адресуем, доступен сразу (до notmuch).
    corr_inner = NotmuchMessageIdInner.from_optional_wire(message_id)
    if corr_inner is None:  # инвариант: content-addressed MID всегда непустой
        return _surface_error(surface, "internal: empty ingress id", err_type="api_error", status=500)
    corr = corr_inner.value

    route = IsomorphIngressRoute(
        channel=BridgeIngressChannel.ISOMORPH.value,
        api_surface=surface.value, model=model, v=1, stream=stream,
    )
    ingress = build_bridge_ingress_email(
        channel=BridgeIngressChannel.ISOMORPH,
        body=tail_body or "(empty)",
        route=route,
        message_id=message_id,
        in_reply_to=in_reply_to,
        raw_capture=raw.decode("utf-8", errors="replace"),
    )

    fut = state.registry.register(corr, api_surface=surface.value, stream=stream)
    if state.verbose:
        log.info("ingress", mid=corr, surface=surface.value, stream=stream,
                 irt=(in_reply_to.value if in_reply_to else None))
    # run_fdm — sync subprocess: только через worker-thread.
    await _to_thread_run_sync(state.deliver, ingress)

    if stream:
        return StreamingResponse(
            _event_stream(request, state, surface, corr, fut, model,
                          timeout_override=request_timeout_override),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return await _await_json(state, surface, corr, fut,
                             timeout_override=request_timeout_override)


def _keepalive_frame(surface: IsomorphApiSurface) -> SseFrame:
    if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES:
        return SseFrame.of_event("ping", '{"type":"ping"}')
    return SseFrame.of_comment("keep-alive")


def _sse_encoder(surface: IsomorphApiSurface):
    return (encoders.encode_anthropic_sse if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES
            else encoders.encode_openai_sse)


def _error_frame(surface: IsomorphApiSurface, message: str) -> SseFrame:
    err = (encoders.anthropic_error_sse if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES
           else encoders.openai_error_sse)
    return err(message, err_type="api_error")


async def _event_stream(
    request: Request, state: _AppState, surface: IsomorphApiSurface,
    corr: str, fut: "asyncio.Future[IsomorphBridgePushPayload]", model: str,
    *, timeout_override: int | None = None,
) -> AsyncIterator[str]:
    iso = state.settings.bridges.isomorph
    request_timeout = timeout_override if timeout_override is not None else iso.request_timeout_sec
    deadline = asyncio.get_running_loop().time() + request_timeout

    def emit(frame: SseFrame) -> str:
        # Единственное место рендера SseFrame → сырая строка (край StreamingResponse).
        if state.verbose:
            log.info("sse_chunk", mid=corr, frame=frame)
        return frame.render()

    try:
        while not fut.done():
            if await request.is_disconnected():
                state.registry.discard(corr, fut)
                if state.verbose:
                    log.info("client_disconnected", mid=corr)
                return
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                state.registry.discard(corr, fut)
                yield emit(_error_frame(surface, "upstream timeout"))
                return
            try:
                await asyncio.wait_for(asyncio.shield(fut), timeout=min(iso.keepalive_sec, remaining))
            except asyncio.TimeoutError:
                yield emit(_keepalive_frame(surface))
                continue
            except asyncio.CancelledError:
                state.registry.discard(corr, fut)
                return

        payload = fut.result()
        payload = msgspec.structs.replace(payload, model=payload.model or model)
        if payload.error_message:
            yield emit(_error_frame(surface, payload.error_message))
            return
        for frame in _sse_encoder(surface)(payload):
            yield emit(frame)
    finally:
        state.registry.forget(corr, fut)


async def _await_json(
    state: _AppState, surface: IsomorphApiSurface, corr: str,
    fut: "asyncio.Future[IsomorphBridgePushPayload]",
    *, timeout_override: int | None = None,
) -> Response:
    iso = state.settings.bridges.isomorph
    request_timeout = timeout_override if timeout_override is not None else iso.request_timeout_sec
    try:
        payload = await asyncio.wait_for(asyncio.shield(fut), timeout=request_timeout)
    except asyncio.TimeoutError:
        state.registry.discard(corr, fut)
        return _surface_error(surface, "upstream timeout", err_type="api_error", status=504)
    except asyncio.CancelledError:
        state.registry.discard(corr, fut)
        return _surface_error(surface, "cancelled", err_type="api_error", status=499)
    finally:
        state.registry.forget(corr, fut)

    if payload.error_message:
        return _surface_error(surface, payload.error_message, err_type="api_error", status=500)

    if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES:
        return Response(encoders.encode_anthropic_json(payload), media_type="application/json")
    return Response(encoders.encode_openai_json(payload), media_type="application/json")


# ============================ routes ============================


async def route_messages(request: Request) -> Response:
    return await inference_handler(request, IsomorphApiSurface.ANTHROPIC_MESSAGES)


async def route_chat_completions(request: Request) -> Response:
    return await inference_handler(request, IsomorphApiSurface.OPENAI_CHAT_COMPLETIONS)


async def route_models(request: Request) -> Response:
    state: _AppState = request.app.state.iso
    return JSONResponse(models_list_payload(state.settings))


async def route_health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


async def route_internal_push(request: Request) -> Response:
    state: _AppState = request.app.state.iso
    body = await request.body()
    headers = _headers_lower(request)
    client_host = request.client.host if request.client else None
    result = handle_push(
        body, headers, client_host=client_host,
        registry=state.registry, push_secret=state.settings.bridges.isomorph.push_secret,
    )
    if result.status >= 400:
        return JSONResponse({"detail": result.detail}, status_code=result.status)
    return Response(status_code=result.status)


def build_app(deliver: DeliverFn, *, settings: ThreliumSettings, verbose: bool) -> Starlette:
    app = Starlette(routes=[
        Route("/v1/messages", route_messages, methods=["POST"]),
        Route("/v1/chat/completions", route_chat_completions, methods=["POST"]),
        Route("/v1/models", route_models, methods=["GET"]),
        Route("/health", route_health, methods=["GET"]),
        Route("/internal/v1/push", route_internal_push, methods=["POST"]),
    ])
    app.state.iso = _AppState(deliver, settings, verbose=verbose)
    return app
