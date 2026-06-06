"""E2e: ПРЯМАЯ проверка SSE wire-схемы моста ``isomorph`` (без Cline) — Anthropic-события и OpenAI-чанки.

Тест сам — прямой ``stream:true`` HTTP-клиент ИЗНУТРИ SUT (``curl -N``), читает СЫРОЙ SSE-поток и проверяет
строгую схему вендора побайтово (независимо от толерантности реального Cline):
- **Anthropic** (``/v1/messages``): ``message_start → content_block_start → content_block_delta →
  content_block_stop → message_delta → message_stop`` (плюс возможный ``event: ping`` keep-alive до push);
- **OpenAI** (``/v1/chat/completions``): первый чанк с ``role``, content-чанк, usage-чанк с пустым
  ``choices``, терминатор ``[DONE]`` (плюс возможный ``: keep-alive`` комментарий).

Изоляция (E2E_ISOLATION §2/§7): свой ``stub_tag`` + ГОТОВЫЙ thread-root через ``E2E_MID:<...>`` в теле
(мост берёт его напрямую — без content-hash/даты). Стабы — переиспользуем L0-цепочку json-вариантов (FSM-путь
тот же; surface меняет лишь кодирование запроса/ответа моста, не стадии). keepalive_sec=20 < оборот FSM (~30c) →
в потоке естественно появляется keep-alive ДО ответа (покрываем заодно).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Generator

import pytest

from threlium.types import IsomorphApiSurface

from .toolkit import E2EComposeRuntime, poll_until
from .toolkit.constants import TIMEOUT_POLL_LIVE_MAIL
from .toolkit.isomorph_cline import (
    bridge_post_json_with_pushed_error,
    bridge_post_sse,
    clean_isomorph_test_threads,
    e2e_explicit_root_corr,
    e2e_explicit_root_mid,
    e2e_root_prompt_token,
    nm_count_in_test_thread,
    parse_sse_events,
    wait_bridge_health,
)
from .toolkit.workers import wait_for_sut_threlium_user_workers_idle
from .wiremock_client import (
    composite_context_key,
    upsert_wiremock_mapping_directory,
    wiremock_public_base,
    wiremock_state_seed_context,
)

_ISO_PORT = 8040
_API_KEY = "e2e-isomorph-api-key"
_MODEL = "claude-sonnet-4-6"
_REPLY_MARKER = "ok from llm-mock"
_STUBS_ROOT = Path(__file__).parent / "wiremock_stubs"


def _seed(rt: E2EComposeRuntime, *, stub_tag: str, stub_dir: Path, marker: str) -> None:
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    wait_for_sut_threlium_user_workers_idle(rt.project_name, timeout=30.0)
    wait_bridge_health(rt, port=_ISO_PORT)
    clean_isomorph_test_threads(rt, marker)
    upsert_wiremock_mapping_directory(wm_base, stub_dir, stub_tag=stub_tag)
    wiremock_state_seed_context(wm_base, composite_context_key(stub_tag, e2e_explicit_root_mid(marker)))


def _body(surface: IsomorphApiSurface, marker: str) -> dict[str, object]:
    user = f"ping {e2e_root_prompt_token(marker)} [{marker}]"
    if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES:
        return {
            "model": _MODEL, "max_tokens": 1024, "stream": True,
            "system": "you are an e2e probe",
            "messages": [{"role": "user", "content": user}],
        }
    return {
        "model": _MODEL, "stream": True,
        "messages": [
            {"role": "system", "content": "you are an e2e probe"},
            {"role": "user", "content": user},
        ],
    }


# ============================ Anthropic ============================

_A_MARKER = "isomorph-anthropic-sse-wire-e2e"
# Переиспользуем L0-цепочку json-варианта (FSM-путь тот же). hasContext stub_tag ЗАШИТ в файлах стабов
# (upsert его НЕ подставляет) → сидим тем же tag; изоляция держится на СВОЁМ thread-root (explicit MID).
_A_STUB_TAG = "stub-isomorph-anthropic-json-e2e-01"
_A_STUB_DIR = _STUBS_ROOT / "test_isomorph_bridge_anthropic_json_e2e"


@pytest.fixture()
def isomorph_sse_anthropic(e2e_runtime: E2EComposeRuntime) -> Generator[E2EComposeRuntime, None, None]:
    _seed(e2e_runtime, stub_tag=_A_STUB_TAG, stub_dir=_A_STUB_DIR, marker=_A_MARKER)
    try:
        yield e2e_runtime
    finally:
        wait_for_sut_threlium_user_workers_idle(e2e_runtime.project_name, timeout=60.0)


def test_isomorph_bridge_anthropic_sse_wire_shape(isomorph_sse_anthropic: E2EComposeRuntime) -> None:
    """Anthropic SSE: строгая последовательность событий message_start..message_stop + текст ответа."""
    rt = isomorph_sse_anthropic
    raw = bridge_post_sse(
        rt, port=_ISO_PORT, path="/v1/messages",
        body=_body(IsomorphApiSurface.ANTHROPIC_MESSAGES, _A_MARKER),
        api_key=_API_KEY, surface=IsomorphApiSurface.ANTHROPIC_MESSAGES,
        timeout=TIMEOUT_POLL_LIVE_MAIL,
    )
    events = parse_sse_events(raw)
    names = [ev for ev, _ in events if ev is not None]
    # Строгий каркас Anthropic-стрима. `ping` (keep-alive до push, при FSM > keepalive_sec) допускается
    # ГДЕ УГОДНО — включая перед message_start — поэтому из проверки порядка его исключаем.
    for required in ("message_start", "content_block_start", "content_block_delta",
                     "content_block_stop", "message_delta", "message_stop"):
        assert required in names, f"missing Anthropic SSE event {required!r}; got {names}\n{raw[:600]}"
    framing = [n for n in names if n != "ping"]
    assert framing[0] == "message_start", f"first non-ping event must be message_start; got {names[:4]}"
    assert framing[-1] == "message_stop", f"last non-ping event must be message_stop; got {names[-4:]}"
    # Текст ответа доехал в content_block_delta.
    deltas = "".join(data for ev, data in events if ev == "content_block_delta")
    assert _REPLY_MARKER in deltas, f"reply text not in deltas: {deltas[:200]!r}"


# ============================ OpenAI ============================

_O_MARKER = "isomorph-openai-sse-wire-e2e"
_O_STUB_TAG = "stub-isomorph-openai-json-e2e-01"  # зашитый tag json-стабов (см. _A_STUB_TAG)
_O_STUB_DIR = _STUBS_ROOT / "test_isomorph_bridge_openai_json_e2e"


@pytest.fixture()
def isomorph_sse_openai(e2e_runtime: E2EComposeRuntime) -> Generator[E2EComposeRuntime, None, None]:
    _seed(e2e_runtime, stub_tag=_O_STUB_TAG, stub_dir=_O_STUB_DIR, marker=_O_MARKER)
    try:
        yield e2e_runtime
    finally:
        wait_for_sut_threlium_user_workers_idle(e2e_runtime.project_name, timeout=60.0)


def test_isomorph_bridge_openai_sse_wire_shape(isomorph_sse_openai: E2EComposeRuntime) -> None:
    """OpenAI SSE: role-в-первом чанке, content-чанк, usage-чанк с пустым choices, терминатор [DONE]."""
    rt = isomorph_sse_openai
    raw = bridge_post_sse(
        rt, port=_ISO_PORT, path="/v1/chat/completions",
        body=_body(IsomorphApiSurface.OPENAI_CHAT_COMPLETIONS, _O_MARKER),
        api_key=_API_KEY, surface=IsomorphApiSurface.OPENAI_CHAT_COMPLETIONS,
        timeout=TIMEOUT_POLL_LIVE_MAIL,
    )
    events = parse_sse_events(raw)
    datas = [data for _, data in events]
    assert any(d == "[DONE]" for d in datas), f"missing OpenAI [DONE] terminator\n{raw[:600]}"
    assert datas[-1] == "[DONE]", f"last frame must be [DONE]; got {datas[-2:]}"
    chunks = [json.loads(d) for d in datas if d and d != "[DONE]" and d.startswith("{")]
    assert chunks, f"no chat.completion.chunk frames\n{raw[:600]}"
    assert all(c.get("object") == "chat.completion.chunk" for c in chunks), "non-chunk object in stream"
    # role в первом чанке (delta.role), content где-то, usage-чанк с пустым choices.
    first_delta = chunks[0].get("choices", [{}])[0].get("delta", {})
    assert first_delta.get("role") == "assistant", f"first chunk delta.role != assistant: {first_delta}"
    content = "".join(
        (c.get("choices", [{}])[0].get("delta", {}) or {}).get("content") or ""
        for c in chunks if c.get("choices")
    )
    assert _REPLY_MARKER in content, f"reply text not in chunk deltas: {content[:200]!r}"
    assert any(c.get("choices") == [] and c.get("usage") for c in chunks), "missing usage chunk (empty choices)"


# ============================ client disconnect (long-hold) ============================

_D_MARKER = "isomorph-disconnect-e2e"
_D_STUB_TAG = "stub-isomorph-anthropic-json-e2e-01"  # зашитый tag json-стабов (см. _A_STUB_TAG)
_D_STUB_DIR = _STUBS_ROOT / "test_isomorph_bridge_anthropic_json_e2e"


@pytest.fixture()
def isomorph_disconnect(e2e_runtime: E2EComposeRuntime) -> Generator[E2EComposeRuntime, None, None]:
    _seed(e2e_runtime, stub_tag=_D_STUB_TAG, stub_dir=_D_STUB_DIR, marker=_D_MARKER)
    try:
        yield e2e_runtime
    finally:
        wait_for_sut_threlium_user_workers_idle(e2e_runtime.project_name, timeout=60.0)


def test_isomorph_bridge_client_disconnect_mid_hold(isomorph_disconnect: E2EComposeRuntime) -> None:
    """Разрыв клиента ПОСРЕДИ long-hold: мост чистит pending своего коннекта (generator ``finally`` →
    ``forget``) и переживает; in-flight ход НЕ обрывается — FSM независим от коннекта, доходит до конца
    (glue archive, ARCHIVE-FIRST), поздний egress-push = no-op. СВОЙ thread-root (свежий reasoning-контекст
    → без stale phase-latch/finalize-loop). Проверка: health отвечает + glue хода всё равно появляется."""
    rt = isomorph_disconnect
    body = _body(IsomorphApiSurface.ANTHROPIC_MESSAGES, _D_MARKER)
    # curl --max-time 4 << оборот FSM (~30c) → клиент обрывается ПОСРЕДИ удержания (exec_run не бросает на
    # rc!=0 → возвращает частичный вывод). Мост детектит разрыв и чистит pending своего коннекта.
    bridge_post_sse(
        rt, port=_ISO_PORT, path="/v1/messages", body=body,
        api_key=_API_KEY, surface=IsomorphApiSurface.ANTHROPIC_MESSAGES, timeout=4.0,
    )
    wait_bridge_health(rt, port=_ISO_PORT)  # мост жив после разрыва
    # Ход доезжает несмотря на разрыв: egress пишет glue (ARCHIVE-FIRST), затем поздний push в мост = no-op.
    poll_until(
        lambda: True if nm_count_in_test_thread(rt, _D_MARKER, "from:egress_isomorph@localhost") >= 1 else None,
        timeout=TIMEOUT_POLL_LIVE_MAIL, desc="turn completes despite client disconnect (glue archived)",
    )


# ============================ FSM error → error envelope ============================

_E_MARKER = "isomorph-error-envelope-e2e"
_E_STUB_TAG = "stub-isomorph-openai-json-e2e-01"  # зашитый tag json-стабов (см. _A_STUB_TAG)
_E_STUB_DIR = _STUBS_ROOT / "test_isomorph_bridge_openai_json_e2e"
_PUSH_SECRET = "e2e-isomorph-push-secret"  # group_vars/e2e.yml bridges.isomorph.push_secret


@pytest.fixture()
def isomorph_error(e2e_runtime: E2EComposeRuntime) -> Generator[E2EComposeRuntime, None, None]:
    # Стабы засижены → реальный ход доезжает чисто в фоне (late push = no-op), teardown idle без зависа.
    _seed(e2e_runtime, stub_tag=_E_STUB_TAG, stub_dir=_E_STUB_DIR, marker=_E_MARKER)
    try:
        yield e2e_runtime
    finally:
        wait_for_sut_threlium_user_workers_idle(e2e_runtime.project_name, timeout=60.0)


def test_isomorph_bridge_error_envelope_json(isomorph_error: E2EComposeRuntime) -> None:
    """``egress``-push с ``error_message`` → мост отдаёт held JSON-запросу error-envelope: HTTP 500 + тело
    ошибки вендора (OpenAI ``{"error": {...}}``). Инъекция error-push должна резолвить held-запрос
    РАНЬШЕ, чем реальный FSM-ход дойдёт до egress_isomorph (idempotent push: первый выигрывает, поздний
    — 204). Окно инъекции: больше регистрации pending (~доли секунды), но меньше FSM-хода. После
    dispatch-fix (enginewire) FSM-ход — единицы секунд (не ~30c), поэтому инъекция = 1.0c (а не 5.0c)."""
    rt = isomorph_error
    body = {**_body(IsomorphApiSurface.OPENAI_CHAT_COMPLETIONS, _E_MARKER), "stream": False}
    status, resp = bridge_post_json_with_pushed_error(
        rt, port=_ISO_PORT, path="/v1/chat/completions", body=body,
        api_key=_API_KEY, surface=IsomorphApiSurface.OPENAI_CHAT_COMPLETIONS,
        corr=e2e_explicit_root_corr(_E_MARKER), error_message="e2e injected fatal",
        push_secret=_PUSH_SECRET, delay=1.0, timeout=60.0,
    )
    assert status == 500, resp
    assert "e2e injected fatal" in resp, resp
    assert "error" in resp.lower(), resp
