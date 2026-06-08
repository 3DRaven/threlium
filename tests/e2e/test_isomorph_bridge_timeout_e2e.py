"""E2e: upstream-timeout → **504**. Мост отдаёт 504, если egress-push не пришёл за ``request_timeout_sec``.

Параллельно-совместим (``-n N``): таймаут моста для ЭТОГО запроса понижается до 8c **per-request** директивой
``E2E_REQUEST_TIMEOUT_SEC:8`` в теле (e2e-режим, обобщение ``E2E_MID:``, см. ``threlium.e2e_directives``) —
БЕЗ понижения глобального конфига моста + рестарта (был serial-only). Соседние ходы используют дефолтный
таймаут → не ломаются. Стабы засижены → реальный ход доезжает чисто в фоне (поздний push = no-op), teardown
idle без зависа.
"""
from __future__ import annotations

from pathlib import Path
from typing import Generator

import pytest

from threlium.types import IsomorphApiSurface

from .toolkit import E2EComposeRuntime
from .toolkit.isomorph_cline import (
    bridge_post_json,
    clean_isomorph_test_threads,
    e2e_explicit_root_mid,
    e2e_root_prompt_token,
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
_MARKER = "isomorph-timeout-e2e"
_STUB_TAG = "stub-isomorph-openai-json-e2e-01"  # зашитый tag json-стабов
_STUB_DIR = Path(__file__).parent / "wiremock_stubs" / "test_isomorph_bridge_openai_json_e2e"
_LOW_TIMEOUT = 2  # per-request таймаут моста для этого запроса (директива E2E_REQUEST_TIMEOUT_SEC)


@pytest.fixture()
def isomorph_timeout_scenario(e2e_runtime: E2EComposeRuntime) -> Generator[E2EComposeRuntime, None, None]:
    rt = e2e_runtime
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    wait_for_sut_threlium_user_workers_idle(rt.project_name, timeout=30.0)
    clean_isomorph_test_threads(rt, _MARKER)
    upsert_wiremock_mapping_directory(wm_base, _STUB_DIR, stub_tag=_STUB_TAG)
    wiremock_state_seed_context(wm_base, composite_context_key(_STUB_TAG, e2e_explicit_root_mid(_MARKER)))
    try:
        yield rt
    finally:
        wait_for_sut_threlium_user_workers_idle(rt.project_name, timeout=60.0)


def test_isomorph_bridge_upstream_timeout_504(isomorph_timeout_scenario: E2EComposeRuntime) -> None:
    """``E2E_REQUEST_TIMEOUT_SEC:8`` в теле → мост ждёт push 8c; push не успевает (FSM ~30c) → снимает pending
    и отдаёт 504 upstream timeout (``_await_json``). curl --max-time 40 > 8 → ловим именно мостовой 504, не
    клиентский обрыв. Per-request → совместимо с ``-n N`` (глобальный конфиг моста не трогается)."""
    rt = isomorph_timeout_scenario
    user = f"ping {e2e_root_prompt_token(_MARKER)} [{_MARKER}] E2E_REQUEST_TIMEOUT_SEC:{_LOW_TIMEOUT}"
    body: dict[str, object] = {
        "model": _MODEL, "stream": False,
        "messages": [
            {"role": "system", "content": "you are an e2e probe"},
            {"role": "user", "content": user},
        ],
    }
    status, resp = bridge_post_json(
        rt, port=_ISO_PORT, path="/v1/chat/completions", body=body,
        api_key=_API_KEY, surface=IsomorphApiSurface.OPENAI_CHAT_COMPLETIONS, timeout=40.0,
    )
    assert status == 504, resp
    assert "timeout" in resp.lower(), resp
