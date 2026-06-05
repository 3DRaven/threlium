"""E2e: happy-path канала ``isomorph`` — **Anthropic surface, НЕ-SSE** (``/v1/messages``, ``stream:false``).

В отличие от cline-тестов (реальный клиент + SSE), здесь тест сам — прямой HTTP-клиент: POST ИЗНУТРИ SUT
на loopback моста. ``stream:false`` → мост держит соединение (long-hold), FSM прогоняет ход
(ingress → … → ``egress_isomorph`` push), мост отдаёт финальный **JSON** (``Message``) — ветка
``_await_json``. SSE-зеркало — [cline-вариант](test_isomorph_bridge_anthropic_cline_e2e.py).

Тест владеет телом запроса целиком → thread-root **предвычисляется** из ЭТОГО тела тем же кодом моста
(:func:`thread_root_from_body`), без Cline / даты / шаблона системного промпта. State-контекст WireMock
сидится **до** POST (сид данных, НЕ генерация стабов) — гонки нет. Изоляция: своя папка стабов, свой
``stub_tag``; setup чистит ТОЛЬКО прошлые треды этого теста (по маркеру), teardown ничего не стирает.
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
    bridge_post_json,
    build_continuation_body,
    clean_isomorph_test_threads,
    extract_reply_text,
    nm_count,
    nm_count_in_test_thread,
    nm_oldest_message_id,
    nm_test_thread_count,
    thread_root_from_body,
    wait_bridge_health,
)
from .toolkit.workers import wait_for_sut_threlium_user_workers_idle
from .wiremock_client import (
    composite_context_key,
    upsert_wiremock_mapping_directory,
    wiremock_public_base,
    wiremock_state_reset_phase,
    wiremock_state_seed_context,
)

_ISO_PORT = 8040
_API_KEY = "e2e-isomorph-api-key"
_MODEL = "claude-sonnet-4-6"
_SURFACE = IsomorphApiSurface.ANTHROPIC_MESSAGES
_PATH = "/v1/messages"
_STUB_TAG = "stub-isomorph-anthropic-json-e2e-01"
_STUB_DIR = Path(__file__).parent / "wiremock_stubs" / "test_isomorph_bridge_anthropic_json_e2e"
_MARKER = "isomorph-anthropic-json-e2e"
_REPLY_MARKER = "ok from llm-mock"
#: Тело запроса целиком во власти теста (system + user сольются в один хвост → детерминированный thread-root).
_BODY: dict[str, object] = {
    "model": _MODEL,
    "max_tokens": 1024,
    "stream": False,
    "system": "you are an e2e probe",
    "messages": [{"role": "user", "content": f"ping [{_MARKER}]"}],
}


def _thread_root() -> str:
    return thread_root_from_body(_SURFACE, _BODY)


@pytest.fixture()
def isomorph_json(e2e_runtime: E2EComposeRuntime) -> Generator[E2EComposeRuntime, None, None]:
    """Setup ДО guard'а unmatched: settle, scoped-чистка СВОИХ прошлых тредов, стабы, СИД thread-root.

    Сид предвычисленного thread-root **до** POST → первый LLM-вызов уже сматчен, гонки нет. Teardown НЕ
    стирает данные (остаются для отладки); свои прошлые данные затрёт setup следующего прогона.
    """
    rt = e2e_runtime
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    wait_for_sut_threlium_user_workers_idle(rt.project_name, timeout=30.0)
    wait_bridge_health(rt, port=_ISO_PORT)  # мост мог ещё не подняться после сессионного cold-reset
    clean_isomorph_test_threads(rt, _MARKER)
    upsert_wiremock_mapping_directory(wm_base, _STUB_DIR, stub_tag=_STUB_TAG)
    wiremock_state_seed_context(wm_base, composite_context_key(_STUB_TAG, _thread_root()))
    try:
        yield rt
    finally:
        wait_for_sut_threlium_user_workers_idle(rt.project_name, timeout=60.0)


def test_isomorph_bridge_anthropic_json_happy_path(isomorph_json: E2EComposeRuntime) -> None:
    """POST /v1/messages stream:false → bridge → FSM (WireMock) → egress push → финальный JSON Message."""
    rt = isomorph_json
    status, resp = bridge_post_json(
        rt, port=_ISO_PORT, path=_PATH, body=_BODY, api_key=_API_KEY, surface=_SURFACE
    )
    assert status == 200, resp
    payload = json.loads(resp)
    # Anthropic JSON-форма (encode_anthropic_json): type=message + content[].text.
    assert payload.get("type") == "message", payload
    texts = "".join(b.get("text", "") for b in payload.get("content", []) if isinstance(b, dict))
    assert _REPLY_MARKER in texts, payload

    # Скоуп по _MARKER (teardown не стирает → глобальный from:egress поймал бы чужой glue).
    assert nm_oldest_message_id(rt, f"from:isomorph@localhost and {_MARKER}") == _thread_root().strip("<>")
    assert nm_count(rt, f"from:isomorph@localhost and {_MARKER}") >= 1, "no isomorph ingress in notmuch"
    assert nm_count_in_test_thread(rt, _MARKER, "from:egress_isomorph@localhost") >= 1, "no egress glue"


def test_isomorph_bridge_anthropic_json_multiturn_continuity(isomorph_json: E2EComposeRuntime) -> None:
    """Голосование/непрерывность: ход-2 несёт ответ хода-1 как last-assistant → мост голосует по notmuch
    (``hash(reply_1)`` == glue хода-1) → IRT = тот glue → ОДИН тред. Оба хода делят thread-root (старейший
    tag:route = ingress хода-1) → их LLM-вызовы матчат тот же засиженный контекст (без нового сида)."""
    rt = isomorph_json
    # happy-path делит thread-root с этим тестом (тот же _BODY-промпт) → оставляет phase_tasks_ledger_done
    # в ОБЩЕМ контексте WireMock. Сбрасываем фазу ДО хода-1 (а не только между ходами), иначе reasoning
    # хода-1 видит чужую защёлку → пропускает закрытие задач → finalize-loop (open subtasks → 120s hang).
    wiremock_state_reset_phase(
        wiremock_public_base(rt.wiremock_host, rt.wiremock_port),
        composite_context_key(_STUB_TAG, _thread_root()),
    )
    s1, r1 = bridge_post_json(rt, port=_ISO_PORT, path=_PATH, body=_BODY, api_key=_API_KEY, surface=_SURFACE)
    assert s1 == 200, r1
    reply1 = extract_reply_text(_SURFACE, r1)
    assert _REPLY_MARKER in reply1, r1

    # Дать фоновым стадиям хода-1 (reflect/memory/lightrag) досвестись: их запись в notmuch иначе
    # блокирует notmuch-чтение голосования хода-2 в мосту (resolve_in_reply_to зависает).
    wait_for_sut_threlium_user_workers_idle(rt.project_name, timeout=60.0)
    # Дождаться, пока glue хода-1 (egress_isomorph, archive-first) ПРОИНДЕКСИРОВАН notmuch: in-work-проверка
    # моста (#ingress > #glue) иначе под нагрузкой (-n2) видит ход-1 «в полёте» и отдаёт ход-2 409.
    poll_until(
        lambda: True if nm_count_in_test_thread(rt, _MARKER, "from:egress_isomorph@localhost") >= 1 else None,
        timeout=TIMEOUT_POLL_LIVE_MAIL, desc="turn-1 glue indexed before turn-2",
    )
    # Сбросить reasoning-защёлку phase_tasks_ledger_done на ОБЩЕМ контексте треда (E2E_ISOLATION §2.1):
    # иначе ход-2 видит её от хода-1 → пропускает фазу закрытия задач → finalize-loop (open subtasks).
    wiremock_state_reset_phase(
        wiremock_public_base(rt.wiremock_host, rt.wiremock_port),
        composite_context_key(_STUB_TAG, _thread_root()),
    )
    body2 = build_continuation_body(_SURFACE, _BODY, reply1, f"continue [{_MARKER}]")
    # Мост может вернуть 409 "prior turn still in flight" (под -n2 фоновые стадии хода-1 ещё дорабатывают) —
    # штатный in-work-контракт с инструкцией "retry after its reply". 409 НЕ создаёт ingress (отказ до
    # обработки), поэтому ретраи не дублируют ход. Ретраим, пока ход-2 не примут.
    def _post_turn2() -> tuple[int, str] | None:
        s, r = bridge_post_json(rt, port=_ISO_PORT, path=_PATH, body=body2, api_key=_API_KEY, surface=_SURFACE)
        return (s, r) if s != 409 else None

    s2, r2 = poll_until(_post_turn2, timeout=TIMEOUT_POLL_LIVE_MAIL, desc="turn-2 accepted (not 409 in-work)")
    assert s2 == 200, r2
    assert _REPLY_MARKER in extract_reply_text(_SURFACE, r2), r2

    # Непрерывность: оба хода в ОДНОМ треде (иначе ход-2 ушёл бы в orphan → 2 треда), 2 разных ingress.
    # Glue считаем >=1, а не ==2: оба хода отдают ОДИН и тот же канонный ответ "ok from llm-mock" →
    # один контент-адресуемый glue-MID = hash(reply) → notmuch дедуп (штатный fork-by-collision §6.4).
    assert nm_test_thread_count(rt, _MARKER) == 1, "turn-2 orphaned → voting/continuity broke"
    assert nm_count(rt, f"from:isomorph@localhost and {_MARKER}") == 2, "expected 2 distinct ingress turns"
    assert nm_count_in_test_thread(rt, _MARKER, "from:egress_isomorph@localhost") >= 1, "no egress glue"
