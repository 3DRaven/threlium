"""E2e: ПОЛНЫЙ round-trip канала ``isomorph`` реальным Cline CLI — **OpenAI surface** (``/v1/chat/completions``).

Зеркало [test_isomorph_bridge_anthropic_cline_e2e.py](test_isomorph_bridge_anthropic_cline_e2e.py), но Cline
настроен ``openai-compatible``-провайдером (Vercel ``@ai-sdk/openai-compatible``) и шлёт
``POST /v1/chat/completions`` со ``stream:true`` (+ форсированный ``stream_options.include_usage``) изнутри
SUT. Мост держит соединение (long-hold + keep-alive), FSM прогоняет ход → ``egress_isomorph`` пушит ответ →
Cline получает OpenAI-SSE. Два слоя моков: **Cline = реальный клиент**, **WireMock = LiteLLM** за FSM.

**Изоляция ([E2E_ISOLATION.md](../../docs/E2E_ISOLATION.md) §2/§7).** Стабы — статическая L0-цепочка в
``wiremock_stubs/test_isomorph_bridge_openai_cline_e2e/`` (своя папка, свой ``stub_tag``). OpenAI шлёт
системный промпт первым элементом ``messages`` (role=system) + промпт + ``[SYSTEM]``-суффикс ДВУМЯ
отдельными user-сообщениями; мост сливает ВСЁ присланное в один хвост (см. ``bridges/isomorph/history.py``),
поэтому контент-адресуемый thread-root тот же по логике, что и на Anthropic. Тест **предвычисляет** его тем
же кодом моста (см. :mod:`tests.e2e.toolkit.isomorph_cline`) и сидит **State-контекст** WireMock **до**
запуска Cline (сид данных, НЕ генерация стабов) — гонки нет.
"""
from __future__ import annotations

from pathlib import Path
from typing import Generator

import pytest

from threlium.types import IsomorphApiSurface

from .toolkit import E2EComposeRuntime, poll_until
from .toolkit.constants import TIMEOUT_POLL_LIVE_MAIL
from .toolkit.isomorph_cline import (
    clean_isomorph_test_threads,
    cline_received,
    configure_cline,
    e2e_explicit_root_mid,
    e2e_root_prompt_token,
    nm_count,
    nm_count_in_test_thread,
    nm_oldest_message_id,
    start_cline_background,
    sut_exec,
    wait_bridge_health,
)
from .toolkit.workers import wait_for_sut_threlium_user_workers_idle
from .wiremock_client import (
    composite_context_key,
    upsert_wiremock_mapping_directory,
    wiremock_public_base,
    wiremock_state_seed_context,
)

# group_vars/e2e.yml → bridges.isomorph.listen_port / api_key.
_ISO_PORT = 8040
_API_KEY = "e2e-isomorph-api-key"
_MODEL = "claude-sonnet-4-6"
_PROVIDER = "openai-compatible"
_SURFACE = IsomorphApiSurface.OPENAI_CHAT_COMPLETIONS
_STUB_TAG = "stub-isomorph-openai-cline-e2e-01"
_STUB_DIR = Path(__file__).parent / "wiremock_stubs" / "test_isomorph_bridge_openai_cline_e2e"
_CLINE_DATA = "/tmp/cline-openai-e2e"
_CLINE_CWD = "/tmp/cline-openai-e2e-work"
_CLINE_OUT = "/tmp/cline_openai_e2e_out.json"
#: Уникальный токен промпта → дата-независимая scoped-чистка ТОЛЬКО тредов этого теста (см. фикстуру).
_MARKER = "isomorph-openai-cline-e2e"
#: Промпт несёт ГОТОВЫЙ thread-root как `E2E_MID:<...>` (мост в e2e берёт напрямую, без content-hash).
_PROMPT = f"reply pong {e2e_root_prompt_token(_MARKER)} [{_MARKER}]"
#: Текст финального ответа из reasoning-стаба (100_chat_reasoning_egress_tool → response_finalize content).
_REPLY_MARKER = "ok from llm-mock"


def _thread_root() -> str:
    return e2e_explicit_root_mid(_MARKER)


@pytest.fixture()
def isomorph_cline(e2e_runtime: E2EComposeRuntime) -> Generator[E2EComposeRuntime, None, None]:
    """Setup ДО guard'а unmatched: settle, scoped-чистка СВОИХ прошлых тредов, стабы, СИД thread-root, cline.

    Сид предвычисленного thread-root **до** тела (и до Cline) → первый LLM-вызов уже сматчен, гонки нет.
    Чистим по :data:`_MARKER` ТОЛЬКО прошлые прогоны этого теста (не трогая другие isomorph-тесты/отладку).
    Teardown намеренно НЕ стирает данные (остаются для отладки) — лишь убивает Cline и ждёт idle, чтобы
    следующий тест стартовал на простаивающем пайплайне; свои прошлые данные затрёт setup следующего прогона.
    """
    rt = e2e_runtime
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    sut_exec(rt, f"pkill -f {_CLINE_DATA} 2>/dev/null || true")  # только свой cline (по data-dir): parallel-safe  # добить Cline от упавшего прогона (teardown мог не успеть)
    wait_for_sut_threlium_user_workers_idle(rt.project_name, timeout=30.0)
    wait_bridge_health(rt, port=_ISO_PORT)  # мост мог ещё не подняться после сессионного cold-reset
    clean_isomorph_test_threads(rt, _MARKER)
    upsert_wiremock_mapping_directory(wm_base, _STUB_DIR, stub_tag=_STUB_TAG)
    wiremock_state_seed_context(wm_base, composite_context_key(_STUB_TAG, _thread_root()))
    configure_cline(
        rt, provider=_PROVIDER, api_key=_API_KEY, model=_MODEL,
        base_url=f"http://127.0.0.1:{_ISO_PORT}/v1", data_dir=_CLINE_DATA, cwd=_CLINE_CWD,
    )
    try:
        yield rt
    finally:
        sut_exec(rt, f"pkill -f {_CLINE_DATA} 2>/dev/null || true")  # только свой cline (по data-dir): parallel-safe
        wait_for_sut_threlium_user_workers_idle(rt.project_name, timeout=60.0)


def test_isomorph_bridge_openai_cline_full_roundtrip(isomorph_cline: E2EComposeRuntime) -> None:
    """Cline (OpenAI chat-completions) → bridge → FSM (WireMock) → egress_isomorph push → Cline получает SSE."""
    rt = isomorph_cline
    start_cline_background(
        rt, provider=_PROVIDER, data_dir=_CLINE_DATA, cwd=_CLINE_CWD, out_path=_CLINE_OUT, prompt=_PROMPT
    )

    # Ход завершается записью glue-archive egress_isomorph (ARCHIVE-FIRST до push клиенту). Таймаут
    # LIVE_MAIL (а не SHORT): хвост несёт весь системный промпт Cline (~2 КБ) → enrich/lightrag/embeddings
    # делают больше работы, плюс холодный первый ход после рестарта пайплайна — оборот ~30 c.
    # Скоуп по _MARKER: teardown не стирает данные, поэтому глобальный from:egress поймал бы glue
    # другого isomorph-теста. Считаем egress ВНУТРИ треда этого теста.
    poll_until(
        lambda: True if nm_count_in_test_thread(rt, _MARKER, "from:egress_isomorph@localhost") >= 1 else None,
        timeout=TIMEOUT_POLL_LIVE_MAIL, desc="egress_isomorph glue archive",
    )
    # Cline получил OpenAI-SSE-ответ (байтовый оракул wire-совместимости chat-completions).
    poll_until(
        lambda: True if cline_received(rt, _CLINE_OUT, _REPLY_MARKER) else None,
        timeout=TIMEOUT_POLL_LIVE_MAIL, desc="cline received reply",
    )

    # Предвычисленный thread-root == фактический ingress-MID (иначе Cline сменил формат запроса/системный
    # промпт/формат даты → перезахватить шаблон в toolkit/cline_system_prompt.tmpl). Скоуп по _MARKER.
    assert nm_oldest_message_id(rt, f"from:isomorph@localhost and {_MARKER}") == _thread_root().strip("<>"), (
        "Cline first-request format changed → re-capture cline_system_prompt.tmpl / CLINE_USER_SUFFIX"
    )
    assert nm_count(rt, f"from:isomorph@localhost and {_MARKER}") >= 1, "no isomorph ingress in notmuch"
    assert nm_count_in_test_thread(rt, _MARKER, "from:egress_isomorph@localhost") >= 1, "no egress glue"
