"""E2e: канал ``isomorph`` — входящий HTTP-мост в SUT (Cline-совместимый LLM-провайдер).

**На уже поднятом e2e-стеке** (session ``compose_stack`` + per-test ``e2e_runtime``): без compose,
bake и Ansible в этом модуле. Проверяется, что ``threlium-bridge@isomorph.service`` поднят в baked-образе
и отдаёт корректный wire на границе HTTP (health / models / auth / push-secret).

**Изоляция e2e** ([E2E_ISOLATION.md](../../docs/E2E_ISOLATION.md), [TESTING.md](../../docs/TESTING.md)):
эти кейсы **намеренно** дёргают только HTTP-границу моста и **не запускают FSM** (GET, либо POST,
отбиваемый на auth/secret до ``deliver``). Значит **ни одного запроса к WireMock** не порождается →
не нужны State-Extension setup/seed/teardown, и guard «zero unmatched» в ``pytest_runtest_call`` /
``pytest_sessionfinish`` не нарушается. Запросы идут изнутри SUT (как реальный клиент) через
``service_exec`` curl на ``127.0.0.1:<listen_port>`` (мост bind'ится на loopback, к WireMock не ходит).

**Полный round-trip** (Cline → bridge → FSM → WireMock LiteLLM → SSE-ответ + archive ``egress_isomorph``)
порождает WireMock-трафик и потому ОБЯЗАН следовать модели §2/§7 [E2E_ISOLATION.md](../../docs/E2E_ISOLATION.md):
коррелятор ``X-Threlium-Thread-Root`` = **контент-адресуемый** ingress-MID, который тест предвычисляет из
тела запроса (`bridges.isomorph.history.extract_tail` + `IsomorphContentHashWire`), сидирует контекст
(`composite_context_key(stub_tag, thread_root)`) и регистрирует ту же L0-цепочку стабов, что mailflow.
Этот сценарий валидируется на живом harness — см. [docs/BRIDGE_ISOMORPH.md](../../docs/BRIDGE_ISOMORPH.md).
"""
from __future__ import annotations

import json

from .toolkit import (
    E2EComposeRuntime,
    TIMEOUT_POLL_SHORT,
    poll_until,
    service_exec,
)

# e2e group_vars/e2e.yml → bridges.isomorph.listen_port / api_key.
_ISO_BASE = "http://127.0.0.1:8040"
_API_KEY = "e2e-isomorph-api-key"


def _curl(rt: E2EComposeRuntime, argv_tail: str) -> tuple[int, str]:
    """curl изнутри SUT → (http_status, body). Статус — последняя строка вывода."""
    cmd = f"curl -sS --max-time 10 -w '\\n%{{http_code}}' {argv_tail}"
    r = service_exec(rt.project_name, "sut", ["bash", "-lc", cmd])
    out = r.stdout or ""
    lines = out.rsplit("\n", 1)
    if len(lines) != 2 or not lines[1].strip().isdigit():
        return (r.returncode if r.returncode else -1, out)
    return (int(lines[1].strip()), lines[0])


def _wait_bridge_health(rt: E2EComposeRuntime) -> None:
    def _ok() -> bool | None:
        status, body = _curl(rt, f"{_ISO_BASE}/health")
        return True if (status == 200 and '"ok"' in body) else None

    poll_until(
        _ok,
        timeout=TIMEOUT_POLL_SHORT,
        desc="isomorph bridge /health 200",
    )


def test_isomorph_bridge_health_and_models(e2e_runtime: E2EComposeRuntime) -> None:
    rt = e2e_runtime
    _wait_bridge_health(rt)

    status, body = _curl(rt, f"{_ISO_BASE}/health")
    assert status == 200, body
    assert json.loads(body) == {"status": "ok"}

    status, body = _curl(rt, f"{_ISO_BASE}/v1/models")
    assert status == 200, body
    payload = json.loads(body)
    assert payload["object"] == "list"
    assert payload["data"] and all("id" in m for m in payload["data"])


def test_isomorph_bridge_auth_rejected(e2e_runtime: E2EComposeRuntime) -> None:
    rt = e2e_runtime
    _wait_bridge_health(rt)

    # OpenAI surface без ключа → 401 + OpenAI error-envelope.
    body_json = json.dumps({"model": "x", "stream": False, "messages": [{"role": "user", "content": "hi"}]})
    status, body = _curl(
        rt,
        f"-X POST -H 'content-type: application/json' -d '{body_json}' {_ISO_BASE}/v1/chat/completions",
    )
    assert status == 401, body
    assert json.loads(body)["error"]["type"] == "authentication_error"


def test_isomorph_bridge_push_requires_secret(e2e_runtime: E2EComposeRuntime) -> None:
    rt = e2e_runtime
    _wait_bridge_health(rt)

    push = json.dumps({
        "ingress_mid": "nope", "api_surface": "openai_chat_completions", "finish_reason": "stop",
        "model": "x", "text": "t", "tool_blocks": [], "usage": {"prompt": 0, "completion": 0, "total": 0},
        "error_message": "",
    })
    # без push_secret → 403
    status, _ = _curl(
        rt, f"-X POST -H 'content-type: application/json' -d '{push}' {_ISO_BASE}/internal/v1/push"
    )
    assert status == 403
