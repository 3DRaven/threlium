"""Общий harness для e2e реального Cline CLI против isomorph-моста (Anthropic + OpenAI surfaces).

Cline для фикс-промпта в пустом cwd шлёт ДЕТЕРМИНИРОВАННЫЙ первый запрос. Хвост, который мост хеширует
в ingress-MID (= thread-root), — это ВСЁ присланное клиентом, слитое в один ``<system>`` (якоря — только
ответы Threlium-ассистента, которых на первом ходу нет; см. ``bridges/isomorph/history.py``): системный
промпт агента Cline + промпт пользователя + фиксированный ``[SYSTEM]``-суффикс. Системный промпт Cline
статичен с точностью до двух полей ``<env>`` — ``Date`` и ``Working Directory`` — оба известны тесту
заранее (сегодня + наш ``--cwd``). Поэтому тест **предвычисляет** thread-root тем же кодом моста
(:func:`parse_history` + :func:`ingress_message_id`), подставив сегодняшнюю дату и cwd в захваченный
шаблон :data:`_TEMPLATE_PATH`, и сидит контекст WireMock ДО запуска Cline — гонки «ingress→distill» нет.

Системный промпт Cline идентичен на обоих surface'ах (Anthropic держит его top-level полем ``system``,
OpenAI — первым элементом ``messages`` с ``role=system``); отличаются только обёртки сообщений. Если Cline
сменит шаблон/формат даты — предвычисленный thread-root разойдётся с фактическим (assert в тесте поймает),
и нужно перезахватить :data:`_TEMPLATE_PATH` / :data:`CLINE_USER_SUFFIX` (см. ``scripts`` debug-хелперы).
"""
from __future__ import annotations

import base64
import datetime
import json
import shlex
from pathlib import Path

from threlium.bridges.isomorph.history import ingress_message_id, parse_history
from threlium.types import IsomorphApiSurface

from . import TIMEOUT_POLL_SHORT, E2EComposeRuntime, poll_until, service_exec
from .constants import E2E_SUT_NOTMUCH_BASH_EXPORT

_TEMPLATE_PATH = Path(__file__).parent / "cline_system_prompt.tmpl"
_SYSTEM_TEMPLATE = _TEMPLATE_PATH.read_text(encoding="utf-8")

#: Суффикс, который Cline (`-y`) дописывает отдельным user-сообщением к первому запросу (захвачено).
CLINE_USER_SUFFIX = (
    "[SYSTEM] This run is not complete until you call one of these terminal completion tools: "
    "submit_and_exit. Continue working if requirements are not met. If the task is complete, "
    "call the appropriate terminal completion tool now."
)


def cline_today_mdy(today: datetime.date | None = None) -> str:
    """Сегодня в формате Cline ``<env>`` Date — ``M/D/YYYY`` без ведущих нулей (как ``6/5/2026``)."""
    d = today or datetime.date.today()
    return f"{d.month}/{d.day}/{d.year}"


def _cline_system_prompt(*, cwd: str, date: str) -> str:
    return _SYSTEM_TEMPLATE.replace("{{DATE}}", date).replace("{{CWD}}", cwd)


def thread_root_from_body(surface: IsomorphApiSurface, body: dict[str, object]) -> str:
    """thread-root (= ingress-MID первого хода) из ПРОИЗВОЛЬНОГО тела запроса — тем же кодом, что и мост.

    Для прямых HTTP-тестов (тест сам владеет телом → тривиальный предвычет, без Cline/даты/шаблона).
    Возвращает каноничную RFC-форму ``<...@localhost>`` — ключ контекста WireMock-стабов."""
    return ingress_message_id(parent_value="", tail_body=parse_history(surface, body).tail_body).value


def precompute_isomorph_thread_root(
    surface: IsomorphApiSurface, *, prompt: str, cwd: str, date: str | None = None
) -> str:
    """thread-root (= ingress-MID первого хода) тем же кодом, что и мост — из реконструкции запроса Cline.

    Реконструирует тело так, как его пришлёт Cline на данном surface (системный промпт с подставленными
    Date/cwd + промпт + ``[SYSTEM]``-суффикс), затем гоняет через ``parse_history`` + ``ingress_message_id``.
    Возвращает каноничную RFC-форму ``<...@localhost>`` — ровно то, что мост поставит в заголовок
    ``X-Threlium-Thread-Root`` и чем ключуется контекст WireMock-стабов.
    """
    sysp = _cline_system_prompt(cwd=cwd, date=date or cline_today_mdy())
    if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES:
        body: dict[str, object] = {
            "system": [{"type": "text", "text": sysp}],
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "text", "text": CLINE_USER_SUFFIX, "cache_control": {"type": "ephemeral"}},
                    ],
                }
            ],
        }
    else:
        body = {
            "messages": [
                {"role": "system", "content": sysp},
                {"role": "user", "content": prompt},
                {"role": "user", "content": CLINE_USER_SUFFIX},
            ]
        }
    return thread_root_from_body(surface, body)


# --- SUT-side хелперы (через service_exec как пользователь threlium) ------------------------


def sut_exec(rt: E2EComposeRuntime, script: str, *, timeout: float = 30.0) -> str:
    """``bash -lc`` как пользователь ``threlium`` в SUT → ``stdout.strip()``."""
    r = service_exec(
        rt.project_name, "sut",
        ["runuser", "-u", "threlium", "--", "bash", "-lc", script],
        timeout=timeout,
    )
    return (r.stdout or "").strip()


def nm_count(rt: E2EComposeRuntime, query: str) -> int:
    out = sut_exec(rt, f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count {shlex.quote(query)} 2>/dev/null || echo 0")
    return int(out) if out.isdigit() else 0


def nm_oldest_message_id(rt: E2EComposeRuntime, query: str) -> str:
    """``id:<inner>`` старейшего сообщения по запросу (без префикса ``id:``)."""
    out = sut_exec(
        rt,
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; "
        f"notmuch search --sort=oldest-first --output=messages {shlex.quote(query)} 2>/dev/null | head -1",
    )
    return out.removeprefix("id:")


def nm_test_thread_count(rt: E2EComposeRuntime, marker: str) -> int:
    """Сколько РАЗНЫХ notmuch-тредов несут маркер этого теста (continuity: ходы одной беседы → 1 тред)."""
    out = sut_exec(
        rt,
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; "
        f"notmuch search --output=threads {shlex.quote(f'from:isomorph@localhost and {marker}')} 2>/dev/null | wc -l",
    )
    return int(out) if out.isdigit() else 0


def extract_reply_text(surface: IsomorphApiSurface, resp_text: str) -> str:
    """Текст ответа ассистента из JSON-ответа моста (для эха в истории следующего хода)."""
    payload = json.loads(resp_text)
    if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES:
        return "".join(
            b.get("text", "") for b in payload.get("content", []) if isinstance(b, dict)
        )
    return "".join(
        (c.get("message", {}) or {}).get("content", "") or ""
        for c in payload.get("choices", []) if isinstance(c, dict)
    )


def build_continuation_body(
    surface: IsomorphApiSurface, base_body: dict[str, object], assistant_text: str, next_user: str
) -> dict[str, object]:
    """Тело СЛЕДУЮЩЕГО хода: история turn-1 + эхо ответа ассистента + новое user-сообщение.

    Мост проголосует по last-assistant (``hash(assistant_text)`` == glue прошлого хода) → IRT = тот glue →
    тот же тред. ``assistant_text`` ОБЯЗАН быть дословным ответом моста из turn-1 (иначе хеш не сойдётся)."""
    base_msgs = list(base_body.get("messages", []))  # type: ignore[arg-type]
    cont = [*base_msgs, {"role": "assistant", "content": assistant_text}, {"role": "user", "content": next_user}]
    out: dict[str, object] = {k: v for k, v in base_body.items() if k != "messages"}
    out["messages"] = cont
    return out


def nm_count_in_test_thread(rt: E2EComposeRuntime, marker: str, subquery: str) -> int:
    """Сколько сообщений, матчащих ``subquery``, лежит в треде(ах) ЭТОГО теста (по уникальному маркеру).

    Нужно из-за того, что teardown не стирает данные: ``from:egress_isomorph@localhost`` без скоупа поймал
    бы glue ДРУГОГО isomorph-теста. Резолвим thread-id по маркеру (он только в ingress turn-1), затем
    считаем внутри них (``thread:{...}``-подзапрос ломается на пробелах в notmuch 0.38, а ``thread:<id> and
    <subquery>`` — валиден)."""
    q_marker = f"from:isomorph@localhost and {marker}"
    out = sut_exec(
        rt,
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; t=0; "
        f"for tid in $(notmuch search --output=threads {shlex.quote(q_marker)} 2>/dev/null); do "
        f'c=$(notmuch count "$tid and {subquery}" 2>/dev/null || echo 0); t=$((t+c)); done; echo $t',
    )
    return int(out) if out.isdigit() else 0


def wait_bridge_health(rt: E2EComposeRuntime, *, port: int, timeout: float = TIMEOUT_POLL_SHORT) -> None:
    """Дождаться ``GET /health`` 200 у моста изнутри SUT.

    Сессионный cold-reset рестартует ``threlium-bridge@isomorph`` — мост может ещё не слушать, когда тест
    стартует. Cline переживал это ретраями коннекта, но прямой ``curl`` (JSON-тесты) — нет (``curl: (7)``).
    """
    def _ok() -> bool | None:
        out = sut_exec(rt, f"curl -sS --max-time 5 http://127.0.0.1:{port}/health 2>/dev/null || true")
        return True if '"ok"' in out else None

    poll_until(_ok, timeout=timeout, desc="isomorph bridge /health 200")


def clean_isomorph_test_threads(rt: E2EComposeRuntime, marker: str) -> None:
    """Удалить ТОЛЬКО треды ЭТОГО теста — по уникальному токену промпта (дата-независимо, все прошлые прогоны).

    Изоляция по соглашению проекта: setup чистит лишь свои прошлые данные, не трогая ни другие
    isomorph-тесты, ни отладочные треды. Резолвим thread-id'шники по маркеру (``thread:{...}``-подзапрос в
    notmuch 0.38 ломается на пробелах внутри ``{}``, поэтому через ``--output=threads``), затем удаляем ВСЕ
    файлы этих тредов (ingress + glue + egress-archive + фоновые reflect/memory/lightrag) и реиндексируем.
    Teardown намеренно НЕ чистит — данные последнего прогона остаются для отладки до следующего запуска теста.
    """
    q = f"from:isomorph@localhost and {marker}"
    sut_exec(
        rt,
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; "
        f"for tid in $(notmuch search --output=threads {shlex.quote(q)} 2>/dev/null); do "
        f'notmuch search --output=files "$tid" 2>/dev/null; done | xargs -r rm -f; '
        f"notmuch new >/dev/null 2>&1 || true",
    )


def configure_cline(
    rt: E2EComposeRuntime, *, provider: str, api_key: str, model: str, base_url: str, data_dir: str, cwd: str
) -> None:
    """Настроить Cline на мост. ``anthropic`` запрещает ``--baseurl`` (патчим providers.json), а
    ``openai-compatible`` принимает его флагом напрямую. Прямая запись providers.json не годится —
    cline-загрузчик сбрасывает не-каноничную запись (теряет apiKey)."""
    sut_exec(rt, f"rm -rf {shlex.quote(data_dir)}; mkdir -p {shlex.quote(cwd)}")
    if provider == "anthropic":
        sut_exec(
            rt,
            f"cline auth --provider {provider} --apikey {shlex.quote(api_key)} "
            f"--modelid {shlex.quote(model)} --data-dir {shlex.quote(data_dir)}",
        )
        # anthropic-провайдер запрещает --baseurl, поэтому дописываем его в providers.json пост-фактум.
        # ВАЖНО: путь в Python-коде — строковый литерал через repr (`{...!r}`), НЕ shlex.quote: для пути
        # без спецсимволов shlex.quote вернул бы его БЕЗ кавычек → `p=/tmp/...` = SyntaxError → патч молча
        # падает, baseUrl остаётся дефолтным (api.anthropic.com) → Cline бьётся туда и ловит invalid x-api-key.
        providers_json = f"{data_dir}/settings/providers.json"
        patch = (
            f"import json; p={providers_json!r}; d=json.load(open(p)); "
            f"d['providers']['anthropic']['settings']['baseUrl']={base_url!r}; "
            f"json.dump(d, open(p,'w')); "
            f"assert json.load(open(p))['providers']['anthropic']['settings']['baseUrl']=={base_url!r}"
        )
        sut_exec(rt, f"python3 -c {shlex.quote(patch)}")
        # Громкая проверка: baseUrl действительно прописан (иначе Cline пойдёт в облако, а не в мост).
        got = sut_exec(
            rt,
            "python3 -c "
            + shlex.quote(
                f"import json; print(json.load(open({providers_json!r}))"
                "['providers']['anthropic']['settings'].get('baseUrl',''))"
            ),
        )
        if got != base_url:
            raise AssertionError(f"cline anthropic baseUrl patch failed: got {got!r}, want {base_url!r}")
    else:
        sut_exec(
            rt,
            f"cline auth --provider {provider} --apikey {shlex.quote(api_key)} "
            f"--baseurl {shlex.quote(base_url)} --modelid {shlex.quote(model)} "
            f"--data-dir {shlex.quote(data_dir)}",
        )


def start_cline_background(
    rt: E2EComposeRuntime, *, provider: str, data_dir: str, cwd: str, out_path: str, prompt: str,
    cli_timeout_sec: int = 120, task_timeout_sec: int = 110,
) -> None:
    """``nohup``-фоновый headless Cline (`-y --json`) на мост; вывод (включая SSE-ответ) — в ``out_path``."""
    sut_exec(
        rt,
        f"rm -f {shlex.quote(out_path)}; nohup timeout {cli_timeout_sec} cline -y --json "
        f"--provider {provider} --data-dir {shlex.quote(data_dir)} -t {task_timeout_sec} "
        f"--cwd {shlex.quote(cwd)} {shlex.quote(prompt)} > {shlex.quote(out_path)} 2>&1 & echo started",
        timeout=15.0,
    )


def cline_received(rt: E2EComposeRuntime, out_path: str, marker: str) -> bool:
    out = sut_exec(rt, f"grep -c {shlex.quote(marker)} {shlex.quote(out_path)} 2>/dev/null || true")
    return out.isdigit() and int(out) >= 1


def bridge_post_json(
    rt: E2EComposeRuntime, *, port: int, path: str, body: dict[str, object],
    api_key: str, surface: IsomorphApiSurface, timeout: float = 120.0,
) -> tuple[int, str]:
    """Синхронный (``stream:false``) POST JSON-тела в мост ИЗНУТРИ SUT (loopback ``127.0.0.1:<port>``).

    Мост держит соединение, пока FSM не отработает и egress не запушит, затем возвращает финальный JSON
    (ветка ``_await_json``). Возвращает ``(http_status, response_text)``. Тело передаём base64 → ``base64 -d``
    → ``curl --data-binary @-`` (без хрупкого shell-эскейпинга JSON); auth-заголовок per-surface
    (Anthropic — ``x-api-key``, OpenAI — ``Authorization: Bearer``)."""
    raw = json.dumps(body)
    b64 = base64.b64encode(raw.encode("utf-8")).decode("ascii")
    auth = (
        f"-H 'x-api-key: {api_key}'"
        if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES
        else f"-H 'authorization: Bearer {api_key}'"
    )
    cmd = (
        f"echo {b64} | base64 -d | curl -sS --max-time {int(timeout)} -w '\\n%{{http_code}}' "
        f"-X POST -H 'content-type: application/json' {auth} --data-binary @- "
        f"http://127.0.0.1:{port}{path}"
    )
    out = sut_exec(rt, cmd, timeout=timeout + 30.0)
    lines = out.rsplit("\n", 1)
    if len(lines) != 2 or not lines[1].strip().isdigit():
        return (-1, out)
    return (int(lines[1].strip()), lines[0])
