"""WireMock Admin API для e2e: регистрация маппингов, журнал, опционально Jinja2.

**Рекомендуемый** путь — статические ``*.json`` в ``tests/e2e/wiremock_stubs/<тест>/``
(:func:`register_wiremock_mapping_directory`, один каталог на тест). Matrix live e2e на общем
WireMock — :func:`upsert_wiremock_mapping_directory` (``PUT /__admin/mappings/{id}`` + fallback
``POST``), без удаления маппингов в конце прогона. **Не** удалять маппинги чужих сценариев между
тестами — изоляция на общем инстансе через **WireMock State Extension** (``state-matcher`` + сид
контекста; полный сброс контекстов в ``pytest_sessionfinish``, не в function-teardown), см.
``docs/E2E_ISOLATION.md`` и ``compose/wiremock/README.md``.
На **общем** инстансе не вызывать :func:`reset_request_journal` из
кода сценариев — он чистит весь журнал; разрешён **один** автоматический вызов до прогона e2e в
``conftest.py``. **Unmatched** (``GET /__admin/requests/unmatched``) после старта тестов **не**
чистятся: при любых записях падает guard в [`pytest_runtest_call`](../tests/e2e/conftest.py) до и после
тела теста (глобально по инстансу; без повторного ``DELETE /__admin/requests``
кроме одного инфраструктурного сброса в начале прогона: там же единоразово
удаление non-bootstrap маппингов и сброс Store State — см. :func:`reset_non_bootstrap_wiremock_mappings`).
Сам хук guard **не** оборачивают локами;
межпроцессные ``FileLock`` (файл ``e2e_wiremock_admin_api.lock`` в
:func:`~tests.e2e.helpers.e2e_compose_coord_dir`; захват — ``THRELIUM_E2E_WIREMOCK_ADMIN_API_LOCK_TIMEOUT``,
по умолчанию 90 с, значение кэшируется на процесс) — :func:`_wiremock_admin_api_exclusive`: сериализация
всех обращений к ``/__admin/…`` и тесно связанных ``__threlium/…`` на общем WireMock при ``pytest -n N``
(в т.ч. unmatched GET, bootstrap upsert с фиксированными ``id``, ``matrix_rooms``). Чтение журнала по
``stub_tag`` (:func:`journal_entries_for_stub_tag`) — **один** lock на цикл пагинации ``/mappings`` плюс
все ``matchingStub``-GET, чтобы поллинг не отпускал lock между запросами. HTTP к Admin API — через
``requests.Session`` на поток (:func:`_wm_session`). Вложенные вызовы в одном
потоке не берут второй ``FileLock`` (избегаем deadlock). Сценарные стабы — отдельные каталоги и
:func:`wiremock_stub_id_for_e2e_stub_relpath`.
Единственная автоматическая «нулевая» unmatched —
полный cold reset перед сессией (журнал + все маппинги + State). Типовой цикл сценария:
:func:`prepare_wiremock_scenario` (при старте сценария по-прежнему чистит matched-журнал по
``stub_tag``) и :func:`teardown_wiremock_scenario` (**после** теста журнал не трогает — для
ручной отладки). Для выборочной очистки **matched-журнала** по своему ``stub_tag`` вручную:
:func:`remove_wiremock_journal_by_stub_tag`
(``remove-by-metadata`` на журнал; **без** удаления unmatched). Удаление **стабов** чужого
``stub_tag`` через :func:`remove_wiremock_mappings_by_stub_tag` — только если явно чистите **свой**
сценарий, не чужие.
Jinja2 — ``mock_templates/`` и :func:`register_from_template``.
При 500 на полный ``GET /__admin/requests``: ``THRELIUM_E2E_WIREMOCK_STRICT_JOURNAL=1`` запрещает
rebuild по ``matchingStub``; ``THRELIUM_E2E_WIREMOCK_ALLOW_JOURNAL_REBUILD=0`` — тоже.
"""
from __future__ import annotations

import base64
import contextlib
import functools
import hashlib
import json
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence
from urllib.parse import parse_qs, quote

import requests
from jinja2 import Environment, FileSystemLoader
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_delay,
    wait_exponential,
)

from filelock import FileLock

from tests.e2e.log import clip_log_body, log

from .helpers import (
    POLL_INTERVAL,
    TIMEOUT_POLL_SHORT,
    _compose_container,
    e2e_compose_coord_dir,
    poll_until,
)

# Инфраструктура compose: ``compose_bootstrap/*.json`` — ``recordState``
# (``000`` сид ``active``) и embeddings readiness для SMTP/IMAP probe (§TESTING).
# Сценарные OpenAI/Matrix — только в своём каталоге.
WIREMOCK_E2E_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
WIREMOCK_E2E_COMPOSE_BOOTSTRAP_DIR = WIREMOCK_E2E_STUBS_ROOT / "compose_bootstrap"
MOCK_TEMPLATES_DIR = Path(__file__).resolve().parent / "mock_templates"
THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG = "threlium-e2e-compose-bootstrap"

# HTTP к WireMock: один ``requests.Session`` на поток (xdist — отдельные процессы; внутри процесса
# меньше TCP handshake при поллинге журнала).
_wm_tls = threading.local()


def _wm_session() -> requests.Session:
    s = getattr(_wm_tls, "session", None)
    if s is None:
        s = requests.Session()
        _wm_tls.session = s
    return s


def _wm_json_response(r: requests.Response, *, where: str) -> Any:
    """Парсинг JSON ответа Admin API с явной ошибкой (не голый ``JSONDecodeError``)."""
    try:
        return r.json()
    except json.JSONDecodeError as exc:
        preview = (r.text or "")[:400].replace("\n", " ")
        raise RuntimeError(
            f"WireMock Admin JSON decode failed ({where}): HTTP {r.status_code}, {exc}; body≈{preview!r}"
        ) from exc


def _merge_mapping_payload(
    mapping: dict[str, Any], metadata: dict[str, Any]
) -> dict[str, Any]:
    """Копия маппинга с ``metadata``, смёрженным как в :func:`register_mapping` / :func:`upsert_mapping`."""
    payload = dict(mapping)
    existing = payload.get("metadata")
    if isinstance(existing, dict):
        payload["metadata"] = {**existing, **metadata}
    else:
        payload["metadata"] = dict(metadata)
    return payload


def _poll_wiremock_with_tenacity(
    *,
    probe: Callable[[], bool],
    wait_timeout_sec: float | None,
    diag_callback: Callable[[], None] | None,
    build_error: Callable[[], str],
) -> None:
    """Общий поллинг tenacity для assert'ов WireMock (без копипасты импортов/декоратора)."""
    w = float(TIMEOUT_POLL_SHORT) if wait_timeout_sec is None else float(wait_timeout_sec)

    @retry(
        stop=stop_after_delay(w),
        wait=wait_exponential(multiplier=0.25, min=0.5, max=5),
        retry=retry_if_result(lambda r: r is not True) | retry_if_exception_type(Exception),
    )
    def _runner() -> bool | None:
        return True if probe() else None

    try:
        _runner()
    except RetryError:
        if diag_callback is not None:
            try:
                diag_callback()
            except Exception:  # pragma: no cover
                pass
        raise AssertionError(build_error()) from None


# UUID5 namespace: стабильный ``id`` маппинга по относительному пути под ``wiremock_stubs/``.
_THRELIUM_E2E_WIREMOCK_STUB_ID_NS = uuid.UUID("c9e0b8a4-7f2d-5a1e-9c3b-0e0e0e0e0e01")


def wiremock_stub_id_for_e2e_stub_relpath(relative_key: str) -> str:
    """Стабильный UUID маппинга WireMock для ключа ``<каталог_теста>/<файл>.json`` (не только имя файла).

    ``relative_key`` — путь POSIX относительно родителя каталога со стабами (как правило
    ``tests/e2e/wiremock_stubs``), например ``test_matrix_wiremock_live_e2e/008_embeddings_batch.json``,
    чтобы разные тесты с одинаковым именем JSON не получали один ``id``.
    """
    k = str(relative_key).strip().replace("\\", "/").strip("/")
    if not k:
        raise ValueError("wiremock_stub_id_for_e2e_stub_relpath: empty relative_key")
    return str(uuid.uuid5(_THRELIUM_E2E_WIREMOCK_STUB_ID_NS, k))


# Имя поля в ``mapping.metadata`` для ``POST /__admin/mappings/remove-by-metadata`` (WireMock ≥ 2.28).
THRELIUM_E2E_WIREMOCK_STUB_TAG_KEY = "threlium_e2e_stub_tag"

# После дампа журнала: опционально только записи **этого** тега стаба
# (см. :func:`remove_wiremock_journal_by_stub_tag`), не весь журнал.
THRELIUM_E2E_WIREMOCK_CLEAR_JOURNAL_AFTER_TEST = "THRELIUM_E2E_WIREMOCK_CLEAR_JOURNAL_AFTER_TEST"

# Межпроцессный lock для Admin API и связанных стабов на том же инстансе WM (см. ``_wiremock_admin_api_exclusive``).
_E2E_WIREMOCK_ADMIN_API_LOCK = "e2e_wiremock_admin_api.lock"

_WIREMOCK_DUMP_BODY_MAX = 12_000
# Пагинация ``GET /__admin/mappings`` (``limit``/``offset`` в WireMock core), когда нужен полный
# список ``id`` стабов для обхода журнала через ``matchingStub``.
_WIREMOCK_ADMIN_MAPPINGS_PAGE_SIZE = int(
    os.environ.get("THRELIUM_E2E_WIREMOCK_ADMIN_MAPPINGS_PAGE", "500")
)
# Если ``GET …/requests?matchingStub=`` без ``limit`` даёт 500 — один повтор с этим limit (WM не даёт
# offset по журналу; чанки «старше» одним ``since`` не обходятся).
_WIREMOCK_MATCHING_STUB_JOURNAL_LIMIT_ON_500 = int(
    os.environ.get("THRELIUM_E2E_WIREMOCK_MATCHING_STUB_JOURNAL_LIMIT_ON_500", "300")
)

# Якоря фаз live e2e (LLM) — **те же**, что ``bodyPatterns`` в
# ``tests/e2e/wiremock_stubs/test_*_wiremock_live_e2e/*.json`` (канал-агностичные маркеры).
# После перехода на гранулярный ``X-Threlium-Call-Site`` для lightrag фаз prompt-якоря
# **не** используются для entity/keywords: дискриминатор — заголовок, не body.
# Оставлены как fallback / документация.
_E2E_ENTITY_USER_ANCHOR = (
    "Extract entities and relationships from the input text in Data to be Processed below."
)
_E2E_ENTITY_FIRST_PASS_INSTRUCTION_ANCHOR = "**Strict Adherence to Format:**"
_E2E_KEYWORDS_SYSTEM_PREFIX = "You are an expert keyword extractor"
# Текст ответа агента из reasoning-стаба Matrix (100_chat_reasoning_egress_tool.json) —
# должен появиться в теле PUT send/m.room.message.
_MATRIX_AGENT_REPLY_BODY = "ok matrix wiremock live e2e"


def _e2e_hay_matches_enrich_plan_contract(hay: str) -> bool:
    """Семантика ``080_chat_enrich_plan.json`` без ``.*`` по всему телу (избегаем ReDoS на огромных JSON)."""
    low = hay.casefold()
    return (
        ("formulate a retrieval question" in low or "knowledge graph" in low)
        and "indexed email" in low
    )


def _env_truthy(raw: str | None) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in ("1", "true", "yes", "on")


@functools.cache
def _env_journal_strict() -> bool:
    """Запрет «тихой» неполноты журнала (rebuild / усечение matchingStub). Значение env кэшируется на процесс."""
    return _env_truthy(os.environ.get("THRELIUM_E2E_WIREMOCK_STRICT_JOURNAL"))


@functools.cache
def _env_allow_journal_rebuild() -> bool:
    """При 500 на полный ``GET /__admin/requests`` разрешить обход через ``matchingStub`` + unmatched."""
    return _env_truthy(os.environ.get("THRELIUM_E2E_WIREMOCK_ALLOW_JOURNAL_REBUILD", "1"))


@functools.cache
def _wiremock_admin_api_lock_acquire_sec() -> float:
    return float(os.environ.get("THRELIUM_E2E_WIREMOCK_ADMIN_API_LOCK_TIMEOUT", "90.0"))


_admin_api_thread_depth = threading.local()


@contextlib.contextmanager
def _wiremock_admin_api_exclusive(*, timeout: float = TIMEOUT_POLL_SHORT) -> Iterator[None]:
    """Сериализация доступа к WireMock Admin API между pytest-xdist workers.

    Один и тот же поток может входить вложенно (без повторного ``FileLock`` на том же файле).
    ``FileLock`` сериализует **процессы** (xdist); ``threading.local`` — только реентрантность в
    **текущем** потоке. Другой поток того же процесса не унаследует ``depth`` и не должен
    параллелить Admin API без отдельной дисциплины (в e2e обычно один поток на воркер).

    Таймаут **захвата** ``FileLock`` задаётся только ``THRELIUM_E2E_WIREMOCK_ADMIN_API_LOCK_TIMEOUT``
    (по умолчанию 90 с) и **не** смешивается с ``timeout`` HTTP-вызовов снаружи — иначе короткий
    poll-``timeout`` превращался бы в минуты ожидания блокировки.
    """
    depth = int(getattr(_admin_api_thread_depth, "depth", 0))
    if depth:
        _admin_api_thread_depth.depth = depth + 1
        try:
            yield
        finally:
            _admin_api_thread_depth.depth = depth
        return
    coord = e2e_compose_coord_dir()
    coord.mkdir(parents=True, exist_ok=True)
    lock_path = coord / _E2E_WIREMOCK_ADMIN_API_LOCK
    _admin_api_thread_depth.depth = 1
    lock_acquire = _wiremock_admin_api_lock_acquire_sec()
    try:
        with FileLock(str(lock_path), timeout=lock_acquire):
            yield
    finally:
        _admin_api_thread_depth.depth = 0


def _normalize_wiremock_public_root(url: str) -> str:
    """Корень HTTP WireMock ``scheme://host:port`` без хвоста ``/__admin`` (если передали по ошибке)."""
    u = str(url).rstrip("/")
    if u.endswith("/__admin"):
        return u[: -len("/__admin")]
    return u


def wiremock_public_base(host: str, port: int) -> str:
    """Базовый URL WireMock с хоста pytest (mapped port)."""
    return f"http://{host}:{port}"


def wiremock_admin_base(public_base: str) -> str:
    """Префикс Admin API: ``http://host:port/__admin``."""
    root = _normalize_wiremock_public_root(public_base)
    return f"{root}/__admin"


def wiremock_e2e_state_setup_post_url(public_base: str) -> str:
    """Публичный URL служебного стаба с ``recordState``.

    См. ``wiremock_stubs/compose_bootstrap/000_e2e_state_setup.json``.
    """
    root = _normalize_wiremock_public_root(public_base)
    return f"{root}/__threlium/e2e/state/setup"


def composite_context_key(stub_tag: str, correlation_key: str) -> str:
    """Составной ключ контекста WireMock State Extension: ``{stub_tag}::{correlation_key}``.

    Используется как имя контекста при seed и во всех ``hasContext`` шаблонах JSON-стабов.
    Обеспечивает изоляцию параллельных тестов на общем WireMock без ``live_lane``.
    """
    return f"{stub_tag}::{correlation_key}"


def wiremock_state_seed_context(
    public_base: str, correlation_key: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> None:
    """POST на setup-стаб: тело ``{\"correlation_key\": ...}`` → ``recordState`` с тем же ``context``.

    ``correlation_key`` должен быть составным (см. :func:`composite_context_key`).
    """
    with _wiremock_admin_api_exclusive(timeout=timeout):
        url = wiremock_e2e_state_setup_post_url(public_base)
        r = _wm_session().post(url, json={"correlation_key": correlation_key}, timeout=timeout)
        r.raise_for_status()


def wiremock_e2e_state_reasoning_release_post_url(public_base: str) -> str:
    """Публичный URL стаба ``015_e2e_state_reasoning_release.json`` (сценарный каталог)."""
    root = _normalize_wiremock_public_root(public_base)
    return f"{root}/__threlium/e2e/state/reasoning_release"


def wiremock_state_reasoning_gate_release(
    public_base: str,
    correlation_key: str,
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Выставить в контексте State Extension свойство ``reasoning_release`` (снять 307-gate у reasoning).

    ``correlation_key`` должен быть составным (см. :func:`composite_context_key`).
    См. ``docs/E2E_ISOLATION.md`` §8.6.
    """
    with _wiremock_admin_api_exclusive(timeout=timeout):
        url = wiremock_e2e_state_reasoning_release_post_url(public_base)
        r = _wm_session().post(
            url,
            json={"correlation_key": correlation_key},
            timeout=timeout,
        )
        r.raise_for_status()


def wiremock_journal_request_body(entry: dict[str, Any]) -> str:
    """Тело входящего запроса из записи журнала WireMock (plain или base64)."""
    return _journal_request_body(entry)


def wiremock_state_delete_context(
    public_base: str, context: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> None:
    """DELETE ``/__admin/state-extension/contexts/{context}`` (имя контекста URL-encoded).

    Не использовать в prepare/teardown сценариев и live e2e: см. ``docs/E2E_ISOLATION.md``.
    Допустимо в точечных тестах Admin API и ручной отладке.
    """
    with _wiremock_admin_api_exclusive(timeout=timeout):
        admin = wiremock_admin_base(public_base).rstrip("/")
        enc = quote(context, safe="")
        url = f"{admin}/state-extension/contexts/{enc}"
        r = _wm_session().delete(url, timeout=timeout)
        if r.status_code not in (200, 204, 404):
            r.raise_for_status()


def wiremock_state_reset_all_contexts(
    public_base: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> None:
    """DELETE ``/__admin/state-extension/contexts`` — полный сброс Store расширения State на инстансе WM.

    Только для точечных админ-операций и :func:`pytest_sessionfinish` в ``conftest.py``.
    В коде сценариев и live-подготовке **не** вызывать — при ``pytest -n N`` снесёт чужие контексты
    и даст массовый unmatched на общем инстансе.
    """
    with _wiremock_admin_api_exclusive(timeout=timeout):
        admin = wiremock_admin_base(public_base).rstrip("/")
        url = f"{admin}/state-extension/contexts"
        r = _wm_session().delete(url, timeout=timeout)
        if r.status_code not in (200, 204):
            r.raise_for_status()


def reset_wiremock_scenarios(public_base: str, *, timeout: float = TIMEOUT_POLL_SHORT) -> None:
    """POST ``/__admin/scenarios/reset`` — сброс встроенных WireMock ``scenarioName`` / ``requiredScenarioState``.

    На общем инстансе вызывать только для своего прогона после загрузки нужных маппингов,
    иначе сломает параллельные тесты со сценариями.
    """
    with _wiremock_admin_api_exclusive(timeout=timeout):
        admin = wiremock_admin_base(public_base).rstrip("/")
        url = f"{admin}/scenarios/reset"
        r = _wm_session().post(url, timeout=timeout)
        r.raise_for_status()


def wiremock_matrix_register_room(
    public_base: str,
    *,
    room_id: str,
    event_id: str,
    event_body: str = "e2e matrix user message",
    room_name: str = "E2E Matrix Room",
    sender: str = "@remote:mock",
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Добавить комнату в shared list ``matrix_rooms`` (State Extension).

    Стаб ``010_e2e_matrix_register_room.json`` → ``recordState addLast``.
    Параллельные тесты добавляют свои комнаты — стаб ``/sync`` (``response-template``)
    собирает ответ из **всех** элементов list.

    На общем WireMock POST сериализуется через :func:`_wiremock_admin_api_exclusive` (тот же lock, что и Admin API).
    """
    root = _normalize_wiremock_public_root(public_base)
    url = f"{root}/__threlium/e2e/matrix/register_room"
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().post(
            url,
            json={
                "room_id": room_id,
                "event_id": event_id,
                "event_body": event_body,
                "room_name": room_name,
                "sender": sender,
            },
            timeout=timeout,
        )
        r.raise_for_status()


def wiremock_matrix_unregister_room(
    public_base: str,
    *,
    room_id: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Удалить свою комнату из shared list ``matrix_rooms`` (State Extension).

    Стаб ``011_e2e_matrix_unregister_room.json`` → ``deleteState deleteWhere room_id``.

    Сериализация — :func:`_wiremock_admin_api_exclusive` (как у ``register_room``).
    """
    root = _normalize_wiremock_public_root(public_base)
    url = f"{root}/__threlium/e2e/matrix/unregister_room"
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().post(url, json={"room_id": room_id}, timeout=timeout)
        r.raise_for_status()


def wiremock_telegram_register_update(
    public_base: str,
    *,
    update_id: int,
    chat_id: int,
    message_id: int,
    text: str,
    from_id: int = 12345,
    from_first_name: str = "E2EUser",
    from_username: str = "e2e_user",
    msg_date: int = 1700000000,
    message_thread_id: int | None = None,
    chat_title: str = "",
    thread_kind: str = "",
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Добавить update в shared list ``telegram_updates`` (State Extension).

    Стаб ``032_e2e_telegram_register_update.json`` → ``recordState addLast``.
    ``thread_kind`` — непустая строка для forum topic (ветка supergroup + ``message_thread_id`` в
    ``031_telegram_get_updates.json``); пустая — личка.
    """
    root = _normalize_wiremock_public_root(public_base)
    url = f"{root}/__threlium/e2e/telegram/register_update"
    mtid = 0 if message_thread_id is None else int(message_thread_id)
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().post(
            url,
            json={
                "update_id": update_id,
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "from_id": from_id,
                "from_first_name": from_first_name,
                "from_username": from_username,
                "msg_date": int(msg_date),
                "chat_title": chat_title,
                "message_thread_id": mtid,
                "thread_kind": thread_kind,
            },
            timeout=timeout,
        )
        r.raise_for_status()


def wiremock_telegram_unregister_update(
    public_base: str,
    *,
    update_id: int,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Удалить update из ``telegram_updates`` по ``update_id`` (State Extension).

    Стаб ``033_e2e_telegram_unregister_update.json`` → ``deleteState deleteWhere``.
    """
    root = _normalize_wiremock_public_root(public_base)
    url = f"{root}/__threlium/e2e/telegram/unregister_update"
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().post(url, json={"update_id": update_id}, timeout=timeout)
        r.raise_for_status()


def upsert_wiremock_compose_bootstrap_stubs(
    public_base: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> None:
    """Зарегистрировать каталог ``compose_bootstrap/``: State ``recordState`` + embeddings readiness probe.

    Это **единственный** общий для стека набор стабов (фиксированные ``id`` в JSON на диске). Сценарная
    изоляция на общем WireMock — через State Extension и свой каталог ``wiremock_stubs/<тест>/``, не через
    дублирование bootstrap. При ``pytest -n N`` upsert сериализуется :func:`_wiremock_admin_api_exclusive`:
    иначе два воркера после ``PUT 404`` шлют ``POST`` с
    тем же ``id`` → 422 Duplicate stub mapping ID.
    """
    with _wiremock_admin_api_exclusive(timeout=timeout):
        upsert_wiremock_mapping_directory(
            public_base,
            WIREMOCK_E2E_COMPOSE_BOOTSTRAP_DIR,
            stub_tag=THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
            timeout=timeout,
        )


def _list_all_stub_mappings(
    public_base: str,
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
    skip_admin_lock: bool = False,
) -> list[dict[str, Any]]:
    """Все маппинги: ``GET /__admin/mappings`` с пагинацией ``limit``/``offset``.

    По умолчанию весь цикл пагинации под **одним** :func:`_wiremock_admin_api_exclusive`, чтобы не
    отпускать межпроцессный lock между страницами. При ``skip_admin_lock=True`` вызывающий уже
    держит lock (см. :func:`_journal_entries_for_stub_tag_with_trunc`).
    """
    root = _normalize_wiremock_public_root(public_base)
    url = f"{root}/__admin/mappings"
    page = max(1, int(_WIREMOCK_ADMIN_MAPPINGS_PAGE_SIZE))

    def _page_loop() -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            r = _wm_session().get(
                url,
                params={"limit": str(page), "offset": str(offset)},
                timeout=timeout,
            )
            r.raise_for_status()
            data = _wm_json_response(r, where="GET /__admin/mappings")
            if not isinstance(data, dict):
                break
            batch = data.get("mappings")
            if not isinstance(batch, list):
                break
            for m in batch:
                if isinstance(m, dict):
                    out.append(m)
            n_batch = len(batch)
            offset += n_batch
            meta = data.get("meta")
            total = meta.get("total") if isinstance(meta, dict) else None
            if n_batch == 0:
                break
            if n_batch < page:
                break
            if isinstance(total, int) and offset >= total:
                break
        return out

    if skip_admin_lock:
        return _page_loop()
    with _wiremock_admin_api_exclusive(timeout=timeout):
        return _page_loop()


def _stub_uuids_from_mapping_list(
    mappings: Sequence[dict[str, Any]], stub_tag: str
) -> list[str]:
    want = str(stub_tag).strip()
    uuids: list[str] = []
    for m in mappings:
        meta = m.get("metadata")
        if not isinstance(meta, dict):
            continue
        if str(meta.get(THRELIUM_E2E_WIREMOCK_STUB_TAG_KEY) or "").strip() != want:
            continue
        mid = m.get("id")
        if mid:
            uuids.append(str(mid))
    return uuids


def _stub_uuids_for_e2e_stub_tag(
    public_base: str, stub_tag: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> list[str]:
    return _stub_uuids_from_mapping_list(
        _list_all_stub_mappings(public_base, timeout=timeout), stub_tag
    )


def _all_stub_mapping_uuids(
    public_base: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> list[str]:
    uuids: list[str] = []
    for m in _list_all_stub_mappings(public_base, timeout=timeout):
        mid = m.get("id")
        if mid:
            uuids.append(str(mid))
    return uuids


def _journal_request_logged_ms_optional(entry: dict[str, Any]) -> int | None:
    """Метка времени записи журнала (мс) или ``None``, если даты нет / не распарсились."""
    req = entry.get("request")
    if not isinstance(req, dict):
        req = entry
    if not isinstance(req, dict):
        return None
    v = req.get("loggedDate")
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return int(v)
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    s = req.get("loggedDateString")
    if isinstance(s, str) and s.strip():
        t = s.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(t)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    return None


def _journal_entries_sorted_newest_first(
    entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Как у WireMock: новые первыми; без ``loggedDate`` — в хвосте, порядок стабилен по индексу."""
    tagged = list(enumerate(entries))

    def key(t: tuple[int, dict[str, Any]]) -> tuple[int, int, int]:
        i, e = t
        ms = _journal_request_logged_ms_optional(e)
        if ms is not None:
            return (0, -ms, i)
        return (1, 0, i)

    return [e for _, e in sorted(tagged, key=key)]


def _journal_entries_sorted_chrono_asc(
    entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Старые → новые; без даты — после всех строк с известным временем."""
    tagged = list(enumerate(entries))

    def key(t: tuple[int, dict[str, Any]]) -> tuple[int, int, int]:
        i, e = t
        ms = _journal_request_logged_ms_optional(e)
        if ms is not None:
            return (0, ms, i)
        return (1, i, 0)

    return [e for _, e in sorted(tagged, key=key)]


def _unmatched_stable_entry_id(u: dict[str, Any]) -> str:
    rid = u.get("id")
    if rid is not None and str(rid).strip():
        return f"unmatched-wm:{rid}"
    blob = json.dumps(u, sort_keys=True, ensure_ascii=False, default=str)
    digest = hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]
    return f"unmatched-h:{digest}"


def _journal_wrap_unmatched_entries(
    entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Плоские ``/requests/unmatched`` → ``ServeEvent``; стабильный ``id``, без дублей в одном списке."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for u in entries:
        if not isinstance(u, dict):
            continue
        eid = _unmatched_stable_entry_id(u)
        if eid in seen:
            continue
        seen.add(eid)
        out.append({"id": eid, "request": u, "wasMatched": False})
    return out


def _extract_requests_from_journal_response(
    data: dict[str, Any],
) -> tuple[list[dict[str, Any]], int | None]:
    reqs = data.get("requests")
    if not isinstance(reqs, list):
        return [], None
    out = [x for x in reqs if isinstance(x, dict)]
    meta = data.get("meta")
    total = meta.get("total") if isinstance(meta, dict) else None
    return out, total if isinstance(total, int) else None


def _raise_if_strict_matching_stub_truncated(where: str, truncated: bool) -> None:
    if truncated and _env_journal_strict():
        raise RuntimeError(
            f"WireMock: усечён журнал ({where}) при THRELIUM_E2E_WIREMOCK_STRICT_JOURNAL=1 "
            f"(meta.total > len). Уменьшите тела ответов, поднимите heap или "
            f"--max-request-journal-entries."
        )


def _fetch_journal_for_matching_stub(
    public_base: str,
    stub_uuid: str,
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
    skip_admin_lock: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    """Журнал для ``matchingStub``. Возвращает ``(entries, truncated)``.

    ``skip_admin_lock=True`` — вызывающий уже под :func:`_wiremock_admin_api_exclusive`
    (батч чтения журнала по нескольким UUID без отпускания lock между GET).
    """
    root = _normalize_wiremock_public_root(public_base)
    url = f"{root}/__admin/requests"
    uid = str(stub_uuid).strip()
    lim_retry = max(50, int(_WIREMOCK_MATCHING_STUB_JOURNAL_LIMIT_ON_500))

    def _get(params: dict[str, str]) -> requests.Response:
        if skip_admin_lock:
            return _wm_session().get(url, params=params, timeout=timeout)
        with _wiremock_admin_api_exclusive(timeout=timeout):
            return _wm_session().get(url, params=params, timeout=timeout)

    r = _get({"matchingStub": uid})
    if r.status_code != 500:
        r.raise_for_status()
        data = _wm_json_response(r, where=f"GET /__admin/requests matchingStub={uid}")
        if not isinstance(data, dict):
            return [], False
        reqs, total = _extract_requests_from_journal_response(data)
        truncated = bool(total is not None and total > len(reqs))
        if truncated:
            log.error(
                "wiremock_matching_stub_truncated",
                matching_stub=uid,
                meta_total=total,
                len_reqs=len(reqs),
                no_limit=True,
            )
        _raise_if_strict_matching_stub_truncated(f"matchingStub={uid}", truncated)
        return reqs, truncated

    log.warning(
        "wiremock_matching_stub_retry_with_limit",
        matching_stub=uid,
        status_code=r.status_code,
        limit=lim_retry,
    )
    r2 = _get({"matchingStub": uid, "limit": str(lim_retry)})
    r2.raise_for_status()
    data2 = _wm_json_response(r2, where=f"GET /__admin/requests matchingStub={uid} limit")
    if not isinstance(data2, dict):
        return [], False
    reqs2, total2 = _extract_requests_from_journal_response(data2)
    truncated2 = bool(total2 is not None and total2 > len(reqs2))
    if truncated2:
        log.error(
            "wiremock_matching_stub_truncated",
            matching_stub=uid,
            limit=lim_retry,
            meta_total=total2,
            len_reqs=len(reqs2),
        )
    _raise_if_strict_matching_stub_truncated(f"matchingStub={uid}", truncated2)
    return reqs2, truncated2


def _merge_journal_requests_for_matching_stub_uuids(
    public_base: str,
    stub_uuids: Sequence[str],
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
    skip_admin_lock: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    """Matched-журнал по ``matchingStub``; сортировка как у WM; ``truncated`` по ``meta.total``.

    По умолчанию весь цикл под одним :func:`_wiremock_admin_api_exclusive`. При
    ``skip_admin_lock=True`` lock уже у внешнего вызывающего (см. журнал по ``stub_tag``).
    """
    def _body() -> tuple[list[dict[str, Any]], bool]:
        any_trunc = False
        merged: list[dict[str, Any]] = []
        for uid in stub_uuids:
            chunk, tr = _fetch_journal_for_matching_stub(
                public_base, uid, timeout=timeout, skip_admin_lock=True
            )
            any_trunc = any_trunc or tr
            merged.extend(chunk)
        return _journal_entries_sorted_newest_first(merged), any_trunc

    if skip_admin_lock:
        return _body()
    with _wiremock_admin_api_exclusive(timeout=timeout):
        return _body()


def _admin_requests_matched_plus_unmatched_fallback(
    public_base: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> dict[str, Any]:
    """Сбор при 500 на полный ``GET /__admin/requests`` (неполный по определению для удалённых стабов)."""
    if not _env_allow_journal_rebuild():
        raise RuntimeError(
            "GET /__admin/requests вернул 500, а THRELIUM_E2E_WIREMOCK_ALLOW_JOURNAL_REBUILD=0 — "
            "rebuild по matchingStub отключён. Увеличьте heap WireMock, уменьшите тела ответов или "
            "journal (--max-request-journal-entries)."
        )
    if _env_journal_strict():
        raise RuntimeError(
            "GET /__admin/requests вернул 500, а THRELIUM_E2E_WIREMOCK_STRICT_JOURNAL=1 — "
            "rebuild по текущим UUID + unmatched неполон (события удалённых маппингов по API "
            "недостижимы). Устраните 500 на полном GET или временно снимите strict."
        )
    log.error("wiremock_journal_rebuild_deleted_mappings_may_miss")
    uuids = _all_stub_mapping_uuids(public_base, timeout=timeout)
    merged, trunc = _merge_journal_requests_for_matching_stub_uuids(
        public_base, uuids, timeout=timeout
    )
    unmatched_ok = True
    try:
        u_flat = wiremock_unmatched_request_entries(public_base, timeout=timeout)
    except requests.RequestException as exc:
        unmatched_ok = False
        log.error(
            "wiremock_journal_rebuild_unmatched_merge_failed",
            error=repr(exc),
        )
        u_flat = []
    merged.extend(_journal_wrap_unmatched_entries(u_flat))
    ordered = _journal_entries_sorted_newest_first(merged)
    meta: dict[str, Any] = {
        "total": len(ordered),
        "journalRebuilt": True,
        "mayMissDeletedMappingEvents": True,
        "matchingStubTruncated": trunc,
        "unmatchedMerged": unmatched_ok,
    }
    return {"requests": ordered, "meta": meta}


def _admin_requests_get_unfiltered(
    public_base: str,
    *,
    limit: int | None = None,
    since: str | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> dict[str, Any]:
    """``GET …/__admin/requests`` — сырой JSON журнала (matched; полный GET включает unmatched).

    Быстрый путь: один ``GET`` без ``limit``/``since``. При ``500`` — см.
    :func:`_admin_requests_matched_plus_unmatched_fallback` и env
    ``THRELIUM_E2E_WIREMOCK_ALLOW_JOURNAL_REBUILD`` / ``THRELIUM_E2E_WIREMOCK_STRICT_JOURNAL``.
    """
    root = _normalize_wiremock_public_root(public_base)
    url = f"{root}/__admin/requests"
    q: dict[str, str] = {}
    if since is not None and str(since).strip():
        q["since"] = str(since).strip()
    if limit is not None:
        q["limit"] = str(max(1, int(limit)))

    if not q:
        with _wiremock_admin_api_exclusive(timeout=timeout):
            r = _wm_session().get(url, timeout=timeout)
        if r.status_code != 500:
            r.raise_for_status()
            data = _wm_json_response(r, where="GET /__admin/requests (full)")
            return data if isinstance(data, dict) else {}
        log.info("wiremock_journal_rebuild_fallback_start")
        return _admin_requests_matched_plus_unmatched_fallback(public_base, timeout=timeout)

    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().get(url, params=q, timeout=timeout)
    r.raise_for_status()
    data = _wm_json_response(r, where="GET /__admin/requests (paginated)")
    return data if isinstance(data, dict) else {}


def _journal_request_body(entry: dict[str, Any]) -> str:
    """Тело входящего запроса из записи журнала (plain или base64)."""
    req = entry.get("request")
    if not isinstance(req, dict):
        return ""
    body = req.get("body")
    if isinstance(body, str) and body.strip():
        return body
    b64 = req.get("bodyAsBase64")
    if isinstance(b64, str) and b64.strip():
        try:
            return base64.b64decode(b64).decode("utf-8", errors="replace")
        except (ValueError, OSError):
            return ""
    return ""


def _journal_chat_completion_user_content(entry: dict[str, Any]) -> str:
    """Склейка ``messages[].content`` с ``role=user`` из POST chat/completions (без tools/system)."""
    body = _journal_request_body(entry)
    if not body.strip():
        return ""
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return ""
    messages = data.get("messages")
    if not isinstance(messages, list):
        return ""
    parts: list[str] = []
    for msg in messages:
        if not isinstance(msg, dict) or msg.get("role") != "user":
            continue
        content = msg.get("content")
        if isinstance(content, str) and content:
            parts.append(content)
    return "\n".join(parts)


def _journal_request_anchor_haystack(entry: dict[str, Any]) -> str:
    """Тело + заголовки (для e2e: якорь ``X-Threlium-Thread-Root`` может быть только в headers)."""
    parts: list[str] = [_journal_request_body(entry)]
    req = entry.get("request")
    if not isinstance(req, dict):
        return "\n".join(parts)
    hdrs = req.get("headers")
    if isinstance(hdrs, dict):
        for k, v in hdrs.items():
            if isinstance(v, str):
                parts.append(f"{k}: {v}")
            elif isinstance(v, list):
                parts.append(f"{k}: {';'.join(str(x) for x in v)}")
    return "\n".join(parts)


def journal_stub_tag(entry: dict[str, Any]) -> str | None:
    """Значение ``metadata[THRELIUM_E2E_WIREMOCK_STUB_TAG_KEY]`` у сопоставленного стаба в записи журнала."""
    sm = entry.get("stubMapping")
    if not isinstance(sm, dict):
        return None
    meta = sm.get("metadata")
    if not isinstance(meta, dict):
        return None
    v = meta.get(THRELIUM_E2E_WIREMOCK_STUB_TAG_KEY)
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return None


def _journal_entries_for_stub_tag_with_trunc(
    public_base: str,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> tuple[list[dict[str, Any]], bool]:
    """Как :func:`journal_entries_for_stub_tag`, плюс флаг усечения ``matchingStub`` (``meta.total``).

    Один межпроцессный lock на связку «все маппинги + все matchingStub GET» — иначе при поллинге
    каждый воркер отпускал бы lock между страницами ``/mappings`` и между стабами, голодая остальных.
    """
    with _wiremock_admin_api_exclusive(timeout=timeout):
        mappings = _list_all_stub_mappings(
            public_base, timeout=timeout, skip_admin_lock=True
        )
        uuids = _stub_uuids_from_mapping_list(mappings, stub_tag)
        if not uuids:
            return [], False
        return _merge_journal_requests_for_matching_stub_uuids(
            public_base, uuids, timeout=timeout, skip_admin_lock=True
        )


def journal_entries_for_stub_tag(
    public_base: str,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> list[dict[str, Any]]:
    """Записи журнала, обслуженные стабами с ``metadata.threlium_e2e_stub_tag == stub_tag``.

    Без общего ``GET /__admin/requests``: под одним межпроцессным lock — пагинация ``/mappings`` и
    серия ``GET …/requests?matchingStub=<uuid>`` (меньше голодания воркеров при поллинге, чем
    отдельный lock на каждый HTTP-шаг).
    """
    rows, _trunc = _journal_entries_for_stub_tag_with_trunc(
        public_base, stub_tag=stub_tag, timeout=timeout
    )
    return rows


def fetch_wiremock_journal_raw_entries(
    public_base: str,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> list[dict[str, Any]]:
    """Журнал WireMock только для стабов с заданным ``threlium_e2e_stub_tag`` (см. :func:`stub_tag_metadata`)."""
    return journal_entries_for_stub_tag(
        public_base, stub_tag=stub_tag, timeout=timeout
    )


def find_wiremock_requests_by_body_contains(
    public_base: str,
    needle: str,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> list[dict[str, Any]]:
    """Записи журнала с ``stub_tag`` и телом запроса, содержащим ``needle``."""
    return [
        e
        for e in journal_entries_for_stub_tag(
            public_base, stub_tag=stub_tag, timeout=timeout
        )
        if needle in _journal_request_body(e)
    ]


def journal_has_request(
    public_base: str,
    *,
    stub_tag: str,
    method: str,
    url_contains: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> bool:
    """``True``, если в журнале (по ``stub_tag``) есть запись с ``request.method`` и подстрокой в ``request.url``."""
    want_method = method.upper()
    for entry in journal_entries_for_stub_tag(
        public_base, stub_tag=stub_tag, timeout=timeout
    ):
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != want_method:
            continue
        if url_contains in str(req.get("url") or ""):
            return True
    return False


def journal_entries_for_stub_tag_with_header(
    public_base: str,
    *,
    stub_tag: str,
    header_name: str,
    header_value: str,
    url_contains: str | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> list[dict[str, Any]]:
    """Journal entries filtered by stub_tag + request header value (+ optional url substring).

    Header name matching is case-insensitive (WireMock may store headers in lowercase).
    """
    want_lower = header_name.lower()
    results: list[dict[str, Any]] = []
    for entry in journal_entries_for_stub_tag(
        public_base, stub_tag=stub_tag, timeout=timeout
    ):
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        headers = req.get("headers")
        if not isinstance(headers, dict):
            continue
        matched = False
        for hk, hv in headers.items():
            if hk.lower() == want_lower and isinstance(hv, str) and hv == header_value:
                matched = True
                break
        if not matched:
            continue
        if url_contains is not None and url_contains not in str(req.get("url") or ""):
            continue
        results.append(entry)
    return results


@dataclass(frozen=True)
class WiremockCorrelation:
    """Корреляция одного e2e-прогона с журналом WireMock (``test_id`` в ``stub_tag`` и якоря запросов к LLM).

    При ``THRELIUM_E2E_WIREMOCK_CLEAR_JOURNAL_AFTER_TEST`` teardown чистит журнал только по
    ``test_id`` как ``threlium_e2e_stub_tag`` стабов — держите то же значение, что в ``stub_tag_metadata``.
    """

    test_id: str
    public_base: str

    @property
    def admin_base(self) -> str:
        return wiremock_admin_base(self.public_base)


def _truncate_log_text(text: str, *, max_len: int = _WIREMOCK_DUMP_BODY_MAX) -> str:
    t = text if isinstance(text, str) else ""
    if len(t) <= max_len:
        return t
    return t[:max_len] + f"\n… ({len(t) - max_len} bytes truncated)"


def _entry_response_body_preview(entry: dict[str, Any]) -> str:
    for key in ("response", "responseDefinition"):
        block = entry.get(key)
        if not isinstance(block, dict):
            continue
        body = block.get("body")
        if isinstance(body, str) and body.strip():
            return body
        b64 = block.get("bodyAsBase64") or block.get("body_as_base64")
        if isinstance(b64, str) and b64.strip():
            try:
                return base64.b64decode(b64).decode("utf-8", errors="replace")
            except (ValueError, OSError):
                return f"<base64 decode failed, len={len(b64)}>"
    return ""


def log_wiremock_correlation_journal(
    wc: WiremockCorrelation,
    *,
    pytest_nodeid: str,
) -> None:
    """В лог: записи журнала WireMock только для стабов с ``threlium_e2e_stub_tag == test_id``.

    ``find`` по телу ограничен тем же тегом (параллельные прогоны на общем WireMock).
    """
    try:
        find_hits = len(
            find_wiremock_requests_by_body_contains(
                wc.public_base, wc.test_id, stub_tag=wc.test_id
            )
        )
    except Exception as exc:  # noqa: BLE001 — диагностический путь
        find_hits = -1
        log.warning(
            "wiremock_journal_body_scan_failed",
            test_id=wc.test_id,
            error=repr(exc),
        )
    try:
        entries = journal_entries_for_stub_tag(wc.public_base, stub_tag=wc.test_id)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "wiremock_journal_dump_failed",
            nodeid=pytest_nodeid,
            test_id=wc.test_id,
            error=repr(exc),
            exc_info=True,
        )
        return

    # Хронология: старые → новые (общий порядок по loggedDate, не «переворот блоков стабов»).
    entries_chrono = _journal_entries_sorted_chrono_asc(entries)

    log.info(
        "wiremock_correlation_journal",
        nodeid=pytest_nodeid,
        test_id=wc.test_id,
        stub_tag_entries=len(entries_chrono),
        find_body_hits=find_hits,
    )

    lines: list[str] = []
    for i, ent in enumerate(entries_chrono, start=1):
        req_raw = ent.get("request")
        req = req_raw if isinstance(req_raw, dict) else {}
        method = str(req.get("method") or "?")
        url = str(req.get("url") or req.get("absoluteUrl") or "")
        rid = str(ent.get("id") or "")
        req_body = _truncate_log_text(_journal_request_body(ent))
        st = None
        for key in ("response", "responseDefinition"):
            b = ent.get(key)
            if isinstance(b, dict) and "status" in b:
                st = b.get("status")
                break
        resp_body = _truncate_log_text(_entry_response_body_preview(ent))
        lines.append(
            f"--- [{i}/{len(entries_chrono)}] id={rid!r} {method} {url} status={st!r} ---\n"
            f"<<< request body\n{req_body}\n"
            f"<<< response body (preview)\n{resp_body}\n"
        )
    if lines:
        log.debug(
            "wiremock_correlation_journal_entries",
            body=clip_log_body("\n".join(lines)),
        )

    # Только matched-записи с metadata stub_tag; unmatched не трогаем (политика e2e).
    if _env_truthy(os.environ.get(THRELIUM_E2E_WIREMOCK_CLEAR_JOURNAL_AFTER_TEST)):
        try:
            remove_wiremock_journal_by_stub_tag(wc.public_base, tag=wc.test_id)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "wiremock_journal_clear_by_stub_tag_failed",
                test_id=wc.test_id,
                error=repr(exc),
            )


def _render_template(name: str, **context: Any) -> dict[str, Any]:
    env = Environment(
        loader=FileSystemLoader(str(MOCK_TEMPLATES_DIR)),
        autoescape=False,
    )
    raw = env.get_template(name).render(**context)
    return json.loads(raw)


def register_mapping(
    base_url: str,
    mapping: dict[str, Any],
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
    metadata: dict[str, Any],
    skip_admin_lock: bool = False,
) -> str:
    """POST ``/__admin/mappings``; возвращает ``id`` из ответа WireMock 3.

    ``base_url`` — публичный корень WireMock (``http://host:port``), не ``…/__admin``; суффикс
    ``/__admin`` при передаче по ошибке снимается.

    ``metadata`` обязателен и не должен быть пустым: мержится в (или создаёт)
    ``mapping["metadata"]`` для :func:`remove_wiremock_mappings_by_stub_tag` и фильтра журнала.
    """
    if not metadata:
        raise ValueError(
            "register_mapping requires non-empty metadata (use stub_tag_metadata(stub_tag) or merged dict)"
        )
    payload = _merge_mapping_payload(mapping, metadata)
    root = _normalize_wiremock_public_root(base_url)
    url = f"{root}/__admin/mappings"
    lock_ctx = (
        contextlib.nullcontext()
        if skip_admin_lock
        else _wiremock_admin_api_exclusive(timeout=timeout)
    )
    with lock_ctx:
        r = _wm_session().post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        data = _wm_json_response(r, where="POST /__admin/mappings")
    mid = data.get("id")
    if not isinstance(mid, str) or not mid:
        raise RuntimeError(f"WireMock mapping response missing id: {data!r}")
    return mid


def upsert_mapping(
    base_url: str,
    mapping: dict[str, Any],
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
    metadata: dict[str, Any],
    skip_admin_lock: bool = False,
) -> str:
    """Заменить или создать маппинг: ``PUT /__admin/mappings/{id}``; при ``404`` — ``POST`` как
    :func:`register_mapping`.

    ``base_url`` — публичный корень WireMock (как в :func:`register_mapping`).

    В теле маппинга обязателен непустой ``id`` (UUID), чтобы повторные прогоны обновляли тот же стаб.
    ``metadata`` мержится так же, как в :func:`register_mapping`.
    """
    if not metadata:
        raise ValueError(
            "upsert_mapping requires non-empty metadata (use stub_tag_metadata(stub_tag) or merged dict)"
        )
    payload = _merge_mapping_payload(mapping, metadata)
    mid_raw = payload.get("id")
    if not isinstance(mid_raw, str) or not mid_raw.strip():
        raise ValueError("upsert_mapping requires mapping['id'] as non-empty str (fixed UUID)")
    mid = mid_raw.strip()
    payload["id"] = mid
    root = _normalize_wiremock_public_root(base_url)
    put_url = f"{root}/__admin/mappings/{mid}"
    lock_ctx = (
        contextlib.nullcontext()
        if skip_admin_lock
        else _wiremock_admin_api_exclusive(timeout=timeout)
    )
    with lock_ctx:
        r_put = _wm_session().put(put_url, json=payload, timeout=timeout)
        if r_put.status_code in (200, 204):
            return mid
        if r_put.status_code != 404:
            r_put.raise_for_status()
        post_url = root + "/__admin/mappings"
        r_post = _wm_session().post(post_url, json=payload, timeout=timeout)
        r_post.raise_for_status()
        data = _wm_json_response(r_post, where="POST /__admin/mappings (upsert fallback)")
    out = data.get("id")
    if not isinstance(out, str) or not out:
        raise RuntimeError(f"WireMock mapping POST response missing id: {data!r}")
    return out


def register_from_template(
    base_url: str,
    template_name: str,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
    metadata: dict[str, Any] | None = None,
    **context: Any,
) -> str:
    """Рендерит ``mock_templates/<template_name>`` (Jinja2) и регистрирует маппинг с :func:`stub_tag_metadata`."""
    mapping = _render_template(template_name, **context)
    merged = stub_tag_metadata(stub_tag)
    if metadata:
        merged = {**merged, **metadata}
    return register_mapping(base_url, mapping, timeout=timeout, metadata=merged)


def delete_mapping(base_url: str, mapping_id: str, *, timeout: float = TIMEOUT_POLL_SHORT) -> None:
    root = _normalize_wiremock_public_root(base_url)
    url = f"{root}/__admin/mappings/{mapping_id}"
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().delete(url, timeout=timeout)
        if r.status_code not in (200, 204):
            r.raise_for_status()


def remove_wiremock_mappings_by_metadata(
    base_url: str,
    metadata_matcher: dict[str, Any],
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """POST ``/__admin/mappings/remove-by-metadata`` — удалить все стабы, подходящие под matcher.

    ``metadata_matcher`` — тело WireMock, например
    ``{"matchesJsonPath": {"expression": "$.threlium_e2e_stub_tag", "equalTo": "my-tag"}}``.
    См. `WireMock stub metadata <https://wiremock.org/2.x/docs/stub-metadata/>`_.
    """
    root = _normalize_wiremock_public_root(base_url)
    url = f"{root}/__admin/mappings/remove-by-metadata"
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().post(url, json=metadata_matcher, timeout=timeout)
        r.raise_for_status()


def _matcher_wiremock_stub_tag_equal(tag: str) -> dict[str, Any]:
    """Тело matcher'а для remove-by-metadata (стабы и журнал) по :func:`stub_tag_metadata`."""
    return {
        "matchesJsonPath": {
            "expression": f"$.{THRELIUM_E2E_WIREMOCK_STUB_TAG_KEY}",
            "equalTo": tag,
        }
    }


def remove_wiremock_mappings_by_stub_tag(
    base_url: str,
    *,
    tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Удалить стабы с ``metadata["threlium_e2e_stub_tag"] == tag`` (см. :func:`stub_tag_metadata`)."""
    remove_wiremock_mappings_by_metadata(
        base_url,
        _matcher_wiremock_stub_tag_equal(tag),
        timeout=timeout,
    )


def remove_wiremock_journal_by_metadata(
    base_url: str,
    metadata_matcher: dict[str, Any],
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """POST ``/__admin/requests/remove-by-metadata`` — удалить из журнала только подходящие записи.

    На общем WireMock не использовать :func:`reset_request_journal` для изоляции прогона.
    Matcher тот же формат, что для :func:`remove_wiremock_mappings_by_metadata` (поле
    ``metadata`` сопоставленного стаба доступно в записи журнала).

    Таймаут по умолчанию большой: при тысячах записей remove-by-metadata у WireMock
    может занимать минуты.
    """
    root = _normalize_wiremock_public_root(base_url)
    url = f"{root}/__admin/requests/remove-by-metadata"
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().post(url, json=metadata_matcher, timeout=timeout)
        r.raise_for_status()


def remove_wiremock_journal_by_stub_tag(
    base_url: str,
    *,
    tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Удалить из журнала события, обслуженные стабами с ``metadata.threlium_e2e_stub_tag == tag``.

    Записи **unmatched** (без сматченного стаба) сюда не попадают и **не** удаляются этим вызовом.
    """
    remove_wiremock_journal_by_metadata(
        base_url,
        _matcher_wiremock_stub_tag_equal(tag),
        timeout=timeout,
    )


def prepare_wiremock_scenario(
    public_base: str,
    *,
    stub_dir: Path,
    stub_tag: str,
    correlation_key: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Подготовка сценария на общем WireMock (``docs/TESTING.md`` §4.4.x).

    Порядок: :func:`upsert_wiremock_mapping_directory` →
    :func:`remove_wiremock_journal_by_stub_tag` (только matched по тегу; unmatched не трогает) →
    :func:`wiremock_state_seed_context` (составной ключ ``{stub_tag}::{correlation_key}``).

    Bootstrap-стабы (``compose_bootstrap/``) уже зарегистрированы однократно: в ``_leader_post_up``
    при первом подъёме стека и в ``_e2e_wiremock_journal_reset_once`` (cold reset) под IPC-локом.
    Повторный upsert из каждого воркера вызывает гонку (WireMock ``editMapping`` = remove+add).
    :func:`upsert_wiremock_mapping_directory` вызывается с ``reuse_admin_lock=True`` — один
    захват :func:`_wiremock_admin_api_exclusive` на весь prepare; HTTP внутри идут с
    ``skip_admin_lock`` у :func:`upsert_mapping` без лишней вложенности depth-локера.

    Контекст route **не** удаляем перед сидом: уникальные ``correlation_key`` и глобальный
    :func:`wiremock_state_reset_all_contexts` в ``pytest_sessionfinish`` (см. ``docs/E2E_ISOLATION.md``).
    """
    ctx_key = composite_context_key(stub_tag, correlation_key)
    with _wiremock_admin_api_exclusive(timeout=timeout):
        upsert_wiremock_mapping_directory(
            public_base,
            stub_dir,
            stub_tag=stub_tag,
            timeout=timeout,
            reuse_admin_lock=True,
        )
        remove_wiremock_journal_by_stub_tag(public_base, tag=stub_tag, timeout=timeout)
        wiremock_state_seed_context(public_base, ctx_key, timeout=timeout)


def teardown_wiremock_scenario(
    public_base: str,
    *,
    correlation_key: str,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Хук конца сценария: журнал WireMock **не** очищается (matched остаётся для отладки).

    Ранее здесь вызывался :func:`remove_wiremock_journal_by_stub_tag`; по политике репозитория
    после теста журнал не трогаем. Журнал **unmatched** здесь тоже не чистится. Контексты State
    для route **не** удаляем. Полный сброс контекстов — ``pytest_sessionfinish`` →
    :func:`wiremock_state_reset_all_contexts` в ``conftest.py``.

    Параметры ``public_base`` / ``stub_tag`` / ``timeout`` сохранены для совместимости вызовов;
    при необходимости выборочной очистки matched по тегу вызовите
    :func:`remove_wiremock_journal_by_stub_tag` явно.
    """
    _ = public_base, correlation_key, stub_tag, timeout


def stub_tag_metadata(tag: str) -> dict[str, str]:
    """Метаданные для :func:`register_mapping` / :func:`register_wiremock_mapping_directory` (remove-by-tag)."""
    return {THRELIUM_E2E_WIREMOCK_STUB_TAG_KEY: tag}


def reset_request_journal(base_url: str, *, timeout: float = TIMEOUT_POLL_SHORT) -> None:
    """DELETE ``/__admin/requests`` — сбросить журнал serve events в WireMock целиком.

    Вместе с matched-записями обнуляется и то, что отдаёт ``GET /__admin/requests/unmatched``
    (те же события в ``requestJournal``). На общем инстансе вызывать только из инфраструктуры
    pytest (один раз до прогона), не из тела сценариев. Для изоляции между тестами без полного
    сброса: :func:`remove_wiremock_journal_by_stub_tag`.
    """
    root = _normalize_wiremock_public_root(base_url)
    url = f"{root}/__admin/requests"
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().delete(url, timeout=timeout)
        if r.status_code not in (200, 204):
            r.raise_for_status()


def reset_non_bootstrap_wiremock_mappings(
    base_url: str,
    *,
    keep_tags: frozenset[str] = frozenset({THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG}),
    timeout: float = TIMEOUT_POLL_SHORT,
) -> int:
    """Удалить все стабы **кроме** bootstrap (``compose_bootstrap/``).

    Возвращает число удалённых маппингов. Bootstrap стабы (``020_matrix_sync``,
    ``000_e2e_state_setup`` и др.) остаются на месте — bridge@matrix и прочая инфраструктура
    никогда не видят пустой WireMock.

    ``keep_tags`` — набор значений ``metadata.threlium_e2e_stub_tag``, которые НЕ удаляются.
    По умолчанию — только ``threlium-e2e-compose-bootstrap``. Расширяется при необходимости.
    """
    with _wiremock_admin_api_exclusive(timeout=timeout):
        root = _normalize_wiremock_public_root(base_url)
        url = f"{root}/__admin/mappings"
        r = _wm_session().get(url, timeout=timeout)
        r.raise_for_status()
        data = _wm_json_response(r, where="GET /__admin/mappings (reset_non_bootstrap)")
        mappings = data.get("mappings")
        if not isinstance(mappings, list):
            return 0
        removed = 0
        for m in mappings:
            if not isinstance(m, dict):
                continue
            mid = m.get("id")
            if not mid:
                continue
            meta = m.get("metadata") or {}
            tag = meta.get(THRELIUM_E2E_WIREMOCK_STUB_TAG_KEY, "")
            if tag in keep_tags:
                continue
            try:
                delete_mapping(base_url, mid, timeout=timeout)
                removed += 1
            except Exception:  # noqa: BLE001
                log.debug(
                    "wiremock_delete_mapping_skipped",
                    mapping_id=mid,
                    stub_tag=tag,
                )
        return removed


def admin_requests_json(
    public_base: str,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> dict[str, Any]:
    """Словарь с полем ``requests`` — журнал только для стабов с ``threlium_e2e_stub_tag == stub_tag``.

    Поле ``meta.matchingStubTruncated`` — хотя бы один ``GET …/requests?matchingStub=`` вернул
    ``meta.total > len(requests)`` (неполная выборка на стороне WM). При
    ``THRELIUM_E2E_WIREMOCK_STRICT_JOURNAL=1`` такое усечение даёт ``RuntimeError`` ещё на этапе
    чтения журнала; для assert'ов по фазам см. :func:`_e2e_openai_llm_coverage_missing`.
    """
    pb = _normalize_wiremock_public_root(public_base)
    filtered, trunc = _journal_entries_for_stub_tag_with_trunc(
        pb, stub_tag=stub_tag, timeout=timeout
    )
    return {
        "requests": filtered,
        "meta": {"total": len(filtered), "matchingStubTruncated": trunc},
    }


def admin_requests_text(
    public_base: str,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> str:
    """JSON-строка отфильтрованного журнала (поле ``requests``) для assert по подстроке.

    .. deprecated::
        Поиск подстрок в сыром JSON-дампе ненадёжен: слова вроде ``"PUT"`` могут
        встретиться в теле промпта LLM. Используйте :func:`journal_has_request` или
        :func:`journal_entries_for_stub_tag` для структурированных проверок.
    """
    data = admin_requests_json(public_base, stub_tag=stub_tag, timeout=timeout)
    reqs = data.get("requests")
    if not isinstance(reqs, list):
        return "[]"
    return json.dumps(reqs, ensure_ascii=False)


def register_wiremock_mapping_directory(
    base_url: str,
    directory: Path,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
    pattern: str = "*.json",
    stub_metadata: dict[str, Any] | None = None,
    exclude_names: Sequence[str] = (),
    reuse_admin_lock: bool = False,
) -> list[str]:
    """Регистрирует все файлы ``pattern`` из ``directory`` (сортировка по имени — префиксы ``010_`` …).

    ``stub_tag`` обязателен: в каждый маппинг мержится :func:`stub_tag_metadata` (+ опционально ``stub_metadata``).
    """
    merged_meta = stub_tag_metadata(stub_tag)
    if stub_metadata:
        merged_meta = {**merged_meta, **stub_metadata}
    d = directory.resolve()
    if not d.is_dir():
        raise FileNotFoundError(f"WireMock stubs directory not found: {d}")
    skip = frozenset(str(x) for x in exclude_names)
    paths = sorted(d.glob(pattern))
    ids_out: list[str] = []

    def _body() -> None:
        for p in paths:
            if p.name in skip:
                continue
            if not p.is_file() or p.name.startswith("_"):
                continue
            mapping = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(mapping, dict):
                raise TypeError(f"expected JSON object in {p}, got {type(mapping).__name__}")
            ids_out.append(
                register_mapping(
                    base_url,
                    mapping,
                    timeout=timeout,
                    metadata=merged_meta,
                    skip_admin_lock=True,
                )
            )

    if reuse_admin_lock:
        _body()
    else:
        with _wiremock_admin_api_exclusive(timeout=timeout):
            _body()
    return ids_out


def upsert_wiremock_mapping_directory(
    base_url: str,
    directory: Path,
    *,
    stub_tag: str,
    timeout: float = TIMEOUT_POLL_SHORT,
    pattern: str = "*.json",
    stub_metadata: dict[str, Any] | None = None,
    exclude_names: Sequence[str] = (),
    reuse_admin_lock: bool = False,
) -> list[str]:
    """Как :func:`register_wiremock_mapping_directory`, но через :func:`upsert_mapping` (PUT по ``id``).

    Для каждого ``*.json`` задаётся стабильный ``id`` = :func:`wiremock_stub_id_for_e2e_stub_relpath`
    относительно родителя ``directory`` (каталог ``…/wiremock_stubs``), т.е. путь вида
    ``<имя_папки_теста>/<файл>.json``, чтобы не пересекаться с другими тестами при одинаковых именах файлов.
    Если в JSON уже есть непустой ``id``, он сохраняется. Повторный прогон обновляет маппинги без удаления.

    ``reuse_admin_lock=True`` — не брать второй :func:`_wiremock_admin_api_exclusive`; вызывающий уже
    держит межпроцессный lock (например :func:`prepare_wiremock_scenario`). Внутри цикла HTTP-шаги
    идут с ``skip_admin_lock=True`` у :func:`upsert_mapping`, без вложенного depth-локера на поток.
    """
    merged_meta = stub_tag_metadata(stub_tag)
    if stub_metadata:
        merged_meta = {**merged_meta, **stub_metadata}
    d = directory.resolve()
    if not d.is_dir():
        raise FileNotFoundError(f"WireMock stubs directory not found: {d}")
    skip = frozenset(str(x) for x in exclude_names)
    paths = sorted(d.glob(pattern))
    ids_out: list[str] = []

    def _body() -> None:
        for p in paths:
            if p.name in skip:
                continue
            if not p.is_file() or p.name.startswith("_"):
                continue
            mapping = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(mapping, dict):
                raise TypeError(f"expected JSON object in {p}, got {type(mapping).__name__}")
            if not mapping.get("id"):
                stub_root = d.parent.resolve()
                try:
                    rel_key = str(p.resolve().relative_to(stub_root))
                except ValueError:
                    rel_key = f"{d.name}/{p.name}"
                mapping = {**mapping, "id": wiremock_stub_id_for_e2e_stub_relpath(rel_key)}
            ids_out.append(
                upsert_mapping(
                    base_url,
                    mapping,
                    timeout=timeout,
                    metadata=merged_meta,
                    skip_admin_lock=True,
                )
            )

    if reuse_admin_lock:
        _body()
    else:
        with _wiremock_admin_api_exclusive(timeout=timeout):
            _body()
    return ids_out


def _e2e_openai_llm_coverage_missing(data: dict[str, Any], test_id: str) -> list[str]:
    """Фазы LLM (embeddings + chat/completions) для live e2e — канал-агностично.

    Маркеры совпадают с ``bodyPatterns`` в ``wiremock_stubs/test_*_wiremock_live_e2e/`` и
    :func:`_journal_request_anchor_haystack` для chat. ``meta.matchingStubTruncated`` — при strict
    обычно уже ``RuntimeError`` при чтении журнала; иначе предупреждение в списке «не хватает».
    """
    raw_list = data.get("requests")
    meta = data.get("meta")
    if not isinstance(raw_list, list):
        return [f"WireMock journal: нет списка 'requests': {data!r}"]

    if isinstance(meta, dict) and meta.get("matchingStubTruncated"):
        if _env_journal_strict():
            raise RuntimeError(
                "E2e openai coverage: matchingStub-журнал усечён (meta) при "
                "THRELIUM_E2E_WIREMOCK_STRICT_JOURNAL=1 — проверка фаз LLM недостоверна."
            )

    chat_hays: list[str] = []
    chat_entries: list[dict[str, Any]] = []
    emb_posts: list[str] = []
    for entry in raw_list:
        if not isinstance(entry, dict):
            continue
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        method = str(req.get("method") or "").upper()
        url = str(req.get("url") or "")
        if method != "POST":
            continue
        if "/v1/chat/completions" in url or url.rstrip("/").endswith("chat/completions"):
            chat_hays.append(_journal_request_anchor_haystack(entry))
            chat_entries.append(entry)
        elif "/v1/embeddings" in url or url.rstrip("/").endswith("embeddings"):
            emb_posts.append(_journal_request_body(entry))

    missing: list[str] = []
    if isinstance(meta, dict) and meta.get("matchingStubTruncated"):
        missing.append(
            "[целостность журнала] matchingStub усечён (meta.total>len) — список фаз ниже может быть ложноотрицательным"
        )

    if len(emb_posts) < 1:
        missing.append("POST /embeddings (≥1 с телом сценария)")

    def _llm_body_has_correlation(hay: str) -> bool:
        return (
            test_id in hay
            or "openai/threlium_e2e_l0" in hay
            or '"model":"threlium_e2e_l0"' in hay
            or '"model": "threlium_e2e_l0"' in hay
        )

    def _post_has_reasoning(hay: str) -> bool:
        return (
            test_id in hay
            and "<envelope>" in hay
            and '"tools"' in hay
        )

    def _post_has_enrich_plan(hay: str) -> bool:
        return _llm_body_has_correlation(hay) and _e2e_hay_matches_enrich_plan_contract(hay)

    def _entry_has_call_site(entry: dict[str, Any], call_site: str) -> bool:
        req = entry.get("request")
        if not isinstance(req, dict):
            return False
        return _wiremock_headers_get_ci(req.get("headers"), "X-Threlium-Call-Site") == call_site

    if not any(_post_has_reasoning(h) for h in chat_hays):
        missing.append(
            "reasoning (как 100_chat_reasoning_egress_tool.json: <envelope> + tools + test_id)"
        )
    if not any(_post_has_enrich_plan(h) for h in chat_hays):
        missing.append(
            "enrich plan (как 080_chat_enrich_plan.json: regex formulate/kg × indexed email + корреляция модели)"
        )
    if not any(_entry_has_call_site(e, "lightrag_query_keywords") for e in chat_entries):
        missing.append(
            "lightrag keywords (X-Threlium-Call-Site: lightrag_query_keywords)"
        )
    if not any(_entry_has_call_site(e, "lightrag_index_entity") for e in chat_entries):
        missing.append(
            "lightrag entity extraction (X-Threlium-Call-Site: lightrag_index_entity)"
        )

    # kg_query (060_chat_lightrag_kg_query_llm.json) не проверяется: в однопроходном
    # тесте KG пуст к моменту aquery — LightRAG._build_query_context возвращает None
    # и вызов LLM с шаблоном kg_query_context.j2 не происходит конструктивно.

    return missing


def _matrix_egress_coverage_missing(raw_list: list[Any]) -> list[str]:
    """Matrix: PUT ``send/m.room.message`` с телом ответа из reasoning-стаба."""
    put_bodies: list[str] = []
    for entry in raw_list:
        if not isinstance(entry, dict):
            continue
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "PUT":
            continue
        url = str(req.get("url") or "")
        if "send/m.room.message" not in url:
            continue
        put_bodies.append(_journal_request_body(entry))

    missing: list[str] = []
    if not put_bodies:
        missing.append(
            "PUT /_matrix/.../send/m.room.message (ответ агента в Matrix room — не найден)"
        )
    elif not any(_MATRIX_AGENT_REPLY_BODY in b for b in put_bodies):
        missing.append(
            f"PUT send/m.room.message с телом ответа агента ({_MATRIX_AGENT_REPLY_BODY!r}) — "
            "PUT есть, но тело не содержит ожидаемый текст из reasoning-стаба "
            "(100_chat_reasoning_egress_tool.json → egress_matrix → Matrix PUT)"
        )
    return missing


def _telegram_send_message_journal_haystack(raw: str) -> str:
    """PTB (HTTPXRequest) шлёт ``sendMessage`` как ``x-www-form-urlencoded``, не JSON.

    В журнал WireMock попадает сырое тело; для assert по ``reply_body`` с пробелами и по
    ``message_thread_id`` склеиваем декодированные поля формы (``parse_qs``).
    """
    b = raw or ""
    if not b.strip() or b.lstrip().startswith("{"):
        return b
    if "=" not in b:
        return b
    try:
        q = parse_qs(b, keep_blank_values=True)
    except ValueError:
        return b
    parts: list[str] = [b]
    for key in ("chat_id", "message_thread_id", "text", "reply_parameters"):
        for val in q.get(key) or ():
            if val:
                parts.append(f"{key}={val}")
    return "\n".join(parts)


def _telegram_sendmessage_body_matches_egress_expectation(
    raw: str,
    *,
    chat_id: int,
    reply_body: str,
    message_thread_id: int | None,
) -> bool:
    """Тело POST ``sendMessage`` содержит ожидаемый ``chat_id``, текст ответа агента и (для топика) thread."""
    b = raw or ""
    hay = _telegram_send_message_journal_haystack(b)
    sid = str(chat_id)
    if (sid not in b and sid not in hay) or (
        reply_body not in b and reply_body not in hay
    ):
        return False
    if message_thread_id is None:
        return True
    needle_thread = f'"message_thread_id":{message_thread_id}'
    needle_thread_sp = f'"message_thread_id": {message_thread_id}'
    form_needle = f"message_thread_id={message_thread_id}"
    return (
        needle_thread in b
        or needle_thread_sp in b
        or needle_thread in hay
        or needle_thread_sp in hay
        or form_needle in b
        or form_needle in hay
    )


def wiremock_journal_telegram_sendmessage_bodies_matching_agent_reply(
    public_base: str,
    *,
    stub_tag: str,
    chat_id: int,
    reply_body: str,
    message_thread_id: int | None = None,
    timeout: float = 2.0,
) -> list[str]:
    """Тела POST ``…/editMessageText`` в журнале с финальным текстом ответа агента."""
    out: list[str] = []
    for entry in fetch_wiremock_journal_raw_entries(
        public_base, stub_tag=stub_tag, timeout=timeout
    ):
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "POST":
            continue
        url = str(req.get("url") or "")
        if "editMessageText" not in url:
            continue
        body = _journal_request_body(entry)
        if _telegram_sendmessage_body_matches_egress_expectation(
            body,
            chat_id=chat_id,
            reply_body=reply_body,
            message_thread_id=message_thread_id,
        ):
            out.append(body)
    return out


def wiremock_journal_telegram_sendmessage_placeholder_bodies(
    public_base: str,
    *,
    stub_tag: str,
    chat_id: int,
    timeout: float = 2.0,
) -> list[str]:
    """Тела POST ``…/sendMessage`` (placeholder) по ``chat_id`` — для извлечения ``reply_parameters``."""
    sid = str(chat_id)
    out: list[str] = []
    for entry in fetch_wiremock_journal_raw_entries(
        public_base, stub_tag=stub_tag, timeout=timeout
    ):
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "POST":
            continue
        url = str(req.get("url") or "")
        if "sendMessage" not in url:
            continue
        body = _journal_request_body(entry)
        hay = _telegram_send_message_journal_haystack(body)
        if sid in body or sid in hay:
            out.append(body)
    return out


def wiremock_telegram_sendmessage_body_reply_target_message_id(raw: str) -> int | None:
    """``reply_parameters`` из form-urlencoded тела ``sendMessage`` → ``message_id`` (PTB)."""
    b = (raw or "").strip()
    if not b or b.startswith("{"):
        return None
    if "=" not in b:
        return None
    try:
        q = parse_qs(b, keep_blank_values=True)
    except ValueError:
        return None
    for v in q.get("reply_parameters") or ():
        if not v:
            continue
        try:
            obj = json.loads(v)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        mid = obj.get("message_id")
        if isinstance(mid, int):
            return mid
        if isinstance(mid, str) and mid.strip().isdigit():
            return int(mid.strip())
    return None


def _telegram_egress_coverage_missing(
    raw_list: list[Any],
    *,
    chat_id: int,
    reply_body: str,
    message_thread_id: int | None,
) -> list[str]:
    """Telegram: POST ``sendMessage`` (placeholder) + ``editMessageText`` (финальный текст)."""
    has_send = False
    edit_bodies: list[str] = []
    for entry in raw_list:
        if not isinstance(entry, dict):
            continue
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "POST":
            continue
        url = str(req.get("url") or "")
        if "sendMessage" in url:
            has_send = True
        if "editMessageText" in url:
            edit_bodies.append(_journal_request_body(entry))

    missing: list[str] = []
    if not has_send:
        missing.append("POST …/sendMessage (placeholder в Telegram — не найден)")
    if not edit_bodies:
        missing.append("POST …/editMessageText (финальный текст ответа в Telegram — не найден)")
        return missing

    if not any(
        _telegram_sendmessage_body_matches_egress_expectation(
            b,
            chat_id=chat_id,
            reply_body=reply_body,
            message_thread_id=None,
        )
        for b in edit_bodies
    ):
        missing.append(
            "POST editMessageText с ожидаемым chat_id, reply_body из reasoning-стаба"
            " — не найдено (stub_tag журнала)"
        )
    return missing


def _matrix_openai_coverage_missing(base_url: str, test_id: str) -> list[str]:
    """Matrix live e2e: LLM-фазы + Matrix PUT egress."""
    data = admin_requests_json(base_url, stub_tag=test_id)
    miss = _e2e_openai_llm_coverage_missing(data, test_id)
    raw_list = data.get("requests")
    if isinstance(raw_list, list):
        miss.extend(_matrix_egress_coverage_missing(raw_list))
    return miss


def _telegram_openai_coverage_missing(
    base_url: str,
    *,
    test_id: str,
    chat_id: int,
    reply_body: str,
    message_thread_id: int | None,
) -> list[str]:
    """Telegram live e2e: LLM-фазы + ``sendMessage`` placeholder + ``editMessageText`` egress."""
    data = admin_requests_json(base_url, stub_tag=test_id)
    miss = _e2e_openai_llm_coverage_missing(data, test_id)
    raw_list = data.get("requests")
    if isinstance(raw_list, list):
        miss.extend(
            _telegram_egress_coverage_missing(
                raw_list,
                chat_id=chat_id,
                reply_body=reply_body,
                message_thread_id=message_thread_id,
            )
        )
    return miss


def assert_wiremock_matrix_e2e_openai_coverage(
    base_url: str,
    *,
    test_id: str,
) -> None:
    """Проверить журнал WireMock: полный контур Matrix e2e.

    Ожидаемые фазы (опрос до ``TIMEOUT_POLL_SHORT``):

    * POST ``/embeddings`` (≥1) — LightRAG и enrich.
    * POST ``/chat/completions`` — enrich plan, entity extraction, keywords, reasoning.
    * PUT ``send/m.room.message`` — ответ агента в Matrix room с телом из reasoning-стаба.

    KG query (060_chat_lightrag_kg_query_llm.json) не проверяется: в однопроходном тесте
    KG пуст к моменту aquery — LightRAG ``_build_query_context`` возвращает ``None`` в ``hybrid``
    режиме, и вызов LLM с ``kg_query_context.j2`` не происходит конструктивно.
    """
    interval = min(3.0, max(1.0, float(POLL_INTERVAL)))

    def _probe() -> bool | None:
        return True if not _matrix_openai_coverage_missing(base_url, test_id) else None

    try:
        poll_until(
            _probe,
            timeout=float(TIMEOUT_POLL_SHORT),
            interval=interval,
            desc=(
                "WireMock journal: Matrix e2e POST /embeddings + /chat/completions "
                f"(stub_tag={test_id!r}, semantic phases)"
            ),
        )
    except TimeoutError:
        miss = _matrix_openai_coverage_missing(base_url, test_id)
        data = admin_requests_json(base_url, stub_tag=test_id)
        raw_list = data.get("requests")
        preview = ""
        if isinstance(raw_list, list):
            bodies: list[str] = []
            for entry in raw_list:
                if not isinstance(entry, dict):
                    continue
                req = entry.get("request")
                if not isinstance(req, dict):
                    continue
                if str(req.get("method") or "").upper() != "POST":
                    continue
                url = str(req.get("url") or "")
                if "chat/completions" not in url:
                    continue
                bodies.append(_journal_request_body(entry))
            preview = "\n---\n".join(bodies[:3])
        raise AssertionError(
            "WireMock: таймаут ожидания полной цепочки LLM для Matrix e2e: "
            f"не хватает {', '.join(miss)}. test_id={test_id!r}. "
            f"Превью тел chat/completions (до 3): {preview[:12000]!r}"
        ) from None


def assert_wiremock_telegram_e2e_openai_coverage(
    base_url: str,
    *,
    test_id: str,
    chat_id: int,
    reply_body: str,
    message_thread_id: int | None = None,
) -> None:
    """Проверить журнал WireMock: полный контур Telegram e2e (LLM + ``sendMessage`` + ``editMessageText``).

    Ожидаемые фазы (опрос до ``TIMEOUT_POLL_SHORT``):

    * POST ``/embeddings`` (≥1), POST ``/chat/completions`` — как у Matrix live e2e.
    * POST ``…/sendMessage`` — placeholder (невидимый маркер + hourglass).
    * POST ``…/editMessageText`` — финальный текст ответа с ``chat_id``, ``reply_body``.

    KG query не проверяется — та же причина, что в :func:`assert_wiremock_matrix_e2e_openai_coverage`.
    """
    interval = min(3.0, max(1.0, float(POLL_INTERVAL)))

    def _probe() -> bool | None:
        return (
            True
            if not _telegram_openai_coverage_missing(
                base_url,
                test_id=test_id,
                chat_id=chat_id,
                reply_body=reply_body,
                message_thread_id=message_thread_id,
            )
            else None
        )

    try:
        poll_until(
            _probe,
            timeout=float(TIMEOUT_POLL_SHORT),
            interval=interval,
            desc=(
                "WireMock journal: Telegram e2e POST /embeddings + /chat/completions + editMessageText "
                f"(stub_tag={test_id!r})"
            ),
        )
    except TimeoutError:
        miss = _telegram_openai_coverage_missing(
            base_url,
            test_id=test_id,
            chat_id=chat_id,
            reply_body=reply_body,
            message_thread_id=message_thread_id,
        )
        data = admin_requests_json(base_url, stub_tag=test_id)
        raw_list = data.get("requests")
        preview = ""
        if isinstance(raw_list, list):
            bodies: list[str] = []
            for entry in raw_list:
                if not isinstance(entry, dict):
                    continue
                req = entry.get("request")
                if not isinstance(req, dict):
                    continue
                if str(req.get("method") or "").upper() != "POST":
                    continue
                url = str(req.get("url") or "")
                if "chat/completions" not in url and "sendMessage" not in url and "editMessageText" not in url:
                    continue
                bodies.append(_journal_request_body(entry))
            preview = "\n---\n".join(bodies[:5])
        raise AssertionError(
            "WireMock: таймаут ожидания полной цепочки LLM для Telegram e2e: "
            f"не хватает {', '.join(miss)}. test_id={test_id!r} chat_id={chat_id!r}. "
            f"Превью тел POST (до 5, chat/completions + sendMessage + editMessageText): {preview[:12000]!r}"
        ) from None


def describe_wiremock_admin_state(
    host: str, port: int, *, project_name: str | None = None
) -> str:
    """Диагностика: ``GET /__admin/mappings`` + при ``project_name`` — хвост ``docker logs`` wiremock."""
    chunks: list[str] = []
    base = wiremock_public_base(host, port)
    url = f"{base.rstrip('/')}/__admin/mappings"
    try:
        with _wiremock_admin_api_exclusive(timeout=TIMEOUT_POLL_SHORT):
            r = _wm_session().get(url, timeout=TIMEOUT_POLL_SHORT)
            chunks.append(f"--- WireMock GET /__admin/mappings [{r.status_code}] ---\n")
            if r.ok:
                data = _wm_json_response(r, where="GET /__admin/mappings (describe)")
                if isinstance(data, dict):
                    meta = data.get("meta")
                    if isinstance(meta, dict):
                        chunks.append(f"meta: {meta!r}\n")
    except requests.RequestException as e:
        chunks.append(f"(failed GET {url}: {e!r})\n")

    if project_name:
        chunks.append("\n--- wiremock docker logs (tail=400) ---\n")
        try:
            c = _compose_container(project_name, "wiremock")
            raw = c.logs(stdout=True, stderr=True, tail=400)
            chunks.append(raw.decode("utf-8", errors="replace"))
        except Exception as e:  # pragma: no cover
            chunks.append(f"(failed docker logs wiremock: {e!r})")
    return "".join(chunks)


def count_wiremock_chat_completion_posts_for_stub(
    public_base: str,
    *,
    stub_tag: str,
    anchor_needle: str | None = None,
) -> int:
    """Число POST ``/chat/completions`` в журнале записей с ``stub_tag`` (опционально якорь в теле/headers)."""
    n = 0
    for ent in journal_entries_for_stub_tag(public_base, stub_tag=stub_tag):
        req = ent.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "POST":
            continue
        url = str(req.get("url") or req.get("absoluteUrl") or "")
        if "/chat/completions" not in url:
            continue
        haystack = _journal_request_anchor_haystack(ent)
        if anchor_needle is not None and anchor_needle not in haystack:
            continue
        n += 1
    return n


def _journal_entry_response_status(entry: dict[str, Any]) -> int:
    for key in ("response", "responseDefinition"):
        block = entry.get(key)
        if not isinstance(block, dict):
            continue
        try:
            return int(block.get("status") or 0)
        except (TypeError, ValueError):
            continue
    return 0


def _wiremock_headers_get_ci(
    headers_obj: object, canonical_name: str
) -> str | None:
    """Значение заголовка без учёта регистра ключа (запись WireMock / journal / unmatched)."""
    if not isinstance(headers_obj, dict):
        return None
    want = str(canonical_name).strip().lower()
    for k, v in headers_obj.items():
        if str(k).strip().lower() != want:
            continue
        if isinstance(v, list):
            if not v:
                return None
            s = str(v[0]).strip()
            return s if s else None
        s = str(v).strip() if v is not None else ""
        return s if s else None
    return None


def _wiremock_journal_entry_method_url(ent: dict[str, Any]) -> tuple[str, str]:
    """Method + URL для записи журнала WM: и ``ServeEvent`` (вложенный ``request``), и плоский ``LoggedRequest``."""
    req = ent.get("request")
    if isinstance(req, dict):
        method = str(req.get("method") or "").strip().upper() or "?"
        url = str(
            req.get("url") or req.get("absoluteUrl") or req.get("urlPath") or "?"
        )
        return method, url
    method = str(ent.get("method") or "").strip().upper() or "?"
    url = str(
        ent.get("url") or ent.get("absoluteUrl") or ent.get("urlPath") or "?"
    )
    return method, url


def wiremock_unmatched_request_entries(
    public_base: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> list[dict[str, Any]]:
    """Сырой список из ``GET /__admin/requests/unmatched`` (каждый элемент — поля запроса).

    У WireMock **нет** ``limit``/``offset`` для этого списка: при очень большом unmatched
    ответ может быть **500** (OOM при сборке JSON) — тогда ``raise_for_status`` пробросит ошибку.
    На общем инстансе при ``pytest -n N`` параллельные GET без сериализации давали 500;
    сериализация — :func:`_wiremock_admin_api_exclusive`.
    """
    root = _normalize_wiremock_public_root(public_base)
    url = f"{root.rstrip('/')}/__admin/requests/unmatched"
    with _wiremock_admin_api_exclusive(timeout=timeout):
        r = _wm_session().get(url, timeout=timeout)
        r.raise_for_status()
        data = _wm_json_response(r, where="GET /__admin/requests/unmatched")
    if not isinstance(data, dict):
        return []
    raw = data.get("requests")
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, dict)]


def wiremock_unmatched_requests_count(
    public_base: str, *, timeout: float = TIMEOUT_POLL_SHORT
) -> int:
    """``GET /__admin/requests/unmatched`` — число несматченных запросов (короткий лимит на стороне WM)."""
    return len(wiremock_unmatched_request_entries(public_base, timeout=timeout))


def assert_wiremock_unmatched_journal_empty(
    public_base: str,
    *,
    phase: str,
    timeout: float = TIMEOUT_POLL_SHORT,
    preview_limit: int = 8,
) -> None:
    """Жёсткая проверка: журнал ``GET /__admin/requests/unmatched`` пуст (без polling).

    Используется guard'ом e2e в ``conftest.py`` (:func:`pytest_runtest_call`) после setup фикстур —
    до и после тела теста. Автоматически unmatched не сбрасываются после тестов — только одна
    предсессионная очистка журнала WM.
    """
    entries = wiremock_unmatched_request_entries(public_base, timeout=timeout)
    if not entries:
        return
    lines = [
        f"WireMock unmatched journal not empty ({phase}): {len(entries)} request(s). "
        "Ищите причину (стабы, WireMock State / сид контекста, "
        "``X-Threlium-Thread-Root``, порядок teardown). После теста unmatched не чистятся — только "
        "assert; предпрогоновый полный сброс журнала см. ``conftest._e2e_wiremock_journal_reset_once``."
    ]
    for i, ent in enumerate(entries[: max(0, int(preview_limit))]):
        method, url = _wiremock_journal_entry_method_url(ent)
        lines.append(f"  [{i + 1}] {method} {url}")
    extra = len(entries) - preview_limit
    if extra > 0:
        lines.append(f"  ... and {extra} more")
    raise AssertionError("\n".join(lines))


def wiremock_unmatched_requests_count_for_x_threlium_route(
    public_base: str, *, route_wire: str, timeout: float = TIMEOUT_POLL_SHORT
) -> int:
    """Unmatched, у которых ``X-Threlium-Thread-Root`` **точно** совпадает с ожидаемым thread-root MID.

    На общем WireMock журнал несматченных не чистится :func:`remove_wiremock_journal_by_stub_tag`
    (там только matched + ``stubMapping``). Нормативный e2e — глобально пустой unmatched
    (``assert_wiremock_unmatched_journal_empty``); параметр ``x_threlium_route_wire`` у
    :func:`assert_wiremock_zero_unmatched_requests` — только вспомогательная узкая выборка для
    диагностики, не замена глобального guard'а.
    """
    want = str(route_wire).strip()
    if not want:
        return 0
    n = 0
    for ent in wiremock_unmatched_request_entries(public_base, timeout=timeout):
        hdrs = ent.get("headers")
        got = _wiremock_headers_get_ci(hdrs, "X-Threlium-Thread-Root")
        if got is not None and got.strip() == want:
            n += 1
    return n


def count_wiremock_embedding_posts_matching_anchor(
    public_base: str,
    *,
    anchor_needle: str,
    require_status_200: bool = True,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> int:
    """POST ``/embeddings`` в полном журнале (все стабы), с якорем в теле/заголовках, опционально только 200.

    Часть POST обслуживается ``compose_bootstrap`` или каталогом сценария (разные ``stub_tag``), поэтому не фильтруем
    по ``stub_tag`` сценария.
    """
    data = _admin_requests_get_unfiltered(public_base, timeout=timeout)
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    if meta.get("journalRebuilt") and meta.get("unmatchedMerged") is False:
        log.error("wiremock_journal_rebuild_unmatched_omitted_embedding_count_risk")
    raw = data.get("requests")
    if not isinstance(raw, list):
        return 0
    n = 0
    for ent in raw:
        if not isinstance(ent, dict):
            continue
        req = ent.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "POST":
            continue
        url = str(req.get("url") or req.get("absoluteUrl") or "")
        if "/embeddings" not in url:
            continue
        if anchor_needle not in _journal_request_anchor_haystack(ent):
            continue
        if require_status_200 and _journal_entry_response_status(ent) != 200:
            continue
        n += 1
    return n


def assert_wiremock_zero_unmatched_requests(
    public_base: str,
    *,
    diag_callback: Callable[[], None] | None = None,
    wait_timeout_sec: float | None = None,
    x_threlium_route_wire: str | None = None,
) -> None:
    """Опрос: журнал несматченных пуст (моки отвечают без 404 unmatched).

    По умолчанию (**без** ``x_threlium_route_wire``) — глобально по инстансу (политика e2e).
    С ``x_threlium_route_wire`` считаются только unmatched с таким ``X-Threlium-Thread-Root`` — только
    для особых случаев; в обычных сценариях используйте глобальную проверку.
    """
    w = float(TIMEOUT_POLL_SHORT) if wait_timeout_sec is None else float(wait_timeout_sec)

    def _n_unmatched() -> int:
        if x_threlium_route_wire is None:
            return wiremock_unmatched_requests_count(public_base)
        return wiremock_unmatched_requests_count_for_x_threlium_route(
            public_base, route_wire=x_threlium_route_wire
        )

    def _probe() -> bool:
        return _n_unmatched() == 0

    def _msg() -> str:
        n = _n_unmatched()
        total = wiremock_unmatched_requests_count(public_base)
        if x_threlium_route_wire is None:
            return f"expected zero WireMock unmatched requests within {w}s, found {n}"
        return (
            f"expected zero WireMock unmatched for X-Threlium-Thread-Root within {w}s, "
            f"found {n} matching route (total unmatched in journal: {total})"
        )

    _poll_wiremock_with_tenacity(
        probe=_probe,
        wait_timeout_sec=wait_timeout_sec,
        diag_callback=diag_callback,
        build_error=_msg,
    )


def assert_wiremock_min_embedding_posts_matching_anchor(
    public_base: str,
    *,
    anchor_needle: str,
    min_posts: int,
    diag_callback: Callable[[], None] | None = None,
    wait_timeout_sec: float | None = None,
) -> None:
    """Poll: ≥ ``min_posts`` успешных POST ``/embeddings`` с якорем корреляции."""
    w = float(TIMEOUT_POLL_SHORT) if wait_timeout_sec is None else float(wait_timeout_sec)

    def _probe() -> bool:
        return (
            count_wiremock_embedding_posts_matching_anchor(
                public_base,
                anchor_needle=anchor_needle,
                require_status_200=True,
            )
            >= min_posts
        )

    def _msg() -> str:
        n = count_wiremock_embedding_posts_matching_anchor(
            public_base, anchor_needle=anchor_needle, require_status_200=True
        )
        return (
            f"expected at least {min_posts} POST /embeddings (200) with anchor in WireMock journal "
            f"within {w}s, found {n}"
        )

    _poll_wiremock_with_tenacity(
        probe=_probe,
        wait_timeout_sec=wait_timeout_sec,
        diag_callback=diag_callback,
        build_error=_msg,
    )


def count_wiremock_rerank_posts_matching_anchor(
    public_base: str,
    *,
    anchor_needle: str,
    require_status_200: bool = True,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> int:
    """POST ``/rerank`` в полном журнале, с якорем в теле/заголовках, опционально только 200."""
    data = _admin_requests_get_unfiltered(public_base, timeout=timeout)
    raw = data.get("requests")
    if not isinstance(raw, list):
        return 0
    n = 0
    for ent in raw:
        if not isinstance(ent, dict):
            continue
        req = ent.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "POST":
            continue
        url = str(req.get("url") or req.get("absoluteUrl") or "")
        if "/rerank" not in url:
            continue
        if anchor_needle not in _journal_request_anchor_haystack(ent):
            continue
        if require_status_200 and _journal_entry_response_status(ent) != 200:
            continue
        n += 1
    return n


def assert_wiremock_min_rerank_posts_matching_anchor(
    public_base: str,
    *,
    anchor_needle: str,
    min_posts: int,
    diag_callback: Callable[[], None] | None = None,
    wait_timeout_sec: float | None = None,
) -> None:
    """Poll: >= ``min_posts`` POST ``/rerank`` (200) with correlation anchor."""
    w = float(TIMEOUT_POLL_SHORT) if wait_timeout_sec is None else float(wait_timeout_sec)

    def _probe() -> bool:
        return (
            count_wiremock_rerank_posts_matching_anchor(
                public_base,
                anchor_needle=anchor_needle,
                require_status_200=True,
            )
            >= min_posts
        )

    def _msg() -> str:
        n = count_wiremock_rerank_posts_matching_anchor(
            public_base, anchor_needle=anchor_needle, require_status_200=True
        )
        return (
            f"expected at least {min_posts} POST /rerank (200) with anchor in WireMock journal "
            f"within {w}s, found {n}"
        )

    _poll_wiremock_with_tenacity(
        probe=_probe,
        wait_timeout_sec=wait_timeout_sec,
        diag_callback=diag_callback,
        build_error=_msg,
    )


def assert_wiremock_stub_received_min_chat_completions(
    public_base: str,
    *,
    stub_tag: str,
    anchor_needle: str | None,
    min_posts: int = 1,
    diag_callback: Callable[[], None] | None = None,
    wait_timeout_sec: float | None = None,
) -> None:
    """Poll журнала WireMock: ≥ ``min_posts`` POST chat/completions с фильтром ``stub_tag`` (+ якорь)."""
    w = float(TIMEOUT_POLL_SHORT) if wait_timeout_sec is None else float(wait_timeout_sec)

    def _probe() -> bool:
        return (
            count_wiremock_chat_completion_posts_for_stub(
                public_base,
                stub_tag=stub_tag,
                anchor_needle=anchor_needle,
            )
            >= min_posts
        )

    def _msg() -> str:
        n = count_wiremock_chat_completion_posts_for_stub(
            public_base, stub_tag=stub_tag, anchor_needle=anchor_needle
        )
        return (
            f"expected at least {min_posts} POST /chat/completions in WireMock journal "
            f"for stub_tag={stub_tag!r} (anchor={anchor_needle!r}) within {w}s, found {n}"
        )

    _poll_wiremock_with_tenacity(
        probe=_probe,
        wait_timeout_sec=wait_timeout_sec,
        diag_callback=diag_callback,
        build_error=_msg,
    )


def _journal_hay_is_reasoning_chat_completion(hay: str, *, correlation_key: str) -> bool:
    return (
        correlation_key in hay
        and "<envelope>" in hay
        and '"tools"' in hay
    )


def assert_wiremock_reasoning_journal_preserves_context_tail(
    public_base: str,
    *,
    stub_tag: str,
    correlation_key: str,
    tail_marker: str,
    head_marker: str,
    max_body_chars: int,
    journal_slack_chars: int = 4096,
) -> None:
    """Reasoning POST в журнале: хвост маркера сохранён, HEAD отброшен, user content ≤ лимит+slack."""
    entries = journal_entries_for_stub_tag(public_base, stub_tag=stub_tag)
    reasoning_user_bodies: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        method = str(req.get("method") or "").upper()
        url = str(req.get("url") or "")
        if method != "POST":
            continue
        if "/v1/chat/completions" not in url and not url.rstrip("/").endswith("chat/completions"):
            continue
        hay = _journal_request_anchor_haystack(entry)
        if not _journal_hay_is_reasoning_chat_completion(hay, correlation_key=correlation_key):
            continue
        user_body = _journal_chat_completion_user_content(entry)
        if user_body:
            reasoning_user_bodies.append(user_body)
    if not reasoning_user_bodies:
        raise AssertionError(
            f"no reasoning POST chat/completions in WireMock journal for stub_tag={stub_tag!r} "
            f"correlation_key={correlation_key!r}"
        )
    worst = max(reasoning_user_bodies, key=len)
    if tail_marker not in worst:
        raise AssertionError(
            f"reasoning journal user content missing tail marker {tail_marker!r} "
            "(trim_context_text expected to keep tail)"
        )
    if head_marker in worst:
        raise AssertionError(
            f"reasoning journal user content still contains head marker {head_marker!r} "
            "(expected trim from start)"
        )
    cap = max_body_chars + journal_slack_chars
    if len(worst) > cap:
        raise AssertionError(
            f"reasoning journal user content length {len(worst)} exceeds "
            f"{max_body_chars} + slack {journal_slack_chars} = {cap}"
        )
