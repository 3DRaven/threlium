"""RAG loop lifecycle: thread management, coroutine dispatch, drain scheduling."""
from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Coroutine
from typing import Any, TypeVar

from lightrag import LightRAG

from threlium.litellm_route_context import (
    reset_litellm_correlation_ctxvar,
    set_litellm_correlation_ctxvar,
)
from threlium.logutil import logger
from threlium.settings import ThreliumSettings, resolve_llm_endpoint
from threlium.systemd_notify import notify_status
from threlium.types import LitellmRoutingSite
from threlium.types.systemd_status import SystemdStatusBody

from threlium.runners.lightrag._bootstrap import bootstrap_knowledge_dir
from threlium.runners.lightrag._construction import build_rag
from threlium.runners.lightrag._drain import (
    reset_drain_task,
    reset_thread_locks,
    schedule_on_loop,
    thread_lock_for,
)

log = logger.bind(stage="lightrag")

_T = TypeVar("_T")


class _NoOpAsyncLock:
    """No-op async lock: drain⊕aquery⊕bootstrap больше НЕ сериализуются глобально на RAG-loop.

    Индексация развязана от тестов (enrich-барьер в mailflow assert) → она может идти async
    background конкурентно с запросами; единый RAG event-loop под -n4 больше не упирается в
    глобальный mutex. Стор (Redis/Faiss/NetworkX) держит конкурентные ainsert/aquery (доказано:
    0 storage races). Остаётся не-None sentinel для readiness-проверок ``_drain_lock is not None``."""

    async def __aenter__(self) -> "_NoOpAsyncLock":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


_rag_loop: asyncio.AbstractEventLoop | None = None
_rag_thread: threading.Thread | None = None
_daemon_rag: LightRAG | None = None
_drain_lock: asyncio.Lock | None = None
_bootstrap_task: asyncio.Task[None] | None = None
_start_lock = threading.Lock()
_ready_event = threading.Event()
_boot_error: list[BaseException] = []


def _future_timeout_sec(settings: ThreliumSettings) -> float | None:
    llm_ep = resolve_llm_endpoint(settings.litellm, LitellmRoutingSite.LIGHTRAG_LLM)
    v = float(llm_ep.timeout)
    return v if v > 0 else None


def _rag_loop_shutdown_timeout_sec(settings: ThreliumSettings | None) -> float:
    if settings is None:
        return 30.0
    return float(settings.lightrag.rag_loop_shutdown_timeout_sec)


async def _shutdown_rag_loop() -> None:
    """Отменить все задачи loop (кроме текущей), затем flush storages."""
    me = asyncio.current_task()
    work = [t for t in asyncio.all_tasks() if t is not me and not t.done()]
    for t in work:
        t.cancel()
    if work:
        await asyncio.gather(*work, return_exceptions=True)
        log.info("rag_shutdown_cancelled_tasks", count=len(work))
    if _daemon_rag is not None:
        await _daemon_rag.finalize_storages()


def daemon_lightrag() -> LightRAG | None:
    """Инстанс на RAG-loop (после успешного ``start_rag_loop_thread``)."""
    return _daemon_rag


def run_rag_coroutine(
    coro: Coroutine[Any, Any, _T],
    *,
    settings: ThreliumSettings,
    correlation: dict[str, str] | None = None,
) -> _T:
    """Выполнить корутину LightRAG на выделенном loop (из любого потока движка).

    При непустом ``correlation`` устанавливает ContextVar на задаче RAG-loop; llm/embed/rerank-функции
    (``_adapters``) читают его НАПРЯМУЮ из ctxvar (дочерние задачи наследуют контекст) — отдельного
    kwarg-моста больше нет. call-site детерминирован точкой вызова (роль/rerank); route-merge в HTTP
    gated за e2e-флагом.
    """
    if _rag_loop is None:
        raise RuntimeError("lightrag: RAG event loop is not running (start_rag_loop_thread first)")
    timeout = _future_timeout_sec(settings)

    async def _runner() -> _T:
        token = set_litellm_correlation_ctxvar(correlation) if correlation is not None else None
        lock = thread_lock_for(correlation)  # пер-тред барьер: query ждёт in-flight ainsert СВОЕГО треда
        try:
            if lock is not None:
                async with lock:
                    return await coro
            return await coro
        finally:
            if token is not None:
                reset_litellm_correlation_ctxvar(token)

    fut = asyncio.run_coroutine_threadsafe(_runner(), _rag_loop)
    return fut.result(timeout=timeout)


def _bootstrap_timeout_sec(settings: ThreliumSettings) -> float | None:
    """Дедлайн всей bootstrap-индексации (НЕ per-call LLM timeout). 0/неположит. — без лимита."""
    v = float(settings.lightrag.bootstrap_timeout_sec)
    return v if v > 0 else None


async def _run_bootstrap_guarded(
    rag: LightRAG,
    settings: ThreliumSettings,
    lock: asyncio.Lock,
    correlation: dict[str, str] | None,
) -> None:
    """Фоновая bootstrap-индексация knowledge/ на RAG-loop (после ``notify_ready``).

    Не валит engine: истечение ``bootstrap_timeout_sec`` или ошибка только логируются —
    остаток доиндексируется на следующем старте (дедуп через ``doc_status``). Сериализация
    с drain — общий ``lock`` (на каждый батч, внутри ``bootstrap_knowledge_dir``).
    """
    timeout = _bootstrap_timeout_sec(settings)
    token = None
    if settings.e2e.litellm_route_correlation and correlation is not None:
        token = set_litellm_correlation_ctxvar(correlation)
    t0 = time.monotonic()
    try:
        coro = bootstrap_knowledge_dir(rag, settings, lock=lock)
        count = await (asyncio.wait_for(coro, timeout=timeout) if timeout is not None else coro)
        elapsed = time.monotonic() - t0
        if count > 0:
            notify_status(
                SystemdStatusBody.lightrag_bootstrap_complete(doc_count=count, elapsed_sec=elapsed)
            )
            log.info("bootstrap_knowledge_complete", docs=count, elapsed_sec=round(elapsed, 1))
        else:
            log.info("bootstrap_knowledge_empty", elapsed_sec=round(elapsed, 1))
    except asyncio.CancelledError:
        log.info("bootstrap_knowledge_cancelled")
        raise
    except asyncio.TimeoutError:
        notify_status(SystemdStatusBody.lightrag_bootstrap_timeout(timeout_sec=timeout or 0.0))
        log.error(
            "bootstrap_knowledge_timeout",
            timeout_sec=timeout,
            elapsed_sec=round(time.monotonic() - t0, 1),
        )
    except BaseException as ex:
        log.error("bootstrap_knowledge_failed", exc_info=ex)
    finally:
        if token is not None:
            reset_litellm_correlation_ctxvar(token)


def schedule_bootstrap_knowledge(
    settings: ThreliumSettings,
    *,
    correlation: dict[str, str] | None = None,
) -> None:
    """Запланировать bootstrap knowledge/ на RAG-loop без блокировки старта engine.

    Вызывать ПОСЛЕ ``notify_ready`` (sd_notify READY): systemd видит сервис готовым сразу,
    а тяжёлая индексация идёт в фоне на выделенном loop с собственным длинным таймаутом.
    """
    global _bootstrap_task
    if _rag_loop is None or _daemon_rag is None or _drain_lock is None:
        log.warning("schedule_bootstrap_not_ready")
        return
    rag = _daemon_rag
    lock = _drain_lock

    def _spawn() -> None:
        global _bootstrap_task
        if _bootstrap_task is not None and not _bootstrap_task.done():
            return
        _bootstrap_task = asyncio.create_task(
            _run_bootstrap_guarded(rag, settings, lock, correlation)
        )

    _rag_loop.call_soon_threadsafe(_spawn)


def schedule_index_pending(settings: ThreliumSettings) -> None:
    """Запланировать drain pending на RAG-loop (после ``nm_settle``) без ожидания.

    Паттерн sweep: если задача уже запущена — noop. Иначе создать задачу.
    Цепочка продолжается внутри ``drain_single_batch`` (OnSuccess → self-schedule).
    """
    if _rag_loop is None or _daemon_rag is None or _drain_lock is None:
        log.warning("schedule_index_pending_not_ready")
        return
    rag = _daemon_rag
    lock = _drain_lock
    _rag_loop.call_soon_threadsafe(schedule_on_loop, rag, settings, lock)


def _rag_thread_main(settings: ThreliumSettings) -> None:
    global _rag_loop, _daemon_rag, _drain_lock, _bootstrap_task
    notify_status(SystemdStatusBody.lightrag_thread_starting())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _rag_loop = loop
    _drain_lock = _NoOpAsyncLock()  # глобальная сериализация снята (см. _NoOpAsyncLock)
    _bootstrap_task = None
    _boot_error.clear()
    try:

        async def _boot() -> LightRAG:
            notify_status(SystemdStatusBody.lightrag_initializing_storages())
            rag = build_rag(settings)
            await rag.initialize_storages()
            notify_status(SystemdStatusBody.lightrag_storages_ready())
            return rag

        rag = loop.run_until_complete(_boot())
        _daemon_rag = rag
        _ready_event.set()
        loop.run_forever()
    except BaseException as e:
        notify_status(SystemdStatusBody.lightrag_boot_failed(message=str(e)))
        _boot_error.append(e)
        _ready_event.set()
    finally:
        try:
            if not loop.is_closed():
                loop.close()
        except Exception:
            pass
        _rag_loop = None
        _drain_lock = None
        _bootstrap_task = None
        reset_thread_locks()  # loop-bound asyncio.Lock'и — сбросить при пересоздании loop
        reset_drain_task()


def start_rag_loop_thread(settings: ThreliumSettings) -> None:
    """Старт фонового потока с единственным loop для LightRAG."""
    global _rag_thread
    with _start_lock:
        if _rag_thread is not None and _rag_thread.is_alive():
            return
        _ready_event.clear()
        _boot_error.clear()
        t = threading.Thread(
            target=_rag_thread_main,
            args=(settings,),
            name="threlium-rag-loop",
            daemon=True,
        )
        _rag_thread = t
        t.start()
        ok = _ready_event.wait(timeout=120.0)
        if not ok:
            raise RuntimeError("lightrag: RAG loop thread did not become ready within 120s")
        if _boot_error:
            raise RuntimeError("lightrag: RAG loop bootstrap failed") from _boot_error[0]


def stop_rag_loop_thread(*, settings: ThreliumSettings | None = None) -> None:
    """Остановить loop: cancel work-задач, ``finalize_storages``, ``loop.stop`` с MainThread."""
    global _rag_thread, _daemon_rag, _drain_lock, _bootstrap_task
    loop = _rag_loop
    th = _rag_thread
    if loop is None or th is None or not th.is_alive():
        _rag_thread = None
        _daemon_rag = None
        _drain_lock = None
        _bootstrap_task = None
        return
    shutdown_timeout = _rag_loop_shutdown_timeout_sec(settings)

    try:
        fut = asyncio.run_coroutine_threadsafe(_shutdown_rag_loop(), loop)
        fut.result(timeout=shutdown_timeout)
    except Exception as e:
        log.error("shutdown_rag_loop_failed", error=repr(e))
    finally:
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:
            pass
    th.join(timeout=shutdown_timeout + 5.0)
    _rag_thread = None
    _daemon_rag = None
    _drain_lock = None
    _bootstrap_task = None
    reset_thread_locks()
    reset_drain_task()
