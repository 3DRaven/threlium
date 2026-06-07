"""Drain / Sweep scheduling: collect pending → ainsert → tag → self-schedule."""
from __future__ import annotations

import asyncio
import time
from pathlib import Path

from lightrag import LightRAG

from threlium.litellm_correlation_headers import build_litellm_correlation_headers_from_notmuch
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader
from threlium.litellm_route_context import (
    e2e_route_wire_tail,
    reset_litellm_correlation_ctxvar,
    set_litellm_correlation_ctxvar,
)
from threlium.lightrag_drain_query import lightrag_drain_pending_search
from threlium.logutil import logger
from threlium.mail import email_message_from_path
from threlium.mime_reform import message_has_history
from threlium.lightrag_ingest import render_lightrag_ingest_document
from threlium.nm import (
    batch_tag_add,
    notmuch_database,
    read_retry,
    require_inner_message_id_from_notmuch_message,
)
from threlium.settings import (
    ThreliumSettings,
    resolve_llm_endpoint,
)
from threlium.systemd_notify import notify_status
from threlium.types import (
    FsmStage,
    LightragDrainSkipReason,
    LitellmCallSite,
    LitellmCorrelationSnapshot,
    LitellmRoutingSite,
    NotmuchMessageIdInner,
    NotmuchMessageIds,
    NotmuchQueryField,
    NotmuchTag,
    NotmuchThreadScopeId,
)
from threlium.types.systemd_status import SystemdStatusBody

log = logger.bind(stage="lightrag")

_drain_task: asyncio.Task[None] | None = None


def reset_drain_task() -> None:
    """Reset drain task state (called during lifecycle cleanup)."""
    global _drain_task
    _drain_task = None


# Пер-тред (по thread-root корреляции) asyncio.Lock на RAG-loop: сериализует ``ainsert``↔``aquery`` ОДНОГО
# notmuch-треда (каузальный index→query: enrich-query ждёт in-flight индексацию своего треда), но НЕ разные
# треды (свой лок → конкурентно). Ключ — thread-root (есть в e2e; в прод может отсутствовать → без лока,
# eventual consistency). Создаётся/читается ТОЛЬКО на RAG-loop (без гонок); сброс при пересоздании loop.
_thread_locks: dict[str, asyncio.Lock] = {}


def reset_thread_locks() -> None:
    """Сбросить пер-тред локи (loop-bound asyncio.Lock) при пересоздании RAG-loop."""
    _thread_locks.clear()


def thread_lock_for(correlation: dict[str, str] | None) -> asyncio.Lock | None:
    if not correlation:
        return None
    root = correlation.get(LitellmCorrelationHeader.THREAD_ROOT_MID.value)
    if not root:
        return None
    lock = _thread_locks.get(root)
    if lock is None:
        lock = asyncio.Lock()
        _thread_locks[root] = lock
    return lock


def _future_timeout_sec(settings: ThreliumSettings) -> float | None:
    llm_ep = resolve_llm_endpoint(settings.litellm, LitellmRoutingSite.LIGHTRAG_LLM)
    v = float(llm_ep.timeout)
    return v if v > 0 else None


def _effective_batch_size(settings: ThreliumSettings) -> int:
    if settings.e2e.litellm_route_correlation:
        return 1
    return max(1, settings.lightrag.insert_batch)


@read_retry
def _correlation_snapshot_for_path(fp0: Path) -> "LitellmCorrelationSnapshot":
    """Снять LiteLLM-correlation snapshot (VO) по пути письма; ``@read_retry`` reopen-on-modified.

    ``notmuch2.Message`` не покидает сеанс — наружу только ``LitellmCorrelationSnapshot``."""
    with notmuch_database(write=False) as db:
        nm_msg = db.get(str(fp0.resolve()))
        corr = build_litellm_correlation_headers_from_notmuch(
            db, nm_msg, call_site=LitellmCallSite.LIGHTRAG_INDEX
        )
    return LitellmCorrelationSnapshot.from_mapping(corr)


@read_retry
def _collect_batch(limit: int) -> list[tuple[Path, NotmuchMessageIdInner, NotmuchThreadScopeId | None]]:
    """(path, message_id_inner, thread_scope)[…limit] под одной READ-транзакцией → только VO наружу.

    ``@read_retry``: при discard'е ревизии под конкурентной записью сеанс переоткрывается (rag-loop
    в движке многопоточен; ``notmuch2.Message`` не покидает ``with``)."""
    out: list[tuple[Path, NotmuchMessageIdInner, NotmuchThreadScopeId | None]] = []
    selector = lightrag_drain_pending_search()
    with notmuch_database(write=False) as db:
        for msg in db.messages(selector):
            fp = Path(msg.path)
            if not fp.is_file():
                continue
            ids = NotmuchMessageIds.from_notmuch(msg)
            mid_inner = require_inner_message_id_from_notmuch_message(msg)
            out.append((fp, mid_inner, ids.threadid))
            if len(out) >= limit:
                break
    return out


async def _ainsert_with_correlation(
    rag: LightRAG,
    texts: list[str],
    ids: list[str],
    file_paths: list[str],
    settings: ThreliumSettings,
) -> float:
    """ainsert with e2e correlation ctxvar. Returns elapsed seconds."""
    snap = _correlation_snapshot_for_path(Path(file_paths[0]))
    log.debug(
        "drain_e2e_ainsert",
        batch_size=len(ids),
        route_tail=e2e_route_wire_tail(snap.route_wire),
        call_site=snap.call_site,
        first_mid=ids[0],
    )
    # Пер-тред барьер: индексация треда держит его лок, чтобы enrich-aquery ЭТОГО треда дождался
    # завершения (index→query causal order); разные треды — разные локи, конкурентно.
    token = set_litellm_correlation_ctxvar(snap.as_dict())
    lock = thread_lock_for(snap.as_dict())
    try:
        t0 = time.monotonic()
        if lock is not None:
            async with lock:
                await rag.ainsert(texts, ids=ids, file_paths=file_paths)
        else:
            await rag.ainsert(texts, ids=ids, file_paths=file_paths)
        return time.monotonic() - t0
    finally:
        reset_litellm_correlation_ctxvar(token)


async def _ainsert_plain(
    rag: LightRAG, texts: list[str], ids: list[str], file_paths: list[str]
) -> float:
    """ainsert without correlation. Returns elapsed seconds."""
    t0 = time.monotonic()
    await rag.ainsert(texts, ids=ids, file_paths=file_paths)
    return time.monotonic() - t0


async def _ainsert_batch(
    rag: LightRAG,
    pending: list[tuple[Path, NotmuchMessageIdInner, NotmuchThreadScopeId | None]],
    settings: ThreliumSettings,
) -> None:
    """Render pending messages → ainsert → tag as indexed."""
    llm_timeout = _future_timeout_sec(settings)

    texts: list[str] = []
    ids: list[str] = []
    tag_ids: list[NotmuchMessageIdInner] = []
    file_paths: list[str] = []
    skip_tag_ids: list[NotmuchMessageIdInner] = []

    for fp, mid_inner, tid in pending:
        try:
            msg = email_message_from_path(fp)
        except Exception as exc:
            log.error(
                "index_skip",
                reason=LightragDrainSkipReason.RENDER_FAILED.value,
                path=str(fp),
                exc_type=type(exc).__name__,
                exc_msg=str(exc),
            )
            skip_tag_ids.append(mid_inner)
            continue
        # Финальный предикат содержательности (selector даёт лишь tag-негативы): письмо
        # достойно графа, только если несёт ``<history>``-часть. Системные/control-письма
        # (только ``<system>``, без history) индексировать нечем — помечаем skipped, чтобы
        # не оставлять их в вечном pending.
        if not message_has_history(msg):
            to_stage = FsmStage.try_from_incoming_to(msg)
            log.info(
                "index_skip",
                reason=LightragDrainSkipReason.NO_HISTORY.value,
                path=str(fp),
                to_stage=to_stage.value if to_stage is not None else None,
            )
            skip_tag_ids.append(mid_inner)
            continue
        try:
            thread_term = (
                tid.as_notmuch_thread_term()
                if tid is not None
                else NotmuchQueryField.THREAD.term("unknown")
            )
            text = render_lightrag_ingest_document(msg, thread_term=thread_term)
        except Exception as exc:
            log.error(
                "index_skip",
                reason=LightragDrainSkipReason.RENDER_FAILED.value,
                path=str(fp),
                exc_type=type(exc).__name__,
                exc_msg=str(exc),
            )
            skip_tag_ids.append(mid_inner)
            continue
        texts.append(text)
        ids.append(mid_inner.value)
        tag_ids.append(mid_inner)
        file_paths.append(str(fp))

    if skip_tag_ids:
        skipped_tagged = batch_tag_add(skip_tag_ids, NotmuchTag.LIGHTRAG_SKIPPED)
        log.warning(
            "index_skipped_tagged",
            count=len(skip_tag_ids),
            tagged=skipped_tagged,
        )

    if not texts:
        if not skip_tag_ids:
            raise RuntimeError(
                "lightrag: pending batch produced no texts "
                f"(paths={[str(p[0]) for p in pending]!r})"
            )
        return

    if settings.e2e.litellm_route_correlation:
        elapsed = await _ainsert_with_correlation(rag, texts, ids, file_paths, settings)
    else:
        elapsed = await _ainsert_plain(rag, texts, ids, file_paths)

    if llm_timeout is not None and elapsed > 0.8 * llm_timeout:
        log.warning("ainsert_slow", elapsed_sec=round(elapsed, 1), llm_timeout_sec=llm_timeout)
    elif elapsed > 60:
        log.info("ainsert_elapsed", elapsed_sec=round(elapsed, 1), batch_size=len(ids))

    tagged = batch_tag_add(tag_ids, NotmuchTag.LIGHTRAG_INDEXED)
    log.info("ainsert_complete", docs=len(ids), tagged=tagged)
    notify_status(SystemdStatusBody.lightrag_idle_indexed(message_count=len(ids)))


async def drain_single_batch(
    rag: LightRAG, settings: ThreliumSettings, lock: asyncio.Lock
) -> None:
    """One batch: collect → ainsert → tag → sweep (self-schedule if more pending).

    Паттерн sweep (аналог threlium-work@ → OnSuccess → threlium-sweep@):
    задача обрабатывает один батч, после завершения проверяет backlog и
    при наличии pending создаёт следующую задачу.
    """
    global _drain_task
    batch_size = _effective_batch_size(settings)

    try:
        async with lock:
            pending = _collect_batch(batch_size)
            if not pending:
                notify_status(SystemdStatusBody.lightrag_idle_no_pending())
                return
            notify_status(SystemdStatusBody.lightrag_indexing_batch(batch_size=len(pending)))
            await _ainsert_batch(rag, pending, settings)
    except asyncio.CancelledError:
        return
    except BaseException as ex:
        log.error("drain_batch_failed", exc_info=ex)
        raise

    if _collect_batch(1):
        _drain_task = asyncio.create_task(
            drain_single_batch(rag, settings, lock)
        )


def schedule_on_loop(rag: LightRAG, settings: ThreliumSettings, lock: asyncio.Lock) -> None:
    """Create a drain task if none is running (called via loop.call_soon_threadsafe)."""
    global _drain_task
    if _drain_task is not None and not _drain_task.done():
        return
    _drain_task = asyncio.create_task(
        drain_single_batch(rag, settings, lock)
    )
