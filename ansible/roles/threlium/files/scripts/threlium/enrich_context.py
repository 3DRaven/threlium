"""Сбор ``unified_messages`` для стадии enrich (notmuch + ``EmailMessage``)."""
from __future__ import annotations

import itertools
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path

from threlium import nm
from threlium.logutil import logger
from threlium.settings import ThreliumSettings
from threlium.thread_context_filter import iter_irt_ancestors_filtered
from threlium.mime_reform import (
    email_message_from_path,
    iter_history_parts,
    message_has_history,
)
from threlium.types import (
    FsmStage,
    MailHeaderName,
    NotmuchMessageIdInner,
    NotmuchQueryConnective,
    NotmuchQueryField,
    NotmuchTag,
)

log = logger.bind(stage="enrich_context")

_HDR = MailHeaderName


def _sort_email_messages_oldest_first(msgs: list[EmailMessage]) -> list[EmailMessage]:
    def _ts(m: EmailMessage) -> float:
        d = m.get(_HDR.DATE)
        if not d:
            return 0.0
        try:
            dt = parsedate_to_datetime(d)
            return float(dt.timestamp()) if dt is not None else 0.0
        except (TypeError, ValueError, OSError):
            return 0.0

    return sorted(msgs, key=_ts)


def trim_prompt_text(text: str, max_chars: int) -> str:
    """Обрезка **с начала** строки при превышении лимита (старое уходит первым)."""
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[-max_chars:]


def trim_context_text(text: str, max_chars: int) -> str:
    """Единая обрезка контекста enrich/reasoning: хвост, ``max_chars`` из ``enrich.context_max_chars``."""
    return trim_prompt_text(text, max_chars)


@dataclass(frozen=True)
class UnifiedEmailContext:
    """Три бакета unified-контекста, сохраняющие разделение по источнику."""

    all_messages: list[EmailMessage]
    thread_memory_msgs: list[EmailMessage]
    global_memory_msgs: list[EmailMessage]


def _load_paths(paths: list[Path]) -> list[EmailMessage]:
    loaded: list[EmailMessage] = []
    skipped = 0
    for p in paths:
        try:
            loaded.append(email_message_from_path(p))
        except OSError as exc:
            log.warning("load_path_skipped", path=str(p), exc_msg=str(exc))
            skipped += 1
            continue
    if skipped:
        log.warning("load_paths_skipped_total", skipped=skipped, total=len(paths))
    return loaded


def build_unified_email_messages(
    *,
    settings: ThreliumSettings,
    leaf_inner: NotmuchMessageIdInner,
    thread_id: str,
) -> UnifiedEmailContext:
    """Три источника → дедуп по ``Message-ID`` → хронология старые → новые.

    Возвращает :class:`UnifiedEmailContext` с объединённым списком и
    отдельными бакетами ``thread_memory`` / ``global_memory`` для
    гранулярных MIME-частей.
    """
    n_thread = settings.enrich.context_thread_n
    n_tm = settings.enrich.context_thread_memory_n
    n_gm = settings.enrich.context_global_n

    tail_snaps = list(
        itertools.islice(iter_irt_ancestors_filtered(leaf_inner), n_thread)
    )

    tm_q = NotmuchQueryConnective.join_and(
        NotmuchQueryField.THREAD.term(thread_id),
        NotmuchQueryField.TO.term(FsmStage.THREAD_MEMORY.rfc822_mailbox),
    )
    tm_paths = nm.message_paths(tm_q, limit=n_tm, sort_newest_first=True)

    gm_q = NotmuchQueryField.TO.term(FsmStage.GLOBAL_MEMORY.rfc822_mailbox)
    gm_paths = nm.message_paths(gm_q, limit=n_gm, sort_newest_first=True)

    memory_path_keys: set[str] = {
        str(p.resolve()) for p in itertools.chain(tm_paths, gm_paths)
    }

    # Проход по снимкам IRT **старые→новые** (tail_snaps идёт лист→корень = новые→старые,
    # поэтому reversed). После унификации роль письма — наличие ``<history>``-части
    # (:func:`message_has_history`), а не To-стадия. summarized отбрасываем по тегам снимка;
    # memory-письма исключаем (отдельные бакеты).
    #
    # Дедуп по контенту (``EnrichContentId`` ``<{hash}@history>``), а не по Message-ID: письмо
    # берётся, только если несёт хотя бы одну **новую** history-часть. Так relay-блоб
    # ``enrich_fast → reasoning`` (копии уже собранных оригиналов) схлопывается — все его CID
    # уже видены, письмо отбрасывается, а каноничные оригиналы (с корректным From: origin)
    # остаются. Старые-первыми ⇒ предпочитаем оригинал его более поздней relay-копии.
    #
    # Лист (``leaf_inner``) — текущий ход enrich; его ``<history>`` дублирует <user-message>,
    # поэтому пропускаем ровно его. Прошлые ходы (старшие ingress→enrich с history) остаются.
    _summarized_tag = NotmuchTag.CONTEXT_SUMMARIZED.value
    seen_cids: set[str] = set()
    seen_mids: set[str] = set()
    kept: list[EmailMessage] = []
    for snap in reversed(tail_snaps):
        if _summarized_tag in snap.tags:
            continue
        if snap.message_id_inner.value == leaf_inner.value:
            continue
        if str(snap.path.resolve()) in memory_path_keys:
            continue
        if snap.message_id_inner.value in seen_mids:
            continue
        seen_mids.add(snap.message_id_inner.value)
        try:
            m = email_message_from_path(snap.path)
        except OSError as exc:
            log.warning("unified_load_path_skipped", path=str(snap.path), exc_msg=str(exc))
            continue
        cids = {cid.value for cid, _part in iter_history_parts(m)}
        if not cids:
            continue
        if cids <= seen_cids:
            continue
        seen_cids |= cids
        kept.append(m)

    return UnifiedEmailContext(
        all_messages=_sort_email_messages_oldest_first(kept),
        thread_memory_msgs=_sort_email_messages_oldest_first(_load_paths(tm_paths)),
        global_memory_msgs=_sort_email_messages_oldest_first(_load_paths(gm_paths)),
    )


def collect_unified_delta_msgs(leaf_inner: NotmuchMessageIdInner) -> list[EmailMessage]:
    """Содержательные письма, появившиеся с прошлого ``To: reasoning`` (E_prev) до листа.

    Обход IRT лист→корень (с изоляцией субагентов через
    :func:`iter_irt_ancestors_filtered`) обрывается на ближайшем ``To: reasoning`` —
    это E_prev (в multi-cycle — выход прошлого ``enrich_fast``). Всё строго новее этой
    границы, несущее ``<history>``-часть (:func:`message_has_history`, без
    ``tag:context_summarized``), идёт в дельту. По структуре IRT там нет «старых»
    писем, поэтому MID-дедуп не нужен; прошлые циклы отрезаны watermark'ом.

    Stage-agnostic: роль письма — наличие ``<history>``, а не его To-стадия; не зависит
    от того, сколько и какие стадии стоят перед ``enrich_fast``. Возвращает письма;
    извлечение и дедуп ``<history>``-частей выполняет сам ``enrich_fast``.
    """
    summarized = NotmuchTag.CONTEXT_SUMMARIZED.value
    loaded: list[EmailMessage] = []
    for snap in iter_irt_ancestors_filtered(leaf_inner):
        if snap.is_addressed_to_fsm_stage(FsmStage.REASONING):
            break
        if summarized in snap.tags:
            continue
        try:
            m = email_message_from_path(snap.path)
        except OSError as exc:
            log.warning(
                "unified_delta_load_path_skipped", path=str(snap.path), exc_msg=str(exc)
            )
            continue
        if not message_has_history(m):
            continue
        loaded.append(m)
    return _sort_email_messages_oldest_first(loaded)
