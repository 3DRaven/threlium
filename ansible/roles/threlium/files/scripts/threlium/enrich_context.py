"""Сбор ``unified_messages`` для стадии enrich (notmuch + ``EmailMessage``)."""
from __future__ import annotations

import itertools
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path

from threlium import nm
from threlium.context_budget import SERVICE_TRANSITION_STAGES, to_stage_in_unified_role
from threlium.irt_chain import IrtAncestorSnapshot
from threlium.logutil import logger
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.thread_context_filter import iter_irt_ancestors_filtered
from threlium.mime_reform import email_message_from_path
from threlium.types import (
    FsmStage,
    MailHeaderName,
    NotmuchMessageIdInner,
    NotmuchQueryConnective,
    NotmuchQueryField,
    NotmuchTag,
    PromptPath,
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

    # Один проход лист→корень по снимкам IRT: роль и дедуп считаются на снимках
    # (есть To и inner Message-ID), summarized — по тегам снимка, поэтому
    # email_message_from_path вызывается только для писем, реально уходящих в
    # unified (а не для всего хвоста с последующим отбросом). Порядок неважен —
    # итог пересортируется по дате ниже.
    #
    # Ближайший к листу To: ingress — текущий ход пользователя, дублирующий
    # <user-message> (релей ingress→enrich рендерится в user-message-часть);
    # пропускаем его ровно один раз, прошлые ходы (старшие To: ingress) остаются.
    _summarized_tag = NotmuchTag.CONTEXT_SUMMARIZED.value
    by_mid: dict[str, IrtAncestorSnapshot] = {}
    current_user_skipped = False
    for snap in tail_snaps:
        if _summarized_tag in snap.tags:
            continue
        stage = snap.to_fsm_stage()
        if not to_stage_in_unified_role(stage):
            continue
        if not current_user_skipped and stage is FsmStage.INGRESS:
            current_user_skipped = True
            continue
        if str(snap.path.resolve()) in memory_path_keys:
            continue
        by_mid.setdefault(snap.message_id_inner.value, snap)

    loaded: list[EmailMessage] = []
    for snap in by_mid.values():
        try:
            loaded.append(email_message_from_path(snap.path))
        except OSError as exc:
            log.warning("unified_load_path_skipped", path=str(snap.path), exc_msg=str(exc))
            continue

    return UnifiedEmailContext(
        all_messages=_sort_email_messages_oldest_first(loaded),
        thread_memory_msgs=_sort_email_messages_oldest_first(_load_paths(tm_paths)),
        global_memory_msgs=_sort_email_messages_oldest_first(_load_paths(gm_paths)),
    )


def collect_unified_delta_msgs(leaf_inner: NotmuchMessageIdInner) -> list[EmailMessage]:
    """unified-role письма, появившиеся с прошлого ``To: reasoning`` (E_prev) до листа.

    Обход IRT лист→корень (с изоляцией субагентов через
    :func:`iter_irt_ancestors_filtered`) обрывается на ближайшем ``To: reasoning`` —
    это E_prev (в multi-cycle — выход прошлого ``enrich_fast``). Всё строго новее этой
    границы и в unified-роли (:func:`to_stage_in_unified_role`, без
    ``tag:context_summarized``) идёт в дельту. По структуре IRT там нет «старых»
    писем, поэтому MID-дедуп не нужен; прошлые циклы отрезаны watermark'ом.

    Stage-agnostic: не зависит от того, сколько и какие стадии стоят перед
    ``enrich_fast`` — фильтр по роли, а не по конкретным стадиям. Current-user-skip
    из :func:`build_unified_email_messages` здесь намеренно не применяется: дельта не
    пересобирает ``<user-message>``, поэтому ``ingress`` (USER_INPUT) уместен в ней.
    """
    summarized = NotmuchTag.CONTEXT_SUMMARIZED.value
    snaps: list[IrtAncestorSnapshot] = []
    for snap in iter_irt_ancestors_filtered(leaf_inner):
        if snap.is_addressed_to_fsm_stage(FsmStage.REASONING):
            break
        if summarized in snap.tags:
            continue
        if not to_stage_in_unified_role(snap.to_fsm_stage()):
            continue
        snaps.append(snap)

    loaded: list[EmailMessage] = []
    for snap in snaps:
        try:
            loaded.append(email_message_from_path(snap.path))
        except OSError as exc:
            log.warning(
                "unified_delta_load_path_skipped", path=str(snap.path), exc_msg=str(exc)
            )
            continue
    return _sort_email_messages_oldest_first(loaded)


def render_unified_delta_text(
    msgs: list[EmailMessage], *, settings: ThreliumSettings
) -> str:
    """Рендер дельты через ``lightrag/mail_context.j2`` (полное тело), тримминг по лимиту."""
    if not msgs:
        return ""
    raw = render_prompt(
        PromptPath.LIGHTRAG_MAIL_CONTEXT,
        messages=msgs,
        tier_assignments={},
        tier_assignments_types={},
        preview_chars=settings.enrich.tier_preview_chars,
        total_messages=len(msgs),
        service_stage_mailboxes=[s.rfc822_mailbox for s in SERVICE_TRANSITION_STAGES],
    ).strip()
    return trim_context_text(raw, settings.enrich.context_max_chars)
