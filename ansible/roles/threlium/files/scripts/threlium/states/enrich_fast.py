"""enrich_fast@localhost → reasoning@localhost.

Быстрый цикл обратной связи: берёт предыдущий enriched-контекст ``E_prev``
(multipart/mixed с MIME-частями по Content-ID), пересобирает ``<response-state>``
и ``<task-state>`` из CRDT и **аддитивно** дописывает ``<history>`` и ``<system>`` из
окна-дельты (всё, что появилось с прошлого ``To: reasoning`` до листа; старые
``@system`` из ``E_prev`` не копируются) — возвращает в reasoning без повторного RAG.

Контент-адресные CID ``<{sha256(body)}@history>`` дают автоматический дедуп по телу:
оригинал и его relay-копии схлопываются в одну часть. Origin (стадия-источник) — единственная
стадия, теряющая контекст между ходами, поэтому именно ``enrich_fast`` штампует
``X-Threlium-Origin`` на каждой ``<history>``-части из её конвертного ``From:``; ``score``
уже проставлен стадией-источником. Stage-agnostic: фильтр по наличию ``<history>``, а не
по конкретным ``To:``-стадиям.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator
from email.message import EmailMessage

from threlium.enrich_context import collect_unified_delta_msgs
from threlium.formal_reason_gate import assert_formal_reason_relay_after_splice
from threlium.fsm_emit import emit_transition_preserving_payload
from threlium.fsm_emit_semantic import managed_patch_simple_fsm_step
from threlium.logutil import logger
from threlium.mail import email_message_from_path
from threlium.mime_reform import (
    EnrichContentId,
    iter_history_parts,
    iter_system_parts,
    splice_e_prev_with_history,
)
from threlium.ledger_context_parts import crdt_ledger_state, trimmed_crdt_state_texts
from threlium.nm import require_fsm_message_id
from threlium.settings import ThreliumSettings
from threlium.thread_context_filter import iter_irt_ancestors_filtered
from threlium.types import (
    FsmStage,
    MailHeaderName,
    NotmuchMessageIdInner,
)

log = logger.bind(stage="enrich_fast")

_HDR = MailHeaderName


def _find_e_prev(start_inner: NotmuchMessageIdInner) -> EmailMessage | None:
    """Найти ``E_prev``: первый предок своего фрейма, адресованный reasoning@localhost.

    Фрейм-локальный обход: reasoning вложенных субагентов не подхватывается.
    """
    for snap in iter_irt_ancestors_filtered(start_inner):
        if snap.is_addressed_to_fsm_stage(FsmStage.REASONING):
            return email_message_from_path(snap.path)
    return None


def _collect_delta_parts(
    delta_msgs: list[EmailMessage],
    iter_fn: Callable[[EmailMessage], Iterator[tuple[EnrichContentId, EmailMessage]]],
) -> list[tuple[EnrichContentId, EmailMessage]]:
    """Части окна-дельты (history или system) со штампом ``X-Threlium-Origin``.

    Origin ставится один раз на части без него: стадии-источники проставляют только
    ``score``, а ``origin`` восстанавливает enrich_fast по конвертному ``From:`` письма-носителя.
    """
    out: list[tuple[EnrichContentId, EmailMessage]] = []
    for dm in delta_msgs:
        origin = FsmStage.try_from_mailbox(dm.get(_HDR.FROM.value))
        for cid, part in iter_fn(dm):
            if origin is not None and not part.get(_HDR.ORIGIN.value):
                part[_HDR.ORIGIN.value] = origin.rfc822_mailbox
            out.append((cid, part))
    return out


def _collect_delta_history_parts(
    delta_msgs: list[EmailMessage],
) -> list[tuple[EnrichContentId, EmailMessage]]:
    return _collect_delta_parts(delta_msgs, iter_history_parts)


def _collect_delta_system_parts(
    delta_msgs: list[EmailMessage],
) -> list[tuple[EnrichContentId, EmailMessage]]:
    return _collect_delta_parts(delta_msgs, iter_system_parts)


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w, inner = require_fsm_message_id(msg, "enrich_fast")

    e_prev = _find_e_prev(inner)
    if e_prev is None:
        raise RuntimeError(
            "enrich_fast: could not find previous enriched message "
            "(addressed to reasoning@localhost) in IRT chain"
        )

    limit = config.enrich.context_max_chars
    trimmed_summary, task_state_text = trimmed_crdt_state_texts(inner, limit=limit)
    ops = crdt_ledger_state(inner).response_ops

    delta_msgs = collect_unified_delta_msgs(inner)
    history_parts = _collect_delta_history_parts(delta_msgs)
    system_parts = _collect_delta_system_parts(delta_msgs)

    spliced = splice_e_prev_with_history(
        e_prev,
        response_state_text=trimmed_summary,
        task_state_text=task_state_text,
        history_parts=history_parts,
        system_parts=system_parts,
    )

    assert_formal_reason_relay_after_splice(
        spliced.message,
        delta_msgs=delta_msgs,
        message_id=mid_w.value if mid_w else None,
    )

    log.info(
        "spliced_history_parts",
        ops_count=len(ops),
        response_state_chars=len(trimmed_summary),
        delta_msgs=len(delta_msgs),
        delta_history_parts=len(history_parts),
        delta_system_parts=len(system_parts),
        appended_cids=[cid.value for cid in spliced.appended] or None,
        skipped_duplicate_cids=[cid.value for cid in spliced.skipped] or None,
        message_id=mid_w.value if mid_w else None,
    )

    return emit_transition_preserving_payload(
        spliced.message,
        to_addr=FsmStage.REASONING,
        from_stage=stage,
        managed_headers=managed_patch_simple_fsm_step(msg, config),
    )
