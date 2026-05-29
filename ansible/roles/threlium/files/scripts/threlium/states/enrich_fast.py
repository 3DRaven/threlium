"""enrich_fast@localhost → reasoning@localhost.

Быстрый цикл обратной связи: берёт предыдущий enriched-контекст ``E_prev``
(multipart/mixed с MIME-частями по Content-ID), пересобирает ``<response-state>``
из CRDT и **аддитивно** дописывает relay-части входящего письма (с их
оригинальными уникальными Content-ID) — возвращает в reasoning без повторного RAG.

Stage-agnostic: enrich_fast не знает, какая стадия прислала relay-часть; повторные
хопы одной стадии накапливаются (уникальный CID на хоп), а не затирают друг друга.
"""
from __future__ import annotations

from email.message import EmailMessage

from threlium.enrich_context import trim_context_text
from threlium.fsm_emit import emit_transition_preserving_payload
from threlium.fsm_emit_semantic import managed_patch_simple_fsm_step
from threlium.irt_chain import iter_in_reply_to_ancestors_from_inner_id
from threlium.logutil import logger
from threlium.mime_reform import (
    email_message_from_path,
    splice_e_prev_with_incoming_relay,
)
from threlium.response.collect import collect_ops
from threlium.response.state_summary import build_state_summary
from threlium.settings import ThreliumSettings
from threlium.task import build_task_state_summary, collect_task_ops, reduce_task_ops
from threlium.types import (
    FsmStage,
    HopBudgetLine,
    MailHeaderName,
    NotmuchMessageIdInner,
    RfcMessageIdWire,
)

log = logger.bind(stage="enrich_fast")


def _find_e_prev(start_inner: NotmuchMessageIdInner) -> EmailMessage | None:
    """Найти ``E_prev``: первый предок, адресованный reasoning@localhost."""
    chain = iter_in_reply_to_ancestors_from_inner_id(start_inner)
    for snap in chain:
        if snap.is_addressed_to_fsm_stage(FsmStage.REASONING):
            return email_message_from_path(snap.path)
    return None


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID.value)
    inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    if inner is None:
        raise RuntimeError("enrich_fast: no Message-ID on incoming message")

    e_prev = _find_e_prev(inner)
    if e_prev is None:
        raise RuntimeError(
            "enrich_fast: could not find previous enriched message "
            "(addressed to reasoning@localhost) in IRT chain"
        )

    ops = collect_ops(inner)
    summary = build_state_summary(ops)

    limit = config.enrich.context_max_chars
    trimmed_summary = trim_context_text(summary, limit)

    hop_line = HopBudgetLine.parse(msg.get(MailHeaderName.HOP_BUDGET.value))
    task_ledger = reduce_task_ops(collect_task_ops(inner, hop_line))
    task_state_text = trim_context_text(build_task_state_summary(task_ledger), limit)

    spliced = splice_e_prev_with_incoming_relay(
        e_prev, msg, response_state_text=trimmed_summary, task_state_text=task_state_text,
    )

    log.info(
        "spliced_relay_parts",
        ops_count=len(ops),
        response_state_chars=len(trimmed_summary),
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
