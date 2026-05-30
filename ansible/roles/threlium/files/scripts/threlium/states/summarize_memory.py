#!/usr/bin/env python3
"""summarize_memory@localhost: стадия-хранитель итога суммаризации.

Аналог thread_memory — ничего не делает кроме возврата в enrich.
Письмо To: summarize_memory@ несёт ``<history>``-часть (сводку от summarize_context)
и остаётся в Maildir, поэтому попадает в ``<unified-mail-context>`` по предикату
``message_has_history`` (оригиналы при этом помечены ``context_summarized`` и выпадают).
"""
from __future__ import annotations

from email.message import EmailMessage

from threlium.fsm_emit import build_fsm_plain_to_stage
from threlium.logutil import logger
from threlium.mime_reform import email_message_from_path, extract_plain_body
from threlium.thread_context_filter import iter_irt_ancestors_filtered
from threlium.settings import ThreliumSettings
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    MailHeaderName,
    NotmuchMessageIdInner,
    RfcMessageIdWire,
)

log = logger.bind(stage="summarize_memory")


def _find_enrich_trigger_body(inner: NotmuchMessageIdInner) -> str:
    """Walk own-frame IRT chain to find the original enrich-trigger body.

    Фрейм-локальный обход: enrich-триггеры вложенных субагентов игнорируются.
    """
    for snap in iter_irt_ancestors_filtered(inner):
        if snap.is_addressed_to_fsm_stage(FsmStage.ENRICH):
            ancestor = email_message_from_path(snap.path)
            return extract_plain_body(ancestor).strip()
    return ""


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID.value)
    inner = NotmuchMessageIdInner.from_optional_wire(mid_w)

    enrich_body = _find_enrich_trigger_body(inner) if inner else ""
    if not enrich_body:
        enrich_body = extract_plain_body(msg).strip()
        log.warning("enrich_trigger_not_found_in_irt")

    return build_fsm_plain_to_stage(
        msg,
        to_addr=FsmStage.ENRICH,
        from_stage=stage,
        body=FsmTransitionPlainBody.parse(enrich_body),
        settings=config,
    )
