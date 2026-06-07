#!/usr/bin/env python3
"""ingress@localhost (INGRESS_ROUTER): bridge + HITL router + distill gateway.

``docs/INDEX.md`` §8:
  * Case 1 (parent не виден) — orphan-notice + distill → enrich.
  * HITL — IRT до ``From: cli_hitl_out`` → ``cli_resume``.
  * Только ``From:`` bridge (email/telegram/matrix@localhost); internal стадии → enrich напрямую.
"""
from email.message import EmailMessage

from threlium.bridges.ingress_from import require_bridge_from_email
from threlium.ingress_bridge_user_query import (
    assert_bridge_input_has_no_user_query,
    enrich_user_query_from_bridge_system,
)
from threlium.settings import ThreliumSettings
from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.fsm_emit_semantic import emit_bridge_distill_to_enrich
from threlium.ingress_distill import ingress_distill_llm
from threlium.logutil import logger
from threlium.mime_reform import (
    require_unique_threading_rfc822_headers,
    system_part_text,
)
from threlium import nm
from threlium.types.ingress_hitl import (
    HitlParentRouting,
    HitlParentWithIntent,
    HitlParentWithoutIntent,
    classify_hitl_parent_notmuch,
)
from threlium.types import (
    EnrichUserQueryText,
    FsmStage,
    FsmTransitionPlainSubjectLine,
    IngressDistillEnvelope,
    IngressExternalBodyText,
    IngressRouterChildMsg,
    MailHeaderName,
    OrphanNoticePrefixLine,
    RfcInReplyToWire,
    RfcSubjectWire,
    bridge_channel_from_email,
)

log = logger.bind(stage="ingress")

ORPHAN_NOTICE = (
    "[Threlium notice: this message replies to a thread we don't have in "
    "our union index (parent Message-ID not found). Treating it as a new "
    "external thread.]"
)


def _prefix_body_for_distill(
    user_query: EnrichUserQueryText,
    prefix_text: str | None,
) -> EnrichUserQueryText:
    p = OrphanNoticePrefixLine.parse(prefix_text).value if prefix_text else ""
    user_body = user_query.value.strip()
    if not p:
        return user_query
    return EnrichUserQueryText.require_value(
        name="distill body",
        raw=p + "\n\n" + user_body,
    )


def _emit_bridge_distill_to_enrich(
    msg: EmailMessage, stage: FsmStage, *, orphan: bool = False, config: ThreliumSettings,
) -> EmailMessage:
    require_bridge_from_email(msg)
    assert_bridge_input_has_no_user_query(msg)
    user_query = enrich_user_query_from_bridge_system(msg)
    orphan_notice = OrphanNoticePrefixLine.parse(ORPHAN_NOTICE) if orphan else None
    distill_body = _prefix_body_for_distill(
        user_query,
        orphan_notice.value if orphan_notice else None,
    )
    envelope = IngressDistillEnvelope.from_email(
        msg,
        channel=bridge_channel_from_email(msg),
        full_body=IngressExternalBodyText.parse(distill_body.value),
        orphan_notice=orphan_notice,
    )
    result = ingress_distill_llm(envelope, msg, config=config)
    return emit_bridge_distill_to_enrich(
        msg,
        stage,
        user_query=distill_body,
        original_user_message=user_query,
        settings=config,
        distill_parts=result.parts,
    )


def _emit_to_cli_resume(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings,
) -> EmailMessage:
    require_bridge_from_email(msg)
    # Мост уже кладёт тело пользователя в <system> (контракт проекта; email _build_canonical /
    # build_bridge_ingress_email). Читаем его напрямую — без сплющивания в plain.
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.CLI_RESUME,
        from_stage=stage,
        system=system_part_text(msg).strip(),
        subject_line=_preserved_subject(msg),
        settings=config,
    )


def _preserved_subject(msg: EmailMessage) -> FsmTransitionPlainSubjectLine | None:
    subj = RfcSubjectWire.parse_present_from_email(msg, MailHeaderName.SUBJECT)
    if subj is None:
        return None
    return FsmTransitionPlainSubjectLine.parse(subj.value)


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    require_unique_threading_rfc822_headers(msg)
    require_bridge_from_email(msg)
    assert_bridge_input_has_no_user_query(msg)
    irt_wire = IngressRouterChildMsg.from_email(msg).in_reply_to
    if irt_wire is None:
        return _emit_bridge_distill_to_enrich(msg, stage, config=config)

    # Резолв родителя по IRT + HITL-классификация — в ОДНОМ коротком READ-сеансе, материализуем плоский
    # HitlParentRouting (None = orphan); ``notmuch2.Message`` не покидает сеанс. LLM-эмиссия (distill /
    # cli_resume) — ВНЕ сеанса (не идемпотентна). ``docs/TYPES.md`` «границы API».
    routing = _classify_ingress_parent_routing(irt_wire)
    if routing is None:
        log.info("irt_parent_not_found_orphan")
        return _emit_bridge_distill_to_enrich(msg, stage, orphan=True, config=config)

    match routing:
        case HitlParentWithoutIntent():
            return _emit_bridge_distill_to_enrich(msg, stage, config=config)
        case HitlParentWithIntent():
            return _emit_to_cli_resume(msg, stage, config=config)


@nm.read_retry
def _classify_ingress_parent_routing(
    irt_wire: RfcInReplyToWire,
) -> HitlParentRouting | None:
    """Открыть БД, найти родителя по IRT и классифицировать HITL → плоский VO (``None`` = orphan).

    ``@nm.read_retry``: при discard'е ревизии под конкурентной записью сеанс (lookup + обход предков)
    переоткрывается и материализуется заново; наружу — только ``HitlParentRouting`` / ``None``."""
    with nm.notmuch_database(write=False) as db:
        parent_msg = nm.parent_message_for_in_reply_in_db(db, irt_wire)
        if parent_msg is None:
            return None
        return classify_hitl_parent_notmuch(db, parent_msg)
