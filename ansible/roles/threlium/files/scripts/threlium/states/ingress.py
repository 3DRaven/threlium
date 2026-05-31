#!/usr/bin/env python3
"""ingress@localhost (INGRESS_ROUTER): правила SUBAGENT_TABLE §ingress_router.

`docs/INDEX.md` §8: hard-fail на нарушение FSM-инварианта,
graceful обработка только для «новый внешний тред» (Case 1). Стадии
не индексируют — `notmuch insert` делает fdm, см. `docs/INDEX.md` §1/§4.

Fail-fast матрица (`docs/INDEX.md` §8):

  * Case 1 (parent не виден в notmuch) — graceful: новый внешний тред
    идёт в ``enrich`` (единственный legal-вход в ``reasoning``,
    `docs/FSM.md §2.1`); orphan-notice префиксуется в distill envelope.
  * HITL — обход предков по IRT (1–N шагов) до From: cli_hitl_out →
    ``cli_resume``.
"""
from email.message import EmailMessage

from threlium.settings import ThreliumSettings
from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.fsm_emit_semantic import emit_transition_simple_step_preserving_payload
from threlium.ingress_distill import ingress_distill_llm
from threlium.logutil import logger
from threlium.mime_reform import (
    EnrichContentId,
    _make_inline_text_part,
    extract_plain_body,
    ingress_external_body_text,
    ingress_pipeline_email,
    require_unique_threading_rfc822_headers,
)
from threlium import nm
from threlium.types.ingress_hitl import (
    HitlParentWithIntent,
    HitlParentWithoutIntent,
    classify_hitl_parent_notmuch,
)
from threlium.types import (
    FsmStage,
    FsmTransitionPlainSubjectLine,
    IngressDistillEnvelope,
    IngressExternalBodyText,
    IngressRouterChildMsg,
    MailHeaderName,
    OrphanNoticePrefixLine,
    bridge_channel_from_email,
)
from threlium.types.content_score import ThreliumContentScoreWire

log = logger.bind(stage="ingress")

ORPHAN_NOTICE = (
    "[Threlium notice: this message replies to a thread we don't have in "
    "our union index (parent Message-ID not found). Treating it as a new "
    "external thread.]"
)


def _prefix_body_for_distill(
    full_body: str,
    prefix_text: str | None,
) -> str:
    p = OrphanNoticePrefixLine.parse(prefix_text).value if prefix_text else ""
    user_body = full_body.strip()
    if not p:
        return user_body
    return p + "\n\n" + user_body


def _emit_to_enrich(
    msg: EmailMessage, stage: FsmStage, *, orphan: bool = False, settings: ThreliumSettings,
) -> EmailMessage:
    body_vo = ingress_external_body_text(msg)
    orphan_notice = OrphanNoticePrefixLine.parse(ORPHAN_NOTICE) if orphan else None
    distill_body = _prefix_body_for_distill(
        body_vo.value,
        orphan_notice.value if orphan_notice else None,
    )
    envelope = IngressDistillEnvelope.from_email(
        msg,
        channel=bridge_channel_from_email(msg),
        full_body=IngressExternalBodyText.parse(distill_body),
        orphan_notice=orphan_notice,
    )
    result = ingress_distill_llm(envelope, msg, config=settings)
    out = emit_transition_simple_step_preserving_payload(
        msg,
        to_addr=FsmStage.ENRICH,
        from_stage=stage,
        settings=settings,
    )
    score = ThreliumContentScoreWire.from_score(settings.history.score_for(stage))
    for hp in result.parts:
        out.attach(
            _make_inline_text_part(
                EnrichContentId.from_history_body(hp.text),
                hp.text,
                score=score,
            )
        )
    return out


def _emit_to_cli_resume(
    msg: EmailMessage, stage: FsmStage, *, settings: ThreliumSettings,
) -> EmailMessage:
    msg = ingress_pipeline_email(msg)
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.CLI_RESUME,
        from_stage=stage,
        system=extract_plain_body(msg).strip(),
        subject_line=_preserved_subject(msg),
        settings=settings,
    )


def _preserved_subject(msg: EmailMessage) -> FsmTransitionPlainSubjectLine | None:
    """Сохранить исходный Subject входа (без ``Re:``-префикса билдера) для enrich-шаблона."""
    raw = msg.get(MailHeaderName.SUBJECT)
    return FsmTransitionPlainSubjectLine.parse(raw) if raw else None


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    require_unique_threading_rfc822_headers(msg)
    irt_wire = IngressRouterChildMsg.from_email(msg).in_reply_to
    if irt_wire is None:
        return _emit_to_enrich(msg, stage, settings=config)

    with nm.open_parent_message_for_in_reply_to(irt_wire) as parent_msg:
        if parent_msg is None:
            log.info("irt_parent_not_found_orphan")
            return _emit_to_enrich(msg, stage, orphan=True, settings=config)

        match classify_hitl_parent_notmuch(parent_msg):
            case HitlParentWithoutIntent():
                return _emit_to_enrich(msg, stage, settings=config)
            case HitlParentWithIntent():
                return _emit_to_cli_resume(msg, stage, settings=config)
