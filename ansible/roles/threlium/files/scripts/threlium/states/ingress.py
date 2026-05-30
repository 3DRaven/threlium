#!/usr/bin/env python3
"""ingress@localhost (INGRESS_ROUTER): правила SUBAGENT_TABLE §ingress_router.

`docs/INDEX.md` §8: hard-fail на нарушение FSM-инварианта,
graceful обработка только для «новый внешний тред» (Case 1). Стадии
не индексируют — `notmuch insert` делает fdm, см. `docs/INDEX.md` §1/§4.

Fail-fast матрица (`docs/INDEX.md` §8):

  * Case 1 (parent не виден в notmuch) — graceful: новый внешний тред
    идёт в ``enrich`` (единственный legal-вход в ``reasoning``,
    `docs/FSM.md §2.1`); orphan-notice префиксуется в начало body.
  * HITL — обход предков по IRT (1–N шагов) до From: cli_hitl_out →
    ``cli_resume``.
"""
from email.message import EmailMessage

from threlium.settings import ThreliumSettings
from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.logutil import logger
from threlium.mime_reform import (
    extract_plain_body,
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
    IngressRouterChildMsg,
    MailHeaderName,
    OrphanNoticePrefixLine,
)

log = logger.bind(stage="ingress")

ORPHAN_NOTICE = (
    "[Threlium notice: this message replies to a thread we don't have in "
    "our union index (parent Message-ID not found). Treating it as a new "
    "external thread.]"
)


def _prefix_plain_body_in_message(
    incoming: EmailMessage,
    prefix_text: str,
    *,
    stage: FsmStage,
) -> EmailMessage:
    """Префикс в начало plain-body сообщения; результат — text/plain; charset=utf-8.

    `docs/INDEX.md` §8 Case 1: orphan-notice вставляется первой строкой тела
    для LLM. Для multipart — берётся первый ``text/plain`` через
    :func:`extract_plain_body`. Заголовки переносятся целиком, кроме MIME
    Content-* (тело пересобирается как ``text/plain``).
    """
    p = OrphanNoticePrefixLine.parse(prefix_text).value
    user_body = extract_plain_body(incoming).strip()
    if not p:
        return incoming
    new_body = p + "\n\n" + user_body
    out = EmailMessage()
    skip = frozenset(
        {
            "content-type",
            "content-transfer-encoding",
            "mime-version",
            "content-disposition",
        }
    )
    for k, v in incoming.items():
        if k.lower() in skip:
            continue
        if k in out:
            out.add_header(k, v)
        else:
            out[k] = v
    out.set_content(new_body, subtype="plain", charset="utf-8")
    log.info("prefixed_notice")
    return out


def _preserved_subject(msg: EmailMessage) -> FsmTransitionPlainSubjectLine | None:
    """Сохранить исходный Subject входа (без ``Re:``-префикса билдера) для enrich-шаблона."""
    raw = msg.get(MailHeaderName.SUBJECT)
    return FsmTransitionPlainSubjectLine.parse(raw) if raw else None


def _emit_to_enrich(
    msg: EmailMessage, stage: FsmStage, *, orphan: bool = False, settings: ThreliumSettings,
) -> EmailMessage:
    if orphan:
        msg = _prefix_plain_body_in_message(msg, ORPHAN_NOTICE, stage=stage)
    msg = ingress_pipeline_email(msg)
    # Ход пользователя входит в долгую память как <history>-часть (origin поставит
    # enrich_fast), а тело-команда для enrich едет в <system>. enrich читает текст
    # через get_body (часть text/plain; inline).
    user_body = extract_plain_body(msg).strip()
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.ENRICH,
        from_stage=stage,
        history=user_body,
        system=user_body,
        subject_line=_preserved_subject(msg),
        settings=settings,
    )


def _emit_to_cli_resume(
    msg: EmailMessage, stage: FsmStage, *, settings: ThreliumSettings,
) -> EmailMessage:
    msg = ingress_pipeline_email(msg)
    # HITL-ответ пользователя — управляющий сигнал (yes/no): только <system> (cli_resume
    # читает его через system_part_text). Содержательная история возникнет ниже по потоку.
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.CLI_RESUME,
        from_stage=stage,
        system=extract_plain_body(msg).strip(),
        subject_line=_preserved_subject(msg),
        settings=settings,
    )


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
