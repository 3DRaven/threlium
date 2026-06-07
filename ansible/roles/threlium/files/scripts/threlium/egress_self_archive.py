"""Запись факта отправки egress_* в стадию ``archive`` (см. docs/ARCHITECTURE §2.6)."""
from __future__ import annotations

from email.message import EmailMessage

import msgspec

import threlium.nm as nm
from threlium.fsm_emit import build_fsm_plain_to_stage
from threlium.settings import ThreliumSettings
from threlium.ingress_route_resolve import resolve_route_for_egress_fsm_from_email
from threlium.prompts import render_prompt
from threlium.types import (
    FsmPlainToStageSubjectLine,
    FsmStage,
    FsmTransitionPlainBody,
    IrtHashWire,
    MailHeaderName,
    PromptPath,
    RfcMessageIdWire,
)

_HDR = MailHeaderName


def channel_label_for_stage(stage: FsmStage) -> str:
    if stage is FsmStage.EGRESS_EMAIL:
        return "email"
    if stage is FsmStage.EGRESS_TELEGRAM:
        return "telegram"
    if stage is FsmStage.EGRESS_MATRIX:
        return "matrix"
    if stage is FsmStage.EGRESS_ISOMORPH:
        return "isomorph"
    raise ValueError(f"egress sent-record: unsupported stage {stage!r}")


def build_egress_sent_record_to_archive(
    task: EmailMessage,
    *,
    stage: FsmStage,
    sent_raw: str,
    glue_message_id_wire: RfcMessageIdWire | None = None,
    settings: ThreliumSettings,
) -> EmailMessage:
    """Собрать RFC822 на ``archive@localhost``; subject/body из Jinja, FSM-заголовки через билдер §5.

    ``glue_message_id_wire``: канонический MID, построенный из внешнего API-присвоенного
    идентификатора (Telegram ``message_id``, Matrix ``event_id``). Обеспечивает непрерывность
    IRT-цепочки при ответе пользователя на сообщение бота.
    """
    label = channel_label_for_stage(stage)
    ctx: dict[str, object] = {
        "egress_stage": stage.value,
        "channel_label": label,
        "sent_raw": sent_raw,
    }
    subject = render_prompt(PromptPath.EGRESS_SELF_ARCHIVE_SUBJECT, **ctx).strip()
    body = render_prompt(PromptPath.EGRESS_SELF_ARCHIVE_BODY, **ctx)
    if not subject:
        raise RuntimeError("egress sent-record: empty Subject after template render")
    if not str(body).strip():
        raise RuntimeError("egress sent-record: empty body after template render")

    out = build_fsm_plain_to_stage(
        task,
        to_addr=FsmStage.ARCHIVE,
        from_stage=stage,
        body=FsmTransitionPlainBody.parse(str(body)),
        subject_line=FsmPlainToStageSubjectLine.parse(subject),
        message_id_wire=glue_message_id_wire,
        settings=settings,
    )

    route = resolve_route_for_egress_fsm_from_email(task)
    out[_HDR.ROUTE] = route.route_wire.value

    return out


class ExistingEgressArchive(msgspec.Struct, frozen=True):
    """Существующий archive-record egress-стадии (обнаруженный по IRT = task MID)."""

    glue_message_id: RfcMessageIdWire


def find_existing_egress_archive(
    task: EmailMessage,
) -> ExistingEgressArchive | None:
    """Найти archive-record для egress task по ``Threliumirthash:`` (indexed ``X-Threlium-Irt-Hash``).

    Возвращает ``None`` если archive ещё не записан (первый запуск или crash до fdm).
    """
    task_mid = nm.require_inner_message_id_from_fsm_email(task)
    q = IrtHashWire.from_irt_header_value(
        task_mid.as_angle_bracket_header()
    ).as_notmuch_index_term()
    return _find_existing_egress_archive_by_query(q)


@nm.read_retry
def _find_existing_egress_archive_by_query(q: str) -> ExistingEgressArchive | None:
    """Открыть → быстро материализовать glue-MID в VO → закрыть; ``Message`` не покидает сеанс.

    ``@nm.read_retry`` переоткрывает БД при discard'е ревизии под конкурентной записью."""
    with nm.notmuch_database(write=False) as db:
        msg = nm.first_message_for_query(db, q, newest_first=True)
        if msg is None:
            return None
        raw_mid = str(msg.messageid)
        return ExistingEgressArchive(
            glue_message_id=RfcMessageIdWire.parse(f"<{raw_mid}>"),
        )
