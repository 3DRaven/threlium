"""HITL-ветвление родителя для ingress: обход предков по IRT до cli_hitl_out (§2 плана).

Единственная фабрика union ``HitlParentRouting``.
Ответ пользователя ссылается на egress_* (не на cli_hitl_out), поэтому нужен
короткий подъём по In-Reply-To (типично 1–3 шага), пока не найдён
``From: cli_hitl_out@localhost`` или не встречен reasoning/ingress (не-HITL).
"""
from __future__ import annotations

import msgspec
import notmuch2  # pyright: ignore[reportMissingImports]

from .fsm_stage import FsmStage
from threlium.mail_header_names import MailHeaderName
from .nm_addressed import notmuch_message_sent_from_fsm_stage
from .notmuch_message_id import NotmuchMessageIdInner
from .rfc import RfcInReplyToWire

_MAX_HITL_WALK_DEPTH = 5

_NON_HITL_STAGES = frozenset({
    FsmStage.REASONING,
    FsmStage.INGRESS,
    FsmStage.ENRICH,
    FsmStage.SUBAGENT_INTENT,
    FsmStage.SUBAGENT_END,
})


class HitlParentWithoutIntent(msgspec.Struct, frozen=True):
    """Родитель без HITL-маркера → enrich."""


class HitlParentWithIntent(msgspec.Struct, frozen=True):
    """HITL-маркер: обход IRT-предков нашёл cli_hitl_out → маршрутизация в cli_resume."""


HitlParentRouting = HitlParentWithoutIntent | HitlParentWithIntent


def classify_hitl_parent_notmuch(
    db: notmuch2.Database,
    parent_nm_msg: notmuch2.Message,
) -> HitlParentRouting:
    """Детекция HITL: обход предков по IRT от parent (1-N шагов) до cli_hitl_out.

    Вызывается из ``ingress.main`` под уже открытым READ ``db`` (один сеанс с lookup родителя;
    обёртка ``nm.read_retry`` на стороне ingress переоткроет при discard'е ревизии). ``parent_nm_msg``
    и ходовые ``notmuch2.Message`` валидны только в этом ``db`` — наружу возвращается плоский
    ``HitlParentRouting`` VO, не ``Message``.

    Алгоритм:
        1. Начать с parent_nm_msg.
        2. Если From: cli_hitl_out → HITL → cli_resume.
        3. Если From: reasoning/ingress/enrich/subagent_* → не HITL.
        4. Иначе (egress_router, egress_*) → подняться по IRT на шаг вверх.
        5. Лимит MAX_HITL_WALK_DEPTH шагов.
    """
    from threlium import nm as _nm

    current = parent_nm_msg
    for _ in range(_MAX_HITL_WALK_DEPTH):
        if notmuch_message_sent_from_fsm_stage(current, FsmStage.CLI_HITL_OUT):
            return HitlParentWithIntent()

        for stage in _NON_HITL_STAGES:
            if notmuch_message_sent_from_fsm_stage(current, stage):
                return HitlParentWithoutIntent()

        irt_raw = _nm.header_field_optional(current, MailHeaderName.IN_REPLY_TO)
        irt_w = RfcInReplyToWire.parse_present_optional(
            None if irt_raw is None else str(irt_raw)
        )
        if irt_w is None:
            return HitlParentWithoutIntent()

        parent_inner = NotmuchMessageIdInner.from_optional_raw(irt_w.value)
        if parent_inner is None:
            return HitlParentWithoutIntent()

        next_msg = _nm.first_notmuch_message_for_inner_id(db, parent_inner)
        if next_msg is None:
            return HitlParentWithoutIntent()
        current = next_msg

    return HitlParentWithoutIntent()
