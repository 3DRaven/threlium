"""Классификатор глубины субагента по IRT-цепочке (§1 плана рефакторинга).

Обход строго вверх по ``In-Reply-To`` от текущего узла.
``subagent_end`` → depth -= 1, ``subagent_intent`` → depth += 1.
"""
from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage

from threlium import nm
from threlium.irt_chain import IrtSubagentMarker, iter_in_reply_to_ancestors_from_inner_id
from threlium.types import (
    HopBudgetLine,
    MailHeaderName,
    NotmuchMessageIdInner,
    RfcMessageIdWire,
)


@dataclass(frozen=True)
class SubagentDepthResult:
    """depth > 0 → внутри незакрытого субагента; depth == 0 → корень."""
    depth: int


def classify_subagent_depth_from_inner(
    start_inner: NotmuchMessageIdInner,
) -> SubagentDepthResult:
    """Баланс depth по IRT от ``start_inner`` до корня."""
    depth = 0
    for snap in iter_in_reply_to_ancestors_from_inner_id(start_inner):
        marker = snap.subagent_marker()
        if marker is IrtSubagentMarker.SUBAGENT_END:
            depth -= 1
        elif marker is IrtSubagentMarker.SUBAGENT_INTENT:
            depth += 1
        if depth > 0:
            return SubagentDepthResult(depth=depth)
    return SubagentDepthResult(depth=depth)


def classify_subagent_depth_from_email(msg: EmailMessage) -> SubagentDepthResult:
    """Баланс depth от ``Message-ID`` конверта."""
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID)
    inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    if inner is None:
        raise RuntimeError(
            "FSM-инвариант: classify_subagent_depth требует непустой Message-ID"
        )
    return classify_subagent_depth_from_inner(inner)


@dataclass(frozen=True)
class SubagentIntentAncestorHeaders:
    """Hop предка ``subagent_intent``, снятый и распарсенный внутри DB-сеанса."""

    hop: HopBudgetLine


def find_matching_subagent_intent_ancestor(
    start_inner: NotmuchMessageIdInner,
) -> SubagentIntentAncestorHeaders:
    """Найти ближайший незакрытый ``subagent_intent`` по IRT от ``start_inner``.

    Возвращает hop с предка непосредственно перед intent в цепочке
    (обычно enrich/ingress родителя до делегирования), уже как VO.
    """
    depth = 0
    for snap in iter_in_reply_to_ancestors_from_inner_id(start_inner):
        marker = snap.subagent_marker()
        if marker is IrtSubagentMarker.SUBAGENT_END:
            depth -= 1
        elif marker is IrtSubagentMarker.SUBAGENT_INTENT:
            depth += 1
            if depth == 1:
                parent_inner = snap.in_reply_to_inner()
                if parent_inner is None:
                    raise RuntimeError(
                        "FSM-инвариант: subagent_intent без In-Reply-To предка"
                    )
                return _parent_hop_headers(parent_inner)
    raise RuntimeError(
        "FSM-инвариант: не найден незакрытый subagent_intent "
        f"в IRT-цепочке от {start_inner.as_angle_bracket_header()}"
    )


def hop_from_intent_parent(ancestor: SubagentIntentAncestorHeaders) -> HopBudgetLine:
    """Hop 1-в-1 с предка перед subagent_intent."""
    return ancestor.hop


@nm.read_retry
def _parent_hop_headers(inner: NotmuchMessageIdInner) -> SubagentIntentAncestorHeaders:
    """Снять hop с письма ``inner``: открыть БД, быстро материализовать VO, закрыть.

    ``@nm.read_retry`` — ``notmuch2.Message`` не покидает сеанс; при discard'е ревизии под
    конкурентной записью сеанс переоткрывается (см. ``docs/TYPES.md`` «границы API»)."""
    with nm.notmuch_database(write=False) as db:
        msg = nm.first_notmuch_message_for_inner_id(db, inner)
        if msg is None:
            raise RuntimeError(
                "FSM-инвариант: предок subagent_intent не найден в индексе "
                f"(Message-ID={inner.as_angle_bracket_header()})"
            )
        hop = HopBudgetLine.parse(nm.header_field_optional(msg, MailHeaderName.HOP_BUDGET))
        return SubagentIntentAncestorHeaders(hop=hop)
