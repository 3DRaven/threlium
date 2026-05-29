"""Цепочка предков только по ``In-Reply-To`` (лист → корень).

Общий примитив для :mod:`threlium.ingress_route_resolve` и enrich-контекста.

**Материализация:** :func:`iter_in_reply_to_ancestors_from_inner_id` возвращает
``list[IrtAncestorSnapshot]`` — иммутабельные снимки, снятые под одним коротким
read-сеансом notmuch. Курсор Xapian закрывается до начала тяжёлой бизнес-логики;
``notmuch2.Message`` не утекает за пределы ``with notmuch_database``.
"""
from __future__ import annotations

from dataclasses import dataclass
from email.utils import getaddresses
from pathlib import Path

import notmuch2  # pyright: ignore[reportMissingImports]

from threlium import nm
from threlium.types import (
    FsmStage,
    HopBudgetLine,
    IngressRouteB62Wire,
    MailHeaderName,
    NotmuchMessageIdInner,
    RfcFromWire,
    RfcInReplyToWire,
    RfcReferencesWire,
    RfcSubjectWire,
    RfcToWire,
)


@dataclass(frozen=True)
class IrtAncestorSnapshot:
    """Иммутабельный снимок ``notmuch2.Message`` из обхода IRT-цепочки.

    Валиден после закрытия ``notmuch_database`` (все данные скопированы).
    """
    message_id_inner: NotmuchMessageIdInner
    path: Path
    tags: frozenset[str]
    header_from: RfcFromWire | None
    header_to: RfcToWire | None
    header_route: IngressRouteB62Wire | None
    header_references: RfcReferencesWire | None
    header_in_reply_to: RfcInReplyToWire | None
    header_subject: RfcSubjectWire | None
    header_hop_budget: HopBudgetLine | None

    def hop_stack_depth(self) -> int:
        """Глубина hop-стека = число токенов ``X-Threlium-Hop-Budget`` (каждый subagent push добавляет токен).

        Корневой фрейм треда — глубина 1; отсутствие/пустой заголовок трактуется как 1
        (корень). Используется для изоляции task-ledger субагента по фрейму.
        """
        if self.header_hop_budget is None:
            return 1
        parts = self.header_hop_budget.value.split()
        return len(parts) if parts else 1

    def is_sent_from_fsm_stage(self, stage: FsmStage) -> bool:
        """Аналог ``nm_addressed.notmuch_message_sent_from_fsm_stage`` на снимке."""
        if self.header_from is None:
            return False
        want = stage.rfc822_mailbox.lower()
        for _, addr in getaddresses([self.header_from.value]):
            if addr and addr.strip().lower() == want:
                return True
        return False

    def is_addressed_to_fsm_stage(self, stage: FsmStage) -> bool:
        """Аналог ``nm_addressed.notmuch_message_addressed_to_fsm_stage`` на снимке."""
        if self.header_to is None:
            return False
        want = stage.rfc822_mailbox.lower()
        for _, addr in getaddresses([self.header_to.value]):
            if addr and addr.strip().lower() == want:
                return True
        return False

    def in_reply_to_inner(self) -> NotmuchMessageIdInner | None:
        """Распарсенный inner Message-ID из ``In-Reply-To`` (или ``None``)."""
        return NotmuchMessageIdInner.from_optional_raw(
            self.header_in_reply_to.value if self.header_in_reply_to is not None else None
        )

    def to_fsm_stage(self) -> FsmStage | None:
        """Стадия из ``To:`` снимка (``None`` если не ровно одна FSM-стадия @localhost)."""
        return FsmStage.try_from_to_header_value(
            self.header_to.value if self.header_to is not None else None
        )


def _snapshot_from_nm_message(nm_msg: notmuch2.Message, mid: NotmuchMessageIdInner) -> IrtAncestorSnapshot:
    return IrtAncestorSnapshot(
        message_id_inner=mid,
        path=Path(str(nm_msg.path)),
        tags=frozenset(nm_msg.tags),
        header_from=RfcFromWire.parse_present_from_nm_message(nm_msg, MailHeaderName.FROM.value),
        header_to=RfcToWire.parse_present_from_nm_message(nm_msg, MailHeaderName.TO.value),
        header_route=IngressRouteB62Wire.parse_present_from_nm_message(
            nm_msg, MailHeaderName.ROUTE.value
        ),
        header_references=RfcReferencesWire.parse_present_from_nm_message(
            nm_msg, MailHeaderName.REFERENCES.value
        ),
        header_in_reply_to=RfcInReplyToWire.parse_present_from_nm_message(
            nm_msg, MailHeaderName.IN_REPLY_TO.value
        ),
        header_subject=RfcSubjectWire.parse_present_from_nm_message(
            nm_msg, MailHeaderName.SUBJECT.value
        ),
        header_hop_budget=HopBudgetLine.parse_present_from_nm_message(
            nm_msg, MailHeaderName.HOP_BUDGET.value
        ),
    )


def _require_matching_indexed_mid(
    nm_msg: notmuch2.Message, expected: NotmuchMessageIdInner
) -> NotmuchMessageIdInner:
    indexed = nm.require_inner_message_id_from_notmuch_message(nm_msg)
    if not indexed.equals_case_insensitive(expected):
        raise RuntimeError(
            "notmuch Message-ID не согласован с цепочкой конверта/In-Reply-To: "
            f"index={indexed.value!r} expected={expected.value!r} path={nm_msg.path!r}"
        )
    return indexed


def _next_parent_inner_raw(irt_w: RfcInReplyToWire | None) -> NotmuchMessageIdInner | None:
    return NotmuchMessageIdInner.from_optional_raw(
        irt_w.value if irt_w is not None else None
    )


def _leaf_not_in_index_msg(inner: NotmuchMessageIdInner) -> str:
    return (
        "FSM-инвариант: лист не найден в union notmuch по inner Message-ID "
        f"(Message-ID={inner.as_angle_bracket_header()})"
    )


def _parent_missing_msg(parent: NotmuchMessageIdInner) -> str:
    return (
        "FSM-инвариант: разрыв IRT-цепочки — предок объявлен в In-Reply-To, "
        "но отсутствует в индексе "
        f"(Message-ID={parent.as_angle_bracket_header()})"
    )


def _materialize_irt_chain(
    db: notmuch2.Database, start_inner: NotmuchMessageIdInner
) -> list[IrtAncestorSnapshot]:
    """Лист → корень по IRT; все данные вычитываются под открытым read ``db``."""
    result: list[IrtAncestorSnapshot] = []
    seen_inner: set[str] = set()
    next_inner: NotmuchMessageIdInner | None = start_inner
    is_first = True

    while next_inner is not None:
        nm_msg = nm.first_notmuch_message_for_inner_id(db, next_inner)
        if nm_msg is None:
            if is_first:
                raise RuntimeError(_leaf_not_in_index_msg(start_inner))
            raise RuntimeError(_parent_missing_msg(next_inner))
        is_first = False

        indexed = _require_matching_indexed_mid(nm_msg, next_inner)
        key = indexed.value.casefold()
        if key in seen_inner:
            raise RuntimeError(
                "FSM-инвариант: цикл в цепочке In-Reply-To "
                f"(Message-ID={indexed.as_angle_bracket_header()})"
            )
        seen_inner.add(key)

        snap = _snapshot_from_nm_message(nm_msg, indexed)
        result.append(snap)

        parent = _next_parent_inner_raw(snap.header_in_reply_to)
        if parent is None:
            break
        next_inner = parent

    return result


def materialize_irt_chain_under_db(
    db: notmuch2.Database, start_inner: NotmuchMessageIdInner
) -> list[IrtAncestorSnapshot]:
    """Как :func:`iter_in_reply_to_ancestors_from_inner_id`, но под уже открытым READ ``db``."""
    return _materialize_irt_chain(db, start_inner)


def iter_in_reply_to_ancestors_from_inner_id(
    start_inner: NotmuchMessageIdInner,
) -> list[IrtAncestorSnapshot]:
    """Лист → корень: мгновенная материализация под одним read-сеансом notmuch.

    Xapian-курсор закрывается сразу после вычитки; возвращённые снимки
    ``IrtAncestorSnapshot`` валидны бессрочно (иммутабельные ``frozen dataclass``).
    """
    with nm.notmuch_database(write=False) as db:
        return _materialize_irt_chain(db, start_inner)

