"""Разрешение ingress-маршрута по цепочке предков только через ``In-Reply-To``."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from email.message import EmailMessage
from typing import TypeVar

import notmuch2  # pyright: ignore[reportMissingImports]

from threlium import nm
from threlium.irt_chain import (
    IrtAncestorSnapshot,
    iter_in_reply_to_ancestors_from_inner_id,
)

from threlium.types import (
    BridgeIngressChannel,
    FsmStage,
    IngressRoute,
    IngressRouteB62Wire,
    IngressRouterResolvedChannelSlug,
    MailHeaderName,
    NotmuchMessageIdInner,
    NotmuchQueryConnective,
    NotmuchTag,
    RfcMessageIdWire,
    RfcReferencesWire,
    RfcSubjectWire,
)

_RouteT = TypeVar("_RouteT", bound=IngressRoute)


def egress_addr_for_channel(channel: IngressRouterResolvedChannelSlug) -> FsmStage:
    """Имя канала (как в wire ``X-Threlium-Route``) → виртуальный ``To:`` терминальной egress-стадии.

    Вызывается из ``egress_router`` **после** того, как канал извлечён из
    :func:`resolve_route_for_egress_fsm_from_email` / :func:`resolve_route_from_in_reply_to_ancestors`
    (обход предков по ``In-Reply-To`` от ``Message-ID`` текущего письма;
    на каждом шаге — ``tag:route``). Пустой канал — ``RuntimeError``.
    """
    slug = channel.value
    if not slug:
        raise RuntimeError("egress_addr_for_channel: channel is empty")
    try:
        ch = BridgeIngressChannel(slug.strip().lower())
    except ValueError:
        raise RuntimeError(
            f"egress_addr_for_channel: unknown channel slug {slug!r}; "
            f"expected one of {[e.value for e in BridgeIngressChannel]}"
        )
    if ch == BridgeIngressChannel.TELEGRAM:
        return FsmStage.EGRESS_TELEGRAM
    if ch == BridgeIngressChannel.MATRIX:
        return FsmStage.EGRESS_MATRIX
    if ch == BridgeIngressChannel.ISOMORPH:
        return FsmStage.EGRESS_ISOMORPH
    return FsmStage.EGRESS_EMAIL


@dataclass
class ResolvedRoute:
    channel: IngressRouterResolvedChannelSlug
    api_payload: IngressRoute
    message_id_inner: NotmuchMessageIdInner
    route_wire: IngressRouteB62Wire
    ancestor_references_wire: RfcReferencesWire | None = None
    ancestor_subject_wire: RfcSubjectWire | None = None

    @property
    def message_id(self) -> str:
        """Уголковая форма для совместимости / логов (RFC822 ``Message-ID``)."""
        return self.message_id_inner.as_angle_bracket_header()


def _resolve_from_route_fields(
    mid: NotmuchMessageIdInner,
    tags: frozenset[str] | set[str],
    route_hdr: IngressRouteB62Wire | None,
    refs_hdr: RfcReferencesWire | None,
    subj_hdr: RfcSubjectWire | None,
) -> ResolvedRoute | None:
    """Общая логика резолва маршрута из полей сообщения (snapshot или live)."""
    if NotmuchTag.ROUTE.value not in tags:
        return None
    if route_hdr is None:
        raise RuntimeError(
            "FSM-инвариант нарушен: есть "
            f"tag:{NotmuchTag.ROUTE.value}, но нет заголовка "
            f"{MailHeaderName.ROUTE.value} (Message-ID={mid.as_angle_bracket_header()})"
        )
    rw = route_hdr
    ing = IngressRouteB62Wire.parse_route_from_optional_header(rw)
    if ing is None:
        raise RuntimeError(
            "FSM-инвариант нарушен: есть "
            f"tag:{NotmuchTag.ROUTE.value}, но {MailHeaderName.ROUTE.value} "
            "не даёт типизированный маршрут "
            f"(Message-ID={mid.as_angle_bracket_header()}, wire={rw.value!r})"
        )

    return ResolvedRoute(
        channel=IngressRouterResolvedChannelSlug.parse(str(ing.channel)),
        api_payload=ing,
        message_id_inner=mid,
        route_wire=rw,
        ancestor_references_wire=refs_hdr,
        ancestor_subject_wire=subj_hdr,
    )


def _try_resolve_from_snapshot(snap: IrtAncestorSnapshot) -> ResolvedRoute | None:
    return _resolve_from_route_fields(
        snap.message_id_inner,
        snap.tags,
        snap.header_route,
        snap.header_references,
        snap.header_subject,
    )


def _try_resolve_from_notmuch_message(nm_msg: notmuch2.Message) -> ResolvedRoute | None:
    mid = nm.require_inner_message_id_from_notmuch_message(nm_msg)
    return _resolve_from_route_fields(
        mid,
        set(nm_msg.tags),
        IngressRouteB62Wire.parse_present_from_nm_message(nm_msg, MailHeaderName.ROUTE.value),
        RfcReferencesWire.parse_present_from_nm_message(nm_msg, MailHeaderName.REFERENCES.value),
        RfcSubjectWire.parse_present_from_nm_message(nm_msg, MailHeaderName.SUBJECT.value),
    )


_EGRESS_TASK_NO_ROUTE_ANCESTOR = (
    "egress task: no ancestor with tag:route and non-empty X-Threlium-Route "
    "along In-Reply-To chain"
)


def egress_fsm_start_inner_from_email(msg: EmailMessage) -> NotmuchMessageIdInner:
    """Стартовый inner для IRT-обхода egress: ``Message-ID`` текущего письма."""
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID)
    leaf = NotmuchMessageIdInner.from_optional_wire(mid_w)
    if leaf is None:
        raise RuntimeError(
            "FSM-инвариант: egress FSM резолв требует непустой "
            f"{MailHeaderName.MESSAGE_ID.value} на конверте"
        )
    return leaf


def resolve_route_for_egress_fsm_from_email(msg: EmailMessage) -> ResolvedRoute:
    """Маршрут для ``egress_router`` / ``egress_*``: лист MID, затем IRT вверх до ``tag:route``."""
    return resolve_route_from_in_reply_to_ancestors(egress_fsm_start_inner_from_email(msg))


@dataclass(frozen=True)
class EgressAncestorSnapshot:
    """Иммутабельный снимок предка egress-задачи с типизированным маршрутом.

    Все данные материализованы из :class:`ResolvedRoute` (IRT-цепочка),
    без повторного открытия notmuch.
    """
    route: IngressRoute
    ancestor_mid: NotmuchMessageIdInner
    header_references: RfcReferencesWire | None
    header_subject: RfcSubjectWire | None


def _egress_snap_from_resolved(resolved: ResolvedRoute) -> EgressAncestorSnapshot:
    return EgressAncestorSnapshot(
        route=resolved.api_payload,
        ancestor_mid=resolved.message_id_inner,
        header_references=resolved.ancestor_references_wire,
        header_subject=resolved.ancestor_subject_wire,
    )


def resolve_egress_task_route_ancestor(
    msg: EmailMessage,
    expect_type: type[_RouteT],
    *,
    wrong_route_type_message: Callable[[IngressRoute], str],
) -> tuple[_RouteT, EgressAncestorSnapshot]:
    """Типизированный маршрут + снимок предка ``egress_*`` задачи.

    Один READ-сеанс notmuch (IRT-цепочка через
    :func:`~threlium.irt_chain.iter_in_reply_to_ancestors_from_inner_id`);
    ``ResolvedRoute`` уже содержит все нужные данные — повторное открытие БД
    для чтения предка не требуется.
    """
    resolved = resolve_route_for_egress_fsm_from_email(msg)
    r = resolved.api_payload
    if not isinstance(r, expect_type):
        raise RuntimeError(wrong_route_type_message(r))
    return r, _egress_snap_from_resolved(resolved)


def _route_from_irt_snapshots(
    chain: list[IrtAncestorSnapshot],
) -> ResolvedRoute:
    """Первый snapshot с ``tag:route`` → ``ResolvedRoute``; иначе ``RuntimeError``."""
    for snap in chain:
        resolved = _try_resolve_from_snapshot(snap)
        if resolved is not None:
            return resolved
    raise RuntimeError(_EGRESS_TASK_NO_ROUTE_ANCESTOR)


def resolve_egress_task_route_ancestor_with_thread_correlation(
    msg: EmailMessage,
    expect_type: type[_RouteT],
    *,
    wrong_route_type_message: Callable[[IngressRoute], str],
) -> tuple[_RouteT, EgressAncestorSnapshot, ResolvedRoute]:
    """IRT-предок + thread-корреляция (``tag:route`` в корне) → только VO наружу.

    Расширенная версия :func:`resolve_egress_task_route_ancestor` для egress-стадий с e2e-корреляцией
    LiteLLM (Matrix). IRT-цепочка — через единый stage-scoped механизм материализации
    :func:`~threlium.irt_chain.iter_in_reply_to_ancestors_from_inner_id` (РОВНО раз на ``start_inner`` за
    стадию, переиспользуется остальными обходами); поиск route-tag — свой короткий ``@nm.read_retry``-сеанс
    по тому же ``start_inner``. Оба читают иммутабельные данные (цепочка предков + route-tag корня —
    стабильны), отдельные сеансы безопасны и не утекают ``notmuch2.Message``."""
    start_inner = egress_fsm_start_inner_from_email(msg)
    chain = iter_in_reply_to_ancestors_from_inner_id(start_inner)
    route_resolved = _route_from_irt_snapshots(chain)
    r = route_resolved.api_payload
    if not isinstance(r, expect_type):
        raise RuntimeError(wrong_route_type_message(r))
    snap = _egress_snap_from_resolved(route_resolved)
    thread_resolved = _resolve_route_from_thread_oldest_route_tag_by_inner(start_inner)
    return r, snap, thread_resolved


def resolve_route_from_thread_oldest_route_tag_under_db(
    db: notmuch2.Database, mid_inner: NotmuchMessageIdInner
) -> ResolvedRoute:
    """То же, что публичный резолв по треду, но под уже открытым READ ``db`` (один сеанс)."""
    tid = nm.thread_id_for_header_message_id_in_db(db, mid_inner)
    if tid is None:
        raise RuntimeError(
            "FSM-инвариант: письмо не в индексе notmuch или без thread id "
            f"(Message-ID={mid_inner.as_angle_bracket_header()})"
        )
    q = NotmuchQueryConnective.join_and(
        tid.as_notmuch_thread_term(),
        NotmuchTag.ROUTE.as_tag_query_term(),
    )
    sort = notmuch2.Database.SORT.OLDEST_FIRST
    for nm_msg in db.messages(q, sort=sort):
        resolved = _try_resolve_from_notmuch_message(nm_msg)
        if resolved is not None:
            return resolved

    raise RuntimeError(
        "FSM-инвариант: в треде нет письма с "
        f"{NotmuchTag.ROUTE.as_tag_query_term()} и валидным маршрутом "
        f"(thread={tid.as_notmuch_thread_term()!r})"
    )


def resolve_route_from_thread_oldest_route_tag(msg_or_headers: EmailMessage) -> ResolvedRoute:
    """Самое старое в notmuch-треде письмо с ``tag:route`` и валидным ``X-Threlium-Route``.

    Используется для e2e-корреляции LiteLLM (стабильный wire на весь тред). При невозможности
    резолва — ``RuntimeError`` (инвариант FSM), не ``None``.
    """
    mid_w = RfcMessageIdWire.parse_present_from_email(
        msg_or_headers, MailHeaderName.MESSAGE_ID
    )
    mid_inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    if mid_inner is None:
        raise RuntimeError(
            "FSM-инвариант: resolve_route_from_thread_oldest_route_tag требует непустой "
            f"{MailHeaderName.MESSAGE_ID.value} на конверте"
        )
    return _resolve_route_from_thread_oldest_route_tag_by_inner(mid_inner)


@nm.read_retry
def _resolve_route_from_thread_oldest_route_tag_by_inner(
    mid_inner: NotmuchMessageIdInner,
) -> ResolvedRoute:
    """Короткий READ-сеанс (``@nm.read_retry``): материализует ``ResolvedRoute`` по треду."""
    with nm.notmuch_database(write=False) as db:
        return resolve_route_from_thread_oldest_route_tag_under_db(db, mid_inner)


def resolve_route_from_in_reply_to_ancestors(
    start_inner: NotmuchMessageIdInner,
) -> ResolvedRoute:
    """Найти wire-маршрут по цепочке ``In-Reply-To`` (только IRT, без union References).

    От листа ``start_inner`` в индексе, затем по предкам: на **каждом** узле
    проверяются ``tag:route`` и непустой ``X-Threlium-Route``. При отсутствии узла —
    ``RuntimeError`` (инвариант FSM), не ``None``.
    """
    for snap in iter_in_reply_to_ancestors_from_inner_id(start_inner):
        resolved = _try_resolve_from_snapshot(snap)
        if resolved is not None:
            return resolved
    raise RuntimeError(_EGRESS_TASK_NO_ROUTE_ANCESTOR)
