"""Якорь треда по ``X-Threlium-Space-Id``: один notmuch-запрос вместо tail-index.

По известному ``ThreliumSpaceB62Wire`` строит запрос
``tag:route AND from:<bridge> AND Threliumspace:"<wire>"`` → первое (newest)
сообщение = якорь; из якоря → thread scope → newest MID в треде.
"""
from __future__ import annotations

import notmuch2  # pyright: ignore[reportMissingImports]

import threlium.nm as nm
from threlium.types import (
    FsmStage,
    NotmuchBridgeFromLocalhost,
    NotmuchMessageIdInner,
    NotmuchQueryConnective,
    NotmuchTag,
    NotmuchThreadScopeId,
    ThreliumSpaceB62Wire,
)

_SORT_NEWEST = notmuch2.Database.SORT.NEWEST_FIRST

#: Потолок ожидания archive-glue предыдущего хода пространства (deadline pending-элемента в цикле моста).
#: Покрывает самый долгий контур (reasoning ~120c + egress/archive); при превышении сообщение ingest'ится
#: best-effort (ход считается зависшим), чтобы pending не копился вечно. Per-space — соседние не ждут.
SPACE_SETTLE_TIMEOUT_SEC = 240.0


def _newest_message_mid_in_thread(
    db: notmuch2.Database, tid: NotmuchThreadScopeId,
) -> NotmuchMessageIdInner:
    """Message-ID самого нового сообщения в треде (любой тег / From)."""
    q = tid.as_notmuch_thread_term()
    for nm_msg in db.messages(q, sort=_SORT_NEWEST):
        return nm.require_inner_message_id_from_notmuch_message(nm_msg)
    raise RuntimeError(
        f"пустой тред при ненулевом якоре (thread={tid.value!r})"
    )


def space_thread_settled(
    db: notmuch2.Database,
    *,
    bridge: NotmuchBridgeFromLocalhost,
    space_wire: ThreliumSpaceB62Wire,
) -> bool:
    """Завершён ли предыдущий ход пространства = можно ли ПРИЦЕПЛЯТЬ новое сообщение к archive-glue.

    Инвариант линейности треда (THREAD_MODEL §1.2): ``ingress(Un) → … → archive(GLUE) → ingress(Un+1)``.
    Следующее сообщение моста должно прицепляться к **archive-glue** завершённого хода, а НЕ к промежуточному
    FSM-узлу хода ещё в обработке (иначе форк линейной IRT-цепочки → ломается маршрут/ledger/response-буфер,
    §3). Мост ДОЛЖЕН дождаться этого условия перед ingest (per-space сериализация на доставке).

    Возвращает ``True`` если:
    - у пространства ещё НЕТ треда (новое пространство — ждать нечего), либо
    - КАЖДЫЙ принятый ход пространства уже архивирован: ``count(тред AND to:archive@localhost) >=
      count(тред AND from:<bridge>)``. Каждый ход моста = одно ingress-письмо ``from:<bridge>`` (route-корень,
      fdm ``ins_ingress_route_*``); его завершение = одна archive-glue ``to:archive@localhost``
      (``ins_stage_archive``, терминал контура). Счётчики — устойчивы к coarse-резолюции дат notmuch
      (NEWEST_FIRST дал бы egress-узел, не archive — archive НЕ самый новый узел треда).
    ``False`` если есть непроархивированный ход (в обработке) → вызывающий мост поллит дальше.
    """
    q = NotmuchQueryConnective.join_and(
        NotmuchTag.ROUTE.as_tag_query_term(),
        bridge.as_from_query_term(),
        space_wire.as_notmuch_index_term(),
    )
    anchor: notmuch2.Message | None = None
    for nm_msg in db.messages(q, sort=_SORT_NEWEST):
        anchor = nm_msg
        break
    if anchor is None:
        return True

    anchor_mid = nm.require_inner_message_id_from_notmuch_message(anchor)
    tid = nm.thread_id_for_header_message_id_in_db(db, anchor_mid)
    if tid is None:
        return True

    thread_term = tid.as_notmuch_thread_term()
    received = db.count_messages(
        NotmuchQueryConnective.join_and(thread_term, bridge.as_from_query_term())
    )
    archived = db.count_messages(
        NotmuchQueryConnective.join_and(thread_term, f"to:{FsmStage.ARCHIVE.rfc822_mailbox}")
    )
    return archived >= received


@nm.read_retry
def space_thread_settled_read(
    *,
    bridge: NotmuchBridgeFromLocalhost,
    space_wire: ThreliumSpaceB62Wire,
) -> bool:
    """Один READ-снимок :func:`space_thread_settled` (открывает БД, ``@nm.read_retry`` reopen-on-modified).

    НЕ блокирует и НЕ поллит — разовая проверка «предыдущий ход пространства завершён (archive-glue есть)?».
    Поллинг делает сам цикл моста, НЕ блокируя соседние пространства: неотстоявшееся сообщение паркуется в
    in-memory pending-очередь и перепроверяется на следующем тике, пока сообщения ДРУГИХ пространств идут
    параллельно (треды независимы — THREAD_MODEL: serial-per-thread, parallel-across-threads)."""
    with nm.notmuch_database(write=False) as db:
        return space_thread_settled(db, bridge=bridge, space_wire=space_wire)


def resolve_bridge_tail_mid_for_space(
    db: notmuch2.Database,
    *,
    bridge: NotmuchBridgeFromLocalhost,
    space_wire: ThreliumSpaceB62Wire,
) -> NotmuchMessageIdInner | None:
    """Newest MID в треде, привязанном к ``space_wire``, или ``None`` для нового пространства.

    Алгоритм:
    1. Один запрос ``tag:route AND from:<bridge> AND Threliumspace:"<wire>"``
       с ``NEWEST_FIRST``, берём первое сообщение — якорь.
    2. Из якоря — ``thread:`` id → scope треда.
    3. По scope — newest message (любой тег) → его MID.

    ``None`` возвращается только если запрос пуст (первое сообщение в пространстве).
    """
    q = NotmuchQueryConnective.join_and(
        NotmuchTag.ROUTE.as_tag_query_term(),
        bridge.as_from_query_term(),
        space_wire.as_notmuch_index_term(),
    )
    anchor: notmuch2.Message | None = None
    for nm_msg in db.messages(q, sort=_SORT_NEWEST):
        anchor = nm_msg
        break
    if anchor is None:
        return None

    anchor_mid = nm.require_inner_message_id_from_notmuch_message(anchor)
    tid = nm.thread_id_for_header_message_id_in_db(db, anchor_mid)
    if tid is None:
        raise RuntimeError(
            f"якорь найден, но thread id отсутствует (mid={anchor_mid.value!r})"
        )
    return _newest_message_mid_in_thread(db, tid)
