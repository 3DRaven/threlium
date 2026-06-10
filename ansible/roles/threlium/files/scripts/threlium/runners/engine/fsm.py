"""FSM-пайплайн одного сообщения треда (бывший ``runners.worker``)."""
from __future__ import annotations

import threading
from email.message import EmailMessage
from pathlib import Path

from structlog.contextvars import bind_contextvars, reset_contextvars

from threlium.delivery import run_fdm
from threlium.irt_chain import irt_chain_materialization_cache
from threlium.logutil import logger
from threlium.mail import email_message_from_bytes, serialize_rfc822_for_wire
from threlium.settings import ThreliumSettings
from threlium.litellm_correlation_headers import build_litellm_correlation_headers
from threlium.litellm_route_context import (
    e2e_route_wire_tail,
    reset_litellm_correlation_ctxvar,
    set_litellm_correlation_ctxvar,
)
from threlium.nm import inner_message_id_for_path, nm_settle, settle_recovery_for_stage
from threlium.runners.lightrag import schedule_index_pending
from threlium.states.registry import STAGE_MAIN_HANDLERS
from threlium.systemd_notify import notify_status
from threlium.types.systemd_status import SystemdStatusBody
from threlium.types import (
    FsmStage,
    LitellmCallSite,
    LitellmCorrelationSnapshot,
    MailHeaderName,
    NotmuchMessageIdInner,
    NotmuchQueryConnective,
    NotmuchQueryField,
    NotmuchTag,
    NotmuchThreadScopeId,
    RfcMessageIdWire,
)

import threlium.nm as nm


def _require_non_empty_message_id(msg: EmailMessage) -> None:
    """FSM-инвариант: вход воркера с непустым ``Message-ID`` (inner)."""
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID)
    if NotmuchMessageIdInner.from_optional_wire(mid_w) is None:
        raise RuntimeError("FSM-инвариант: входное письмо без непустого Message-ID")


def _find_unread_in_thread(stage: FsmStage, thread_id: str) -> Path | None:
    """Oldest unread message for ``stage`` + ``thread_id`` via notmuch query.

    Explicit ``sort_newest_first=False`` guarantees FIFO within a thread.
    """
    query = NotmuchQueryConnective.join_and(
        NotmuchTag.UNREAD.as_tag_query_term(),
        NotmuchQueryField.THREAD.term(thread_id),
        NotmuchQueryField.TO.term(stage.rfc822_mailbox),
    )
    return nm.first_message_path(query, sort_newest_first=False)


def _run_stage(
    stage_vo: FsmStage,
    file_path: Path,
    *,
    settings: ThreliumSettings,
    thread_scope: NotmuchThreadScopeId | None = None,
) -> bytes:
    """Десериализовать файл и вызвать handler стадии in-process."""
    data = file_path.read_bytes()
    msg = email_message_from_bytes(data) if data else EmailMessage()

    _require_non_empty_message_id(msg)

    mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID)
    mid_display = (mid_w.value.strip() if mid_w is not None and mid_w.value.strip() else "?")
    _log = logger.bind(stage=stage_vo.value)
    # Корреляция в structlog-contextvars: ``message_id`` (+ notmuch-thread) авто-проступает во ВСЕ логи
    # синхронного дерева стадии (хендлер и всё, что он зовёт), а не протягивается руками в каждый вызов —
    # ``merge_contextvars`` (logutil, первый в цепочке процессоров) добавляет их к каждой записи. ``reset``
    # в finally даёт per-message-скоуп при переиспользовании потока воркером (как и litellm-ctxvar ниже).
    _cv = bind_contextvars(
        message_id=mid_display,
        notmuch_thread_id=(thread_scope.value if thread_scope is not None else None),
    )
    try:
        _log.info("fsm_enter")

        incoming_stage = FsmStage.from_incoming_to(msg)
        if incoming_stage != stage_vo:
            raise RuntimeError(
                f"FSM mis-routing: worker stage={stage_vo.value!r}, "
                f"{MailHeaderName.TO}: stage={incoming_stage.value!r}"
            )

        handler = STAGE_MAIN_HANDLERS[stage_vo]

        # IRT-цепочка иммутабельна в пределах обработки одной стадии, а хендлер+хелперы обходят её МНОГО раз
        # (enrich_context — 5 мест, task/collect, route-resolve, subagent-classifier, response/collect, …).
        # Активируем scope материализации ОДИН раз здесь, в stage-runner'е (универсально для любой стадии) —
        # внутри ``iter_in_reply_to_ancestors_from_inner_id`` материализует цепочку из notmuch РОВНО раз на
        # ``start_inner``, остальные обходы — из ambient-кэша scope (без правки сотен call-site'ов и без
        # глобального кэша со staleness между растущими сообщениями треда).
        with irt_chain_materialization_cache():
            if not settings.e2e.litellm_route_correlation:
                out_msg = handler(msg, stage_vo, config=settings)
            else:
                corr = build_litellm_correlation_headers(msg, call_site=LitellmCallSite.FSM)
                snap = LitellmCorrelationSnapshot.from_mapping(corr)
                _log.debug(
                    "e2e_fsm_corr_set",
                    thread=threading.current_thread().name,
                    ident=threading.get_ident(),
                    route_tail=e2e_route_wire_tail(snap.route_wire),
                    call_site=snap.call_site,
                )
                # ContextVar (а не TLS): на синхронном FSM-потоке ведёт себя как thread-local (set→read на
                # одном потоке), а token-reset гарантирует per-message-скоуп при переиспользовании потока.
                token = set_litellm_correlation_ctxvar(snap.as_dict())
                try:
                    out_msg = handler(msg, stage_vo, config=settings)
                finally:
                    reset_litellm_correlation_ctxvar(token)

        if out_msg is None:
            _log.info("fsm_result_terminal")
            return b""
        if not isinstance(out_msg, EmailMessage):
            raise TypeError(
                f"handler {stage_vo.value!r} returned {type(out_msg).__name__}, "
                f"expected EmailMessage | None"
            )
        next_stage = FsmStage.from_incoming_to(out_msg)
        _log.info("fsm_result_transition", next_stage=next_stage.value)
        return serialize_rfc822_for_wire(out_msg)
    finally:
        reset_contextvars(**_cv)


def process_thread_message(
    stage_vo: FsmStage, scope: NotmuchThreadScopeId, settings: ThreliumSettings
) -> None:
    """Обработать одно unread-письмо для пары (стадия, notmuch thread).

    Вызывается из долгоживущего движка (сокет) — без CLI ``%i``.
    """
    settle_recovery_for_stage(stage_vo.value)

    file_path = _find_unread_in_thread(stage_vo, scope.value)
    if file_path is None:
        notify_status(SystemdStatusBody.engine_idle_no_unread())
        return

    inner = inner_message_id_for_path(file_path)

    notify_status(
        SystemdStatusBody.engine_fsm_processing(
            stage=stage_vo,
            thread_scope=scope,
        )
    )
    try:
        out = _run_stage(stage_vo, file_path, settings=settings, thread_scope=scope)
        if out:
            run_fdm(out)
        nm_settle(inner)
        schedule_index_pending(settings)
    except BaseException as e:
        notify_status(SystemdStatusBody.engine_fsm_error(message=str(e)))
        raise
    notify_status(SystemdStatusBody.engine_idle())
