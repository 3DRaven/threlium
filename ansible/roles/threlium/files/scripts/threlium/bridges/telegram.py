#!/usr/bin/env python3
"""Telegram long-poll мост → ingress@localhost через fdm.

Чекпоинт ``update_id`` восстанавливается из union-notmuch (`docs/INDEX.md` §1,
root = ``stages/``) через :mod:`threlium.nm` и декодирование routing payload
из заголовка ``X-Threlium-Route`` (b62(JSON), см. ``TelegramIngressRoute`` в :mod:`threlium.types`).

Long poll через ``python-telegram-bot`` (хелперы в этом же модуле); ошибки API не глотаются
(systemd перезапускает сервис).
"""
from __future__ import annotations

import asyncio
import msgspec
import notmuch2  # pyright: ignore[reportMissingImports]
import sys
import time
from collections.abc import Callable
from email.message import EmailMessage
from typing import Any, Coroutine, TypeVar

from telegram import Bot as TelegramBot, Message, ReplyParameters  # type: ignore[import-untyped]

import threlium.nm as nm
from threlium.bridges import BridgeInReplyTo, build_bridge_ingress_email
from threlium.bridges.notmuch_space_anchor import resolve_bridge_tail_mid_for_space
from threlium.invisible_task_mid import is_egress_placeholder_message
from threlium.logutil import logger
from threlium.systemd_notify import notify_status
from threlium.types.systemd_status import SystemdStatusBody
from threlium.settings import ThreliumSettings
from threlium.types import (
    IngressRouteB62Wire,
    MailHeaderName,
    NotmuchBridgeFromLocalhost,
    NotmuchMessageIdInner,
    NotmuchQueryConnective,
    NotmuchTag,
    RfcMessageIdWire,
    TelegramBridgeInboundCaptionOrText,
    TelegramIngressRoute,
    TelegramNativeId,
    TelegramPtbOutboundReplyBody,
    ThreliumSpaceB62Wire,
    telegram_space_from_ingress_route,
)

_HDR = MailHeaderName

log = logger.bind(stage="bridge_telegram")
DEFAULT_ALLOWED_UPDATES: tuple[str, ...] = ("message", "edited_message")


class _TelegramOutboundSendKwargs(msgspec.Struct, frozen=True):
    """Поля для ``bot.send_message`` кроме ``reply_parameters`` (объект PTB)."""

    chat_id: int
    text: str
    message_thread_id: int | None = None

_T = TypeVar("_T")


def telegram_token(settings: ThreliumSettings) -> str:
    """Токен бота из настроек; без него — жёсткая ошибка FSM."""
    token = settings.bridges.telegram.bot_token
    if not token:
        raise RuntimeError("THRELIUM_BRIDGES__TELEGRAM__BOT_TOKEN required")
    return token


def run_ptb(coro: Coroutine[Any, Any, _T]) -> _T:
    """Запуск одной корутины PTB из синхронного кода (мост, egress-стадия)."""
    return asyncio.run(coro)


async def send_reply_text(
    bot: TelegramBot,
    routing: TelegramIngressRoute,
    body: TelegramPtbOutboundReplyBody,
) -> Message:
    """Отправить plain-текст в чат с ответом на ``routing.message_id`` (ReplyParameters).

    Возвращает ``Message`` с API-присвоенным ``message_id`` (нужен для glue-archive MID).
    """
    text = body.value
    if len(text) > 4096:
        log.warning("truncating_outbound_text", original_length=len(text), chat_id=routing.chat_id)
        text = text[:4096]
    if not text:
        raise RuntimeError(
            "send_reply_text: empty outbound text after strip (refuse silent placeholder)"
        )

    raw = msgspec.to_builtins(
        _TelegramOutboundSendKwargs(
            chat_id=routing.chat_id,
            text=text,
            message_thread_id=routing.message_thread_id,
        )
    )
    kwargs: dict[str, object] = {k: v for k, v in raw.items() if v is not None}
    kwargs["reply_parameters"] = ReplyParameters(message_id=routing.message_id)

    return await bot.send_message(**kwargs)


def telegram_native_id_from_sent_message(msg: Message) -> TelegramNativeId:
    """Идентичность TG-сообщения из ответа PTB ``send_message`` (glue-archive ``Message-ID``)."""
    mtid_norm = (
        int(msg.message_thread_id)
        if msg.message_thread_id is not None
        else None
    )
    return TelegramNativeId(
        v=1,
        chat_id=int(msg.chat_id),
        message_id=int(msg.message_id),
        message_thread_id=mtid_norm,
    )


async def edit_message_text(
    bot: TelegramBot,
    *,
    chat_id: int,
    message_id: int,
    text: str,
) -> Message:
    """Обёртка ``bot.edit_message_text`` для замены placeholder финальным текстом.

    Telegram Bot API ``editMessageText`` не принимает ``message_thread_id`` —
    сообщение редактируется in-place в том треде, где было отправлено.
    """
    if len(text) > 4096:
        log.warning("truncating_edit_text", original_length=len(text))
        text = text[:4096]
    result = await bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
    )
    if not isinstance(result, Message):
        raise RuntimeError(f"edit_message_text: unexpected result type {type(result).__name__}")
    return result


async def send_placeholder_text(
    bot: TelegramBot,
    routing: TelegramIngressRoute,
    placeholder_text: str,
) -> Message:
    """Отправить placeholder в чат с ``reply_parameters`` на ``routing.message_id``."""
    raw = msgspec.to_builtins(
        _TelegramOutboundSendKwargs(
            chat_id=routing.chat_id,
            text=placeholder_text,
            message_thread_id=routing.message_thread_id,
        )
    )
    kwargs: dict[str, object] = {k: v for k, v in raw.items() if v is not None}
    kwargs["reply_parameters"] = ReplyParameters(message_id=routing.message_id)
    return await bot.send_message(**kwargs)


def telegram_effective_message_bridge_in_reply_to(
    *,
    msg: Message,
    db: notmuch2.Database,
) -> BridgeInReplyTo:
    """IRT для ``effective_message``: явный ``reply_to_message`` или fallback по якорю Space."""
    chat_id = int(msg.chat_id)
    mtid_norm = int(msg.message_thread_id) if msg.message_thread_id is not None else None
    reply = msg.reply_to_message
    if reply is not None:
        r_chat = reply.chat
        p_chat_id = int(r_chat.id) if r_chat is not None else chat_id
        p_mtid_raw = reply.message_thread_id
        p_mtid = int(p_mtid_raw) if p_mtid_raw is not None else None
        parent_native = TelegramNativeId(
            v=1,
            chat_id=p_chat_id,
            message_id=int(reply.message_id),
            message_thread_id=p_mtid,
        )
        return RfcMessageIdWire.from_native(parent_native)
    route_stub = TelegramIngressRoute(
        channel="telegram", v=1,
        chat_id=chat_id, message_id=0, message_thread_id=mtid_norm, update_id=0,
    )
    space = telegram_space_from_ingress_route(route_stub)
    sw = ThreliumSpaceB62Wire.from_threlium_space(space)
    return resolve_bridge_tail_mid_for_space(
        db, bridge=NotmuchBridgeFromLocalhost.TELEGRAM, space_wire=sw,
    )


def _parse_telegram_routing(route_wire: IngressRouteB62Wire | None) -> TelegramIngressRoute:
    """Wire ``X-Threlium-Route`` → ``TelegramIngressRoute`` (строго: нет wire / не telegram → ошибка FSM)."""
    r = IngressRouteB62Wire.parse_route_from_optional_header(route_wire)
    if r is None:
        raise RuntimeError(
            "FSM-инвариант: письмо из выборки notmuch с X-Threlium-Route "
            "не содержит непустого заголовка X-Threlium-Route"
        )
    if not isinstance(r, TelegramIngressRoute):
        raise RuntimeError(
            f"FSM-инвариант: ожидался TelegramIngressRoute, получен {type(r).__name__} (channel={r.channel!r})"
        )
    return r


def _max_update_id() -> int:
    """Последний ``update_id`` из самого нового ``tag:route from:telegram`` письма."""
    q = NotmuchQueryConnective.join_and(
        NotmuchTag.ROUTE.as_tag_query_term(),
        NotmuchBridgeFromLocalhost.TELEGRAM.as_from_query_term(),
    )
    with nm.notmuch_database(write=False) as db:
        for nm_msg in db.messages(q, sort=notmuch2.Database.SORT.NEWEST_FIRST):
            route_w = IngressRouteB62Wire.parse_present_from_nm_message(
                nm_msg, MailHeaderName.ROUTE.value
            )
            if route_w is None:
                continue
            return int(_parse_telegram_routing(route_w).update_id)
    return 0


async def _poll_loop(
    deliver: Callable[[EmailMessage], None],
    *,
    settings: ThreliumSettings,
) -> None:
    token_val = telegram_token(settings)
    offset = _max_update_id() + 1

    def deliver_msg(
        t: str,
        chat_id: str,
        msg_id: int,
        update_id: int,
        *,
        in_reply_to: BridgeInReplyTo,
        message_thread_id: int | None,
    ) -> None:
        r = TelegramIngressRoute(
            channel="telegram",
            v=1,
            chat_id=int(chat_id),
            update_id=update_id,
            message_id=msg_id,
            message_thread_id=message_thread_id,
        )
        native = TelegramNativeId(v=1, chat_id=int(chat_id), message_id=msg_id,
                                   message_thread_id=message_thread_id)
        mid = RfcMessageIdWire.from_native(native).value
        raw_obj: dict[str, object] = {
            "route": msgspec.to_builtins(r),
            "body": t,
            "chat_id": int(chat_id),
            "message_id": msg_id,
            "update_id": update_id,
        }
        if message_thread_id is not None:
            raw_obj["message_thread_id"] = message_thread_id
        raw_capture = msgspec.json.encode(raw_obj).decode("utf-8")
        space = telegram_space_from_ingress_route(r)
        sw = ThreliumSpaceB62Wire.from_threlium_space(space)
        msg = build_bridge_ingress_email(
            channel="telegram",
            body=t,
            route=r,
            message_id=mid,
            in_reply_to=in_reply_to,
            raw_capture=raw_capture,
            space_wire=sw,
        )
        notify_status(
            SystemdStatusBody.bridge_telegram_delivering(
                chat_id=str(chat_id),
                message_id=msg_id,
            )
        )
        deliver(msg)

    bot_api_base = settings.bridges.telegram.bot_api_base
    bot_kw: dict[str, Any] = {}
    if bot_api_base:
        bot_kw["base_url"] = bot_api_base
    async with TelegramBot(token_val, **bot_kw) as bot:
        await bot.get_me()
        notify_status(SystemdStatusBody.bridge_telegram_connected_idle())
        while True:
            updates = await bot.get_updates(
                offset=offset,
                timeout=60,
                allowed_updates=list(DEFAULT_ALLOWED_UPDATES),
                read_timeout=70,
                write_timeout=70,
                connect_timeout=70,
            )
            if updates:
                candidate_mids: set[NotmuchMessageIdInner] = set()
                for update in updates:
                    em = update.effective_message
                    if em:
                        mtid = int(em.message_thread_id) if em.message_thread_id is not None else None
                        native = TelegramNativeId(
                            v=1, chat_id=int(em.chat_id),
                            message_id=int(em.message_id),
                            message_thread_id=mtid,
                        )
                        candidate_mids.add(
                            NotmuchMessageIdInner.from_present_wire(
                                RfcMessageIdWire.from_native(native)
                            )
                        )

                known_mids: set[NotmuchMessageIdInner] = set()
                with nm.notmuch_database(write=False) as db:
                    for mid_nm in candidate_mids:
                        if nm.notmuch_index_has_message_id_in_db(db, mid_nm):
                            known_mids.add(mid_nm)

                for update in updates:
                    offset = update.update_id + 1
                    msg = update.effective_message
                    if not msg:
                        continue
                    text = TelegramBridgeInboundCaptionOrText.parse(
                        msg.text or msg.caption
                    ).value
                    if not text:
                        continue

                    reply_parent = msg.reply_to_message
                    if reply_parent is not None:
                        parent_text = reply_parent.text or reply_parent.caption or ""
                        if is_egress_placeholder_message(parent_text):
                            log.info("reply_to_placeholder_skip", chat_id=msg.chat_id, message_id=msg.message_id)
                            continue

                    mtid_norm = (
                        int(msg.message_thread_id)
                        if msg.message_thread_id is not None
                        else None
                    )

                    native = TelegramNativeId(
                        v=1, chat_id=int(msg.chat_id),
                        message_id=int(msg.message_id),
                        message_thread_id=mtid_norm,
                    )
                    mid_wire = RfcMessageIdWire.from_native(native)
                    mid_nm = NotmuchMessageIdInner.from_present_wire(mid_wire)
                    if mid_nm in known_mids:
                        log.info("duplicate_skip", chat_id=msg.chat_id, message_id=msg.message_id)
                        continue

                    with nm.notmuch_database(write=False) as db:
                        irt = telegram_effective_message_bridge_in_reply_to(
                            msg=msg,
                            db=db,
                        )

                    deliver_msg(
                        text,
                        str(msg.chat_id),
                        int(msg.message_id),
                        update.update_id,
                        in_reply_to=irt,
                        message_thread_id=mtid_norm,
                    )
            notify_status(SystemdStatusBody.bridge_telegram_connected_idle())
            time.sleep(0)


def run_bridge(deliver: Callable[[EmailMessage], None], *, settings: ThreliumSettings) -> None:
    if not str(settings.home):
        log.error("threlium_home_required")
        sys.exit(1)
    if not settings.bridges.telegram.bot_token:
        log.error("bot_token_required")
        sys.exit(1)

    run_ptb(_poll_loop(deliver, settings=settings))
