"""Msgspec-схемы native-id и ingress-маршрута (MESSAGES §2 / §8).

Канонизация wire Message-ID: :class:`threlium.types.rfc.RfcMessageIdWire`, маршрут b62:
:class:`threlium.types.ingress.IngressRouteB62Wire` / :func:`threlium.types.ingress.ingress_route_from_json_str`.
"""
from __future__ import annotations

from enum import StrEnum
from typing import NewType, Self, TypeVar

import msgspec

from ._core import NonEmptyStr


class IsomorphApiSurface(StrEnum):
    """Вендорный wire-формат HTTP-ответа клиенту (выбор не slug-ом канала, а полем маршрута)."""

    ANTHROPIC_MESSAGES = "anthropic_messages"
    OPENAI_CHAT_COMPLETIONS = "openai_chat_completions"


class ExternalRfcMidWire(msgspec.Struct, frozen=True):
    """Внешний RFC ``Message-ID`` (SMTP, поле ``reply_target_rfc_message_id`` в JSON маршрута email)."""

    value: str

    @classmethod
    def parse_optional(cls, raw: str | None) -> Self | None:
        if raw is None:
            return None
        s = str(raw).strip()
        if not s:
            return None
        v = s if s.startswith("<") and s.endswith(">") else f"<{s.strip('<>')}>"
        return cls(value=v)

# Opaque Matrix wire (JSON плоские строки; ``NewType`` — дискриминация на границе Python).
MatrixSyncBatchCursor = NewType("MatrixSyncBatchCursor", str)
MatrixRoomEventId = NewType("MatrixRoomEventId", str)
# Идентификатор комнаты Matrix (CS API ``room_id``, opaque ``!…`` / alias).
MatrixRoomId = NewType("MatrixRoomId", str)
# Клиентский ``txnId`` для ``PUT …/send/…`` (идемпотентность отправки).
MatrixRoomSendTxnId = NewType("MatrixRoomSendTxnId", str)
# IMAP UID письма в папке моста (RFC 3501); на границе FETCH — opaque int, в imaplib — str.
ImapFolderUid = NewType("ImapFolderUid", int)


def imap_folder_uid_from_int(n: int) -> ImapFolderUid:
    """``int`` → :class:`ImapFolderUid` (RFC 3501: UID ≥ 1)."""
    if n < 1:
        raise ValueError(f"IMAP UID must be >= 1, got {n!r}")
    return ImapFolderUid(n)


def imap_folder_uid_as_imaplib_arg(uid: ImapFolderUid) -> str:
    """Единственное место ``str(...)`` для ``client.uid('fetch', …)`` / imap_tools move."""
    return str(int(uid))


class EmailNativeId(msgspec.Struct, frozen=True):
    v: int
    message_id: NonEmptyStr


class TelegramNativeId(msgspec.Struct, frozen=True):
    """Идентичность Telegram-сообщения для канонического ``<b62@localhost>``.

    Минимальные поля, уникально идентифицирующие сообщение в Telegram.
    Checkpoint-данные (``update_id``) — в ``TelegramIngressRoute``, не здесь.
    """

    v: int
    chat_id: int
    message_id: int
    message_thread_id: int | None

    @classmethod
    def from_route(cls, r: TelegramIngressRoute) -> Self:
        """Identity из маршрута (без checkpoint ``update_id``)."""
        return cls(v=1, chat_id=r.chat_id, message_id=r.message_id,
                   message_thread_id=r.message_thread_id)


class MatrixNativeId(msgspec.Struct, frozen=True):
    """Идентичность Matrix-события для канонического ``<b62@localhost>``.

    Checkpoint-данные (``sync_batch``, ``reply_to_event_id``) — в ``MatrixIngressRoute``.
    """

    v: int
    room_id: MatrixRoomId
    event_id: MatrixRoomEventId

    @classmethod
    def from_route(cls, r: MatrixIngressRoute) -> Self:
        """Identity из маршрута (без checkpoint ``sync_batch``)."""
        return cls(v=1, room_id=r.room_id, event_id=r.event_id)


class IsomorphContentId(msgspec.Struct, frozen=True):
    """Контент-адресуемая идентичность isomorph-сообщения для канонического ``<b62@localhost>``.

    ``content_hash`` — sha256-hex от нормализованного контента (ответ Threlium / хвост запроса
    Cline). Один и тот же контент → один и тот же канонический Message-ID, поэтому мост может
    пересчитать ``In-Reply-To`` из last-assistant без чтения notmuch (см. docs/THREAD_MODEL §isomorph).
    """

    v: int
    content_hash: NonEmptyStr


class IsomorphSnowflakeId(msgspec.Struct, frozen=True):
    """Снежинка-идентичность isomorph-сообщения для канонического ``<b62@localhost>``.

    ``snowflake`` — уникальный 63-битный k-сортируемый id (``time|instance|seq``), генерируемый
    мостом/egress на приёме/ответе. В ОТЛИЧИЕ от :class:`IsomorphContentId` (контент-хеш) даёт
    уникальный MID независимо от тела — идентичные сообщения НЕ сливаются в notmuch (коллизий нет
    в корне). Тред-непрерывность: egress кладёт ``snowflake`` glue в невидимый водяной знак ответа;
    мост декодит его из last-assistant следующего хода → ``In-Reply-To`` (без content-голосования).
    """

    v: int
    snowflake: int


NativeId = (
    EmailNativeId | TelegramNativeId | MatrixNativeId | IsomorphContentId | IsomorphSnowflakeId
)

TNative = TypeVar(
    "TNative",
    EmailNativeId,
    TelegramNativeId,
    MatrixNativeId,
    IsomorphContentId,
    IsomorphSnowflakeId,
)


class EmailIngressRoute(msgspec.Struct, frozen=True):
    """Маршрут email-ingress; ``imap_uid`` / ``imap_uidvalidity`` — checkpoint INBOX.

    Аналогично ``update_id`` (Telegram) / ``sync_batch`` (Matrix) checkpoint-данные живут
    здесь, не в ``EmailNativeId``. Пара ``(imap_uidvalidity, imap_uid)`` по RFC 3501/9051:
    UID монотонен и уникален только в связке с ``UIDVALIDITY`` папки; при смене validity
    UID переназначаются. Заполняются только мостом на ingress (после IMAP fetch); для
    e2e / legacy-писем — отсутствуют (``None``).
    """

    channel: NonEmptyStr
    origin: NonEmptyStr
    v: int = 1
    #: RFC ``Message-ID`` этого входного письма (внешний контур), для SMTP ``In-Reply-To`` на ответ агента.
    reply_target_rfc_message_id: ExternalRfcMidWire | None = None
    #: IMAP UID письма в INBOX моста (watermark для выборки ``UID <uid+1>:*``).
    imap_uid: int | None = None
    #: ``UIDVALIDITY`` папки INBOX на момент fetch (пара к ``imap_uid`` по RFC 3501).
    imap_uidvalidity: int | None = None


class TelegramIngressRoute(msgspec.Struct, frozen=True):
    channel: NonEmptyStr
    v: int
    chat_id: int
    message_id: int
    message_thread_id: int | None
    update_id: int


class MatrixIngressRoute(msgspec.Struct, frozen=True):
    channel: NonEmptyStr
    v: int
    room_id: MatrixRoomId
    event_id: MatrixRoomEventId
    sync_batch: MatrixSyncBatchCursor | None
    reply_to_event_id: MatrixRoomEventId | None = None


class IsomorphIngressRoute(msgspec.Struct, frozen=True):
    """Маршрут isomorph-ingress (HTTP-мост многих LLM API поверх одного FSM-контура).

    Коррелятор pending↔push — НЕ отдельный ``request_id``, а **контент-адресуемый ``Message-ID``
    ingress** (per-turn-уникален, доступен сразу на первом ходу до notmuch): мост регистрирует pending
    под ним, egress пушит ``ancestor_mid`` ближайшего ``tag:route`` предка (= этот ingress). ``thread_id``
    notmuch не годится (нет на первом ходу + не per-turn). ``api_surface`` — вендорный wire-формат ответа
    (см. :class:`IsomorphApiSurface`), а не slug канала. ``model`` — эхо клиенту (лимит окна), не выбор LLM.
    """

    channel: NonEmptyStr
    api_surface: NonEmptyStr
    model: NonEmptyStr
    v: int = 1
    stream: bool = True


IngressRoute = (
    EmailIngressRoute | TelegramIngressRoute | MatrixIngressRoute | IsomorphIngressRoute
)

_OPTIONAL_STR_KEYS_EMPTY_TO_NONE = frozenset({"sync_batch", "reply_to_event_id"})


def normalize_ingress_route_dict(d: dict[str, object]) -> dict[str, object]:
    """Единственная фабрика границы **JSON / dict →** :class:`IngressRoute` (перед ``msgspec.convert``).

    Strip для строковых полей; пустая строка после strip → ошибка (кроме опциональных
    ключей в ``_OPTIONAL_STR_KEYS_EMPTY_TO_NONE``). Не дублировать эту нормализацию
    в других местах.
    """
    out: dict[str, object] = {}
    for k, v in d.items():
        if k == "reply_target_rfc_message_id":
            if v is None:
                out[k] = None
            elif isinstance(v, dict):
                inner = v.get("value")
                if inner is None:
                    raise msgspec.ValidationError(
                        "reply_target_rfc_message_id: missing value when object is present"
                    )
                if not isinstance(inner, str):
                    raise msgspec.ValidationError(
                        "reply_target_rfc_message_id.value must be a string"
                    )
                t = inner.strip()
                out[k] = None if not t else {"value": t}
            else:
                raise msgspec.ValidationError(
                    "reply_target_rfc_message_id must be null or an object {\"value\": \"...\"}"
                )
            continue
        if isinstance(v, str):
            t = v.strip()
            if k in _OPTIONAL_STR_KEYS_EMPTY_TO_NONE and not t:
                out[k] = None
            elif not t:
                raise msgspec.ValidationError(
                    f"ingress route field {k!r} is empty or whitespace-only"
                )
            else:
                out[k] = t
        else:
            out[k] = v
    return out
