"""Разбор/сборка MIME для мостов и стадий — поверх stdlib ``email``."""
from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from email.message import EmailMessage, Message
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self, TypeAlias

import msgspec

from threlium.logutil import logger
from threlium.mail_header_names import MailHeaderName
from threlium.types.fsm_stage import FsmStage
from threlium.types.bridge_raw import RawIngressCaptureAttachmentFilename

if TYPE_CHECKING:
    from threlium.types.content_score import ThreliumContentScoreWire
    from threlium.types.fsm_stage import FsmStage
    from threlium.types.fsm_strings import (
        EnrichGlobalMemoryText,
        EnrichGraphAnswerText,
        EnrichThreadMemoryText,
        EnrichUnifiedMailContextText,
    )
    from threlium.types.notmuch_message_id import NotmuchMessageIdInner

    _EnrichOptionalText: TypeAlias = (
        EnrichGraphAnswerText
        | EnrichUnifiedMailContextText
        | EnrichThreadMemoryText
        | EnrichGlobalMemoryText
        | None
    )

log = logger.bind(stage="mime_reform")


class EnrichPartId(StrEnum):
    """Content-ID для MIME-частей письма enrich/enrich_fast -> reasoning."""

    USER_MESSAGE = "<user-message>"
    GRAPH_ANSWER = "<graph-answer>"
    UNIFIED_MAIL_CONTEXT = "<unified-mail-context>"
    THREAD_MEMORY = "<thread-memory>"
    GLOBAL_MEMORY = "<global-memory>"
    RESPONSE_STATE = "<response-state>"
    TASK_INIT = "<task-init>"
    TASK_STATE = "<task-state>"
    HISTORY = "<history>"
    SYSTEM = "<system>"
    USER_QUERY = "<user-query>"


# Единственное relay-семейство после унификации (docs/FSM.md): любая содержательная
# нагрузка от любой стадии едет как ``<history>``-часть. CID контент-адресный
# ``<{sha256(body)}@history>`` (хеш тела — local-part, ``history`` — домен), поэтому
# идентичное тело (оригинал и relay-копия) даёт один CID и схлопывается при дедупе —
# тип в CID не кодируется, семантику даёт ``X-Threlium-Origin`` + tool spec.
# ``system`` НЕ релеится/не индексируется (носитель payload-команды между стадиями),
# поэтому в RELAY_FAMILIES его нет — только в _CONTENT_ADDRESSED_FAMILIES для family-детекции.
RELAY_FAMILIES: Final[tuple[EnrichPartId, ...]] = (EnrichPartId.HISTORY,)

# Семейства с контент-адресным CID ``<{sha256(body)}@{family}>`` (хеш тела — local-part,
# семейство — домен): ``history`` (релеится/индексируется) и ``system`` (payload-команда,
# не релеится). Используется только для разбора домена в :attr:`EnrichContentId.family`.
_CONTENT_ADDRESSED_FAMILIES: Final[tuple[EnrichPartId, ...]] = (
    EnrichPartId.HISTORY,
    EnrichPartId.SYSTEM,
)

# Фиксированные Content-ID полного ``enrich`` (build_enriched_multipart): не relay,
# enrich_fast их не дописывает из входящего письма (``<response-state>`` / ``<task-state>``
# он пересобирает сам; ``<task-init>`` переносится как есть для durable-collect).
_CORE_PART_IDS: Final[frozenset[EnrichPartId]] = frozenset(
    {
        EnrichPartId.USER_MESSAGE,
        EnrichPartId.GRAPH_ANSWER,
        EnrichPartId.UNIFIED_MAIL_CONTEXT,
        EnrichPartId.THREAD_MEMORY,
        EnrichPartId.GLOBAL_MEMORY,
        EnrichPartId.RESPONSE_STATE,
        EnrichPartId.TASK_INIT,
        EnrichPartId.TASK_STATE,
    }
)

_HDR = MailHeaderName


def _cid_token(part_id: EnrichPartId) -> str:
    """``EnrichPartId.HISTORY`` → ``'history'`` (CID-токен без угловых скобок)."""
    return part_id.value.strip("<>")


class EnrichContentId(msgspec.Struct, frozen=True):
    """VO ``Content-ID`` MIME-части enrich/relay (wire ``<...>``).

    Один смысл «идентификатор части enriched-контекста»: либо канонический
    (``<user-message>``, ``<response-state>``, …), либо уникальный relay-CID
    ``<{family}@{inner-mid}>`` одного хопа. Семейство и принадлежность к core —
    через свойства, а не разбор сырых строк в бизнес-логике.
    """

    value: str

    @classmethod
    def from_part_id(cls, part_id: EnrichPartId) -> Self:
        """Канонический CID семейного enum (``<observation-note>``)."""
        return cls(value=part_id.value)

    @classmethod
    def from_relay(cls, base: EnrichPartId, source_inner: "NotmuchMessageIdInner") -> Self:
        """Уникальный relay-CID ``<{family}@{inner}>`` для одного хопа.

        Привязка к ``Message-ID`` входящего письма стадии делает CID уникальным на
        хоп (stage-agnostic, без зависимости от ``From``). CID внутренний — в egress
        не уходит.
        """
        return cls(value=f"<{_cid_token(base)}@{source_inner.value}>")

    @classmethod
    def from_history_body(cls, text: str) -> Self:
        """Контент-адресный CID ``<{sha256(body)}@history>`` (хеш только по телу).

        Хеш по телу части (без заголовков ``Origin``/``Score``/CID): ``X-Threlium-Origin``
        штампует ``enrich_fast`` постфактум, поэтому у оригинала-производителя и relay-копии
        тела совпадают, а заголовки — нет; хеш по телу делает их один CID → дедуп схлопывает.
        Идиома ``TaskSubtaskContentId.from_text`` / ``IrtHashWire.from_irt_header_value``
        (кодек только внутри VO).
        """
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return cls(value=f"<{digest}@{_cid_token(EnrichPartId.HISTORY)}>")

    @classmethod
    def from_system_body(cls, text: str) -> Self:
        """Контент-адресный CID ``<{sha256(body)}@system>`` — носитель payload-команды.

        Зеркало :meth:`from_history_body`; домен ``system`` отличает механический payload
        от его history-копии: одинаковое тело → одинаковый хеш, но разный домен → разные
        CID, поэтому дедуп их НЕ схлопывает (одна часть для механики, одна для памяти).
        ``system`` не входит в :data:`RELAY_FAMILIES` — не релеится и не индексируется.
        """
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return cls(value=f"<{digest}@{_cid_token(EnrichPartId.SYSTEM)}>")

    @classmethod
    def from_mime_part(cls, part: EmailMessage) -> Self | None:
        """Граница: ``Content-ID`` leaf-части после strip; отсутствие/пусто → ``None``."""
        raw = part.get("Content-ID")
        if raw is None:
            return None
        cleaned = str(raw).strip()
        return cls(value=cleaned) if cleaned else None

    @property
    def family(self) -> EnrichPartId | None:
        """Семейство relay; ``None`` для core/чужих.

        Контент-адресный формат ``<{hash}@history>``: семейство определяется по **домену**
        (часть после ``@``), а хеш-local-part уникален на контент. Канонический ``<history>``
        (без ``@``) — то же семейство (определяется по самому токену).
        """
        inner = self.value.strip().strip("<>")
        token = inner.split("@", 1)[1] if "@" in inner else inner
        for fam in _CONTENT_ADDRESSED_FAMILIES:
            if token == _cid_token(fam):
                return fam
        return None

    @property
    def is_core(self) -> bool:
        """Фиксированная часть полного ``enrich`` (не relay-хвост)."""
        return any(self.value == p.value for p in _CORE_PART_IDS)

def _extract_plain_body_from_message(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                raw = part.get_payload(decode=True)
                if isinstance(raw, bytes):
                    return raw.decode(part.get_content_charset() or "utf-8", errors="replace")
                return "" if raw is None else str(raw)
        return ""
    raw = msg.get_payload(decode=True)
    if isinstance(raw, bytes):
        return raw.decode(msg.get_content_charset() or "utf-8", errors="replace")
    if raw is None:
        pl = msg.get_payload()
        return "" if pl is None else str(pl)
    return str(raw)


def extract_plain_body(msg: EmailMessage) -> str:
    """Текстовое тело EmailMessage: первый text/plain, иначе raw payload."""
    return _extract_plain_body_from_message(msg)


def ingress_external_body_text(msg: EmailMessage) -> "IngressExternalBodyText":
    """Полное внешнее тело для ingress distill (без raw-capture attachment).

    CONTEXT_CONTRACT §2 (внешняя граница): если письмо несёт ровно одну ``<system>``-часть —
    тело берётся из неё (``system_part_text``, канонический payload), а не из «первого
    text/plain». Иначе (bridge-вход без ``<system>``) — первый ``text/plain`` / ``text/html``,
    пропуская attachment ``threlium-raw-ingress.txt``.
    """
    from threlium.types.ingress_distill import IngressExternalBodyText

    system_parts = iter_system_parts(msg)
    if len(system_parts) == 1:
        sys_text = _leaf_part_text(system_parts[0][1])
        if sys_text.strip():
            return IngressExternalBodyText.parse(sys_text)

    raw_fn = RawIngressCaptureAttachmentFilename.canonical().value
    if msg.is_multipart():
        for part in msg.walk():
            if part.is_multipart():
                continue
            fn = part.get_filename()
            if fn and str(fn) == raw_fn:
                continue
            ctype = part.get_content_type()
            if ctype in ("text/plain", "text/html"):
                text = _leaf_part_text(part)  # type: ignore[arg-type]
                if text.strip():
                    return IngressExternalBodyText.parse(text)
    return IngressExternalBodyText.parse(extract_plain_body(msg))


def ingress_raw_email_capture(incoming: EmailMessage) -> str:
    """Все заголовки входящего MIME + пустая строка + только ``text/plain`` тело (как :func:`extract_plain_body`)."""
    lines: list[str] = []
    for key, val in incoming.items():
        lines.append(f"{key}: {val}")
    lines.append("")
    lines.append(extract_plain_body(incoming))
    return "\n".join(lines)


def require_unique_threading_rfc822_headers(msg: EmailMessage) -> None:
    """Fail-fast на входе ``ingress``: более одного физического ``In-Reply-To`` или ``References``.

    Дубликаты недопустимы после канонизации email-моста: ``EmailMessage`` может накапливать
    несколько одноимённых заголовков; ``get()`` и notmuch parent lookup опираются на первый —
    ломается инвариант треда (см. ``bridges.email._build_canonical`` skip IRT/Refs).
    """
    for hdr in (_HDR.IN_REPLY_TO, _HDR.REFERENCES):
        vals = msg.get_all(hdr)
        n = len(vals) if vals else 0
        if n > 1:
            raise RuntimeError(
                "FSM-инвариант: ожидается не более одного заголовка "
                f"{hdr!r}, получено {n}. Дубли In-Reply-To/References ломают "
                "поиск родителя в notmuch; проверьте email-мост и RFC822 на диске."
            )


def ingress_pipeline_email(incoming: EmailMessage) -> EmailMessage:
    """Письмо для handoff после ingress: моночасть ``text/plain``, без multipart/вложений.

    Заголовки переносятся как в orphan-префиксе (:mod:`threlium.states.ingress`), тело —
    :func:`extract_plain_body`.
    """
    out = EmailMessage()
    skip = frozenset(
        {
            _HDR.CONTENT_TYPE.value.lower(),
            _HDR.CONTENT_TRANSFER_ENCODING.value.lower(),
            _HDR.MIME_VERSION.value.lower(),
            _HDR.CONTENT_DISPOSITION.value.lower(),
        }
    )
    for k, v in incoming.items():
        if k.lower() in skip:
            continue
        if k in out:
            out.add_header(k, v)
        else:
            out[k] = v
    body = extract_plain_body(incoming)
    out.set_content(body, subtype="plain", charset="utf-8")
    return out


def _make_inline_text_part(
    content_id: EnrichPartId | EnrichContentId,
    text: str,
    *,
    score: "ThreliumContentScoreWire | None" = None,
    origin: "FsmStage | None" = None,
) -> EmailMessage:
    """MIME text/plain part с Content-ID и Content-Disposition: inline.

    ``content_id`` — семейный enum (``EnrichPartId``) или VO ``EnrichContentId``
    (например контент-адресный history-CID ``<{hash}@history>``); оба несут ``.value``.

    Опц. per-part заголовки (только для ``<history>``-частей, граница через
    ``MailHeaderName``, без сырых строк): ``score`` → ``X-Threlium-Content-Score``
    (ставит источник из настроек), ``origin`` → ``X-Threlium-Origin`` (штампует
    enrich_fast). Переживают relay-копирование ``out.attach(part)``.
    """
    part = EmailMessage()
    part.set_content(text, subtype="plain", charset="utf-8")
    part.add_header("Content-Disposition", "inline")
    part.replace_header("Content-Type", "text/plain; charset=\"utf-8\"")
    part["Content-ID"] = content_id.value
    if score is not None:
        part[_HDR.CONTENT_SCORE.value] = score.value
    if origin is not None:
        part[_HDR.ORIGIN.value] = origin.rfc822_mailbox
    return part


def _copy_envelope_headers(src: EmailMessage, dst: EmailMessage) -> None:
    """Скопировать заголовки из src в dst, пропуская MIME-структурные."""
    skip = frozenset(
        {
            _HDR.CONTENT_TYPE.value.lower(),
            _HDR.CONTENT_TRANSFER_ENCODING.value.lower(),
            _HDR.MIME_VERSION.value.lower(),
            _HDR.CONTENT_DISPOSITION.value.lower(),
        }
    )
    for k, v in src.items():
        if k.lower() in skip:
            continue
        if k in dst:
            dst.add_header(k, v)
        else:
            dst[k] = v


def build_enriched_multipart(
    incoming: EmailMessage,
    *,
    user_message_text: str,
    graph_answer: EnrichGraphAnswerText | None,
    unified_mail_context: EnrichUnifiedMailContextText | None,
    thread_memory: EnrichThreadMemoryText | None,
    global_memory: EnrichGlobalMemoryText | None,
    stage: str,
    extra_parts: list[tuple[EnrichContentId, str]] | None = None,
) -> EmailMessage:
    """``multipart/mixed`` с MIME-частями по ``Content-ID`` (RFC 2045/2046).

    Каждый смысловой блок — отдельная ``text/plain`` part с
    ``Content-Disposition: inline`` и уникальным ``Content-ID``.
    """
    container = EmailMessage()
    container.make_mixed()
    _copy_envelope_headers(incoming, container)

    container.attach(
        _make_inline_text_part(EnrichPartId.USER_MESSAGE, user_message_text.strip())
    )

    _VO_PARTS: list[tuple[EnrichPartId, _EnrichOptionalText]] = [
        (EnrichPartId.GRAPH_ANSWER, graph_answer),
        (EnrichPartId.UNIFIED_MAIL_CONTEXT, unified_mail_context),
        (EnrichPartId.THREAD_MEMORY, thread_memory),
        (EnrichPartId.GLOBAL_MEMORY, global_memory),
    ]
    part_ids = [EnrichPartId.USER_MESSAGE.value]
    for pid, vo in _VO_PARTS:
        if vo is not None and vo.value:
            container.attach(_make_inline_text_part(pid, vo.value))
            part_ids.append(pid.value)

    if extra_parts:
        for cid, text in extra_parts:
            container.attach(_make_inline_text_part(cid, text))
            part_ids.append(cid.value)

    logger.bind(stage=stage).info("built_enriched_multipart", parts=part_ids)
    return container


def build_context_backpack_multipart(
    incoming: EmailMessage,
    *,
    user_message_text: str,
    graph_answer: EnrichGraphAnswerText | None,
    thread_memory: EnrichThreadMemoryText | None,
    global_memory: EnrichGlobalMemoryText | None,
    history_parts: Iterable[tuple["EnrichContentId", EmailMessage]] = (),
    stage: str,
    extra_parts: list[tuple["EnrichContentId", str]] | None = None,
) -> EmailMessage:
    """Гранулярный enrich-бэкпак для reasoning (заменяет merged ``<unified-mail-context>``).

    Mandatory leaf: ``<user-message>``, ``<graph-answer>``, ``<thread-memory>``,
    ``<global-memory>`` (если непусты) + ``extra_parts`` (``<response-state>``,
    ``<task-init>``, ``<task-state>``). Unified-история едет **гранулярными**
    ``<{hash}@history>`` leaf-частями (как ``enrich_fast`` splice), не одним блобом —
    reasoning читает их через :func:`iter_history_parts`. Дедуп по контент-CID.
    """
    container = EmailMessage()
    container.make_mixed()
    _copy_envelope_headers(incoming, container)

    container.attach(
        _make_inline_text_part(EnrichPartId.USER_MESSAGE, user_message_text.strip())
    )

    _VO_PARTS: list[tuple[EnrichPartId, _EnrichOptionalText]] = [
        (EnrichPartId.GRAPH_ANSWER, graph_answer),
        (EnrichPartId.THREAD_MEMORY, thread_memory),
        (EnrichPartId.GLOBAL_MEMORY, global_memory),
    ]
    part_ids = [EnrichPartId.USER_MESSAGE.value]
    for pid, vo in _VO_PARTS:
        if vo is not None and vo.value:
            container.attach(_make_inline_text_part(pid, vo.value))
            part_ids.append(pid.value)

    seen: set[EnrichContentId] = set()
    for cid, part in history_parts:
        if cid.is_core or cid in seen:
            continue
        container.attach(part)
        seen.add(cid)
        part_ids.append(cid.value)

    if extra_parts:
        for cid, text in extra_parts:
            container.attach(_make_inline_text_part(cid, text))
            part_ids.append(cid.value)

    logger.bind(stage=stage).info("built_context_backpack", parts=part_ids)
    return container


def _leaf_part_text(part: EmailMessage) -> str:
    """Декодированное тело leaf text/plain MIME-части."""
    raw = part.get_payload(decode=True)
    if isinstance(raw, bytes):
        return raw.decode(part.get_content_charset() or "utf-8", errors="replace")
    return "" if raw is None else str(raw)


def _iter_relay_leaf_parts(msg: EmailMessage) -> list[tuple[EnrichContentId, EmailMessage]]:
    """Leaf-части с ``Content-ID`` как ``(EnrichContentId, part)`` в порядке walk.

    Единая граница чтения ``Content-ID`` из MIME — выше неё бизнес-логика видит
    только VO, не сырые заголовки.
    """
    out: list[tuple[EnrichContentId, EmailMessage]] = []
    if not msg.is_multipart():
        return out
    for part in msg.walk():
        if part.is_multipart():
            continue
        cid = EnrichContentId.from_mime_part(part)  # type: ignore[arg-type]
        if cid is not None:
            out.append((cid, part))  # type: ignore[arg-type]
    return out


def history_part_text(part: EmailMessage) -> str:
    """Декодированное тело leaf ``<history>``-части (граница чтения MIME-тела)."""
    return _leaf_part_text(part)


def system_leaf_part_text(part: EmailMessage) -> str:
    """Декодированное тело leaf ``<system>``-части (граница чтения MIME-тела)."""
    return _leaf_part_text(part)


def part_origin_stage(part: EmailMessage) -> FsmStage | None:
    """``X-Threlium-Origin`` leaf-части → :class:`FsmStage`; иначе ``None``."""
    raw = part.get(MailHeaderName.ORIGIN.value)
    if not raw or not str(raw).strip():
        return None
    local = str(raw).strip().split("@", 1)[0]
    return FsmStage.try_from_mailbox(f"{local}@localhost")


def part_origin_label(part: EmailMessage) -> str:
    """Local-part ``X-Threlium-Origin`` leaf-части; иначе ``?``."""
    stage = part_origin_stage(part)
    return stage.value if stage is not None else "?"


def iter_history_parts(msg: EmailMessage) -> list[tuple[EnrichContentId, EmailMessage]]:
    """Leaf ``<history>``-части письма как ``(EnrichContentId, part)`` в порядке walk.

    Единый аксессор для всех потребителей (enrich/enrich_fast/reasoning/drain): семейство
    ``HISTORY`` по домену CID; per-part заголовки (score/origin) читаются вызывающим из
    ``part`` через VO. Core-части и чужие CID отфильтрованы.
    """
    return [
        (cid, part)
        for cid, part in _iter_relay_leaf_parts(msg)
        if cid.family is EnrichPartId.HISTORY
    ]


def concat_history_parts_text(msg: EmailMessage) -> str:
    """Конкатенация всех непустых ``<history>``-частей (§5/§7 CONTEXT_CONTRACT).

    Agent-facing текст письма с history — не ``get_body`` / первый text/plain.
  """
    chunks = [
        text
        for _cid, part in iter_history_parts(msg)
        if (text := history_part_text(part).strip())
    ]
    return "\n\n---\n\n".join(chunks)


def last_history_part_text(msg: EmailMessage) -> str:
    """Текст последней непустой ``<history>``-части (canonical user turn для enrich)."""
    parts = iter_history_parts(msg)
    last: str | None = None
    for _cid, part in parts:
        text = history_part_text(part).strip()
        if text:
            last = text
    if last is None:
        raise RuntimeError(
            "FSM-инвариант: ожидалась хотя бы одна непустая <history>-часть "
            "(ingress distill brief)"
        )
    return last


def message_has_history(msg: EmailMessage) -> bool:
    """Предикат «письмо несёт ≥1 непустую ``<history>``-часть» (= содержательное).

    Заменяет ``to_stage_in_unified_role``/``content_indexable_to_stage`` (классификацию по
    ``To:``): «сервисность» = отсутствие history-части, без таблицы стадий.
    """
    for _cid, part in iter_history_parts(msg):
        if _leaf_part_text(part).strip():
            return True
    return False


def iter_system_parts(msg: EmailMessage) -> list[tuple[EnrichContentId, EmailMessage]]:
    """Leaf ``<system>``-части письма как ``(EnrichContentId, part)`` (по контракту — одна)."""
    return [
        (cid, part)
        for cid, part in _iter_relay_leaf_parts(msg)
        if cid.family is EnrichPartId.SYSTEM
    ]


def message_has_system(msg: EmailMessage) -> bool:
    """Предикат «письмо несёт ≥1 непустую ``<system>``-часть» (payload-команду)."""
    for _cid, part in iter_system_parts(msg):
        if _leaf_part_text(part).strip():
            return True
    return False


def email_without_system_parts(msg: EmailMessage) -> EmailMessage:
    """Копия ``multipart/mixed`` без leaf ``<system>``-частей (ingress history-only relay)."""
    if not message_has_system(msg):
        return msg
    from copy import deepcopy

    if not msg.is_multipart():
        return msg
    kept: list[EmailMessage] = []
    for part in msg.get_payload():
        if not isinstance(part, EmailMessage):
            kept.append(part)
            continue
        cid = EnrichContentId.from_mime_part(part)
        if cid is not None and cid.family is EnrichPartId.SYSTEM:
            continue
        kept.append(deepcopy(part))
    if not kept:
        out = EmailMessage()
        out.set_content("", subtype="plain", charset="utf-8")
        return out
    out = EmailMessage()
    out.set_payload(kept)
    ct = msg.get(MailHeaderName.CONTENT_TYPE) or msg.get_content_type() or "multipart/mixed"
    out[MailHeaderName.CONTENT_TYPE] = ct
    mv = msg.get(MailHeaderName.MIME_VERSION)
    if mv:
        out[MailHeaderName.MIME_VERSION] = mv
    return out


def system_part_text(msg: EmailMessage) -> str:
    """Тело единственной ``<system>``-части — носитель payload-команды между стадиями.

    Строгая граница чтения payload (замена :func:`extract_plain_body` для всех внутренних
    чтений): стадии-потребители (cli_exec, egress_*, response_finalize, tool-входы,
    durable-редьюсеры) берут команду отсюда. Контракт — ровно одна ``<system>``-часть.

    Отсутствие ``<system>`` → ``RuntimeError`` (инвариант «payload только в ``<system>``»);
    несколько частей — тоже нарушение контракта.
    """
    parts = iter_system_parts(msg)
    if not parts:
        raise RuntimeError(
            "FSM-инвариант: ожидалась одна <system>-часть (носитель payload), не найдено "
            "ни одной. Производитель должен класть payload в system через "
            "build_fsm_step_to_stage(system=...)."
        )
    if len(parts) > 1:
        raise RuntimeError(
            f"FSM-инвариант: ожидалась ровно одна <system>-часть, найдено {len(parts)}."
        )
    return _leaf_part_text(parts[0][1])


def system_part_text_from_path(path: Path | str) -> str:
    """Тело единственной ``<system>``-части письма, прочитанного с диска (Maildir).

    Граница «байты с диска → payload» по контракту ``docs/CONTEXT_CONTRACT.md`` §2:
    ретроспективное чтение payload из FSM-писем (cli_resume intent, durable
    ``tasks_upsert`` / ``response_*`` при IRT-обходе) идёт через ``<system>``-CID, а **не**
    через :func:`extract_plain_body` (первый ``text/plain``). Fail-fast как у
    :func:`system_part_text`: отсутствие / >1 ``<system>`` → ``RuntimeError``.
    """
    from threlium.mail import email_message_from_path

    return system_part_text(email_message_from_path(path))


def extract_part_by_content_id(msg: EmailMessage, content_id: EnrichPartId) -> str | None:
    """Текст MIME-part с заданным ``Content-ID``, или ``None``."""
    target = EnrichContentId.from_part_id(content_id)
    for cid, part in _iter_relay_leaf_parts(msg):
        if cid == target:
            return _leaf_part_text(part)
    return None


def attach_user_query_part(
    out: EmailMessage,
    user_query: "EnrichUserQueryText",
) -> None:
    """Прикрепить фиксированную ``<user-query>``-часть (вход enrich@)."""
    from threlium.types.fsm_strings import EnrichUserQueryText

    if not isinstance(user_query, EnrichUserQueryText):
        raise TypeError("attach_user_query_part: expected EnrichUserQueryText")
    out.attach(
        _make_inline_text_part(EnrichPartId.USER_QUERY, user_query.value)
    )


def require_enrich_user_query_text(msg: EmailMessage) -> "EnrichUserQueryText":
    """Fail-fast: ровно одна непустая ``<user-query>``-часть на ``To: enrich@``."""
    from threlium.types.fsm_strings import EnrichUserQueryText

    target = EnrichContentId.from_part_id(EnrichPartId.USER_QUERY)
    found: list[str] = []
    for cid, part in _iter_relay_leaf_parts(msg):
        if cid == target:
            text = _leaf_part_text(part).strip()
            if text:
                found.append(text)
    if not found:
        raise RuntimeError(
            "FSM-инвариант: ожидалась ровно одна непустая <user-query>-часть "
            "(canonical user turn на To: enrich@), не найдено"
        )
    if len(found) > 1:
        raise RuntimeError(
            f"FSM-инвариант: ожидалась ровно одна <user-query>-часть, найдено {len(found)}"
        )
    return EnrichUserQueryText.require(name="user-query", raw=found[0])


@dataclass(frozen=True)
class RelaySpliceResult:
    """Итог ``splice_e_prev_with_history``: письмо + diff history-CID для логов."""

    message: EmailMessage
    appended: tuple[EnrichContentId, ...]
    skipped: tuple[EnrichContentId, ...]


def splice_e_prev_with_history(
    e_prev: EmailMessage,
    *,
    response_state_text: str,
    task_state_text: str | None = None,
    history_parts: Iterable[tuple[EnrichContentId, EmailMessage]] = (),
    system_parts: Iterable[tuple[EnrichContentId, EmailMessage]] = (),
) -> RelaySpliceResult:
    """``E_prev`` + сырые ``<history>`` / ``<system>`` из окна-дельты → multipart для reasoning.

    Stage-agnostic быстрый цикл ``enrich_fast``:

    * копирует части ``E_prev``, **кроме** ``@system`` (свежие system — только из дельты);
    * **пересобирает** ``<response-state>`` из ``response_state_text`` (CRDT) и — если
      передан ``task_state_text`` — ``<task-state>`` из него (детерминированный recompute);
    * **дописывает в хвост** ``history_parts`` и ``system_parts`` (append+dedup по CID).

    Дедуп по контенту: повторный ``Content-ID`` (= идентичное тело, оригинал и relay-копия)
    не добавляется и попадает в ``skipped``; новые — в ``appended``. Никакой ``To:``-логики:
    схлопывание чисто по ``EnrichContentId`` (контент-хеш).
    """
    out = EmailMessage()
    out.make_mixed()
    _copy_envelope_headers(e_prev, out)

    rs = EnrichContentId.from_part_id(EnrichPartId.RESPONSE_STATE)
    ts = EnrichContentId.from_part_id(EnrichPartId.TASK_STATE)
    seen: set[EnrichContentId] = set()
    replaced_rs = False
    replaced_ts = False

    for cid, part in _iter_relay_leaf_parts(e_prev):
        if cid.family is EnrichPartId.SYSTEM:
            continue
        if cid == rs:
            out.attach(_make_inline_text_part(EnrichPartId.RESPONSE_STATE, response_state_text))
            replaced_rs = True
        elif cid == ts and task_state_text is not None:
            out.attach(_make_inline_text_part(EnrichPartId.TASK_STATE, task_state_text))
            replaced_ts = True
        else:
            out.attach(part)
        seen.add(cid)

    if not replaced_rs:
        out.attach(_make_inline_text_part(EnrichPartId.RESPONSE_STATE, response_state_text))
        seen.add(rs)

    if task_state_text is not None and not replaced_ts:
        out.attach(_make_inline_text_part(EnrichPartId.TASK_STATE, task_state_text))
        seen.add(ts)

    appended: list[EnrichContentId] = []
    skipped: list[EnrichContentId] = []
    for cid, part in history_parts:
        if cid.is_core:
            continue
        if cid in seen:
            skipped.append(cid)
            continue
        out.attach(part)
        seen.add(cid)
        appended.append(cid)

    for cid, part in system_parts:
        if cid.is_core:
            continue
        if cid in seen:
            skipped.append(cid)
            continue
        out.attach(part)
        seen.add(cid)
        appended.append(cid)

    return RelaySpliceResult(message=out, appended=tuple(appended), skipped=tuple(skipped))


