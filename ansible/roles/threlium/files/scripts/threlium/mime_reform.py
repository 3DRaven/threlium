"""Разбор/сборка MIME для мостов и стадий — поверх stdlib ``email``."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from email import policy
from email.message import EmailMessage, Message
from email.parser import BytesParser
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self, TypeAlias

import msgspec

from threlium.logutil import logger
from threlium.mail_header_names import MailHeaderName

if TYPE_CHECKING:
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
    RESPONSE_OBSERVATION = "<response-observation>"
    MEMORY_NOTE = "<memory-note>"
    OBSERVATION_NOTE = "<observation-note>"


# Семейства relay-частей (источник: вспомогательные стадии → enrich_fast → reasoning).
# Каждый хоп штампует уникальный CID ``<{family}@{inner-mid}>`` (EnrichContentId.from_relay),
# поэтому повторные вызовы одной стадии не затирают друг друга, а накапливаются.
# ``RESPONSE_OBSERVATION`` (бывш. ``PLAN_STATE``) — нарратив-обзор буфера ответа + задач
# от ``response_observe``.
RELAY_FAMILIES: Final[tuple[EnrichPartId, ...]] = (
    EnrichPartId.OBSERVATION_NOTE,
    EnrichPartId.RESPONSE_OBSERVATION,
    EnrichPartId.MEMORY_NOTE,
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
    """``EnrichPartId.OBSERVATION_NOTE`` → ``'observation-note'`` (без угловых скобок)."""
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
    def from_mime_part(cls, part: EmailMessage) -> Self | None:
        """Граница: ``Content-ID`` leaf-части после strip; отсутствие/пусто → ``None``."""
        raw = part.get("Content-ID")
        if raw is None:
            return None
        cleaned = str(raw).strip()
        return cls(value=cleaned) if cleaned else None

    @property
    def family(self) -> EnrichPartId | None:
        """Семейство relay по префиксу до первого ``@``; ``None`` для core/чужих.

        Бэк-компат: канонический ``<observation-note>`` (без суффикса) — то же семейство.
        """
        base_token = self.value.strip().strip("<>").split("@", 1)[0]
        for fam in RELAY_FAMILIES:
            if base_token == _cid_token(fam):
                return fam
        return None

    @property
    def is_core(self) -> bool:
        """Фиксированная часть полного ``enrich`` (не relay-хвост)."""
        return any(self.value == p.value for p in _CORE_PART_IDS)

# Единая политика сериализации RFC822 для приложения: ``email.policy.SMTP`` с
# ``max_line_length=0`` (без refold длинных заголовков) и ``linesep`` как Unix LF.
# Используется для fdm stdin / ``notmuch insert``, handoff движка, egress msmtp prep,
# round-trip в :func:`canonicalize_mime` (см. docs/INDEX.md §4 — ранее ``reformail -c``).
RFC822_FOR_INSERT: Final = policy.SMTP.clone(max_line_length=0, linesep="\n")

_PARSE_RFC822: Final = policy.default.clone(max_line_length=0)


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


def _make_inline_text_part(content_id: EnrichPartId | EnrichContentId, text: str) -> EmailMessage:
    """MIME text/plain part с Content-ID и Content-Disposition: inline.

    ``content_id`` — семейный enum (``EnrichPartId``) или VO ``EnrichContentId``
    (например уникальный relay-CID ``<observation-note@…>``); оба несут ``.value``.
    """
    part = EmailMessage()
    part.set_content(text, subtype="plain", charset="utf-8")
    part.add_header("Content-Disposition", "inline")
    part.replace_header("Content-Type", "text/plain; charset=\"utf-8\"")
    part["Content-ID"] = content_id.value
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


def group_relay_notes_by_family(msg: EmailMessage) -> dict[EnrichPartId, list[str]]:
    """Тексты relay-частей, сгруппированные по семейству (oldest-first, как в MIME)."""
    grouped: dict[EnrichPartId, list[str]] = {fam: [] for fam in RELAY_FAMILIES}
    for cid, part in _iter_relay_leaf_parts(msg):
        fam = cid.family
        if fam is None:
            continue
        text = _leaf_part_text(part).strip()
        if text:
            grouped[fam].append(text)
    return grouped


def collect_relay_parts_of_families(
    msg: EmailMessage, families: Iterable[EnrichPartId]
) -> list[tuple[EnrichContentId, str]]:
    """Relay-части указанных семейств как ``(EnrichContentId, text)`` (CID сохраняется)."""
    wanted = frozenset(families)
    out: list[tuple[EnrichContentId, str]] = []
    for cid, part in _iter_relay_leaf_parts(msg):
        if cid.family in wanted:
            text = _leaf_part_text(part).strip()
            if text:
                out.append((cid, text))
    return out


def extract_part_by_content_id(msg: EmailMessage, content_id: EnrichPartId) -> str | None:
    """Текст MIME-part с заданным ``Content-ID``, или ``None``."""
    target = EnrichContentId.from_part_id(content_id)
    for cid, part in _iter_relay_leaf_parts(msg):
        if cid == target:
            return _leaf_part_text(part)
    return None


@dataclass(frozen=True)
class RelaySpliceResult:
    """Итог ``splice_e_prev_with_incoming_relay``: письмо + diff relay-CID для логов."""

    message: EmailMessage
    appended: tuple[EnrichContentId, ...]
    skipped: tuple[EnrichContentId, ...]


def splice_e_prev_with_incoming_relay(
    e_prev: EmailMessage,
    incoming: EmailMessage,
    *,
    response_state_text: str,
    task_state_text: str | None = None,
) -> RelaySpliceResult:
    """``E_prev`` + relay-части входящего письма → новый multipart для reasoning.

    Stage-agnostic быстрый цикл ``enrich_fast``:

    * копирует все части ``E_prev``;
    * **пересобирает** ``<response-state>`` из ``response_state_text`` (CRDT) и — если
      передан ``task_state_text`` — ``<task-state>`` из него (детерминированный recompute);
    * **дописывает в хвост** relay-части ``incoming`` (не core, см.
      :meth:`EnrichContentId.is_core`) — как есть, с их оригинальным ``Content-ID``.

    Дубли по ``Content-ID`` не добавляются (идемпотентность при повторном проходе) и
    попадают в ``skipped``; новые — в ``appended``. Уникальность relay-CID
    (``<family@inner-mid>``) гарантирует, что каждый новый хоп — отдельная часть, а
    не перезапись предыдущей.
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
    for cid, part in _iter_relay_leaf_parts(incoming):
        if cid.is_core:
            continue
        if cid in seen:
            skipped.append(cid)
            continue
        out.attach(part)
        seen.add(cid)
        appended.append(cid)

    return RelaySpliceResult(message=out, appended=tuple(appended), skipped=tuple(skipped))


def canonicalize_mime(msg: EmailMessage) -> EmailMessage:
    """Round-trip MIME средствами stdlib ``email``.

    Сериализация :data:`RFC822_FOR_INSERT` (Unix LF, без refold длинных строк) →
    парсинг ``policy.default``. Заменяет прежний
    ``reformime -r -s0``: без subprocess, без внешних бинарников.

    Эквивалентно типичному use-case на наших данных, где сообщение уже
    распарсено либо воркером (``parse_rfc822``), либо ``BytesParser(default)``
    (мост ``bridges.email`` — long-running IMAP IDLE bridge).
    """
    return BytesParser(policy=policy.default).parsebytes(
        msg.as_bytes(policy=RFC822_FOR_INSERT)
    )  # type: ignore[return-value]


def parse_rfc822(data: bytes) -> EmailMessage:
    """Разбор байт → EmailMessage (policy.default + ``max_line_length=0`` на парсе)."""
    return BytesParser(policy=_PARSE_RFC822).parsebytes(data)  # type: ignore[return-value]


def email_message_from_bytes(data: bytes) -> EmailMessage:
    """Алиас :func:`parse_rfc822` для явной границы «байты → полное письмо»."""
    return parse_rfc822(data)


def email_message_from_path(path: Path | str) -> EmailMessage:
    """Один проход ``read_bytes`` + :func:`parse_rfc822` (runner'ы, pending)."""
    return parse_rfc822(Path(path).read_bytes())


