"""FSM-билдеры MIME: EmailMessage → EmailMessage (docs/FSM.md §5)."""
from __future__ import annotations


from collections.abc import Mapping
from email.message import EmailMessage
from email.utils import formatdate
from typing import Protocol, TypeAlias

from threlium.settings import ThreliumSettings
from threlium.mime_reform import (
    RELAY_FAMILIES,
    EnrichContentId,
    EnrichPartId,
    _make_inline_text_part,
)
from threlium.types import (
    CanonicalMidWire,
    FsmPlainToStageSubjectLine,
    FsmRePrefixedSubjectLine,
    FsmStage,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    HopBudgetLine,
    IrtHashWire,
    NotmuchMessageIdInner,
    RfcInReplyToWire,
    RfcMessageIdWire,
    ThreliumContentScoreWire,
    MailHeaderName,
)


class StateHandler(Protocol):
    """Контракт ``threlium.states.<stage>.main`` (keyword-only ``config`` — см. воркер)."""

    def __call__(
        self,
        msg: EmailMessage,
        stage: FsmStage,
        *,
        config: ThreliumSettings,
    ) -> EmailMessage | None: ...


HDR_ROUTE = MailHeaderName.ROUTE.value
HDR_HOP_BUDGET = MailHeaderName.HOP_BUDGET.value
HDR_FROM = MailHeaderName.FROM.value
HDR_TO = MailHeaderName.TO.value
HDR_SUBJECT = MailHeaderName.SUBJECT.value
HDR_DATE = MailHeaderName.DATE.value
HDR_MESSAGE_ID = MailHeaderName.MESSAGE_ID.value
HDR_IN_REPLY_TO = MailHeaderName.IN_REPLY_TO.value
HDR_IRT_HASH = MailHeaderName.IRT_HASH.value


ManagedFsmHeaderValue: TypeAlias = RfcInReplyToWire | HopBudgetLine
ManagedFsmHeaderPatch: TypeAlias = Mapping[MailHeaderName, ManagedFsmHeaderValue]


def _default_root_hop_max(settings: ThreliumSettings) -> int:
    return settings.hop.budget_root


def _default_sub_hop_max(settings: ThreliumSettings) -> int:
    return settings.hop.budget_sub


def advance_hop_budget_for_simple_step(line: HopBudgetLine, settings: ThreliumSettings) -> HopBudgetLine:
    """Декремент хвоста hop-стека: ``'48 44'`` → ``'48 43'``, ``'47'`` → ``'46'``.

    Тонкая обёртка над :meth:`HopBudgetLine.advance_simple_step` (арифметика — в VO, дефолт
    уровня — из settings).
    """
    return line.advance_simple_step(root_default=_default_root_hop_max(settings))


def push_subagent_hop_budget(line: HopBudgetLine, settings: ThreliumSettings) -> HopBudgetLine | None:
    """PUSH: декремент хвоста + append(sub_max). ``None`` если хвост после step < 1."""
    return line.push_subagent(
        root_default=_default_root_hop_max(settings),
        sub_max=_default_sub_hop_max(settings),
    )


def hop_budget_remaining(line: HopBudgetLine, settings: ThreliumSettings) -> int:
    """Оставшийся бюджет текущего уровня (хвост стека). ``0`` = исчерпан."""
    return line.remaining(root_default=_default_root_hop_max(settings))


def irt_wire_from_incoming_message_id(incoming: EmailMessage) -> RfcInReplyToWire | None:
    """``In-Reply-To`` из ``Message-ID`` входящего письма (эквивалент прежнего ``prev_mid`` в emit)."""
    mid_w = RfcMessageIdWire.parse_present_from_email(incoming, HDR_MESSAGE_ID)
    prev_mid = _msgid_normalized(mid_w.value if mid_w is not None else None)
    return RfcInReplyToWire.parse_present_optional(prev_mid) if prev_mid else None


def _msgid_normalized(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    if s.startswith("<") and s.endswith(">"):
        return s
    return f"<{s.strip('<> ')}>"


def _apply_managed_headers(
    out: EmailMessage,
    managed_headers: Mapping[MailHeaderName, ManagedFsmHeaderValue] | None,
) -> None:
    """Записать только переданные managed VO (без дефолтов с входа)."""
    if not managed_headers:
        return
    for name, vo in managed_headers.items():
        if name == MailHeaderName.IN_REPLY_TO:
            if not isinstance(vo, RfcInReplyToWire):
                raise TypeError(
                    f"{MailHeaderName.IN_REPLY_TO} expects RfcInReplyToWire, got {type(vo).__name__}"
                )
            if vo.value.strip():
                irt_hdr = _msgid_normalized(vo.value)
                if irt_hdr:
                    out[HDR_IN_REPLY_TO] = irt_hdr
                    out[HDR_IRT_HASH] = IrtHashWire.from_irt_header_value(irt_hdr).value
        elif name == MailHeaderName.HOP_BUDGET:
            if not isinstance(vo, HopBudgetLine):
                raise TypeError(
                    f"{MailHeaderName.HOP_BUDGET} expects HopBudgetLine, got {type(vo).__name__}"
                )
            if vo.value.strip():
                out[HDR_HOP_BUDGET] = vo.value
        else:
            raise ValueError(f"unsupported managed header key: {name!r}")


def emit_transition_preserving_payload(
    incoming: EmailMessage,
    *,
    to_addr: FsmStage,
    from_stage: FsmStage,
    managed_headers: ManagedFsmHeaderPatch | None = None,
    request_echo: str | None = None,
    settings: ThreliumSettings | None = None,
) -> EmailMessage:
    """Новое RFC822 с тем же MIME-телом, что у входа; обновляет envelope и managed по карте.

    Whitelist-подход: в новом письме только заголовки из
    :meth:`MailHeaderName.propagate_from_incoming` (Subject) + явно пересобранные
    (From, To, Date, Message-ID) + записанные из ``managed_headers``
    (``MailHeaderName`` → VO). Дефолты «IRT из MID входа», «advance hop»
    не применяются — используйте обёртки в :mod:`threlium.fsm_emit_semantic`.

    ``request_echo`` — как в :func:`build_fsm_step_to_stage`: прикрепить ДОПОЛНИТЕЛЬНУЮ
    ``<hash@history>``-часть (эхо входящего запроса) к сохранённому payload, предзаштамповав
    ``X-Threlium-Origin = incoming From`` (истинный автор запроса — вызывающий). Нужно стадиям,
    которые релеят payload, но обязаны положить запрос в долгую память на своей границе
    (напр. ``subagent_intent``: иначе задача субагенту изолируется фильтром фрейма и теряется
    из истории родителя). Требует ``settings`` (для ``score_for``)."""
    # --- Body ---
    out = EmailMessage()
    payload = incoming.get_payload(decode=False)
    if incoming.is_multipart() and isinstance(payload, list):
        out.set_payload(payload)
        ct = incoming.get(MailHeaderName.CONTENT_TYPE) or incoming.get_content_type() or "multipart/mixed"
        out[MailHeaderName.CONTENT_TYPE] = ct
        mv = incoming.get(MailHeaderName.MIME_VERSION)
        if mv:
            out[MailHeaderName.MIME_VERSION] = mv
    else:
        raw = incoming.get_payload(decode=True)
        if isinstance(raw, bytes):
            charset = incoming.get_content_charset() or "utf-8"
            subtype = (incoming.get_content_subtype() or "plain").lower()
            out.set_content(raw.decode(charset, errors="replace"), subtype=subtype, charset=charset)
        else:
            out.set_content("" if payload is None else str(payload), subtype="plain", charset="utf-8")

    # --- Propagate whitelist (Subject) ---
    for hdr in MailHeaderName.propagate_from_incoming():
        v = incoming.get(hdr)
        if v is not None:
            out[hdr] = v

    # --- Rebuilt (envelope) ---
    out[HDR_FROM] = from_stage.rfc822_mailbox
    out[HDR_TO] = to_addr.rfc822_mailbox
    out[HDR_DATE] = formatdate(localtime=True)
    mid_new = RfcMessageIdWire.internal_for_fsm()
    CanonicalMidWire.assert_from_wire(mid_new)
    out[HDR_MESSAGE_ID] = mid_new.value

    if request_echo is not None and request_echo.strip():
        if settings is None:
            raise ValueError("emit_transition_preserving_payload: request_echo requires settings")
        echo = request_echo
        echo_origin = FsmStage.try_from_mailbox(incoming.get(HDR_FROM))
        echo_score_stage = echo_origin if echo_origin is not None else from_stage
        echo_score = ThreliumContentScoreWire.from_score(
            settings.history.score_for(echo_score_stage)
        )
        out.attach(
            _make_inline_text_part(
                EnrichContentId.from_history_body(echo),
                echo,
                score=echo_score,
                origin=echo_origin,
            )
        )

    _apply_managed_headers(out, managed_headers)

    return out


def build_fsm_plain_to_stage(
    incoming: EmailMessage,
    *,
    to_addr: FsmStage,
    from_stage: FsmStage,
    body: FsmTransitionPlainBody,
    subject_line: FsmTransitionPlainSubjectLine | None = None,
    message_id_wire: RfcMessageIdWire | None = None,
    settings: ThreliumSettings,
) -> EmailMessage:
    """Новое письмо на ``to_addr`` с body-payload в одной ``<system>``-части
    (``docs/FSM.md`` §5.1). Канал определяется в ``egress_router`` по ``X-Threlium-Route``.

    Контракт ``<system>``: голое тело больше не носитель payload — ``body`` едет
    ``multipart/mixed`` с единственной ``<system>``-частью (CID ``<{sha256(body)}@system>``),
    которую потребитель читает через :func:`threlium.mime_reform.system_part_text`. Часть
    ``text/plain; inline``, поэтому ``get_body``/``extract_plain_body`` её тоже находят.

    ``message_id_wire``: опционально для стадий, которым нужен заранее известный
    ``Message-ID``.
    """
    mid_w = RfcMessageIdWire.parse_present_from_email(incoming, HDR_MESSAGE_ID)
    irt_inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    irt = irt_inner.as_angle_bracket_header() if irt_inner is not None else None
    subj = FsmPlainToStageSubjectLine.parse(incoming.get(HDR_SUBJECT))

    out_subj = (
        subject_line.value
        if subject_line is not None
        else FsmRePrefixedSubjectLine.from_plain_to_stage(subj).value
    )
    out = EmailMessage()
    out.make_mixed()
    out[HDR_FROM] = from_stage.rfc822_mailbox
    out[HDR_TO] = to_addr.rfc822_mailbox
    out[HDR_SUBJECT] = out_subj
    out[HDR_DATE] = formatdate(localtime=True)
    mid_new = message_id_wire if message_id_wire is not None else RfcMessageIdWire.internal_for_fsm()
    CanonicalMidWire.assert_from_wire(mid_new)
    out[HDR_MESSAGE_ID] = mid_new.value
    if irt:
        out[HDR_IN_REPLY_TO] = irt
        out[HDR_IRT_HASH] = IrtHashWire.from_irt_header_value(irt).value
    system_body = body.value.strip()
    out.attach(_make_inline_text_part(EnrichContentId.from_system_body(system_body), system_body))

    out[HDR_HOP_BUDGET] = advance_hop_budget_for_simple_step(
        HopBudgetLine.parse_from_email(incoming), settings
    ).value

    return out


def build_fsm_multipart_to_stage(
    incoming: EmailMessage,
    *,
    to_addr: FsmStage,
    from_stage: FsmStage,
    parts: list[tuple[EnrichPartId, str]],
    settings: ThreliumSettings,
) -> EmailMessage:
    """Новое multipart/mixed письмо с Content-ID частями на ``to_addr``.

    Тредовые заголовки — от входа (как ``build_fsm_plain_to_stage``).
    ``parts``: список ``(EnrichPartId, text)`` — каждая пара становится
    inline text/plain MIME-частью с ``Content-ID``.

    Relay-семейства (:data:`RELAY_FAMILIES`) получают **уникальный** CID
    ``<family@{inner-mid}>`` от ``Message-ID`` входящего письма — так повторные
    хопы одной стадии накапливаются в ``enrich_fast``, а не затирают друг друга.
    """
    mid_w = RfcMessageIdWire.parse_present_from_email(incoming, HDR_MESSAGE_ID)
    irt_inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    irt = irt_inner.as_angle_bracket_header() if irt_inner is not None else None
    subj = FsmPlainToStageSubjectLine.parse(incoming.get(HDR_SUBJECT))

    out = EmailMessage()
    out.make_mixed()
    out[HDR_FROM] = from_stage.rfc822_mailbox
    out[HDR_TO] = to_addr.rfc822_mailbox
    out[HDR_SUBJECT] = FsmRePrefixedSubjectLine.from_plain_to_stage(subj).value
    out[HDR_DATE] = formatdate(localtime=True)
    mid_new = RfcMessageIdWire.internal_for_fsm()
    CanonicalMidWire.assert_from_wire(mid_new)
    out[HDR_MESSAGE_ID] = mid_new.value
    if irt:
        out[HDR_IN_REPLY_TO] = irt
        out[HDR_IRT_HASH] = IrtHashWire.from_irt_header_value(irt).value

    for part_id, text in parts:
        if irt_inner is not None and part_id in RELAY_FAMILIES:
            content_id = EnrichContentId.from_relay(part_id, irt_inner)
        else:
            content_id = EnrichContentId.from_part_id(part_id)
        out.attach(_make_inline_text_part(content_id, text))

    out[HDR_HOP_BUDGET] = advance_hop_budget_for_simple_step(
        HopBudgetLine.parse_from_email(incoming), settings
    ).value

    return out


def build_fsm_step_to_stage(
    incoming: EmailMessage,
    *,
    to_addr: FsmStage,
    from_stage: FsmStage,
    history: str | None = None,
    request_echo: str | None = None,
    system: str | None = None,
    subject_line: FsmTransitionPlainSubjectLine | None = None,
    settings: ThreliumSettings,
) -> EmailMessage:
    """Единый choke-point эмита FSM-шага: 0+ ``<history>`` + 0/1 ``<system>``.

    ``history`` — содержательный текст для долгой памяти агента (ОТВЕТ стадии-источника):
    становится одной ``<history>``-частью с контент-адресным CID ``<{sha256(body)}@history>``
    и per-part заголовком ``X-Threlium-Content-Score`` = ``settings.history.score_for(from_stage)``
    (скоринг отправителя; origin НЕ ставится — его проставит ``enrich_fast`` из конвертного
    ``From:`` = ``from_stage``). Пустой/``None`` history → часть не добавляется.

    ``request_echo`` — модель «callee владеет историей»: ЭХО входящего запроса (что у стадии
    спросили), которое стадия решает положить в память НАРЯДУ со своим ответом. Становится
    ОТДЕЛЬНОЙ ``<history>``-частью (другое тело → другой ``<{hash}@history>``, дедуп не
    схлопывает с ``history``). Истинный автор запроса — вызывающий, поэтому ``X-Threlium-Origin``
    **предзаштамповывается** здесь из конвертного ``From:`` входящего письма (а не enrich_fast):
    enrich_fast трогает только части без origin. Score — ``score_for`` стадии-автора запроса.

    ``system`` — единственная payload-команда для принимающей стадии (заменяет голое тело
    письма): становится одной ``<system>``-частью с контент-адресным CID
    ``<{sha256(body)}@system>`` (без score/origin, не релеится). Потребитель читает её
    через :func:`threlium.mime_reform.system_part_text`. Любая половина опускаема.

    ``subject_line`` — кастомный Subject (например route-эмит reasoning); иначе ``Re: <subj>``.

    Тредовые заголовки/конверт/hop — как :func:`build_fsm_multipart_to_stage`.
    """
    mid_w = RfcMessageIdWire.parse_present_from_email(incoming, HDR_MESSAGE_ID)
    irt_inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    irt = irt_inner.as_angle_bracket_header() if irt_inner is not None else None
    subj = FsmPlainToStageSubjectLine.parse(incoming.get(HDR_SUBJECT))

    out = EmailMessage()
    out.make_mixed()
    out[HDR_FROM] = from_stage.rfc822_mailbox
    out[HDR_TO] = to_addr.rfc822_mailbox
    out[HDR_SUBJECT] = (
        subject_line.value
        if subject_line is not None
        else FsmRePrefixedSubjectLine.from_plain_to_stage(subj).value
    )
    out[HDR_DATE] = formatdate(localtime=True)
    mid_new = RfcMessageIdWire.internal_for_fsm()
    CanonicalMidWire.assert_from_wire(mid_new)
    out[HDR_MESSAGE_ID] = mid_new.value
    if irt:
        out[HDR_IN_REPLY_TO] = irt
        out[HDR_IRT_HASH] = IrtHashWire.from_irt_header_value(irt).value

    if history is not None and history.strip():
        body = history
        score = ThreliumContentScoreWire.from_score(settings.history.score_for(from_stage))
        out.attach(
            _make_inline_text_part(
                EnrichContentId.from_history_body(body), body, score=score
            )
        )

    if request_echo is not None and request_echo.strip():
        echo = request_echo
        echo_origin = FsmStage.try_from_mailbox(incoming.get(HDR_FROM))
        echo_score_stage = echo_origin if echo_origin is not None else from_stage
        echo_score = ThreliumContentScoreWire.from_score(
            settings.history.score_for(echo_score_stage)
        )
        out.attach(
            _make_inline_text_part(
                EnrichContentId.from_history_body(echo),
                echo,
                score=echo_score,
                origin=echo_origin,
            )
        )

    # system привязан к контракту «payload только в <system>»: прикрепляем, как только
    # значение передано (даже пустую строку — напр. finalize из буфера без inline-контента),
    # иначе потребитель system_part_text fail-fast. None → стадия history-only (To: enrich_fast).
    if system is not None:
        out.attach(
            _make_inline_text_part(EnrichContentId.from_system_body(system), system)
        )

    out[HDR_HOP_BUDGET] = advance_hop_budget_for_simple_step(
        HopBudgetLine.parse_from_email(incoming), settings
    ).value

    return out
