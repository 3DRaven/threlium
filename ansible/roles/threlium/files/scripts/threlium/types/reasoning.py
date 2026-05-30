"""Доменные типы стадии ``reasoning`` (``docs/TYPES.md`` уровни 2–3).

Границы:
* входящее письмо → :class:`ReasoningIncomingEnvelope` / :class:`ReasoningEnrichContext`;
* ответ LiteLLM tool_call → :class:`ReasoningToolFunctionName`, :class:`ReasoningToolCallArgumentsWire`;
* решение → :class:`ReasoningRouteDecision` (целевая :class:`~threlium.types.fsm_stage.FsmStage` + email VO).
"""
from __future__ import annotations

from email.message import EmailMessage
from typing import Self

import msgspec
from litellm.types.utils import ChatCompletionMessageToolCall, Message, ModelResponse

from threlium.mime_reform import (
    EnrichPartId,
    extract_part_by_content_id,
    history_part_text,
    iter_history_parts,
)

from ._core import _OptionalStripEmpty, _RequiredNonEmpty
from .fsm_strings import (
    EnrichGlobalMemoryText,
    EnrichGraphAnswerText,
    EnrichThreadMemoryText,
    EnrichUnifiedMailContextText,
    ReasoningAssistantMessageText,
    ReasoningToolRouteEmailBody,
    ReasoningToolRouteEmailSubject,
)
from .hop_cap import HopBudgetLine
from .rfc import RfcFromWire, RfcInReplyToWire, RfcMessageIdWire, RfcReferencesWire, RfcSubjectWire
from threlium.mail_header_names import MailHeaderName

from .fsm_stage import FsmStage
from .reasoning_routes import REASONING_TARGET_STAGES

_HDR = MailHeaderName


class ReasoningUserMessageText(_OptionalStripEmpty):
    """Текст MIME-части ``<user-message>`` для промпта reasoning."""


class ReasoningResponseStateText(_OptionalStripEmpty):
    """Текст MIME-части ``<response-state>`` для промпта reasoning."""


class ReasoningTaskStateText(_OptionalStripEmpty):
    """Текст MIME-части ``<task-state>`` для промпта reasoning."""


class ReasoningToolFunctionName(_RequiredNonEmpty):
    """Имя ``function.name`` из tool_call LiteLLM; совпадает с local-part целевой FSM-стадии."""

    def target_stage(self) -> FsmStage:
        stage = FsmStage.parse(self.value)
        if stage not in REASONING_TARGET_STAGES:
            raise RuntimeError(
                f"reasoning: unknown tool route {self.value!r} "
                f"(not in REASONING_TARGET_STAGES)"
            )
        return stage

    @classmethod
    def parse_tool_call(cls, tc: ChatCompletionMessageToolCall) -> Self:
        func = tc.function
        if func is None or not func.name:
            raise RuntimeError("reasoning: tool_call without function.name")
        return cls.require(name="function.name", raw=func.name)


class ReasoningToolCallArgumentsWire(_OptionalStripEmpty):
    """Сырой JSON аргументов tool_call (wire до jsonschema / msgspec)."""

    @classmethod
    def from_tool_call(cls, tc: ChatCompletionMessageToolCall) -> Self:
        func = tc.function
        if func is None:
            raise RuntimeError("reasoning: tool_call without function")
        raw = func.arguments
        if isinstance(raw, bytes):
            return cls.parse(raw.decode("utf-8", errors="replace"))
        return cls.parse(raw if isinstance(raw, str) else "")


class ReasoningIncomingEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """Заголовки входящего письма для блока ``<envelope>`` в ``reasoning/user.j2``."""

    message_id: RfcMessageIdWire | None
    in_reply_to: RfcInReplyToWire | None
    references: RfcReferencesWire | None
    subject: RfcSubjectWire | None
    from_hdr: RfcFromWire | None
    hop_budget: HopBudgetLine

    @classmethod
    def from_email(cls, msg: EmailMessage, *, hop_budget: HopBudgetLine) -> Self:
        return cls(
            message_id=RfcMessageIdWire.parse_present_from_email(msg, _HDR.MESSAGE_ID),
            in_reply_to=RfcInReplyToWire.parse_present_from_email(msg, _HDR.IN_REPLY_TO),
            references=RfcReferencesWire.parse_present_from_email(msg, _HDR.REFERENCES),
            subject=RfcSubjectWire.parse_present_from_email(msg, _HDR.SUBJECT),
            from_hdr=RfcFromWire.parse_present_from_email(msg, _HDR.FROM),
            hop_budget=hop_budget,
        )


class ReasoningHistoryEntry(msgspec.Struct, frozen=True, kw_only=True):
    """Одна ``<history>``-часть для хронологического стрима в ``reasoning/user.j2``.

    После унификации виды (observation/memory/plan) в CID не кодируются: семантику
    несёт ``origin`` (стадия-источник из ``X-Threlium-Origin``, штампует enrich_fast)
    плюс tool spec, по которому модель знает каждую стадию. Дедуп по контент-адресному
    CID уже выполнен в splice — здесь только текст + метка источника.
    """

    origin: str
    text: str


def _origin_label(part: EmailMessage) -> str:
    """Метка источника ``<history>``-части: local-part ``X-Threlium-Origin`` или ``?``."""
    raw = part.get(_HDR.ORIGIN.value)
    if raw and str(raw).strip():
        return str(raw).strip().split("@", 1)[0]
    return "?"


def _budget_history(entries: list[ReasoningHistoryEntry], max_chars: int) -> list[ReasoningHistoryEntry]:
    """Урезание стрима по суммарному телу с хвоста (новейшее остаётся)."""
    if max_chars <= 0 or not entries:
        return entries
    kept: list[ReasoningHistoryEntry] = []
    total = 0
    for entry in reversed(entries):
        if kept and total + len(entry.text) > max_chars:
            break
        kept.append(entry)
        total += len(entry.text)
    kept.reverse()
    return kept


def _extract_context_part_vo(
    vo_type: type[_OptionalStripEmpty],
    msg: EmailMessage,
    part_id: EnrichPartId,
    max_chars: int,
) -> _OptionalStripEmpty | None:
    from threlium.enrich_context import trim_context_text

    raw = extract_part_by_content_id(msg, part_id)
    if raw is None:
        return None
    trimmed = trim_context_text(raw.strip(), max_chars)
    if not trimmed:
        return None
    parsed = vo_type.parse(trimmed)
    return parsed if parsed.value else None


class ReasoningEnrichContext(msgspec.Struct, frozen=True, kw_only=True):
    """MIME-контекст enrich для ``reasoning/user.j2`` (сборка на границе, не в роутере)."""

    user_message: ReasoningUserMessageText | None
    knowledge_graph: EnrichGraphAnswerText | None
    mail_context: EnrichUnifiedMailContextText | None
    thread_memory: EnrichThreadMemoryText | None
    global_memory: EnrichGlobalMemoryText | None
    response_state: ReasoningResponseStateText | None
    task_state: ReasoningTaskStateText | None
    history: tuple[ReasoningHistoryEntry, ...]

    @classmethod
    def from_email(cls, msg: EmailMessage, *, max_chars: int) -> Self:
        entries: list[ReasoningHistoryEntry] = []
        for _cid, part in iter_history_parts(msg):
            text = history_part_text(part).strip()
            if not text:
                continue
            entries.append(ReasoningHistoryEntry(origin=_origin_label(part), text=text))
        history = tuple(_budget_history(entries, max_chars))
        return cls(
            user_message=_extract_context_part_vo(
                ReasoningUserMessageText, msg, EnrichPartId.USER_MESSAGE, max_chars
            ),
            knowledge_graph=_extract_context_part_vo(
                EnrichGraphAnswerText, msg, EnrichPartId.GRAPH_ANSWER, max_chars
            ),
            mail_context=_extract_context_part_vo(
                EnrichUnifiedMailContextText, msg, EnrichPartId.UNIFIED_MAIL_CONTEXT, max_chars
            ),
            thread_memory=_extract_context_part_vo(
                EnrichThreadMemoryText, msg, EnrichPartId.THREAD_MEMORY, max_chars
            ),
            global_memory=_extract_context_part_vo(
                EnrichGlobalMemoryText, msg, EnrichPartId.GLOBAL_MEMORY, max_chars
            ),
            response_state=_extract_context_part_vo(
                ReasoningResponseStateText, msg, EnrichPartId.RESPONSE_STATE, max_chars
            ),
            task_state=_extract_context_part_vo(
                ReasoningTaskStateText, msg, EnrichPartId.TASK_STATE, max_chars
            ),
            history=history,
        )


class ReasoningRouteDecision(msgspec.Struct, frozen=True, kw_only=True):
    """Исход reasoning: целевая стадия и тело/subject исходящего plain-письма."""

    target: FsmStage
    subject: ReasoningToolRouteEmailSubject
    body: ReasoningToolRouteEmailBody

    @classmethod
    def from_rendered(
        cls,
        target: FsmStage,
        *,
        subject: str,
        body: str,
    ) -> Self:
        return cls(
            target=target,
            subject=ReasoningToolRouteEmailSubject.parse(subject),
            body=ReasoningToolRouteEmailBody.parse(body),
        )


def reasoning_assistant_message(resp: ModelResponse) -> Message:
    choices = resp.choices or []
    if not choices:
        raise RuntimeError("reasoning: empty litellm choices")
    msg = choices[0].message
    if msg is None:
        raise RuntimeError("reasoning: litellm choice without message")
    return msg


def reasoning_finish_reason(resp: ModelResponse) -> str | None:
    choices = resp.choices or []
    if not choices:
        return None
    fr = choices[0].finish_reason
    if fr is None:
        return None
    return str(fr)


def reasoning_first_tool_call(msg: Message) -> ChatCompletionMessageToolCall | None:
    tcs = msg.tool_calls
    if not tcs:
        return None
    return tcs[0]


def reasoning_assistant_plain_text(msg: Message) -> ReasoningAssistantMessageText:
    content = msg.content
    if isinstance(content, str):
        return ReasoningAssistantMessageText.parse(content)
    return ReasoningAssistantMessageText.parse(None)


__all__ = [
    "ReasoningEnrichContext",
    "ReasoningHistoryEntry",
    "ReasoningIncomingEnvelope",
    "ReasoningResponseStateText",
    "ReasoningRouteDecision",
    "ReasoningTaskStateText",
    "ReasoningToolCallArgumentsWire",
    "ReasoningToolFunctionName",
    "ReasoningUserMessageText",
    "reasoning_assistant_message",
    "reasoning_assistant_plain_text",
    "reasoning_finish_reason",
    "reasoning_first_tool_call",
]
