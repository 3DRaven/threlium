"""FSM- и stage-специфичные strip-VO (не RFC, не мосты, не lightrag)."""
from __future__ import annotations

import msgspec

from typing import TYPE_CHECKING, Self

from ._core import _OptionalStripEmpty, _OptionalStripNone, _RequiredNonEmpty, _SingleLineHeaderWire

if TYPE_CHECKING:
    from threlium.types.ingress_distill import IngressExternalBodyText


class MessageIdHeaderNormalizationInput(_OptionalStripNone):
    """Сырое ``Message-ID`` после strip до нормализации в inner (см. ``NotmuchMessageIdInner.from_optional_raw``)."""


class ReasoningAssistantMessageText(_OptionalStripEmpty):
    """Текстовый ``content`` ответа ассистента litellm (reasoning)."""


class ReasoningToolRouteEmailSubject(_SingleLineHeaderWire):
    """Subject из ``tool_call`` маршрута reasoning."""


class ReasoningToolRouteEmailBody(_OptionalStripEmpty):
    """Тело из ``tool_call`` маршрута reasoning."""


class FsmPlainToStageSubjectLine(_SingleLineHeaderWire):
    """Subject входящего письма для ``build_fsm_plain_to_stage`` (ветка ``Re:``)."""


class FsmRePrefixedSubjectLine(_SingleLineHeaderWire):
    """Исходящий Subject ``Re: …`` для FSM-эмита (без ручного fold/unfold)."""

    @classmethod
    def from_plain_to_stage(cls, subj: FsmPlainToStageSubjectLine) -> Self:
        return cls.parse(f"Re: {subj.value}")


class OrphanNoticePrefixLine(_OptionalStripEmpty):
    """Первая строка prefix для orphan-notice в :mod:`threlium.states.ingress` (INDEX §8 Case 1)."""


class EnrichLightragQuestionSubjectLine(_OptionalStripEmpty):
    """Subject входа enrich как часть вопроса к LightRAG."""


class ReflectJinjaSubjectContext(_OptionalStripEmpty):
    """Subject входа для шаблонов ``reflect/*.j2``."""


class IngressRouterResolvedChannelSlug(_OptionalStripEmpty):
    """Канал из ``ResolvedRoute`` после union-lookup (egress_router)."""


class EnrichGraphAnswerText(_OptionalStripEmpty):
    """JSON envelope из LightRAG aquery для MIME-части ``<graph-answer>``."""


class EnrichUnifiedMailContextText(_OptionalStripEmpty):
    """Рендер хронологии треда + memory-писем для ``<unified-mail-context>``."""


class EnrichThreadMemoryText(_OptionalStripEmpty):
    """Рендер thread_memory-записей текущего треда для ``<thread-memory>``."""


class EnrichGlobalMemoryText(_OptionalStripEmpty):
    """Рендер global_memory-записей из всех тредов для ``<global-memory>``."""


class FsmTransitionPlainBody(_OptionalStripEmpty):
    """Тело text/plain для :func:`threlium.fsm_emit.build_fsm_plain_to_stage`."""


class FsmTransitionPlainSubjectLine(_SingleLineHeaderWire):
    """Исходящий Subject для ``build_fsm_plain_to_stage`` (strip + reject CRLF)."""


class EnrichObservationNoteText(_OptionalStripEmpty):
    """Текст MIME-части <observation-note> (formal_reason / memory_query → enrich_fast)."""


class EnrichUserQueryText(_OptionalStripEmpty):
    """Канонический сырой user turn в ``<user-query>`` CID на ``To: enrich@``.

    Не путать с :class:`~threlium.types.reasoning.ReasoningUserMessageText` — тот про
    отрендеренный ``<user-message>`` (envelope в Jinja enrich→reasoning).
    """

    @classmethod
    def require_value(cls, *, name: str, raw: str | None) -> Self:
        req = _RequiredNonEmpty.require(name=name, raw=raw)
        return cls.parse(req.value)

    @classmethod
    def from_external_body(cls, body: IngressExternalBodyText) -> Self:
        return cls.parse(body.value)


class EnrichCalleeHistoryText(_OptionalStripEmpty):
    """Собственный ``<history>``-ответ callee на переходе → enrich (error notice, …)."""


class EnrichRequestEchoText(_OptionalStripEmpty):
    """Тело request_echo (subagent_intent); callee решает, класть ли."""
