"""FSM- и stage-специфичные strip-VO (не RFC, не мосты, не lightrag)."""
from __future__ import annotations

import msgspec

from ._core import _OptionalStripEmpty, _OptionalStripNone


class MessageIdHeaderNormalizationInput(_OptionalStripNone):
    """Сырое ``Message-ID`` после strip до нормализации в inner (см. ``NotmuchMessageIdInner.from_optional_raw``)."""


class ReasoningAssistantMessageText(_OptionalStripEmpty):
    """Текстовый ``content`` ответа ассистента litellm (reasoning)."""


class ReasoningToolRouteEmailSubject(_OptionalStripEmpty):
    """Subject из ``tool_call`` маршрута reasoning."""


class ReasoningToolRouteEmailBody(_OptionalStripEmpty):
    """Тело из ``tool_call`` маршрута reasoning."""


class FsmPlainToStageSubjectLine(_OptionalStripEmpty):
    """Subject входящего письма для ``build_fsm_plain_to_stage`` (ветка ``Re:``)."""


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


class FsmTransitionPlainSubjectLine(_OptionalStripEmpty):
    """Исходящий Subject для ``build_fsm_plain_to_stage`` (strip на границе; длина для RFC822 режется в билдере)."""


class EnrichObservationNoteText(_OptionalStripEmpty):
    """Текст MIME-части <observation-note> (formal_reason / memory_query → enrich_fast)."""
