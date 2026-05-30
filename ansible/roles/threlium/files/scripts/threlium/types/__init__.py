"""Доменные строки Threlium: msgspec ``Struct`` + ``Annotated`` + единая нормализация.

Правило **strip до ``msgspec.convert``** (уровень 1, ``docs/TYPES.md``): экстракторы
кладут в словарь только непустые строки после ``strip``; отсутствие ключа даёт
``default`` поля либо ``msgspec.ValidationError`` для ``NonEmptyStr``.

Реализация разнесена по подмодулям (``rfc``, ``env``, ``ingress``, …); публичный
API — реэкспорт ниже и ``__all__`` (как у бывшего монолита ``types.py``).

**Публичные VO** наследуют приватные базы ``_*`` в ``types._core``; у атомарных
строк смысл несёт **имя класса**, полезная нагрузка — **``.value``**.
Для опциональных RFC-заголовков письма — ``parse_present_optional`` /
``_OptionalStripEmpty.parse_present_from_email`` / ``parse_present_from_nm_message`` (present-or-None), см. ``docs/TYPES.md``.

Имена полей заголовков письма (RFC 5322 и ``X-Threlium-*``) — :class:`~threlium.mail_header_names.MailHeaderName` (строковые константы ``threlium.fsm_emit.HDR_*`` берутся из ``value`` этого enum).

Публичные сценарные модели: ``IngressRouterChildMsg``, ``LiteLlmAcompletionKwargs`` / ``LiteLlmChatMessage`` (граница LiteLLM), ``LightragChunkRecord``, ``CliIntentPayload``, ``ReasoningToolRouteArgs``, ``EngineWireRequest`` / ``EngineWireOk`` / ``EngineWireError`` (граница UNIX-engine), ``MatrixClientRoomMessageMTextContent`` (Matrix CS ``m.room.message`` → nio).
HITL-родитель: ``HitlParentRouting`` (``HitlParentWithoutIntent`` | ``HitlParentWithIntent``) и ``classify_hitl_parent_notmuch``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from ._core import NonEmptyStr
from .bridges import (
    BridgeEmailSubjectLine,
    MatrixOutboundPlainBodyWire,
    MatrixRoomNameWire,
    TelegramBridgeInboundCaptionOrText,
    TelegramPtbOutboundReplyBody,
    matrix_homeserver_url,
)
from .bridge_ingress_channel import BridgeIngressChannel
from .bridge_raw import RawIngressCaptureAttachmentFilename
from .cli_intent_policy import (
    CliExecDecision,
    CliIntentDecision,
    CliIntentPolicy,
    CliRouteCollision,
)
from .cli_mail import CliIntentPayload
from .engine_socket import EngineWireError, EngineWireOk, EngineWireRequest
from .fsm_stage import FsmStage
from .fsm_strings import (
    EnrichGlobalMemoryText,
    EnrichGraphAnswerText,
    EnrichLightragQuestionSubjectLine,
    EnrichObservationNoteText,
    EnrichThreadMemoryText,
    EnrichUnifiedMailContextText,
    FsmPlainToStageSubjectLine,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    IngressRouterResolvedChannelSlug,
    MessageIdHeaderNormalizationInput,
    OrphanNoticePrefixLine,
    ReasoningAssistantMessageText,
    ReasoningToolRouteEmailBody,
    ReasoningToolRouteEmailSubject,
    ReflectJinjaSubjectContext,
)
from .hop_cap import (
    HopBudgetLine,
    ThreliumCapabilitiesBudgetLine,
)
from .identity import (
    EmailIngressRoute,
    EmailNativeId,
    ExternalRfcMidWire,
    IngressRoute,
    MatrixIngressRoute,
    MatrixNativeId,
    MatrixRoomEventId,
    MatrixRoomId,
    MatrixRoomSendTxnId,
    MatrixSyncBatchCursor,
    NativeId,
    TelegramIngressRoute,
    TelegramNativeId,
    TNative,
)
from threlium.mail_header_names import MailHeaderName
from .matrix_client_room_message import (
    MatrixClientRoomMessageInReplyTo,
    MatrixClientRoomMessageMTextContent,
    MatrixClientRoomMessageRelatesTo,
    build_matrix_client_room_message_m_text_content,
    matrix_client_room_message_m_text_content_as_dict_for_nio,
)
from .ingress import (
    EmailStruct,
    IngressRouteB62Wire,
    IngressRouterChildMsg,
    ingress_route_from_json_str,
)
from .ingress_hitl import (
    HitlParentRouting,
    HitlParentWithIntent,
    HitlParentWithoutIntent,
    classify_hitl_parent_notmuch,
)
from .threlium_space import (
    MatrixSpaceV1,
    TelegramSpaceV1,
    ThreliumSpace,
    ThreliumSpaceB62Wire,
    ThreliumSpaceHashWire,
    matrix_space_from_room_id,
    normalize_threlium_space_dict,
    telegram_space_from_ingress_route,
)
from .content_score import ThreliumContentScoreWire
from .irt_hash import IrtHashWire
from .lightrag import (
    LightragChunkRecord,
    LightragLiteLlmCompletionBody,
    LightragWorkerBatchThreadIdKey,
)
from .litellm_completion_kwargs import (
    LiteLlmAcompletionKwargs,
    LiteLlmAembeddingKwargs,
    LiteLlmArerankKwargs,
    LiteLlmChatMessage,
    lite_llm_acompletion_to_dict,
    lite_llm_aembedding_to_dict,
    lite_llm_arerank_to_dict,
)
from .lightrag_prompt_library_key import LightragPromptLibraryKey
from .lightrag_document_header import LightragDocumentHeader
from .lightrag_drain import LightragDrainSkipReason
from .litellm_call_site import LitellmCallSite
from .litellm_correlation_header import LitellmCorrelationHeader
from .litellm_routing_site import LitellmRoutingSite
from .prompt_path import (
    PromptPath,
    REASONING_EMAIL_BODY_BY_STAGE,
    REASONING_EMAIL_SUBJECT_BY_STAGE,
    REASONING_TOOL_SPEC_BY_STAGE,
)
from .notmuch import (
    NotmuchMessageIds,
    NotmuchQuerySortFlag,
    NotmuchThreadScopeId,
    UnionNotmuchFromHeaderWire,
    UnionNotmuchRouteHeaderWire,
)
from .notmuch_message_id import NotmuchMessageIdInner
from .notmuch_query import NotmuchBridgeFromLocalhost, NotmuchIndexedHeader, NotmuchQuery, NotmuchQueryConnective, NotmuchQueryField
from .notmuch_tag import NotmuchTag
from .reasoning_routes import REASONING_TARGET_STAGES
# ``.reasoning`` тянет ``litellm.types.utils`` → весь ``litellm`` (~1.5 c импорта).
# Lazy через PEP 562 ``__getattr__`` (ниже): submitter/тонкие импортёры ``threlium.types``
# не платят за litellm; символы грузятся при первом обращении (в engine). См. ниже _LAZY_REASONING.
if TYPE_CHECKING:
    from .reasoning import (
        ReasoningEnrichContext,
        ReasoningIncomingEnvelope,
        ReasoningResponseStateText,
        ReasoningRouteDecision,
        ReasoningTaskStateText,
        ReasoningToolCallArgumentsWire,
        ReasoningToolFunctionName,
        ReasoningUserMessageText,
        reasoning_assistant_message,
        reasoning_assistant_plain_text,
        reasoning_finish_reason,
        reasoning_first_tool_call,
    )
from .reasoning_tool_args import (
    CliIntentToolArgs,
    EgressRouterToolArgs,
    GlobalMemoryToolArgs,
    MemoryQueryToolArgs,
    NewSubtaskArg,
    ReasoningToolRouteArgs,
    ReflectToolArgs,
    ResponseAppendToolArgs,
    ResponseEditToolArgs,
    FormalReasonToolArgs,
    ResponseFinalizeToolArgs,
    ResponseObserveToolArgs,
    SubagentIntentToolArgs,
    SubtaskStatusUpdateArg,
    TasksUpsertToolArgs,
    ThreadMemoryToolArgs,
    reasoning_tool_struct_for_route,
)
from .rfc import (
    CanonicalMidWire,
    RfcDateWire,
    RfcFromWire,
    RfcInReplyToWire,
    RfcMessageIdWire,
    RfcReferencesWire,
    RfcSenderWire,
    RfcSubjectWire,
    RfcToWire,
    references_angle_bracket_tokens,
    truncate_rfc_references_wire,
)
from .systemd_status import SystemdStatusBody
from .task_ledger import (
    SubtaskStatus,
    TaskBlockerText,
    TaskDiscoveryNoteText,
    TaskLedger,
    TaskNextActionText,
    TaskSubtaskContentId,
    TaskSubtaskState,
    TaskSubtaskText,
)
from .knowledge_stage import (
    LogicInferenceMode,
    FormalReasonDerivedErrorText,
    FormalReasonDerivedTtlText,
    FormalReasonErrorKind,
    FormalReasonFatalErrorText,
    FormalReasonQueryErrorText,
    FormalReasonQueryResultText,
    FormalReasonReportText,
    FormalReasonStagePayload,
    MemoryQueryStagePayload,
)

__all__ = [
    "BridgeEmailSubjectLine",
    "BridgeIngressChannel",
    "CanonicalMidWire",
    "classify_hitl_parent_notmuch",
    "CliExecDecision",
    "CliIntentDecision",
    "CliIntentPolicy",
    "CliRouteCollision",
    "CliIntentPayload",
    "CliIntentToolArgs",
    "EmailStruct",
    "EmailIngressRoute",
    "EmailNativeId",
    "EnrichLightragQuestionSubjectLine",
    "EngineWireError",
    "EngineWireOk",
    "EngineWireRequest",
    "ExternalRfcMidWire",
    "EgressRouterToolArgs",
    "FsmPlainToStageSubjectLine",
    "FsmStage",
    "FsmTransitionPlainBody",
    "FsmTransitionPlainSubjectLine",
    "GlobalMemoryToolArgs",
    "HopBudgetLine",
    "HitlParentRouting",
    "HitlParentWithIntent",
    "HitlParentWithoutIntent",
    "IngressRoute",
    "IngressRouteB62Wire",
    "IngressRouterChildMsg",
    "IngressRouterResolvedChannelSlug",
    "IrtHashWire",
    "ThreliumContentScoreWire",
    "ingress_route_from_json_str",
    "LightragChunkRecord",
    "LightragDocumentHeader",
    "LightragDrainSkipReason",
    "LightragLiteLlmCompletionBody",
    "LightragPromptLibraryKey",
    "LightragWorkerBatchThreadIdKey",
    "LitellmCallSite",
    "LitellmCorrelationHeader",
    "LitellmRoutingSite",
    "LiteLlmAcompletionKwargs",
    "LiteLlmAembeddingKwargs",
    "LiteLlmArerankKwargs",
    "LiteLlmChatMessage",
    "lite_llm_acompletion_to_dict",
    "lite_llm_aembedding_to_dict",
    "lite_llm_arerank_to_dict",
    "MailHeaderName",
    "matrix_homeserver_url",
    "MatrixClientRoomMessageInReplyTo",
    "MatrixClientRoomMessageMTextContent",
    "MatrixClientRoomMessageRelatesTo",
    "build_matrix_client_room_message_m_text_content",
    "matrix_client_room_message_m_text_content_as_dict_for_nio",
    "MatrixIngressRoute",
    "MatrixNativeId",
    "MatrixOutboundPlainBodyWire",
    "MatrixRoomEventId",
    "MatrixRoomId",
    "MatrixRoomNameWire",
    "MatrixRoomSendTxnId",
    "MatrixSyncBatchCursor",
    "MatrixSpaceV1",
    "matrix_space_from_room_id",
    "MemoryQueryStagePayload",
    "MemoryQueryToolArgs",
    "MessageIdHeaderNormalizationInput",
    "NewSubtaskArg",
    "SubtaskStatusUpdateArg",
    "TasksUpsertToolArgs",
    "EnrichGlobalMemoryText",
    "EnrichGraphAnswerText",
    "EnrichObservationNoteText",
    "EnrichThreadMemoryText",
    "EnrichUnifiedMailContextText",
    "NonEmptyStr",
    "NotmuchBridgeFromLocalhost",
    "NotmuchIndexedHeader",
    "NotmuchMessageIds",
    "NotmuchMessageIdInner",
    "NotmuchQuery",
    "NotmuchQueryConnective",
    "NotmuchQueryField",
    "NotmuchQuerySortFlag",
    "NotmuchTag",
    "NotmuchThreadScopeId",
    "NativeId",
    "normalize_threlium_space_dict",
    "OrphanNoticePrefixLine",
    "PromptPath",
    "ReasoningAssistantMessageText",
    "ReasoningEnrichContext",
    "ReasoningIncomingEnvelope",
    "ReasoningResponseStateText",
    "ReasoningRouteDecision",
    "ReasoningTaskStateText",
    "ReasoningToolCallArgumentsWire",
    "ReasoningToolFunctionName",
    "ReasoningToolRouteArgs",
    "ReasoningToolRouteEmailBody",
    "ReasoningUserMessageText",
    "RawIngressCaptureAttachmentFilename",
    "ReasoningToolRouteEmailSubject",
    "REASONING_EMAIL_BODY_BY_STAGE",
    "REASONING_TARGET_STAGES",
    "REASONING_EMAIL_SUBJECT_BY_STAGE",
    "REASONING_TOOL_SPEC_BY_STAGE",
    "ReflectJinjaSubjectContext",
    "ReflectToolArgs",
    "ResponseAppendToolArgs",
    "ResponseEditToolArgs",
    "ResponseFinalizeToolArgs",
    "ResponseObserveToolArgs",
    "RfcDateWire",
    "RfcFromWire",
    "RfcInReplyToWire",
    "RfcMessageIdWire",
    "RfcReferencesWire",
    "RfcSenderWire",
    "RfcSubjectWire",
    "RfcToWire",
    "SubagentIntentToolArgs",
    "SubtaskStatus",
    "TaskBlockerText",
    "TaskDiscoveryNoteText",
    "TaskLedger",
    "TaskNextActionText",
    "TaskSubtaskContentId",
    "TaskSubtaskState",
    "TaskSubtaskText",
    "LogicInferenceMode",
    "FormalReasonDerivedErrorText",
    "FormalReasonDerivedTtlText",
    "FormalReasonErrorKind",
    "FormalReasonFatalErrorText",
    "FormalReasonQueryErrorText",
    "FormalReasonQueryResultText",
    "FormalReasonReportText",
    "FormalReasonStagePayload",
    "FormalReasonToolArgs",
    "SystemdStatusBody",
    "TelegramBridgeInboundCaptionOrText",
    "TelegramIngressRoute",
    "TelegramNativeId",
    "TelegramPtbOutboundReplyBody",
    "TelegramSpaceV1",
    "telegram_space_from_ingress_route",
    "ThreliumCapabilitiesBudgetLine",
    "ThreliumSpace",
    "ThreliumSpaceB62Wire",
    "ThreliumSpaceHashWire",
    "ThreadMemoryToolArgs",
    "TNative",
    "references_angle_bracket_tokens",
    "reasoning_assistant_message",
    "reasoning_assistant_plain_text",
    "reasoning_finish_reason",
    "reasoning_first_tool_call",
    "reasoning_tool_struct_for_route",
    "truncate_rfc_references_wire",
    "UnionNotmuchFromHeaderWire",
    "UnionNotmuchRouteHeaderWire",
]

# PEP 562 lazy: символы ``.reasoning`` (тянет litellm) грузятся при первом обращении,
# а не при ``import threlium.types``. Кешируются в ``globals()`` после первого доступа.
_LAZY_REASONING = frozenset(
    {
        "ReasoningEnrichContext",
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
    }
)


def __getattr__(name: str) -> object:
    if name in _LAZY_REASONING:
        import importlib

        module = importlib.import_module(".reasoning", __name__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted([*globals().keys(), *_LAZY_REASONING])
