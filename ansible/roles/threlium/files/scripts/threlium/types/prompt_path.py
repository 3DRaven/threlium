"""Относительные пути Jinja2-шаблонов под ``$THRELIUM_HOME/prompts/``.

Единый :class:`PromptPath` для всех вызовов :func:`threlium.prompts.render_prompt`;
шаблоны ``lightrag/<ключ PROMPTS>.j2`` для overlay — через
:class:`~threlium.types.lightrag_prompt_library_key.LightragPromptLibraryKey`
(см. :meth:`~threlium.types.lightrag_prompt_library_key.LightragPromptLibraryKey.prompt_path`).
Динамические ``reasoning/<local-part>/…`` — через
:data:`REASONING_TOOL_SPEC_BY_STAGE` / :data:`REASONING_EMAIL_SUBJECT_BY_STAGE` /
:data:`REASONING_EMAIL_BODY_BY_STAGE` (ключ — целевая :class:`~threlium.types.fsm_stage.FsmStage`,
совпадающая с именем tool / каталогом под ``prompts/reasoning/``).
"""
from __future__ import annotations

from enum import StrEnum

from .fsm_stage import FsmStage
from .lightrag_prompt_library_key import LightragPromptLibraryKey
from .reasoning_routes import REASONING_TARGET_STAGES


class PromptPath(StrEnum):
    """Путь шаблона относительно каталога ``prompts/`` (как в ``FileSystemLoader``).

    Полный перечень совпадает с ``threlium_required_prompts`` в Ansible defaults
    (``ansible/roles/threlium/defaults/main.yml``).
    """

    INGRESS_ORPHAN_NOTICE = "ingress/orphan_notice.j2"

    CLI_EXEC_OBSERVATION = "cli_exec/observation.j2"

    FORMAL_REASON_OBSERVATION_PASSED = "formal_reason/observation_passed.j2"
    FORMAL_REASON_OBSERVATION_FATAL = "formal_reason/observation_fatal.j2"
    FORMAL_REASON_OBSERVATION_SUPPLEMENTAL_ERROR = (
        "formal_reason/observation_supplemental_error.j2"
    )
    FORMAL_REASON_OBSERVATION_SHACL_NEGATIVE = (
        "formal_reason/observation_shacl_negative.j2"
    )
    MEMORY_QUERY_OBSERVATION = "memory_query/observation.j2"

    EGRESS_SELF_ARCHIVE_SUBJECT = "egress/self_archive_subject.j2"
    EGRESS_SELF_ARCHIVE_BODY = "egress/self_archive_body.j2"

    CLI_HITL_OUT_CONFIRM = "cli_hitl_out/confirm.j2"
    CLI_HITL_OUT_UNPARSABLE = "cli_hitl_out/unparsable.j2"
    CLI_HITL_OUT_SUBJECT = "cli_hitl_out/subject.j2"
    CLI_INTENT_INVALID = "cli_intent/invalid.j2"
    CLI_INTENT_INVALID_SUBJECT = "cli_intent/invalid_subject.j2"
    CLI_INTENT_ROUTE_COLLISION = "cli_intent/route_collision.j2"

    CLI_RESUME_INTENT_NOT_FOUND = "cli_resume/intent_not_found.j2"
    CLI_RESUME_INTENT_NOT_FOUND_SUBJECT = "cli_resume/intent_not_found_subject.j2"
    CLI_RESUME_BAD_INTENT = "cli_resume/bad_intent.j2"
    CLI_RESUME_BAD_INTENT_SUBJECT = "cli_resume/bad_intent_subject.j2"
    CLI_RESUME_NOT_CONFIRMED = "cli_resume/not_confirmed.j2"
    CLI_RESUME_NOT_CONFIRMED_SUBJECT = "cli_resume/not_confirmed_subject.j2"
    CLI_RESUME_CONFIRM_CLI_HITL_TOOL_SPEC = "cli_resume/tools/confirm_cli_hitl_tool_spec.j2"
    CLI_RESUME_CLASSIFY_SYSTEM = "cli_resume/classify_system.j2"
    CLI_RESUME_CLASSIFY_USER = "cli_resume/classify_user.j2"

    SUBAGENT_INTENT_BUDGET_EXHAUSTED = "subagent_intent/budget_exhausted.j2"
    SUBAGENT_INTENT_BUDGET_EXHAUSTED_SUBJECT = (
        "subagent_intent/budget_exhausted_subject.j2"
    )

    REFLECT_CONTINUE = "reflect/continue.j2"
    REFLECT_FINAL = "reflect/final.j2"

    REASONING_USER = "reasoning/user.j2"
    REASONING_SYSTEM = "reasoning/system.j2"
    REASONING_LENGTH_RECOVERY_SYSTEM = "reasoning/length_recovery_system.j2"
    REASONING_BUDGET_EXHAUSTED = "reasoning/budget_exhausted.j2"
    REASONING_FORMAL_REASON_GATE = "reasoning/formal_reason_gate.j2"

    REASONING_EGRESS_ROUTER_TOOL_SPEC = "reasoning/egress_router/tool_spec.j2"
    REASONING_EGRESS_ROUTER_EMAIL_SUBJECT = "reasoning/egress_router/email_subject.j2"
    REASONING_EGRESS_ROUTER_EMAIL_BODY = "reasoning/egress_router/email_body.j2"
    REASONING_CLI_INTENT_TOOL_SPEC = "reasoning/cli_intent/tool_spec.j2"
    REASONING_CLI_INTENT_EMAIL_SUBJECT = "reasoning/cli_intent/email_subject.j2"
    REASONING_CLI_INTENT_EMAIL_BODY = "reasoning/cli_intent/email_body.j2"
    REASONING_THREAD_MEMORY_TOOL_SPEC = "reasoning/thread_memory/tool_spec.j2"
    REASONING_THREAD_MEMORY_EMAIL_SUBJECT = (
        "reasoning/thread_memory/email_subject.j2"
    )
    REASONING_THREAD_MEMORY_EMAIL_BODY = "reasoning/thread_memory/email_body.j2"
    REASONING_GLOBAL_MEMORY_TOOL_SPEC = "reasoning/global_memory/tool_spec.j2"
    REASONING_GLOBAL_MEMORY_EMAIL_SUBJECT = (
        "reasoning/global_memory/email_subject.j2"
    )
    REASONING_GLOBAL_MEMORY_EMAIL_BODY = "reasoning/global_memory/email_body.j2"
    REASONING_SUBAGENT_INTENT_TOOL_SPEC = "reasoning/subagent_intent/tool_spec.j2"
    REASONING_SUBAGENT_INTENT_EMAIL_SUBJECT = (
        "reasoning/subagent_intent/email_subject.j2"
    )
    REASONING_SUBAGENT_INTENT_EMAIL_BODY = (
        "reasoning/subagent_intent/email_body.j2"
    )
    REASONING_REFLECT_TOOL_SPEC = "reasoning/reflect/tool_spec.j2"
    REASONING_REFLECT_EMAIL_SUBJECT = "reasoning/reflect/email_subject.j2"
    REASONING_REFLECT_EMAIL_BODY = "reasoning/reflect/email_body.j2"

    REASONING_RESPONSE_APPEND_TOOL_SPEC = "reasoning/response_append/tool_spec.j2"
    REASONING_RESPONSE_APPEND_EMAIL_SUBJECT = "reasoning/response_append/email_subject.j2"
    REASONING_RESPONSE_APPEND_EMAIL_BODY = "reasoning/response_append/email_body.j2"
    REASONING_RESPONSE_EDIT_TOOL_SPEC = "reasoning/response_edit/tool_spec.j2"
    REASONING_RESPONSE_EDIT_EMAIL_SUBJECT = "reasoning/response_edit/email_subject.j2"
    REASONING_RESPONSE_EDIT_EMAIL_BODY = "reasoning/response_edit/email_body.j2"
    REASONING_RESPONSE_OBSERVE_TOOL_SPEC = "reasoning/response_observe/tool_spec.j2"
    REASONING_RESPONSE_OBSERVE_EMAIL_SUBJECT = "reasoning/response_observe/email_subject.j2"
    REASONING_RESPONSE_OBSERVE_EMAIL_BODY = "reasoning/response_observe/email_body.j2"
    REASONING_TASKS_UPSERT_TOOL_SPEC = "reasoning/tasks_upsert/tool_spec.j2"
    REASONING_TASKS_UPSERT_EMAIL_SUBJECT = "reasoning/tasks_upsert/email_subject.j2"
    REASONING_TASKS_UPSERT_EMAIL_BODY = "reasoning/tasks_upsert/email_body.j2"
    REASONING_RESPONSE_FINALIZE_TOOL_SPEC = "reasoning/response_finalize/tool_spec.j2"
    REASONING_RESPONSE_FINALIZE_EMAIL_SUBJECT = "reasoning/response_finalize/email_subject.j2"
    REASONING_RESPONSE_FINALIZE_EMAIL_BODY = "reasoning/response_finalize/email_body.j2"

    REASONING_FORMAL_REASON_TOOL_SPEC = "reasoning/formal_reason/tool_spec.j2"
    REASONING_FORMAL_REASON_EMAIL_SUBJECT = "reasoning/formal_reason/email_subject.j2"
    REASONING_FORMAL_REASON_EMAIL_BODY = "reasoning/formal_reason/email_body.j2"
    REASONING_MEMORY_QUERY_TOOL_SPEC = "reasoning/memory_query/tool_spec.j2"
    REASONING_MEMORY_QUERY_EMAIL_SUBJECT = "reasoning/memory_query/email_subject.j2"
    REASONING_MEMORY_QUERY_EMAIL_BODY = "reasoning/memory_query/email_body.j2"

    INGRESS_RESPONSE_NOT_FORMED = "ingress/response_not_formed.j2"
    INGRESS_TASK_INCOMPLETE = "ingress/task_incomplete.j2"
    INGRESS_TASKS_UPSERT_ERROR = "ingress/tasks_upsert_error.j2"

    LIGHTRAG_ENRICH_TASK_PLAN = "lightrag/enrich_task_plan.j2"
    TASK_STATE_SUMMARY = "task/state_summary.j2"

    RESPONSE_OBSERVE_STATE_SUMMARY = "response_observe/state_summary.j2"
    RESPONSE_OBSERVE_SYSTEM = "response_observe/observe_system.j2"
    RESPONSE_OBSERVE_USER = "response_observe/observe_user.j2"

    RESPONSE_FINALIZE_COMPOSE = "response_finalize/compose.j2"
    RESPONSE_FINALIZE_FALLBACK_SUBJECT = "response_finalize/fallback_subject.j2"
    RESPONSE_FINALIZE_CHUNK_ASSEMBLY = "response_finalize/chunk_assembly.j2"

    RESPONSE_EDIT_ERROR_INVALID_BODY = "response_edit/error_invalid_body.j2"
    RESPONSE_EDIT_ERROR_INVALID_POSITION = "response_edit/error_invalid_position.j2"

    SUMMARIZE_CONTEXT_SYSTEM = "summarize_context/system.j2"
    SUMMARIZE_CONTEXT_USER = "summarize_context/user.j2"

    RUNNERS_LIGHTRAG_ADDON_PARAMS = "runners/lightrag/addon_params.j2"

    LIGHTRAG_ENRICH_QUERY_PLAN = "lightrag/enrich_query_plan.j2"
    LIGHTRAG_ENRICH_INCOMING_USER_TEXT = "lightrag/enrich_incoming_user_text.j2"
    LIGHTRAG_ENRICH_AQUERY_USER = "lightrag/enrich_aquery_user.j2"
    LIGHTRAG_MAIL_CONTEXT = "lightrag/mail_context.j2"
    LIGHTRAG_INGEST_BODY = "lightrag/ingest_body.j2"
    LIGHTRAG_ENTITY_EXTRACTION_SYSTEM_PROMPT = (
        f"lightrag/{LightragPromptLibraryKey.ENTITY_EXTRACTION_SYSTEM_PROMPT.value}.j2"
    )
    LIGHTRAG_ENTITY_EXTRACTION_USER_PROMPT = (
        f"lightrag/{LightragPromptLibraryKey.ENTITY_EXTRACTION_USER_PROMPT.value}.j2"
    )
    LIGHTRAG_ENTITY_CONTINUE_EXTRACTION_USER_PROMPT = (
        f"lightrag/{LightragPromptLibraryKey.ENTITY_CONTINUE_EXTRACTION_USER_PROMPT.value}.j2"
    )
    LIGHTRAG_ENTITY_EXTRACTION_EXAMPLES = (
        f"lightrag/{LightragPromptLibraryKey.ENTITY_EXTRACTION_EXAMPLES.value}.j2"
    )
    LIGHTRAG_SUMMARIZE_ENTITY_DESCRIPTIONS = (
        f"lightrag/{LightragPromptLibraryKey.SUMMARIZE_ENTITY_DESCRIPTIONS.value}.j2"
    )
    LIGHTRAG_FAIL_RESPONSE = f"lightrag/{LightragPromptLibraryKey.FAIL_RESPONSE.value}.j2"
    LIGHTRAG_RAG_RESPONSE = f"lightrag/{LightragPromptLibraryKey.RAG_RESPONSE.value}.j2"
    LIGHTRAG_NAIVE_RAG_RESPONSE = (
        f"lightrag/{LightragPromptLibraryKey.NAIVE_RAG_RESPONSE.value}.j2"
    )
    LIGHTRAG_KG_QUERY_CONTEXT = (
        f"lightrag/{LightragPromptLibraryKey.KG_QUERY_CONTEXT.value}.j2"
    )
    LIGHTRAG_NAIVE_QUERY_CONTEXT = (
        f"lightrag/{LightragPromptLibraryKey.NAIVE_QUERY_CONTEXT.value}.j2"
    )
    LIGHTRAG_KEYWORDS_EXTRACTION = (
        f"lightrag/{LightragPromptLibraryKey.KEYWORDS_EXTRACTION.value}.j2"
    )

    LIGHTRAG_EXTRACT_KNOWLEDGE_GRAPH_TOOL_SPEC = (
        "lightrag/tools/extract_knowledge_graph_tool_spec.j2"
    )
    LIGHTRAG_SUMMARIZE_DESCRIPTIONS_TOOL_SPEC = (
        "lightrag/tools/summarize_descriptions_tool_spec.j2"
    )
    LIGHTRAG_EXTRACT_QUERY_KEYWORDS_TOOL_SPEC = (
        "lightrag/tools/extract_query_keywords_tool_spec.j2"
    )
    LIGHTRAG_GENERATE_RAG_ANSWER_TOOL_SPEC = (
        "lightrag/tools/generate_rag_answer_tool_spec.j2"
    )
    LIGHTRAG_KEYWORDS_EXTRACTION_EXAMPLES = (
        f"lightrag/{LightragPromptLibraryKey.KEYWORDS_EXTRACTION_EXAMPLES.value}.j2"
    )


for _LIB_KEY in LightragPromptLibraryKey:
    _LIB_KEY.prompt_path()


REASONING_TOOL_SPEC_BY_STAGE: dict[FsmStage, PromptPath] = {
    FsmStage.CLI_INTENT: PromptPath.REASONING_CLI_INTENT_TOOL_SPEC,
    FsmStage.THREAD_MEMORY: PromptPath.REASONING_THREAD_MEMORY_TOOL_SPEC,
    FsmStage.GLOBAL_MEMORY: PromptPath.REASONING_GLOBAL_MEMORY_TOOL_SPEC,
    FsmStage.SUBAGENT_INTENT: PromptPath.REASONING_SUBAGENT_INTENT_TOOL_SPEC,
    FsmStage.REFLECT: PromptPath.REASONING_REFLECT_TOOL_SPEC,
    FsmStage.RESPONSE_APPEND: PromptPath.REASONING_RESPONSE_APPEND_TOOL_SPEC,
    FsmStage.RESPONSE_EDIT: PromptPath.REASONING_RESPONSE_EDIT_TOOL_SPEC,
    FsmStage.RESPONSE_OBSERVE: PromptPath.REASONING_RESPONSE_OBSERVE_TOOL_SPEC,
    FsmStage.RESPONSE_FINALIZE: PromptPath.REASONING_RESPONSE_FINALIZE_TOOL_SPEC,
    FsmStage.FORMAL_REASON: PromptPath.REASONING_FORMAL_REASON_TOOL_SPEC,
    FsmStage.MEMORY_QUERY: PromptPath.REASONING_MEMORY_QUERY_TOOL_SPEC,
    FsmStage.TASKS_UPSERT: PromptPath.REASONING_TASKS_UPSERT_TOOL_SPEC,
}

REASONING_EMAIL_SUBJECT_BY_STAGE: dict[FsmStage, PromptPath] = {
    FsmStage.CLI_INTENT: PromptPath.REASONING_CLI_INTENT_EMAIL_SUBJECT,
    FsmStage.THREAD_MEMORY: PromptPath.REASONING_THREAD_MEMORY_EMAIL_SUBJECT,
    FsmStage.GLOBAL_MEMORY: PromptPath.REASONING_GLOBAL_MEMORY_EMAIL_SUBJECT,
    FsmStage.SUBAGENT_INTENT: PromptPath.REASONING_SUBAGENT_INTENT_EMAIL_SUBJECT,
    FsmStage.REFLECT: PromptPath.REASONING_REFLECT_EMAIL_SUBJECT,
    FsmStage.RESPONSE_APPEND: PromptPath.REASONING_RESPONSE_APPEND_EMAIL_SUBJECT,
    FsmStage.RESPONSE_EDIT: PromptPath.REASONING_RESPONSE_EDIT_EMAIL_SUBJECT,
    FsmStage.RESPONSE_OBSERVE: PromptPath.REASONING_RESPONSE_OBSERVE_EMAIL_SUBJECT,
    FsmStage.RESPONSE_FINALIZE: PromptPath.REASONING_RESPONSE_FINALIZE_EMAIL_SUBJECT,
    FsmStage.FORMAL_REASON: PromptPath.REASONING_FORMAL_REASON_EMAIL_SUBJECT,
    FsmStage.MEMORY_QUERY: PromptPath.REASONING_MEMORY_QUERY_EMAIL_SUBJECT,
    FsmStage.TASKS_UPSERT: PromptPath.REASONING_TASKS_UPSERT_EMAIL_SUBJECT,
}

REASONING_EMAIL_BODY_BY_STAGE: dict[FsmStage, PromptPath] = {
    FsmStage.CLI_INTENT: PromptPath.REASONING_CLI_INTENT_EMAIL_BODY,
    FsmStage.THREAD_MEMORY: PromptPath.REASONING_THREAD_MEMORY_EMAIL_BODY,
    FsmStage.GLOBAL_MEMORY: PromptPath.REASONING_GLOBAL_MEMORY_EMAIL_BODY,
    FsmStage.SUBAGENT_INTENT: PromptPath.REASONING_SUBAGENT_INTENT_EMAIL_BODY,
    FsmStage.REFLECT: PromptPath.REASONING_REFLECT_EMAIL_BODY,
    FsmStage.RESPONSE_APPEND: PromptPath.REASONING_RESPONSE_APPEND_EMAIL_BODY,
    FsmStage.RESPONSE_EDIT: PromptPath.REASONING_RESPONSE_EDIT_EMAIL_BODY,
    FsmStage.RESPONSE_OBSERVE: PromptPath.REASONING_RESPONSE_OBSERVE_EMAIL_BODY,
    FsmStage.RESPONSE_FINALIZE: PromptPath.REASONING_RESPONSE_FINALIZE_EMAIL_BODY,
    FsmStage.FORMAL_REASON: PromptPath.REASONING_FORMAL_REASON_EMAIL_BODY,
    FsmStage.MEMORY_QUERY: PromptPath.REASONING_MEMORY_QUERY_EMAIL_BODY,
    FsmStage.TASKS_UPSERT: PromptPath.REASONING_TASKS_UPSERT_EMAIL_BODY,
}

assert set(REASONING_TOOL_SPEC_BY_STAGE.keys()) == REASONING_TARGET_STAGES, (
    set(REASONING_TOOL_SPEC_BY_STAGE),
    REASONING_TARGET_STAGES,
)
assert REASONING_EMAIL_SUBJECT_BY_STAGE.keys() == REASONING_TOOL_SPEC_BY_STAGE.keys()
assert REASONING_EMAIL_BODY_BY_STAGE.keys() == REASONING_TOOL_SPEC_BY_STAGE.keys()

__all__ = [
    "PromptPath",
    "REASONING_EMAIL_BODY_BY_STAGE",
    "REASONING_EMAIL_SUBJECT_BY_STAGE",
    "REASONING_TOOL_SPEC_BY_STAGE",
]
