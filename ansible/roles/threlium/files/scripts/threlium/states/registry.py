"""Явная карта ``FsmStage`` → handler ``threlium.states.<stage>.main`` для воркера."""
from __future__ import annotations

from threlium.fsm_emit import StateHandler
from threlium.types import FsmStage

from threlium.states import (
    archive,
    cli_exec,
    cli_hitl_out,
    cli_intent,
    cli_resume,
    enrich,
    enrich_fast,
    egress_email,
    egress_matrix,
    egress_router,
    egress_telegram,
    global_memory,
    ingress,
    formal_reason,
    memory_query,
    reasoning,
    reflect,
    response_append,
    response_edit,
    response_finalize,
    response_observe,
    subagent_end,
    subagent_intent,
    summarize_context,
    summarize_memory,
    tasks_upsert,
    thread_memory,
)

STAGE_MAIN_HANDLERS: dict[FsmStage, StateHandler] = {
    FsmStage.INGRESS: ingress.main,
    FsmStage.ENRICH: enrich.main,
    FsmStage.REASONING: reasoning.main,
    FsmStage.REFLECT: reflect.main,
    FsmStage.THREAD_MEMORY: thread_memory.main,
    FsmStage.GLOBAL_MEMORY: global_memory.main,
    FsmStage.SUBAGENT_INTENT: subagent_intent.main,
    FsmStage.SUBAGENT_END: subagent_end.main,
    FsmStage.CLI_INTENT: cli_intent.main,
    FsmStage.CLI_HITL_OUT: cli_hitl_out.main,
    FsmStage.CLI_RESUME: cli_resume.main,
    FsmStage.CLI_EXEC: cli_exec.main,
    FsmStage.RESPONSE_APPEND: response_append.main,
    FsmStage.RESPONSE_EDIT: response_edit.main,
    FsmStage.RESPONSE_OBSERVE: response_observe.main,
    FsmStage.TASKS_UPSERT: tasks_upsert.main,
    FsmStage.ENRICH_FAST: enrich_fast.main,
    FsmStage.RESPONSE_FINALIZE: response_finalize.main,
    FsmStage.EGRESS_ROUTER: egress_router.main,
    FsmStage.EGRESS_EMAIL: egress_email.main,
    FsmStage.EGRESS_TELEGRAM: egress_telegram.main,
    FsmStage.EGRESS_MATRIX: egress_matrix.main,
    FsmStage.FORMAL_REASON: formal_reason.main,
    FsmStage.MEMORY_QUERY: memory_query.main,
    FsmStage.SUMMARIZE_CONTEXT: summarize_context.main,
    FsmStage.SUMMARIZE_MEMORY: summarize_memory.main,
    FsmStage.ARCHIVE: archive.main,
}

if set(STAGE_MAIN_HANDLERS) != set(FsmStage):
    raise RuntimeError(
        "states.registry: STAGE_MAIN_HANDLERS out of sync with FsmStage "
        f"(missing={set(FsmStage) - set(STAGE_MAIN_HANDLERS)}, "
        f"extra={set(STAGE_MAIN_HANDLERS) - set(FsmStage)})"
    )

__all__ = ["STAGE_MAIN_HANDLERS"]
