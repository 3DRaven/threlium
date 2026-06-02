"""msgspec-модели аргументов OpenAI tool_calls для маршрутов стадии reasoning.

После jsonschema.validate входной dict приводится к Struct тем же контрактом, что и ``docs/TYPES.md``
уровень 1 (без повторной «очистки» в роутере).
"""
from __future__ import annotations

from typing import Union

import msgspec

from .fsm_stage import FsmStage
from .knowledge_stage import LogicInferenceMode
from .reasoning_routes import REASONING_TARGET_STAGES
from .task_ledger import SubtaskStatus


class EgressRouterToolArgs(msgspec.Struct, frozen=True):
    subject: str
    body: str


class CliIntentToolArgs(msgspec.Struct, frozen=True):
    argv: list[str]
    reasoning: str
    cwd: str | None = None
    privileged: bool = False


class ThreadMemoryToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    note: str


class GlobalMemoryToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    note: str


class SubagentIntentToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    task: str


class ReflectToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    summary: str
    clarification_request: str


class ResponseAppendToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    content: str


class ResponseEditToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    position: int
    new_content: str | None = None


class ResponseObserveToolArgs(msgspec.Struct, frozen=True):
    reasoning: str


class ResponseFinalizeToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    subject: str
    verification_summary: str
    content: str | None = None


class FormalReasonToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    shapes_ttl: str
    facts_ttl: str
    ontology_ttl: str | None = None
    inference: LogicInferenceMode | None = None
    query: str | None = None
    return_derived: bool = False


class MemoryQueryToolArgs(msgspec.Struct, frozen=True):
    reasoning: str
    query: str


class NewSubtaskArg(msgspec.Struct, frozen=True):
    """Новая подзадача от ``tasks_upsert`` (примитивы на границе jsonschema).

    ``content_id`` считается фабрикой из ``text`` (content-addressed), здесь не передаётся.
    """

    text: str
    status: SubtaskStatus = SubtaskStatus.PENDING


class SubtaskStatusUpdateArg(msgspec.Struct, frozen=True):
    """Статус-изменение существующей подзадачи по видимому в ``<task-state>`` ``content_id``."""

    content_id: str
    status: SubtaskStatus


class TasksUpsertToolArgs(msgspec.Struct, frozen=True):
    """Аргументы tool ``tasks_upsert``: за один вызов add новых subtasks + status существующих.

    Инвариант (проверяется в ``TasksUpsertOp.from_tool_args``): ``new_subtasks`` или
    ``subtask_updates`` непусто.
    """

    reasoning: str
    new_subtasks: list[NewSubtaskArg] = []
    subtask_updates: list[SubtaskStatusUpdateArg] = []
    discovery_append: str | None = None
    next_action: str | None = None
    blockers: str | None = None
    allow_finalize_with_blocker: bool = False


ReasoningToolRouteArgs = Union[
    CliIntentToolArgs,
    ThreadMemoryToolArgs,
    GlobalMemoryToolArgs,
    SubagentIntentToolArgs,
    ReflectToolArgs,
    ResponseAppendToolArgs,
    ResponseEditToolArgs,
    ResponseObserveToolArgs,
    ResponseFinalizeToolArgs,
    FormalReasonToolArgs,
    MemoryQueryToolArgs,
    TasksUpsertToolArgs,
]


_REASONING_ROUTE_STRUCTS: dict[FsmStage, type[msgspec.Struct]] = {
    FsmStage.CLI_INTENT: CliIntentToolArgs,
    FsmStage.THREAD_MEMORY: ThreadMemoryToolArgs,
    FsmStage.GLOBAL_MEMORY: GlobalMemoryToolArgs,
    FsmStage.SUBAGENT_INTENT: SubagentIntentToolArgs,
    FsmStage.REFLECT: ReflectToolArgs,
    FsmStage.RESPONSE_APPEND: ResponseAppendToolArgs,
    FsmStage.RESPONSE_EDIT: ResponseEditToolArgs,
    FsmStage.RESPONSE_OBSERVE: ResponseObserveToolArgs,
    FsmStage.RESPONSE_FINALIZE: ResponseFinalizeToolArgs,
    FsmStage.FORMAL_REASON: FormalReasonToolArgs,
    FsmStage.MEMORY_QUERY: MemoryQueryToolArgs,
    FsmStage.TASKS_UPSERT: TasksUpsertToolArgs,
}

assert set(_REASONING_ROUTE_STRUCTS.keys()) == REASONING_TARGET_STAGES


def reasoning_tool_struct_for_route(route: FsmStage) -> type[msgspec.Struct]:
    """Тип Struct для целевой стадии маршрута reasoning."""
    t = _REASONING_ROUTE_STRUCTS.get(route)
    if t is None:
        raise ValueError(f"unknown reasoning route: {route!r}")
    return t


def formal_reason_stage_payload_from_tool_args(
    args: FormalReasonToolArgs,
) -> FormalReasonStagePayload:
    """``FormalReasonToolArgs`` (reasoning tool) → ``FormalReasonStagePayload`` (stage body)."""
    from .knowledge_stage import FormalReasonStagePayload

    return msgspec.convert(msgspec.to_builtins(args), type=FormalReasonStagePayload)


__all__ = [
    "CliIntentToolArgs",
    "EgressRouterToolArgs",
    "GlobalMemoryToolArgs",
    "FormalReasonToolArgs",
    "formal_reason_stage_payload_from_tool_args",
    "MemoryQueryToolArgs",
    "NewSubtaskArg",
    "ReasoningToolRouteArgs",
    "ReflectToolArgs",
    "ResponseAppendToolArgs",
    "ResponseEditToolArgs",
    "ResponseFinalizeToolArgs",
    "ResponseObserveToolArgs",
    "SubagentIntentToolArgs",
    "SubtaskStatusUpdateArg",
    "TasksUpsertToolArgs",
    "ThreadMemoryToolArgs",
    "reasoning_tool_struct_for_route",
]
