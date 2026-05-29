"""msgspec-модели аргументов OpenAI tool_calls для маршрутов стадии reasoning.

После jsonschema.validate входной dict приводится к Struct тем же контрактом, что и ``docs/TYPES.md``
уровень 1 (без повторной «очистки» в роутере).
"""
from __future__ import annotations

from typing import Union

import msgspec

from .knowledge_stage import LogicInferenceMode
from .task_ledger import SubtaskStatus


class EgressRouterToolArgs(msgspec.Struct, frozen=True):
    subject: str
    body: str


class CliIntentToolArgs(msgspec.Struct, frozen=True):
    argv: list[str]
    reasoning: str
    cwd: str | None = None


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


_REASONING_ROUTE_STRUCTS: dict[str, type[msgspec.Struct]] = {
    "cli_intent": CliIntentToolArgs,
    "thread_memory": ThreadMemoryToolArgs,
    "global_memory": GlobalMemoryToolArgs,
    "subagent_intent": SubagentIntentToolArgs,
    "reflect": ReflectToolArgs,
    "response_append": ResponseAppendToolArgs,
    "response_edit": ResponseEditToolArgs,
    "response_observe": ResponseObserveToolArgs,
    "response_finalize": ResponseFinalizeToolArgs,
    "formal_reason": FormalReasonToolArgs,
    "memory_query": MemoryQueryToolArgs,
    "tasks_upsert": TasksUpsertToolArgs,
}


def reasoning_tool_struct_for_route(route: str) -> type[msgspec.Struct]:
    """Тип Struct для маршрута ``route`` (ключ ``ROUTE_TO_ADDRESS``)."""
    t = _REASONING_ROUTE_STRUCTS.get(route)
    if t is None:
        raise ValueError(f"unknown reasoning route: {route!r}")
    return t


__all__ = [
    "CliIntentToolArgs",
    "EgressRouterToolArgs",
    "GlobalMemoryToolArgs",
    "FormalReasonToolArgs",
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
