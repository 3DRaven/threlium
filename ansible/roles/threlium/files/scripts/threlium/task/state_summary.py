"""Детерминированный рендер ``<task-state>`` (кэш для промпта reasoning / вход observe)."""
from __future__ import annotations

from threlium.prompts import render_prompt
from threlium.types import PromptPath, TaskLedger


def _subtask_views(ledger: TaskLedger) -> list[dict[str, object]]:
    return [
        {
            "content_id": s.content_id.value,
            "text": s.text.value,
            "status": s.status.value,
            "terminal": s.status.is_terminal,
        }
        for s in ledger.subtasks
    ]


def build_task_state_summary(ledger: TaskLedger) -> str:
    """Текстовая сводка ledger с ``content_id`` для патчей через ``tasks_upsert`` (Jinja2)."""
    return render_prompt(
        PromptPath.TASK_STATE_SUMMARY,
        is_empty=ledger.is_empty,
        subtasks=_subtask_views(ledger),
        open_count=len(ledger.open_subtasks()),
        done_count=len(ledger.done_subtasks()),
        cancelled_count=len(ledger.cancelled_subtasks()),
        discovery_note=ledger.discovery_note.value if ledger.discovery_note is not None else None,
        next_action=ledger.next_action.value if ledger.next_action is not None else None,
        blockers=ledger.blockers.value if ledger.blockers is not None else None,
        allow_finalize_with_blocker=ledger.allow_finalize_with_blocker,
    ).strip()
