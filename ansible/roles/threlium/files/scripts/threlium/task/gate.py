"""Жёсткий gate для ``response_finalize``: проверка task-ledger (а не текста LLM)."""
from __future__ import annotations

from threlium.prompts import render_prompt
from threlium.types import PromptPath, TaskLedger


def ledger_has_open_work(ledger: TaskLedger) -> bool:
    """``True`` если ledger блокирует finalize.

    Блокируют:

    * есть подзадача ранга < 2 (``pending`` / ``in_progress``); **или**
    * **все** подзадачи ``cancelled`` и **нет ни одной** ``done`` (guard против
      escape-hatch «отменить всё и выйти»).

    Пустой ledger → ``False`` (fail-open: задачи не заведены — gate не мешает).
    Иначе (есть хотя бы одна ``done``, остальные терминальные) → ``False``.
    """
    if ledger.is_empty:
        return False
    if ledger.open_subtasks():
        return True
    return not ledger.done_subtasks()


def build_task_incomplete_notice(ledger: TaskLedger) -> str:
    """Текст ingress-уведомления, когда finalize заблокирован незавершёнными задачами."""
    open_subtasks = ledger.open_subtasks()
    all_cancelled = not open_subtasks and not ledger.done_subtasks()
    return render_prompt(
        PromptPath.INGRESS_TASK_INCOMPLETE,
        all_cancelled=all_cancelled,
        open_subtasks=[
            {"content_id": s.content_id.value, "text": s.text.value, "status": s.status.value}
            for s in open_subtasks
        ],
        done_subtasks=[
            {"content_id": s.content_id.value, "text": s.text.value}
            for s in ledger.done_subtasks()
        ],
        cancelled_subtasks=[
            {"content_id": s.content_id.value, "text": s.text.value}
            for s in ledger.cancelled_subtasks()
        ],
    ).strip()
