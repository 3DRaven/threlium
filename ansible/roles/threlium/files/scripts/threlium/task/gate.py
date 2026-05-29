"""Жёсткий gate для ``response_finalize``: проверка task-ledger (а не текста LLM)."""
from __future__ import annotations

from threlium.prompts import render_prompt
from threlium.types import PromptPath, TaskLedger


def ledger_has_open_work(ledger: TaskLedger) -> bool:
    """``True`` если ledger блокирует finalize (anti-drift gate, **fail-closed**).

    Блокируют:

    * **пустой** ledger — ни одной записанной подзадачи (fail-closed: даже trivial-ответ
      должен зафиксировать одну подзадачу ``done`` через ``tasks_upsert``); **или**
    * есть подзадача ранга < 2 (``pending`` / ``in_progress``) — кроме осознанного bypass
      ``allow_finalize_with_blocker`` + непустой ``blockers`` (ledger уже заведён); **или**
    * **все** подзадачи ``cancelled`` и **нет ни одной** ``done`` (guard против
      escape-hatch «отменить всё и выйти»).

    Иначе (есть хотя бы одна ``done``, остальные терминальные) → ``False`` (finalize разрешён).
    """
    if ledger.is_empty:
        return True
    if ledger.open_subtasks():
        if ledger.allow_finalize_with_blocker and ledger.blockers is not None:
            return False
        return True
    return not ledger.done_subtasks()


def build_task_incomplete_notice(ledger: TaskLedger) -> str:
    """Текст ingress-уведомления, когда finalize заблокирован gate'ом task-ledger."""
    open_subtasks = ledger.open_subtasks()
    all_cancelled = not ledger.is_empty and not open_subtasks and not ledger.done_subtasks()
    return render_prompt(
        PromptPath.INGRESS_TASK_INCOMPLETE,
        ledger_empty=ledger.is_empty,
        all_cancelled=all_cancelled,
        blockers=ledger.blockers.value if ledger.blockers is not None else None,
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
