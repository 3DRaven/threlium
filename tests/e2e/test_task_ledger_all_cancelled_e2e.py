"""E2E: all-cancelled guard task-ledger (anti-drift gate) на живом стеке.

Сценарий (один пользовательский ход):

1. ``enrich`` сеет 2 подзадачи (стаб ``081`` → ``[A, B]``);
2. reasoning #1 → ``tasks_upsert``: обе подзадачи ``cancelled``;
3. reasoning #2 → ``response_finalize`` — **жёсткий gate** видит «всё cancelled, нет ни одной
   done» и **отбивает** в ``ingress`` (``task_incomplete``, ветка ``all_cancelled``);
4. полный re-enrich (ensure-exists: статусы ``cancelled`` не сбрасываются);
5. reasoning #3 → ``tasks_upsert``: новая подзадача ``C`` сразу ``done`` (cancelled —
   терминально, понизить нельзя; реальную работу фиксируем новой done-подзадачей);
6. reasoning #4 → ``response_finalize`` — gate проходит (есть ``done``) → ``egress_router``.

Покрытие: guard против escape-hatch «отменить всё и выйти» (cancelled без done блокирует
finalize); восстановление через новую done-подзадачу.

Фазовый автомат WireMock (контекст ``stub-task-ledger-allcancel-01::<root>``):
``active`` → ``phase_cancel_all_done`` → ``phase_finalize_blocked_done`` →
``phase_recover_done``.

**Подготовка (вне модуля):** shared compose + baked SUT. Синхронизация кода/шаблонов —
``pytest -n0 tests/e2e/wipe_sync.py`` (тег ``refresh``).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .helpers import (
    MailflowScenarioSpec,
    assert_full_mailflow_pipeline,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    REPO_ROOT,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_TASK_LEDGER_ALLCANCEL_BODY_MARKER = "E2E-TASK-LEDGER-ALLCANCEL-BODY"

TASK_LEDGER_ALLCANCEL_SPEC = MailflowScenarioSpec(
    label="task_ledger_all_cancelled",
    raw_id_prefix="e2e-task-ledger-allcancel-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_all_cancelled_e2e",
    stub_tag="stub-task-ledger-allcancel-01",
    body_head=f"{E2E_TASK_LEDGER_ALLCANCEL_BODY_MARKER}\ne2e task ledger all-cancelled guard test body",
    min_chat_completion_posts=4,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-task-ledger-all-cancelled-verified",
)


@pytest.fixture()
def task_ledger_all_cancelled_processed_stack(live_e2e_stack_ready: str) -> object:
    """WireMock (task_ledger_all_cancelled) -> inject -> \\Seen -> FSM activity (live stack)."""
    with mailflow_inject_and_wait(TASK_LEDGER_ALLCANCEL_SPEC, live_e2e_stack_ready) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_task_ledger_all_cancelled_full_pipeline(
    task_ledger_all_cancelled_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """All-cancelled guard: cancel both -> finalize BLOCKED -> add done -> egress."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        task_ledger_all_cancelled_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            TASK_LEDGER_ALLCANCEL_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
