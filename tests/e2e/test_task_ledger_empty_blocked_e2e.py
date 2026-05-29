"""E2E: fail-closed на **пустом** task-ledger (anti-drift gate) на живом стеке.

Сценарий (один пользовательский ход):

1. ``enrich`` сеет **пустой** ledger — bootstrap-стаб ``009`` отдаёт ``[]`` (override ``081``
   для этого сценария намеренно отсутствует);
2. reasoning #1 → ``response_finalize`` с контентом — **жёсткий gate** видит пустой ledger
   (fail-closed) и **отбивает** в ``ingress`` (``task_incomplete``, ветка ``ledger_empty``);
3. полный re-enrich (ledger всё ещё пуст);
4. reasoning #2 → ``tasks_upsert``: одна подзадача сразу ``done`` (trivial-answer path);
5. reasoning #3 → ``response_finalize`` — gate проходит (есть ``done``) → ``egress_router`` →
   внешний ответ.

Покрытие: пустой ledger блокирует finalize (fail-closed, в отличие от прежнего fail-open);
trivial-ответ закрывается одной ``done``-подзадачей через ``tasks_upsert``.

Фазовый автомат WireMock (контекст ``stub-task-ledger-empty-01::<root>``):
``active`` → ``phase_finalize_empty_blocked`` → ``phase_upsert_close_done``.

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
E2E_TASK_LEDGER_EMPTY_BODY_MARKER = "E2E-TASK-LEDGER-EMPTY-BODY"

TASK_LEDGER_EMPTY_SPEC = MailflowScenarioSpec(
    label="task_ledger_empty_blocked",
    raw_id_prefix="e2e-task-ledger-empty-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_empty_blocked_e2e",
    stub_tag="stub-task-ledger-empty-01",
    body_head=f"{E2E_TASK_LEDGER_EMPTY_BODY_MARKER}\ne2e task ledger empty-blocked fail-closed test body",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-task-ledger-empty-verified",
)


@pytest.fixture()
def task_ledger_empty_processed_stack(live_e2e_stack_ready: str) -> object:
    """WireMock (task_ledger_empty_blocked) -> inject -> \\Seen -> FSM activity (live stack)."""
    with mailflow_inject_and_wait(TASK_LEDGER_EMPTY_SPEC, live_e2e_stack_ready) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_task_ledger_empty_blocked_full_pipeline(
    task_ledger_empty_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """Fail-closed: empty ledger -> finalize BLOCKED -> tasks_upsert(one done) -> egress."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        task_ledger_empty_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            TASK_LEDGER_EMPTY_SPEC,
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
