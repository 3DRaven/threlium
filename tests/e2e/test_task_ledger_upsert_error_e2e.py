"""E2E: валидация ``tasks_upsert`` (unknown content_id) на живом стеке.

Сценарий (один пользовательский ход):

1. ``enrich`` сеет 1 подзадачу (стаб ``081`` → ``[A]``);
2. reasoning #1 → ``tasks_upsert`` с **несуществующим** ``content_id`` в ``subtask_updates`` —
   handler отклоняет (``_ingress_error``) и отбивает в ``ingress`` (``tasks_upsert_error``,
   ledger не мутируется);
3. полный re-enrich (ledger = ``[A]`` pending);
4. reasoning #2 → ``tasks_upsert`` с корректным ``content_id`` подзадачи ``A`` → ``done``;
5. reasoning #3 → ``response_finalize`` — gate проходит (есть ``done``) → ``egress_router``.

Покрытие: ``tasks_upsert`` отвергает update по неизвестному ``content_id`` (bounce в ingress
с notice), повторный корректный upsert проходит и закрывает ledger.

Фазовый автомат WireMock (контекст ``stub-task-ledger-upserterr-01::<root>``):
``active`` → ``phase_upsert_error_done`` → ``phase_upsert_ok_done``.

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
E2E_TASK_LEDGER_UPSERTERR_BODY_MARKER = "E2E-TASK-LEDGER-UPSERTERR-BODY"

TASK_LEDGER_UPSERTERR_SPEC = MailflowScenarioSpec(
    label="task_ledger_upsert_error",
    raw_id_prefix="e2e-task-ledger-upserterr-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_upsert_error_e2e",
    stub_tag="stub-task-ledger-upserterr-01",
    body_head=f"{E2E_TASK_LEDGER_UPSERTERR_BODY_MARKER}\ne2e task ledger upsert-error validation test body",
    min_chat_completion_posts=3,
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
    reply_body_needle="e2e-task-ledger-upsert-error-verified",
)


@pytest.fixture()
def task_ledger_upsert_error_processed_stack(live_e2e_stack_ready: str) -> object:
    """WireMock (task_ledger_upsert_error) -> inject -> \\Seen -> FSM activity (live stack)."""
    with mailflow_inject_and_wait(TASK_LEDGER_UPSERTERR_SPEC, live_e2e_stack_ready) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_task_ledger_upsert_error_full_pipeline(
    task_ledger_upsert_error_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """Upsert validation: bad content_id -> ingress error -> correct upsert -> egress."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        task_ledger_upsert_error_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            TASK_LEDGER_UPSERTERR_SPEC,
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
