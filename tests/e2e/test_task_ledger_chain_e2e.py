"""E2E цепочки task-ledger (anti-drift) на **живом** стеке.

Сценарий (один пользовательский ход, durable план задач):

1. ``enrich`` сеет 2 подзадачи через ``enrich_task_plan`` (стаб 081 → ``[A, B]``);
2. reasoning #1 → ``tasks_upsert``: add подзадачи ``C`` (pending) + ``A`` → ``in_progress``;
3. reasoning #2 → ``response_finalize`` — **жёсткий gate** в Python видит открытую работу
   (``A`` in_progress, ``B`` / ``C`` pending) и **отбивает** в ``ingress`` (``task_incomplete``);
4. полный re-enrich (``ingress → enrich → reasoning``) — ledger переживает re-seed
   (ensure-exists: ``A`` остаётся ``in_progress``, статусы не сбрасываются);
5. reasoning #3 → ``tasks_upsert``: batch ``A`` / ``B`` → ``done``, ``C`` → ``cancelled``;
6. reasoning #4 → ``response_finalize`` — gate проходит (есть ``done``, всё терминальное) →
   ``egress_router`` → внешний ответ.

Покрытие:
- content-addressed identity (``content_id`` подзадач в стабах = ``hash(normalize(text))``);
- add новых подзадач + batch смена статусов в одном ``tasks_upsert``;
- монотонная решётка + ensure-exists при re-enrich (нет регресса ``done``/``in_progress``);
- жёсткий gate ``response_finalize`` (pending/in_progress блокируют отправку);
- ``cancelled`` при наличии ``done`` не мешает финализации.

Фазовый автомат WireMock State Extension (контекст ``stub-task-ledger-chain-01::<root>``):
``active`` → ``phase_upsert_start_done`` → ``phase_finalize_blocked_done`` →
``phase_upsert_close_done``.

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
E2E_TASK_LEDGER_BODY_MARKER = "E2E-TASK-LEDGER-CHAIN-BODY"

TASK_LEDGER_CHAIN_SPEC = MailflowScenarioSpec(
    label="task_ledger_chain",
    raw_id_prefix="e2e-task-ledger-chain-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_chain_e2e",
    stub_tag="stub-task-ledger-chain-01",
    body_head=f"{E2E_TASK_LEDGER_BODY_MARKER}\ne2e task ledger chain anti-drift test body",
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
    reply_body_needle="e2e-task-ledger-verified",
)


@pytest.fixture()
def task_ledger_chain_processed_stack(live_e2e_stack_ready: str) -> object:
    """WireMock (task_ledger_chain) -> inject -> \\Seen -> FSM activity (live stack)."""
    with mailflow_inject_and_wait(TASK_LEDGER_CHAIN_SPEC, live_e2e_stack_ready) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_task_ledger_chain_full_pipeline(
    task_ledger_chain_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """Anti-drift: seed -> tasks_upsert(add+in_progress) -> finalize BLOCKED -> close -> egress."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        task_ledger_chain_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            TASK_LEDGER_CHAIN_SPEC,
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
