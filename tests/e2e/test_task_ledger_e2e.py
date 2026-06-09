"""E2E task-ledger variants (fail-closed gate) — parametrized mailflow scenarios on live stack.

Consolidates chain / empty-blocked / all-cancelled / upsert-error / bypass scenarios.
Each variant keeps its own WireMock stub directory; shared LightRAG stubs remain duplicated
per directory (identical JSON, scenario-specific reasoning phases only).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log

from .toolkit import (
    E2EComposeRuntime,
    E2EComposeRuntime,
    MailflowScenarioSpec,
    assert_full_mailflow_pipeline,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    REPO_ROOT,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"

TASK_LEDGER_SPECS: tuple[MailflowScenarioSpec, ...] = (
    MailflowScenarioSpec(
        label="task_ledger_chain",
        raw_id_prefix="e2e-task-ledger-chain-",
        stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_chain_e2e",
        stub_tag="stub-task-ledger-chain-01",
        body_head="E2E-TASK-LEDGER-CHAIN-BODY\ne2e task ledger chain anti-drift test body",
        min_chat_completion_posts=4,
        min_embedding_posts=1,
        min_rerank_posts=0,
        reply_body_needle="e2e-task-ledger-verified",
        wiremock_journal_ready_needle="call_e2e_finalize_ok",
    ),
    MailflowScenarioSpec(
        label="task_ledger_bypass",
        raw_id_prefix="e2e-task-ledger-bypass-",
        stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_bypass_e2e",
        stub_tag="stub-task-ledger-bypass-01",
        body_head="E2E-TASK-LEDGER-BYPASS-BODY\ne2e task ledger blocker bypass test body",
        min_chat_completion_posts=2,
        min_embedding_posts=1,
        min_rerank_posts=0,
        reply_body_needle="e2e-task-ledger-bypass-verified",
        wiremock_journal_ready_needle="call_e2e_finalize_bypass_ok",
    ),
    MailflowScenarioSpec(
        label="task_ledger_empty_blocked",
        raw_id_prefix="e2e-task-ledger-empty-",
        stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_empty_blocked_e2e",
        stub_tag="stub-task-ledger-empty-01",
        body_head="E2E-TASK-LEDGER-EMPTY-BODY\ne2e task ledger empty-blocked fail-closed test body",
        min_chat_completion_posts=3,
        min_embedding_posts=1,
        min_rerank_posts=0,
        reply_body_needle="e2e-task-ledger-empty-verified",
        wiremock_journal_ready_needle="call_e2e_empty_finalize_ok",
    ),
    MailflowScenarioSpec(
        label="task_ledger_all_cancelled",
        raw_id_prefix="e2e-task-ledger-allcancel-",
        stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_all_cancelled_e2e",
        stub_tag="stub-task-ledger-allcancel-01",
        body_head="E2E-TASK-LEDGER-ALLCANCEL-BODY\ne2e task ledger all-cancelled guard test body",
        min_chat_completion_posts=4,
        min_embedding_posts=1,
        min_rerank_posts=0,
        reply_body_needle="e2e-task-ledger-all-cancelled-verified",
        wiremock_journal_ready_needle="call_e2e_allcancel_finalize_ok",
    ),
    MailflowScenarioSpec(
        label="task_ledger_upsert_error",
        raw_id_prefix="e2e-task-ledger-upserterr-",
        stub_dir=_WIREMOCK_STUBS_ROOT / "test_task_ledger_upsert_error_e2e",
        stub_tag="stub-task-ledger-upserterr-01",
        body_head="E2E-TASK-LEDGER-UPSERTERR-BODY\ne2e task ledger upsert-error validation test body",
        min_chat_completion_posts=3,
        min_embedding_posts=1,
        min_rerank_posts=0,
        reply_body_needle="e2e-task-ledger-upsert-error-verified",
        wiremock_journal_ready_needle="call_e2e_upserterr_finalize_ok",
    ),
)


@pytest.mark.parametrize("spec", TASK_LEDGER_SPECS, ids=[s.label for s in TASK_LEDGER_SPECS])
def test_task_ledger_variant_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
    spec: MailflowScenarioSpec,
) -> None:
    """Parametrized task-ledger gate scenarios (chain / bypass / empty / all-cancel / upsert-error)."""
    try:
        with mailflow_inject_and_wait(spec, e2e_runtime.project_name) as (
            project,
            raw_id,
            _canonical_id,
            nm_inner,
            stub_tag,
            correlation_key,
        ):
            assert_full_mailflow_pipeline(
                spec,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(
                dump_failure_artifacts(e2e_runtime.project_name, repo_root=REPO_ROOT)
            ),
        )
        raise
