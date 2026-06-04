"""E2E: formal_reason with RDFS inference and return_derived → finalize.

Сценарий: reasoning → formal_reason (inference=rdfs, return_derived=true) →
enrich_fast → reasoning → response_finalize.

Покрытие:
- formal_reason возвращает derived_triples в observation-note
- min 2 chat completion (formal_reason + finalize)
"""
from __future__ import annotations

from pathlib import Path

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .formal_reason_assertions import assert_all_reasoning_gate_absent
from .toolkit import (
    E2EComposeRuntime,
    MailflowScenarioSpec,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    REPO_ROOT,
)
from .wiremock_client import wiremock_public_base

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_FORMAL_REASON_INFERENCE_BODY = "E2E-FORMAL-REASON-INFERENCE-BODY"

FORMAL_REASON_INFERENCE_SPEC = MailflowScenarioSpec(
    label="formal_reason_inference",
    raw_id_prefix="e2e-formal-reason-inference-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_formal_reason_inference_e2e",
    stub_tag="stub-formal-reason-inference-01",
    body_head=(
        f"{E2E_FORMAL_REASON_INFERENCE_BODY}\n"
        "e2e formal_reason inference derived triples test body"
    ),
    min_chat_completion_posts=4,
    min_reasoning_chat_completion_posts=2,
    wiremock_journal_ready_needle="call_e2e_tasks_ledger_phase_formal_reason_done_ledger_done",
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.FORMAL_REASON.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-formal-reason-inference-verified-answer",
)


def test_formal_reason_inference_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    with mailflow_inject_and_wait(FORMAL_REASON_INFERENCE_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                FORMAL_REASON_INFERENCE_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            rt = discover_runtime(project, repo_root=REPO_ROOT)
            wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            assert_all_reasoning_gate_absent(wm_base, stub_tag)
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
