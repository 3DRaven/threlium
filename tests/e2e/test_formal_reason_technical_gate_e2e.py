"""E2E: formal_reason QUERY ERROR → technical gate → retry → finalize.

Сценарий: reasoning → formal_reason (битый SPARQL) → enrich_fast → reasoning
(gate: только formal_reason + memory_query) → formal_reason (исправленный query) →
enrich_fast → reasoning (полный toolset) → tasks_upsert → response_finalize.

Стабы: ``wiremock_stubs/test_formal_reason_technical_gate_e2e/``.
"""
from __future__ import annotations

from pathlib import Path


from .formal_reason_assertions import (
    assert_first_fsm_reasoning_gate_absent,
    assert_gated_reasoning_calls,
    assert_journal_contains,
    assert_ungated_reasoning_has_finalize,
)
from .log import clip_log_body, log
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
E2E_FORMAL_REASON_TECH_GATE_BODY = "E2E-FORMAL-REASON-TECH-GATE-BODY"

FORMAL_REASON_TECH_GATE_SPEC = MailflowScenarioSpec(
    label="formal_reason_technical_gate",
    raw_id_prefix="e2e-formal-reason-tech-gate-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_formal_reason_technical_gate_e2e",
    stub_tag="stub-formal-reason-technical-gate-01",
    body_head=(
        f"{E2E_FORMAL_REASON_TECH_GATE_BODY}\n"
        "e2e formal_reason QUERY ERROR technical gate test body"
    ),
    min_chat_completion_posts=4,
    min_reasoning_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
    reply_body_needle="e2e-formal-reason-tech-gate-verified-answer",
    # Длинный gate-контур: poll tasks_ledger в журнале до GreenMail (finalize+egress в его окне).
    wiremock_journal_ready_needle="call_e2e_tasks_ledger_tech_gate",
)


def test_formal_reason_technical_gate_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    with mailflow_inject_and_wait(FORMAL_REASON_TECH_GATE_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                FORMAL_REASON_TECH_GATE_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            rt = discover_runtime(project, repo_root=REPO_ROOT)
            wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            assert_first_fsm_reasoning_gate_absent(
                wm_base, stub_tag, E2E_FORMAL_REASON_TECH_GATE_BODY
            )
            assert_gated_reasoning_calls(wm_base, stub_tag)
            assert_journal_contains(wm_base, stub_tag, "QUERY ERROR")
            assert_ungated_reasoning_has_finalize(
                wm_base, stub_tag, needle="query_result:"
            )
        except Exception:
            log.error(
                "formal_reason_technical_gate_failed",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
