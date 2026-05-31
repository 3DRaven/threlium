"""E2E matrix: fatal parse → gate → memory_query → QUERY ERROR → gate → recovery → finalize.

Один inject, фазовый WireMock State (``stub-formal-reason-gate-matrix-01``).
Стабы: ``wiremock_stubs/test_formal_reason_gate_recovery_matrix_e2e/``.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from threlium.types import FsmStage

from .formal_reason_assertions import (
    assert_gated_reasoning_calls,
    assert_gated_reasoning_includes_memory_query,
    assert_journal_contains,
    assert_ungated_reasoning_has_finalize,
)
from .helpers import (
    MailflowScenarioSpec,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    REPO_ROOT,
)
from .wiremock_client import (
    find_wiremock_requests_by_body_contains,
    wiremock_public_base,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_FORMAL_REASON_GATE_MATRIX_BODY = "E2E-FORMAL-REASON-GATE-MATRIX-BODY"

FORMAL_REASON_GATE_MATRIX_SPEC = MailflowScenarioSpec(
    label="formal_reason_gate_recovery_matrix",
    raw_id_prefix="e2e-formal-reason-gate-matrix-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_formal_reason_gate_recovery_matrix_e2e",
    stub_tag="stub-formal-reason-gate-matrix-01",
    body_head=(
        f"{E2E_FORMAL_REASON_GATE_MATRIX_BODY}\n"
        "e2e formal_reason gate recovery matrix test body"
    ),
    min_chat_completion_posts=8,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.FORMAL_REASON.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.MEMORY_QUERY.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-formal-reason-gate-matrix-verified-answer",
)


def _assert_at_least_two_gated_reasoning_calls(wm_base: str, stub_tag: str) -> None:
    matches = find_wiremock_requests_by_body_contains(
        wm_base, "FORMAL REASON GATE", stub_tag=stub_tag
    )
    chat = [
        e
        for e in matches
        if "/chat/completions" in (e.get("request", {}).get("url") or "")
    ]
    assert len(chat) >= 2, (
        f"expected at least 2 gated reasoning calls, got {len(chat)} "
        f"(stub_tag={stub_tag!r})"
    )


@pytest.fixture()
def formal_reason_gate_matrix_stack(live_e2e_stack_ready: str) -> object:
    with mailflow_inject_and_wait(
        FORMAL_REASON_GATE_MATRIX_SPEC, live_e2e_stack_ready
    ) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_formal_reason_gate_recovery_matrix_full_pipeline(
    formal_reason_gate_matrix_stack: tuple[str, str, str, str, str, str],
) -> None:
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        formal_reason_gate_matrix_stack
    )
    try:
        assert_full_mailflow_pipeline(
            FORMAL_REASON_GATE_MATRIX_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
        rt = discover_runtime(project, repo_root=REPO_ROOT)
        wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
        assert_journal_contains(wm_base, stub_tag, "PARSE ERROR")
        assert_journal_contains(wm_base, stub_tag, "FSM locked")
        assert_journal_contains(wm_base, stub_tag, "QUERY ERROR")
        _assert_at_least_two_gated_reasoning_calls(wm_base, stub_tag)
        assert_gated_reasoning_calls(wm_base, stub_tag)
        assert_gated_reasoning_includes_memory_query(wm_base, stub_tag)
        assert_ungated_reasoning_has_finalize(
            wm_base, stub_tag, needle="query_result:"
        )
    except Exception:
        dump_failure_artifacts(
            FORMAL_REASON_GATE_MATRIX_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            repo_root=REPO_ROOT,
        )
        raise
