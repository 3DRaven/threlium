"""E2E: subagent frame isolation (IrtSubagentMarker) on compose stack.

Two scenarios (separate stub_tag / WireMock state per test):

1. **Response buffer** — L0 ``response_append`` chunk must not appear in L1
   ``response_finalize`` LLM prompt (per-frame ``stop_at_route`` collect).
2. **Task-ledger** — L0 open subtask must not block L1 finalize gate (per-frame
   task collect without parent ledger).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .helpers import (
    MailflowScenarioSpec,
    REPO_ROOT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
)
from .wiremock_client import (
    find_wiremock_requests_by_body_contains,
    journal_entries_for_stub_tag,
    wiremock_public_base,
    _journal_chat_completion_user_content,
    _journal_request_anchor_haystack,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_SUBAGENT_ISO_BODY = "E2E-SUBAGENT-FRAME-ISO"
E2E_L0_BUFFER_MARKER = "E2E-ISO-L0-BUFFER-CHUNK-MUST-NOT-LEAK-TO-L1"
E2E_L1_RESULT_MARKER = "E2E-ISO-L1-FRAME-RESULT-ONLY"

RESPONSE_ISO_SPEC = MailflowScenarioSpec(
    label="subagent_response_frame_iso",
    raw_id_prefix="e2e-subagent-resp-iso-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_subagent_frame_isolation_e2e",
    stub_tag="stub-subagent-frame-iso-01",
    body_head=f"{E2E_SUBAGENT_ISO_BODY}\ne2e subagent response buffer frame isolation",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.RESPONSE_APPEND.value,
        FsmStage.SUBAGENT_INTENT.value,
        FsmStage.SUBAGENT_END.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-subagent-frame-iso-verified",
)

LEDGER_ISO_SPEC = MailflowScenarioSpec(
    label="subagent_ledger_frame_iso",
    raw_id_prefix="e2e-subagent-ledger-iso-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_subagent_ledger_isolation_e2e",
    stub_tag="stub-subagent-ledger-iso-01",
    body_head=f"{E2E_SUBAGENT_ISO_BODY}\ne2e subagent task-ledger frame isolation",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.SUBAGENT_INTENT.value,
        FsmStage.SUBAGENT_END.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-subagent-frame-iso-verified",
)


def _journal_bodies_for_stub(
    project: str, *, stub_tag: str, correlation_key: str, needle: str
) -> list[str]:
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    bodies: list[str] = []
    for entry in journal_entries_for_stub_tag(wm_base, stub_tag=stub_tag):
        if not isinstance(entry, dict):
            continue
        req = entry.get("request")
        if not isinstance(req, dict) or req.get("method") != "POST":
            continue
        url = str(req.get("url") or "")
        if "/chat/completions" not in url:
            continue
        hay = _journal_request_anchor_haystack(entry)
        if correlation_key not in hay:
            continue
        body = _journal_chat_completion_user_content(entry)
        if body and needle in body:
            bodies.append(body)
    return bodies


def _assert_l1_finalize_prompt_excludes_l0_buffer(
    project: str, *, stub_tag: str, correlation_key: str
) -> None:
    """L1 finalize reasoning hop must not see L0 response buffer in its prompt."""
    bodies = _journal_bodies_for_stub(
        project,
        stub_tag=stub_tag,
        correlation_key=correlation_key,
        needle=E2E_L1_RESULT_MARKER,
    )
    assert bodies, (
        f"L1 finalize prompt must contain {E2E_L1_RESULT_MARKER!r} in WM journal "
        f"(stub_tag={stub_tag!r})"
    )
    for body in bodies:
        assert E2E_L0_BUFFER_MARKER not in body, (
            "L0 response_append buffer leaked into L1 frame reasoning prompt "
            f"(IrtSubagentMarker / stop_at_route isolation regression)"
        )
    log.info("subagent_response_frame_iso_l1_prompt_verified", stub_tag=stub_tag)


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_subagent_response_buffer_frame_isolation(deployed_stack: str) -> None:
    """L0 append chunk must not appear in L1 finalize LLM prompt."""
    project = deployed_stack
    try:
        with mailflow_inject_and_wait(RESPONSE_ISO_SPEC, project) as (
            _p,
            raw_id,
            _canon,
            nm_inner,
            stub_tag,
            correlation_key,
        ):
            assert_full_mailflow_pipeline(
                RESPONSE_ISO_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            _assert_l1_finalize_prompt_excludes_l0_buffer(
                project, stub_tag=stub_tag, correlation_key=correlation_key
            )
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_subagent_task_ledger_frame_isolation(deployed_stack: str) -> None:
    """L0 open subtask must not block L1 finalize (isolated per-frame ledger)."""
    project = deployed_stack
    try:
        with mailflow_inject_and_wait(LEDGER_ISO_SPEC, project) as (
            _p,
            raw_id,
            _canon,
            nm_inner,
            stub_tag,
            correlation_key,
        ):
            assert_full_mailflow_pipeline(
                LEDGER_ISO_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            rt = discover_runtime(project, repo_root=REPO_ROOT)
            wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            blocked = find_wiremock_requests_by_body_contains(
                wm_base,
                "task_incomplete",
                stub_tag=stub_tag,
            )
            assert not blocked, (
                "L1 finalize was blocked by L0 open ledger — frame isolation regression"
            )
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
