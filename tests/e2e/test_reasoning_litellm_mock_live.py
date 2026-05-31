"""Полный почтовый контур со стабами ``test_reasoning_litellm_mock_live`` на живом e2e-стеке.

Как ``test_mailflow_e2e``: SMTP → IMAP-бридж → notmuch → WireMock (цепочка до reasoning с tool_calls).
**Без** вызова ``threlium.states.*`` из процесса pytest — только через входящую почту и FSM в SUT.

Стабы с composite context key изолируют сценарий на общем WireMock (State Extension).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

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
REASONING_E2E_BODY_MARKER = "E2E-REASONING-LITELLM-BODY-MARKER"

REASONING_SPEC = MailflowScenarioSpec(
    label="reasoning_litellm",
    raw_id_prefix="e2e-reasoning-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_reasoning_litellm_mock_live",
    stub_tag="stub-reasoning-litellm-live-01",
    body_head=f"{REASONING_E2E_BODY_MARKER}\ne2e reasoning litellm inbound body",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
)


@pytest.fixture()
def reasoning_litellm_processed_stack(deployed_stack: str) -> object:
    """WireMock (reasoning_litellm) → inject → \\Seen → активность FSM."""
    with mailflow_inject_and_wait(REASONING_SPEC, deployed_stack) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_reasoning_litellm_mailflow_hits_wiremock_full_pipeline(
    reasoning_litellm_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """Живой стек → почтовый вход → notmuch → WireMock (reasoning_litellm) → ответ пользователю."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        reasoning_litellm_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            REASONING_SPEC,
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


E2E_LENGTH_RECOVERY_MARKER = "finish_reason=length"


def _assert_length_recovery_retry_in_journal(project: str, stub_tag: str) -> None:
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    matches = find_wiremock_requests_by_body_contains(
        wm_base, E2E_LENGTH_RECOVERY_MARKER, stub_tag=stub_tag
    )
    chat = [
        e
        for e in matches
        if "/chat/completions" in (e.get("request", {}).get("url") or "")
    ]
    assert len(chat) >= 1, (
        "expected at least one reasoning completion after length recovery system hint"
    )
    log.info("length_recovery_retry_verified", hits=len(chat))


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_reasoning_length_recovery_then_tasks_ledger(
    reasoning_litellm_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """``finish_reason=length`` on first completion → recovery hint → successful tool call."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        reasoning_litellm_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            REASONING_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
        _assert_length_recovery_retry_in_journal(project, stub_tag)
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
