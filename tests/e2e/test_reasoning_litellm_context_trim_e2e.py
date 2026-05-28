"""E2e: enrich overflow (summarize_enabled) → summarize_context — TAIL контекста доходит до reasoning, HEAD нет."""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .helpers import (
    E2E_CTX_TRIM_HEAD_MARKER,
    E2E_CTX_TRIM_JOURNAL_SLACK_CHARS,
    E2E_CTX_TRIM_TAIL_MARKER,
    MailflowScenarioSpec,
    REPO_ROOT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
)
from .test_reasoning_litellm_mock_live import REASONING_E2E_BODY_MARKER
from .wiremock_client import (
    assert_wiremock_reasoning_journal_preserves_context_tail,
    wiremock_public_base,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"

# e2e group_vars: context_max_chars=8000
E2E_CONTEXT_MAX_CHARS = 8000

REASONING_CTX_TRIM_SPEC = MailflowScenarioSpec(
    label="reasoning_litellm_ctx_trim",
    raw_id_prefix="e2e-reasoning-trim-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_reasoning_litellm_context_trim_live",
    stub_tag="stub-reasoning-litellm-ctx-trim-01",
    body_head=f"{REASONING_E2E_BODY_MARKER}\ne2e reasoning context trim inbound",
    oversized_trim_body=True,
    warmup_body_extra=E2E_CTX_TRIM_TAIL_MARKER,
    min_chat_completion_posts=1,
    min_embedding_posts=1,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.SUMMARIZE_CONTEXT.value,
        FsmStage.SUMMARIZE_MEMORY.value,
        FsmStage.REASONING.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
)


@pytest.fixture()
def reasoning_ctx_trim_processed_stack(deployed_stack: str) -> object:
    with mailflow_inject_and_wait(REASONING_CTX_TRIM_SPEC, deployed_stack) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_reasoning_litellm_context_trim_mailflow(
    reasoning_ctx_trim_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """Длинное тело → overflow → summarize_context; reasoning LiteLLM в журнале WireMock содержит TAIL, не HEAD."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        reasoning_ctx_trim_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            REASONING_CTX_TRIM_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
        rt = discover_runtime(project, repo_root=REPO_ROOT)
        wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
        assert_wiremock_reasoning_journal_preserves_context_tail(
            wm_base,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
            tail_marker=E2E_CTX_TRIM_TAIL_MARKER,
            head_marker=E2E_CTX_TRIM_HEAD_MARKER,
            max_body_chars=E2E_CONTEXT_MAX_CHARS,
            journal_slack_chars=E2E_CTX_TRIM_JOURNAL_SLACK_CHARS,
        )
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
