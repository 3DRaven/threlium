"""E2E: formal_reason with SHACL violation (conforms=false) → enrich_fast → reasoning → finalize.

Сценарий: reasoning → formal_reason (invalid facts, ex:age -1) → enrich_fast →
reasoning (observation с conforms: False) → response_finalize.

Покрытие:
- formal_reason: conforms=False, violations > 0
- observation-note relay через enrich_fast
- фазовый WireMock State: phase_logic_done

Стабы: ``wiremock_stubs/test_formal_reason_violation_e2e/`` (``stub-formal-reason-violation-01``).
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
E2E_FORMAL_REASON_VIOLATION_BODY = "E2E-FORMAL-REASON-VIOLATION-BODY"
FORMAL_REASON_VIOLATION_SPEC = MailflowScenarioSpec(
    label="formal_reason_violation",
    raw_id_prefix="e2e-formal-reason-viol-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_formal_reason_violation_e2e",
    stub_tag="stub-formal-reason-violation-01",
    body_head=(
        f"{E2E_FORMAL_REASON_VIOLATION_BODY}\n"
        "e2e formal_reason SHACL violation test body"
    ),
    min_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.FORMAL_REASON.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-formal-reason-violation-verified-answer",
)


def _assert_reasoning_saw_violation_observation(project: str, stub_tag: str) -> None:
    """Second reasoning call journal must include SHACL violation observation relay."""
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    for needle in ("conforms: False", "violations:"):
        matches = find_wiremock_requests_by_body_contains(
            wm_base, needle, stub_tag=stub_tag
        )
        chat_matches = [
            e
            for e in matches
            if "/chat/completions" in (e.get("request", {}).get("url") or "")
        ]
        assert chat_matches, (
            f"No chat/completions requests contain {needle!r} "
            f"(stub_tag={stub_tag!r})"
        )
    log.info("formal_reason_violation_observation_verified", stub_tag=stub_tag)


@pytest.fixture()
def formal_reason_violation_processed_stack(deployed_stack: str) -> object:
    with mailflow_inject_and_wait(FORMAL_REASON_VIOLATION_SPEC, deployed_stack) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_formal_reason_violation_full_pipeline(
    formal_reason_violation_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """SHACL violation → observation conforms: False → response_finalize."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        formal_reason_violation_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            FORMAL_REASON_VIOLATION_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
        _assert_reasoning_saw_violation_observation(project, stub_tag)
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
