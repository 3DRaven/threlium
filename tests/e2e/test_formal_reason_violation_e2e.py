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


from tests.e2e.log import clip_log_body, log

from .formal_reason_assertions import (
    assert_journal_contains,
    assert_ungated_reasoning_has_finalize,
    assert_violation_reasoning_without_gate,
)
from .toolkit import (
    E2EComposeRuntime,
    E2EComposeRuntime,
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
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    min_rerank_posts=0,
    reply_body_needle="e2e-formal-reason-violation-verified-answer",
)



def test_formal_reason_violation_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """SHACL violation → observation conforms: False → response_finalize."""
    with mailflow_inject_and_wait(FORMAL_REASON_VIOLATION_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                FORMAL_REASON_VIOLATION_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            rt = discover_runtime(project, repo_root=REPO_ROOT)
            wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            for needle in ("conforms: False", "violations:"):
                assert_journal_contains(wm_base, stub_tag, needle)
            assert_violation_reasoning_without_gate(wm_base, stub_tag)
            assert_ungated_reasoning_has_finalize(
                wm_base, stub_tag, needle="conforms: False"
            )
            log.info("formal_reason_violation_observation_verified", stub_tag=stub_tag)
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
