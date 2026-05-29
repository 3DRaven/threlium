"""E2E: formal_reason with a SPARQL query (no inference) → finalize.

Сценарий: reasoning → formal_reason (conforms=true, query=SELECT) →
enrich_fast → reasoning → response_finalize.

Покрытие:
- formal_reason возвращает query_result (SPARQL bindings) в observation-note
- query без inference работает на combined (data+ont) графе
- min 2 chat completion (formal_reason + finalize)
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
E2E_FORMAL_REASON_QUERY_BODY = "E2E-FORMAL-REASON-QUERY-BODY"

FORMAL_REASON_QUERY_SPEC = MailflowScenarioSpec(
    label="formal_reason_query",
    raw_id_prefix="e2e-formal-reason-query-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_formal_reason_query_e2e",
    stub_tag="stub-formal-reason-query-01",
    body_head=(
        f"{E2E_FORMAL_REASON_QUERY_BODY}\n"
        "e2e formal_reason sparql query result test body"
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
    reply_body_needle="e2e-formal-reason-query-verified-answer",
)


@pytest.fixture()
def formal_reason_query_processed_stack(live_e2e_stack_ready: str) -> object:
    with mailflow_inject_and_wait(FORMAL_REASON_QUERY_SPEC, live_e2e_stack_ready) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_formal_reason_query_full_pipeline(
    formal_reason_query_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        formal_reason_query_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            FORMAL_REASON_QUERY_SPEC,
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
