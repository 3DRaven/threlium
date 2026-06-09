"""E2E тест стадии memory_query → response_finalize.

Сценарий: reasoning → memory_query (retrieve marker) → enrich_fast → reasoning →
response_finalize.

Покрытие:
- memory_query handler: aquery к LightRAG, observation relay через enrich_fast
- Доказательство сквозного data flow: query-маркер доходит до embedding API
- enrich_fast: relay OBSERVATION_NOTE между reasoning хопами

Стабы используют фазовый автомат WireMock State Extension:
- phase_query_done: после первого reasoning → memory_query
- Второй reasoning видит phase_query_done → response_finalize
"""
from __future__ import annotations

from pathlib import Path

from tests.e2e.log import clip_log_body, log

from .toolkit import (
    E2EComposeRuntime,
    E2EComposeRuntime,
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
E2E_MEMORY_QUERY_BODY_MARKER = "E2E-MEMORY-QUERY-BODY"
E2E_MEMORY_QUERY_MARKER = "E2E-MEMORY-QUERY-MARKER"

MEMORY_QUERY_SPEC = MailflowScenarioSpec(
    label="memory_query",
    raw_id_prefix="e2e-mem-query-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_memory_query_e2e",
    stub_tag="stub-memory-query-01",
    body_head=f"{E2E_MEMORY_QUERY_BODY_MARKER}\ne2e memory query verification test body",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    min_rerank_posts=0,
    reply_body_needle="e2e-memory-query-verified-answer",
)


def _assert_embedding_contains_query_marker(project: str, stub_tag: str) -> None:
    """Verify that at least one embedding request contains the query marker text.

    This proves the data round-trip: stub tool_call -> handler parse ->
    rag.aquery(payload.query) -> embedding API with the expected query text.
    """
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    matches = find_wiremock_requests_by_body_contains(
        wm_base, E2E_MEMORY_QUERY_MARKER, stub_tag=stub_tag
    )
    embedding_matches = [
        e for e in matches
        if "/embeddings" in (e.get("request", {}).get("url") or "")
    ]
    assert embedding_matches, (
        f"No embedding requests contain query marker {E2E_MEMORY_QUERY_MARKER!r} "
        f"(found {len(matches)} total matches in journal for stub_tag={stub_tag!r}). "
        "This means memory_query did not pass the expected query text to LightRAG."
    )
    log.info(
        "roundtrip_embedding_marker_verified",
        marker=E2E_MEMORY_QUERY_MARKER,
        embedding_hits=len(embedding_matches),
    )


def test_memory_query_full_pipeline(e2e_runtime: E2EComposeRuntime) -> None:
    """Memory system: memory_query(retrieve) -> response_finalize."""
    with mailflow_inject_and_wait(MEMORY_QUERY_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                MEMORY_QUERY_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            _assert_embedding_contains_query_marker(project, stub_tag)
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
