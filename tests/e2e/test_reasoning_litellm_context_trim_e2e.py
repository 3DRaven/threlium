"""E2e: enrich overflow → summarize_context — TAIL контекста доходит до reasoning, HEAD нет."""
from __future__ import annotations

from pathlib import Path


from tests.e2e.log import clip_log_body, log

from .toolkit import (
    E2EComposeRuntime,
    E2EComposeRuntime,
    E2EComposeRuntime,
    E2E_CTX_TRIM_HEAD_MARKER,
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

REASONING_CTX_TRIM_SPEC = MailflowScenarioSpec(
    label="reasoning_litellm_ctx_trim",
    raw_id_prefix="e2e-reasoning-trim-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_reasoning_litellm_context_trim_live",
    stub_tag="stub-reasoning-litellm-ctx-trim-01",
    body_head=f"{REASONING_E2E_BODY_MARKER}\ne2e reasoning context trim inbound",
    oversized_trim_body=True,
    # RAG-warmup тело (min_rerank_posts=0) не несёт CTX-TRIM HEAD/TAIL маркеров, поэтому
    # ни 075 (HEAD), ни 075_turn2 (TAIL) его distill не матчат. Уникальный warmup-маркер
    # разводит его в отдельный distill-стаб (075_warmup), без priority/doesNotContain.
    warmup_body_extra="E2E-CTX-TRIM-WARMUP-MARKER",
    # Каждый distill-бриф под cap (distill_max_chars=8000); 3 prior + main → overflow.
    summarize_overflow_prior_turns=3,
    min_chat_completion_posts=2,
    min_embedding_posts=1,
    # Как test_summarize_overflow: без RAG-warmup (default min_rerank_posts=1), prior-turn
    # укладывается в 30s greenmail poll.
    min_rerank_posts=0,
)



def test_reasoning_litellm_context_trim_mailflow(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """Длинное тело → overflow → summarize_context; reasoning LiteLLM в журнале WireMock содержит TAIL, не HEAD."""
    with mailflow_inject_and_wait(REASONING_CTX_TRIM_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
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
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
