"""E2e: overflow unified weight (``estimate_unified_weight``) → summarize_context → enrich → reasoning."""
from __future__ import annotations

import uuid
from pathlib import Path


from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage, NotmuchTag

from .helpers import (
    E2EComposeRuntime,
    E2EComposeRuntime,
    E2EComposeRuntime,
    E2E_SUM_ORIG_HEAD_MARKER,
    E2E_SUM_ORIG_PAD_MARKER,
    E2E_SUMMARY_MARKER,
    E2E_SUMMARIZE_LLM_NEEDLE,
    MailflowScenarioSpec,
    REPO_ROOT,
    assert_full_mailflow_pipeline,
    assert_notmuch_folder_contains_body_token,
    assert_notmuch_thread_tag_count,
    discover_runtime,
    dump_failure_artifacts,
    e2e_dense_threlium_ctx_body,
    email_ingress_notmuch_id_inner,
    greenmail_wait_agent_reply_message_id,
    mailflow_inject_and_wait,
    mailflow_wait_fsm_maildir_activity,
    smtp_inject_inbound,
    wait_for_greenmail_inbox_message_gone_host,
    wait_for_greenmail_user_reply,
)
from .test_reasoning_litellm_mock_live import REASONING_E2E_BODY_MARKER
from .wiremock_client import (
    find_wiremock_requests_by_body_contains,
    journal_entries_for_stub_tag,
    wiremock_public_base,
    _journal_chat_completion_user_content,
    _journal_request_anchor_haystack,
    _wiremock_headers_get_ci,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"

SUMMARIZE_CONTEXT_SPEC = MailflowScenarioSpec(
    label="summarize_context_e2e",
    raw_id_prefix="e2e-summarize-ctx-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_summarize_context_e2e",
    stub_tag="stub-summarize-context-e2e-01",
    body_head=f"{REASONING_E2E_BODY_MARKER}\ne2e summarize context overflow inbound",
    summarize_overflow_body=True,
    # Каждый distill-бриф под cap (distill_max_chars=8000); 3 старых хода + основной
    # накапливают unified (mckp_capacity≈4667 при context_max_chars=8000) → overflow → summarize.
    summarize_overflow_prior_turns=3,
    min_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.SUMMARIZE_CONTEXT.value,
        FsmStage.SUMMARIZE_MEMORY.value,
        FsmStage.REASONING.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
)


def _count_summarize_llm_posts(wm_base: str, *, stub_tag: str) -> int:
    return len(
        find_wiremock_requests_by_body_contains(
            wm_base,
            E2E_SUMMARIZE_LLM_NEEDLE,
            stub_tag=stub_tag,
        )
    )


def _summarize_context_user_content_merged(
    wm_base: str,
    *,
    stub_tag: str,
) -> str:
    """Merged user content from summarize_context LLM POSTs (WireMock journal)."""
    parts: list[str] = []
    for entry in journal_entries_for_stub_tag(wm_base, stub_tag=stub_tag):
        if not isinstance(entry, dict):
            continue
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "POST":
            continue
        url = str(req.get("url") or "")
        if "/chat/completions" not in url and not url.rstrip("/").endswith(
            "chat/completions"
        ):
            continue
        call_site = _wiremock_headers_get_ci(
            req.get("headers"), "X-Threlium-Call-Site"
        )
        hay = _journal_request_anchor_haystack(entry)
        if call_site != "summarize_thread_context" and E2E_SUMMARIZE_LLM_NEEDLE not in hay:
            continue
        body = _journal_chat_completion_user_content(entry)
        if body:
            parts.append(body)
    return "\n".join(parts)


def _reasoning_user_bodies_for_correlation(
    wm_base: str,
    *,
    stub_tag: str,
    correlation_key: str,
) -> list[str]:
    out: list[str] = []
    for entry in journal_entries_for_stub_tag(wm_base, stub_tag=stub_tag):
        if not isinstance(entry, dict):
            continue
        req = entry.get("request")
        if not isinstance(req, dict):
            continue
        if str(req.get("method") or "").upper() != "POST":
            continue
        url = str(req.get("url") or "")
        if "/chat/completions" not in url and not url.rstrip("/").endswith("chat/completions"):
            continue
        hay = _journal_request_anchor_haystack(entry)
        if correlation_key not in hay or "<envelope>" not in hay or '"tools"' not in hay:
            continue
        body = _journal_chat_completion_user_content(entry)
        if body:
            out.append(body)
    return out


def _assert_summarize_pipeline_artifacts(
    *,
    project: str,
    nm_inner: str,
    stub_tag: str,
    correlation_key: str,
) -> int:
    assert_notmuch_thread_tag_count(
        project,
        anchor_message_id=nm_inner,
        tag=NotmuchTag.CONTEXT_SUMMARIZED.value,
        min_count=1,
        repo_root=REPO_ROOT,
    )
    assert_notmuch_folder_contains_body_token(
        project,
        stage_folder_id=FsmStage.SUMMARIZE_MEMORY.value,
        body_token=E2E_SUMMARY_MARKER,
        repo_root=REPO_ROOT,
    )
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    n_summarize = _count_summarize_llm_posts(wm_base, stub_tag=stub_tag)
    assert n_summarize >= 1, (
        f"expected at least one summarize_context LLM POST, got {n_summarize}"
    )
    reasoning_bodies = _reasoning_user_bodies_for_correlation(
        wm_base, stub_tag=stub_tag, correlation_key=correlation_key
    )
    assert reasoning_bodies, "no reasoning chat/completions in WireMock journal"
    merged = "\n".join(reasoning_bodies)
    assert E2E_SUMMARY_MARKER in merged, "reasoning context should include durable summary marker"
    assert E2E_SUM_ORIG_PAD_MARKER not in merged, (
        "summarized originals (pad block) must not appear in reasoning user content"
    )
    merged_summarize = _summarize_context_user_content_merged(wm_base, stub_tag=stub_tag)
    assert merged_summarize, "no summarize_context LLM user content in WireMock journal"
    assert "## User intent" in merged_summarize, (
        "summarize overflow bodies must use concat_history_parts_text (distill headings)"
    )
    assert E2E_SUM_ORIG_HEAD_MARKER in merged_summarize, (
        "summarize input must include HEAD from inject (via distill), not drop overflow bodies"
    )
    assert E2E_SUM_ORIG_PAD_MARKER not in merged_summarize, (
        "summarize input must not include raw PAD block (get_body regression)"
    )
    return n_summarize



def test_summarize_overflow_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """Переполнение unified → summarize FSM → тег context_summarized → reasoning с маркером summary."""
    with mailflow_inject_and_wait(SUMMARIZE_CONTEXT_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                SUMMARIZE_CONTEXT_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            _assert_summarize_pipeline_artifacts(
                project=project,
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


def test_summarize_idempotent_second_enrich(e2e_runtime: E2EComposeRuntime) -> None:
    """Второе письмо в том же треде не вызывает повторный summarize LLM."""
    with mailflow_inject_and_wait(SUMMARIZE_CONTEXT_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        assert_full_mailflow_pipeline(
            SUMMARIZE_CONTEXT_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
        n_after_first = _assert_summarize_pipeline_artifacts(
            project=project,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )

        raw_id2 = f"e2e-summarize-ctx2-{uuid.uuid4().hex}@localhost"
        nm_inner2 = email_ingress_notmuch_id_inner(raw_id2)
        rt = discover_runtime(project, repo_root=REPO_ROOT)
        wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
        # Реалистичный threading: второе письмо тредится на ОТВЕТ агента (его Message-ID),
        # а не на исходный inbound. Тогда IRT-цепочка второго хода проходит через tasks_upsert
        # первого хода (egress glue-record), per-frame task-ledger наследуется и finalize-gate
        # проходит без ручного сброса WireMock-латча phase_tasks_ledger_done.
        agent_reply_mid = greenmail_wait_agent_reply_message_id(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            in_reply_to_anchor=raw_id,
        )
        smtp_inject_inbound(
            project,
            checkout="/unused",
            repo_root=REPO_ROOT,
            message_id=raw_id2,
            in_reply_to=agent_reply_mid,
            body=e2e_dense_threlium_ctx_body(
                head="e2e summarize second turn short body",
                correlation_key=correlation_key,
            ),
        )
        wait_for_greenmail_inbox_message_gone_host(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            message_id=raw_id2,
        )
        mailflow_wait_fsm_maildir_activity(
            project, repo_root=REPO_ROOT, message_id=nm_inner2
        )
        wait_for_greenmail_user_reply(
            project,
            raw_id=raw_id2,
            repo_root=REPO_ROOT,
        )

        n_after_second = _count_summarize_llm_posts(wm_base, stub_tag=stub_tag)
        assert n_after_second == n_after_first, (
            f"expected idempotent summarize LLM count {n_after_first}, got {n_after_second}"
        )
