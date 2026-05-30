"""E2E: IRT whitelist unified context — observe seed thread + turn2 reply.

Turn 1: append → observe → finalize (``stub-unified-context-roles-01``).
Turn 2: reply in same thread; reasoning journal must include ingress/observe markers
and must not include enrich-service leak marker.
"""
from __future__ import annotations

import re
import uuid
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
    greenmail_wait_agent_reply_message_id,
    e2e_dense_threlium_ctx_body,
    email_ingress_notmuch_id_inner,
    mailflow_inject_and_wait,
    mailflow_wait_fsm_maildir_activity,
    smtp_inject_inbound,
    wait_for_greenmail_inbox_message_gone_host,
    wait_for_greenmail_user_reply,
)
from .wiremock_client import (
    journal_entries_for_stub_tag,
    wiremock_public_base,
    _journal_chat_completion_user_content,
    _journal_request_anchor_haystack,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_ROLE_INGRESS_SEED = "E2E-ROLE-INGRESS-SEED"
E2E_ROLE_INGRESS_TURN2 = "E2E-ROLE-INGRESS-TURN2"
E2E_OBSERVED_CHUNK = "E2E-OBSERVED-CHUNK"
_SERVICE_STAGE_ADDR_LEAK = "enrich_fast@localhost"

UNIFIED_CONTEXT_TURN1_SPEC = MailflowScenarioSpec(
    label="unified_context_roles_turn1",
    raw_id_prefix="e2e-uctx-roles-seed-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_unified_context_roles_e2e",
    stub_tag="stub-unified-context-roles-01",
    body_head=f"{E2E_ROLE_INGRESS_SEED}\ne2e unified context roles seed",
    min_chat_completion_posts=5,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.RESPONSE_APPEND.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.RESPONSE_OBSERVE.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e",
)


_CONVERSATION_HISTORY_RE = re.compile(
    r"<conversation_history>\s*(.*?)\s*</conversation_history>",
    re.DOTALL | re.IGNORECASE,
)


def _conversation_history_from_reasoning_journal(merged: str) -> str:
    """``<conversation_history>`` only — not per-hop ``<envelope>`` (From relay stage)."""
    return "\n".join(m.group(1) for m in _CONVERSATION_HISTORY_RE.finditer(merged))


def _reasoning_user_bodies(wm_base: str, *, stub_tag: str, correlation_key: str) -> str:
    parts: list[str] = []
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
        if correlation_key not in hay or "<envelope>" not in hay:
            continue
        body = _journal_chat_completion_user_content(entry)
        if body:
            parts.append(body)
    return "\n".join(parts)


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_unified_context_roles_two_turn(deployed_stack: str) -> None:
    """Turn1 observe cycle; turn2 reasoning context = deduped <history> stream (no relay blob)."""
    project = deployed_stack
    try:
        with mailflow_inject_and_wait(UNIFIED_CONTEXT_TURN1_SPEC, project) as (
            _p,
            seed_raw,
            _canon,
            seed_nm,
            stub_tag,
            correlation_key,
        ):
            assert_full_mailflow_pipeline(
                UNIFIED_CONTEXT_TURN1_SPEC,
                project=project,
                raw_id=seed_raw,
                nm_inner=seed_nm,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )

            rt = discover_runtime(project, repo_root=REPO_ROOT)
            wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            agent_reply_mid = greenmail_wait_agent_reply_message_id(
                rt.greenmail_imap_host,
                rt.greenmail_imap_port,
                in_reply_to_anchor=seed_raw,
                body_substring=UNIFIED_CONTEXT_TURN1_SPEC.reply_body_needle or "e2e",
            )
            raw2 = f"e2e-uctx-roles-turn2-{uuid.uuid4().hex}@localhost"
            smtp_inject_inbound(
                project,
                checkout="/unused",
                repo_root=REPO_ROOT,
                message_id=raw2,
                in_reply_to=agent_reply_mid,
                body=e2e_dense_threlium_ctx_body(
                    head=f"{E2E_ROLE_INGRESS_TURN2}\ne2e unified context roles turn2",
                    correlation_key=correlation_key,
                ),
            )
            wait_for_greenmail_inbox_message_gone_host(
                rt.greenmail_imap_host, rt.greenmail_imap_port, message_id=raw2
            )
            nm2 = email_ingress_notmuch_id_inner(raw2)
            mailflow_wait_fsm_maildir_activity(
                project, repo_root=REPO_ROOT, message_id=nm2
            )
            wait_for_greenmail_user_reply(
                project,
                raw_id=raw2,
                repo_root=REPO_ROOT,
                body_substring="e2e-unified-context-roles-verified",
            )

            merged = _reasoning_user_bodies(
                wm_base, stub_tag=stub_tag, correlation_key=correlation_key
            )
            assert E2E_ROLE_INGRESS_SEED in merged, "turn1 ingress marker missing from reasoning"
            assert E2E_ROLE_INGRESS_TURN2 in merged, "turn2 ingress marker missing from reasoning"
            assert E2E_OBSERVED_CHUNK in merged, "observe chunk missing from reasoning context"
            mail_ctx = _conversation_history_from_reasoning_journal(merged)
            assert mail_ctx, "no <conversation_history> in reasoning WireMock journal"
            assert _SERVICE_STAGE_ADDR_LEAK not in mail_ctx.lower(), (
                "enrich_fast relay blob must collapse by content-CID dedup (and To: line is "
                "not rendered): only canonical origins appear in conversation_history"
            )
            # Durability of tool output across turns (the history/system fix): the turn-1
            # observe chunk is produced mid-turn (fast cycle) yet MUST survive into a
            # <conversation_history> section — that section is rebuilt only by the full
            # enrich of turn 2 from <history>-parts of the thread. If it appears only in
            # the fast-cycle <conversation_delta>, full enrich lost it (the original bug).
            assert E2E_OBSERVED_CHUNK in mail_ctx, (
                "turn-1 observe output missing from <conversation_history>: full enrich "
                "did not collect it from thread <history>-parts (history/system regression)"
            )
            log.info("unified_context_roles_ok", correlation_key=correlation_key)
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
