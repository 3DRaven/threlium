"""E2e: IMAP UID checkpoint + UID MOVE + bridge restart без повторной доставки.

Act 1: полный mailflow (минимальные стабы ``test_imap_checkpoint_resume_e2e/``) → письмо A
в ``Threlium.Processed``, не в INBOX; в notmuch ingress с ``imap_uid`` в route.

Act 2: ``restart threlium-bridge@email`` → SMTP с тем же ``Message-ID`` (дубликат в INBOX) →
``duplicate_skip``; count ingress A не растёт.

Act 3: новое письмо B → полный mailflow с тем же каталогом стабов и **отдельным** сидом
WireMock State (новый ``X-Threlium-Thread-Root``). Act 2 — только ``duplicate_skip`` (без LLM).
"""
from __future__ import annotations

import shlex
from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log

from .toolkit import (
    E2EComposeRuntime,
    E2EComposeRuntime,
    E2E_FETCHMAIL_PASS,
    E2E_FETCHMAIL_USER,
    E2E_IMAP_PROCESSED_FOLDER,
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    MailflowScenarioSpec,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    assert_full_mailflow_pipeline,
    assert_imap_inner_mid_in_folder,
    assert_imap_inner_mid_not_in_inbox,
    discover_runtime,
    dump_failure_artifacts,
    email_ingress_imap_checkpoint_from_notmuch,
    email_ingress_notmuch_id_inner,
    mailflow_inject_and_wait,
    notmuch_id_search_term,
    poll_until_backoff,
    restart_email_bridge_service,
    service_exec,
    smtp_inject_inbound,
    wait_for_greenmail_inbox_message_gone_host,
)
from .sut_user_systemd import e2e_threlium_user_unit_journalctl_bash

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_IMAP_CHECKPOINT_BODY = "E2E-IMAP-CHECKPOINT-BODY"

IMAP_CHECKPOINT_ACT1_SPEC = MailflowScenarioSpec(
    label="imap_checkpoint_act1",
    raw_id_prefix="e2e-imap-cp-a-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_imap_checkpoint_resume_e2e",
    stub_tag="stub-imap-checkpoint-resume-01",
    body_head=f"{E2E_IMAP_CHECKPOINT_BODY}\ne2e imap checkpoint act1",
    min_chat_completion_posts=2,
    min_embedding_posts=5,
    min_rerank_posts=0,
)

IMAP_CHECKPOINT_ACT3_SPEC = MailflowScenarioSpec(
    label="imap_checkpoint_act3",
    raw_id_prefix="e2e-imap-cp-b-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_imap_checkpoint_resume_e2e",
    stub_tag="stub-imap-checkpoint-resume-01",
    body_head=f"{E2E_IMAP_CHECKPOINT_BODY}\ne2e imap checkpoint act3 new mail",
    min_chat_completion_posts=2,
    min_embedding_posts=5,
    min_rerank_posts=0,
)


def _notmuch_count(project: str, *, query: str) -> int:
    cmd = [
        "bash",
        "-lc",
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count {shlex.quote(query)}",
    ]
    r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=int(TIMEOUT_POLL_SHORT))
    lines = (r.stdout or "").strip().splitlines()
    raw = lines[-1].strip() if lines else "0"
    try:
        return int(raw)
    except ValueError:
        return 0


def _wait_bridge_duplicate_skip(project: str, *, raw_inner: str) -> None:
    journal_cmd = e2e_threlium_user_unit_journalctl_bash(
        "threlium-bridge@email.service", 400, transport_journal=False
    )

    def _probe() -> bool | None:
        r = service_exec(
            project,
            "sut",
            ["bash", "-lc", journal_cmd],
            repo_root=REPO_ROOT,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        text = (r.stdout or "") + (r.stderr or "")
        for line in text.splitlines():
            if "duplicate_skip" in line and raw_inner in line:
                return True
        return None

    poll_until_backoff(
        _probe, timeout=TIMEOUT_POLL_SHORT, desc=f"bridge duplicate_skip for {raw_inner}"
    )


@pytest.mark.xdist_group(name="bridge_restart")
def test_imap_checkpoint_resume_and_duplicate_skip(e2e_runtime: E2EComposeRuntime) -> None:
    """UID MOVE, route checkpoint, restart bridge, duplicate_skip, новое письмо B."""
    project = e2e_runtime.project_name
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port

    try:
        with mailflow_inject_and_wait(IMAP_CHECKPOINT_ACT1_SPEC, project) as (
            _proj,
            raw_a,
            _canon_a,
            nm_inner_a,
            stub_tag,
            correlation_key,
        ):
            assert_full_mailflow_pipeline(
                IMAP_CHECKPOINT_ACT1_SPEC,
                project=project,
                raw_id=raw_a,
                nm_inner=nm_inner_a,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )

        raw_inner_a = raw_a.strip().strip("<>")
        wait_for_greenmail_inbox_message_gone_host(
            imap_h, imap_p, message_id=raw_a, timeout=TIMEOUT_POLL_SHORT
        )
        assert_imap_inner_mid_not_in_inbox(
            imap_h, imap_p, user=E2E_FETCHMAIL_USER, password=E2E_FETCHMAIL_PASS, inner_mid=raw_inner_a
        )
        assert_imap_inner_mid_in_folder(
            imap_h,
            imap_p,
            user=E2E_FETCHMAIL_USER,
            password=E2E_FETCHMAIL_PASS,
            folder=E2E_IMAP_PROCESSED_FOLDER,
            inner_mid=raw_inner_a,
        )
        uiv, uid = email_ingress_imap_checkpoint_from_notmuch(project, nm_inner=nm_inner_a)
        assert uid > 0, f"expected imap_uid in ingress route, got uid={uid!r} uiv={uiv!r}"
        count_a_before = _notmuch_count(project, query=notmuch_id_search_term(nm_inner_a))
        assert count_a_before >= 1

        restart_email_bridge_service(project)

        smtp_inject_inbound(
            project,
            checkout="/unused",
            repo_root=REPO_ROOT,
            message_id=raw_a,
            body="e2e imap checkpoint duplicate reinject",
        )
        wait_for_greenmail_inbox_message_gone_host(
            imap_h, imap_p, message_id=raw_a, timeout=TIMEOUT_POLL_SHORT
        )
        _wait_bridge_duplicate_skip(project, raw_inner=raw_inner_a)
        count_a_after = _notmuch_count(project, query=notmuch_id_search_term(nm_inner_a))
        assert count_a_after == count_a_before, (
            f"duplicate reinject must not add notmuch copies: before={count_a_before} after={count_a_after}"
        )

        with mailflow_inject_and_wait(IMAP_CHECKPOINT_ACT3_SPEC, project) as (
            _proj_b,
            raw_b,
            _canon_b,
            nm_inner_b,
            stub_tag_b,
            correlation_key_b,
        ):
            assert_full_mailflow_pipeline(
                IMAP_CHECKPOINT_ACT3_SPEC,
                project=project,
                raw_id=raw_b,
                nm_inner=nm_inner_b,
                stub_tag=stub_tag_b,
                correlation_key=correlation_key_b,
            )

        log.info("imap_checkpoint_resume_ok", uid_a=uid, count_a=count_a_after, raw_b=raw_b)
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
