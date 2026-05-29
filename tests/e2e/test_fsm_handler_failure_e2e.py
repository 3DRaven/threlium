"""E2E: reasoning LLM 500 → failed work unit + ingress settle; recovery inject → finalize.

Стабы: ``wiremock_stubs/test_fsm_handler_failure_e2e/`` (``stub-fsm-handler-failure-01``).
"""
from __future__ import annotations

import shlex
import uuid
from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .helpers import (
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    MailflowScenarioSpec,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    e2e_dense_threlium_ctx_body,
    email_ingress_notmuch_id_inner,
    mailflow_inject_and_wait,
    mailflow_wait_fsm_maildir_activity,
    notmuch_id_search_term,
    poll_until_backoff,
    service_exec,
    smtp_inject_inbound,
    wait_for_greenmail_inbox_message_gone_host,
)
from .sut_user_systemd import E2E_THRELIUM_USER, e2e_threlium_user_unit_journalctl_bash

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_FSM_HANDLER_FAILURE_BODY = "E2E-FSM-HANDLER-FAILURE-BODY"
E2E_FSM_HANDLER_FAILURE_RECOVERY = "E2E-FSM-HANDLER-FAILURE-RECOVERY"

FAILURE_ACT1_SPEC = MailflowScenarioSpec(
    label="fsm_handler_failure_act1",
    raw_id_prefix="e2e-fsm-fail-a-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_fsm_handler_failure_e2e",
    stub_tag="stub-fsm-handler-failure-01",
    body_head=f"{E2E_FSM_HANDLER_FAILURE_BODY}\ne2e handler failure act1",
    min_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
    ),
)

RECOVERY_SPEC = MailflowScenarioSpec(
    label="fsm_handler_failure_recovery",
    raw_id_prefix="e2e-fsm-fail-b-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_fsm_handler_failure_e2e",
    stub_tag="stub-fsm-handler-failure-01",
    body_head=f"{E2E_FSM_HANDLER_FAILURE_RECOVERY}\ne2e handler failure recovery",
    min_chat_completion_posts=2,
    min_embedding_posts=0,
    min_rerank_posts=0,
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
    reply_body_needle="e2e-fsm-handler-recovery-answer",
)


def _wait_failed_reasoning_work(project: str, *, nm_inner: str) -> str:
    id_term = notmuch_id_search_term(nm_inner)
    journal = e2e_threlium_user_unit_journalctl_bash(
        "threlium-work@*.service", 300, transport_journal=False
    )

    def _probe() -> str | None:
        r = service_exec(
            project,
            "sut",
            ["bash", "-lc", journal],
            repo_root=REPO_ROOT,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        text = (r.stdout or "") + (r.stderr or "")
        for line in text.splitlines():
            if "threlium-work@reasoning:" in line and "failed" in line.lower():
                return line.strip()
        cmd = [
            "bash",
            "-lc",
            f"runuser -u {E2E_THRELIUM_USER} -- env "
            f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
            "systemctl --user --failed --no-legend 'threlium-work@*' 2>/dev/null || true",
        ]
        r2 = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=30)
        out = (r2.stdout or "") + (r2.stderr or "")
        if "reasoning" in out and "failed" in out.lower():
            return out.strip().splitlines()[0] if out.strip() else "failed"
        return None

    line = poll_until_backoff(
        _probe, timeout=TIMEOUT_POLL_SHORT, desc=f"failed threlium-work@reasoning for {id_term}"
    )
    assert line
    return line


def _wait_act1_reasoning_drained(project: str, *, nm_inner: str) -> None:
    """Act1 после 500+retry должен уйти из reasoning unread, иначе act2 блокируется тем же worker."""
    id_term = notmuch_id_search_term(nm_inner)
    q = f"{id_term} and folder:reasoning/Maildir and tag:unread"

    def _probe() -> bool:
        cmd = [
            "bash",
            "-lc",
            f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count {shlex.quote(q)}",
        ]
        r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=30)
        if r.returncode != 0:
            return False
        try:
            return int((r.stdout or "1").strip().splitlines()[-1].strip()) == 0
        except ValueError:
            return False

    poll_until_backoff(
        _probe,
        timeout=TIMEOUT_POLL_SHORT,
        desc=f"act1 drained from reasoning unread ({id_term})",
    )


def _assert_ingress_settled(project: str, *, nm_inner: str) -> None:
    q = f"{notmuch_id_search_term(nm_inner)} and tag:unread"
    cmd = [
        "bash",
        "-lc",
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count {shlex.quote(q)}",
    ]
    r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=int(TIMEOUT_POLL_SHORT))
    count = (r.stdout or "0").strip().splitlines()[-1].strip()
    assert count == "0", f"ingress should be settled (no unread), count={count!r}"


def _assert_journal_has_traceback(project: str) -> None:
    journal = e2e_threlium_user_unit_journalctl_bash(
        "threlium-work@*.service", 400, transport_journal=False
    )
    r = service_exec(
        project, "sut", ["bash", "-lc", journal], repo_root=REPO_ROOT, timeout=60
    )
    text = (r.stdout or "") + (r.stderr or "")
    assert "Traceback" in text or "Error" in text, "expected traceback in work unit journal"


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_fsm_handler_failure_then_recovery(deployed_stack: str) -> None:
    """Act1: 500 on reasoning; act2: recovery mailflow completes."""
    project = deployed_stack
    rt = discover_runtime(project, repo_root=REPO_ROOT)

    try:
        with mailflow_inject_and_wait(FAILURE_ACT1_SPEC, project) as (
            _p,
            raw_a,
            _c,
            nm_a,
            stub_tag,
            correlation_key,
        ):
            mailflow_wait_fsm_maildir_activity(
                project, repo_root=REPO_ROOT, message_id=nm_a
            )
            _wait_failed_reasoning_work(project, nm_inner=nm_a)
            _assert_journal_has_traceback(project)
            _assert_ingress_settled(project, nm_inner=nm_a)
            _wait_act1_reasoning_drained(project, nm_inner=nm_a)

        raw_b = f"{E2E_FSM_HANDLER_FAILURE_RECOVERY}-{uuid.uuid4().hex}@localhost"
        smtp_inject_inbound(
            project,
            checkout="/unused",
            repo_root=REPO_ROOT,
            message_id=raw_b,
            in_reply_to=raw_a,
            body=e2e_dense_threlium_ctx_body(
                head=RECOVERY_SPEC.body_head,
                correlation_key=correlation_key,
            ),
        )
        wait_for_greenmail_inbox_message_gone_host(
            rt.greenmail_imap_host, rt.greenmail_imap_port, message_id=raw_b
        )
        nm_b = email_ingress_notmuch_id_inner(raw_b)
        mailflow_wait_fsm_maildir_activity(
            project, repo_root=REPO_ROOT, message_id=nm_b
        )
        assert_full_mailflow_pipeline(
            RECOVERY_SPEC,
            project=project,
            raw_id=raw_b,
            nm_inner=nm_b,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
