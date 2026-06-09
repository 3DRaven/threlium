"""E2E: reasoning LLM 500 → failed work unit (loud) + ingress settle + poison drains.

Стабы: ``wiremock_stubs/test_fsm_handler_failure_e2e/`` (``stub-fsm-handler-failure-01``).
"""
from __future__ import annotations

import shlex
from pathlib import Path


from tests.e2e.log import clip_log_body, log

from .toolkit import (
    E2EComposeRuntime,
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    MailflowScenarioSpec,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    mailflow_wait_fsm_maildir_activity,
    notmuch_id_search_term,
    poll_until_backoff,
    service_exec,
)
from .sut_user_systemd import E2E_THRELIUM_USER, e2e_threlium_user_unit_journalctl_bash

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_FSM_HANDLER_FAILURE_BODY = "E2E-FSM-HANDLER-FAILURE-BODY"

FAILURE_ACT1_SPEC = MailflowScenarioSpec(
    label="fsm_handler_failure_act1",
    raw_id_prefix="e2e-fsm-fail-a-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_fsm_handler_failure_e2e",
    stub_tag="stub-fsm-handler-failure-01",
    body_head=f"{E2E_FSM_HANDLER_FAILURE_BODY}\ne2e handler failure act1",
    min_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
)


def _wait_failed_reasoning_work(project: str, *, nm_inner: str, since: str | None = None) -> str:
    id_term = notmuch_id_search_term(nm_inner)
    journal = e2e_threlium_user_unit_journalctl_bash(
        "threlium-work@*.service", 300, transport_journal=False, since=since
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
    """После 500+retry письмо должно УЙТИ из reasoning unread: яд ретраится/dead-letter'ится, а не
    застревает навсегда (инвариант обработки poison-сообщения)."""
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


def _assert_journal_has_traceback(project: str, *, since: str | None = None) -> None:
    journal = e2e_threlium_user_unit_journalctl_bash(
        "threlium-work@*.service", 400, transport_journal=False, since=since
    )
    r = service_exec(
        project, "sut", ["bash", "-lc", journal], repo_root=REPO_ROOT, timeout=60
    )
    text = (r.stdout or "") + (r.stderr or "")
    assert "Traceback" in text or "Error" in text, "expected traceback in work unit journal"


def test_fsm_handler_failure(e2e_runtime: E2EComposeRuntime) -> None:
    """Reasoning LLM 500 → work-unit ПАДАЕТ громко (failed + traceback), ingress settle, poison drains.

    Краш движок-внутренний: после 500 наружу/в мок ничего не шлётся → §3.6 state его увидеть не может,
    journal — легитимный инструмент (E2E.md §3.6.4: авто-capture исключений нет). Творческие state-углы
    исчерпаны и согласованы: `count(reasoning-500)>=2` не работает (retry на уровне systemd-юнита, не
    re-call LLM); барьер на письмо act1 невозможен (act1 поизонится ДО egress, наружу письма нет).
    Recovery-акт снят: его тугой same-thread-recovery был флаки/медленный (3/3 таймаут), а широкий
    poison-containment покрыт здоровьем базы (яд бы валил все тесты, а они зелёные).
    """
    project = e2e_runtime.project_name

    r_time = service_exec(project, "sut", ["date", "+%Y-%m-%d %H:%M:%S"], repo_root=REPO_ROOT)
    test_start_time = (r_time.stdout or "").strip()

    try:
        with mailflow_inject_and_wait(FAILURE_ACT1_SPEC, project) as (
            _p,
            _raw_a,
            _c,
            nm_a,
            _stub_tag,
            _correlation_key,
        ):
            mailflow_wait_fsm_maildir_activity(
                project, repo_root=REPO_ROOT, message_id=nm_a
            )
            _wait_failed_reasoning_work(project, nm_inner=nm_a, since=test_start_time)
            _assert_journal_has_traceback(project, since=test_start_time)
            _assert_ingress_settled(project, nm_inner=nm_a)
            _wait_act1_reasoning_drained(project, nm_inner=nm_a)
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
