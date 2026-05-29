"""E2e: email-мост отсекает reply без непосредственного родителя в индексе (``orphan_skip``).

**Тест-кейс.** В GreenMail инжектируется письмо-reply с ``In-Reply-To`` на случайный
``Message-ID``, которого нет в notmuch SUT. Мост (``bridges/email.py`` → ``process_inbox_tail``)
после дедупа делает проверку непосредственного родителя в индексе и, не найдя его, логирует
``orphan_skip`` и финализирует UID (``UID MOVE`` / ``\\Seen``) **без** доставки в FSM.

**Проверки.** (1) письмо ушло из INBOX (мост его обработал); (2) в журнале
``threlium-bridge@email`` есть ``orphan_skip`` для нашего ``Message-ID``; (3) письмо так и не
попало в notmuch (в FSM не вошло). LLM не вызывается — WireMock-сценарий не нужен.

Стек — уже поднятый (фикстура ``deployed_stack``), как в ``test_mailflow_e2e``.
"""
from __future__ import annotations

import shlex
import uuid

import pytest

from tests.e2e.log import clip_log_body, log

from .helpers import (
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    discover_runtime,
    dump_failure_artifacts,
    email_ingress_notmuch_id_inner,
    notmuch_id_search_term,
    poll_until_backoff,
    service_exec,
    smtp_inject_inbound,
    wait_for_greenmail_inbox_message_gone_host,
)
from .sut_user_systemd import e2e_threlium_user_unit_journalctl_bash


def _wait_bridge_orphan_skip(project: str, *, raw_inner: str) -> None:
    """Дождаться записи ``orphan_skip`` для нашего входящего ``Message-ID`` в журнале моста.

    structlog пишет событие одной строкой в journald (``_TRANSPORT=stdout``), поэтому
    ``transport_journal=False`` и матч по строке, содержащей ``orphan_skip`` и inner MID.
    """
    journal_cmd = e2e_threlium_user_unit_journalctl_bash(
        "threlium-bridge@email.service", 600, transport_journal=False
    )

    def _probe() -> bool | None:
        r = service_exec(
            project, "sut", ["bash", "-lc", journal_cmd], repo_root=REPO_ROOT, timeout=int(TIMEOUT_POLL_SHORT)
        )
        text = (r.stdout or "") + (r.stderr or "")
        for line in text.splitlines():
            if "orphan_skip" in line and raw_inner in line:
                return True
        return None

    poll_until_backoff(_probe, timeout=TIMEOUT_POLL_SHORT, desc=f"bridge orphan_skip for {raw_inner}")


def _assert_not_indexed(project: str, *, nm_inner: str) -> None:
    """Письмо, отсечённое ``orphan_skip``, не доставлено в FSM → его нет в union-notmuch.

    Проверка детерминированная (одна попытка): доставки не было, поэтому индексация невозможна.
    """
    id_term = notmuch_id_search_term(nm_inner)
    cmd = ["bash", "-lc", f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count {shlex.quote(id_term)}"]
    r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=int(TIMEOUT_POLL_SHORT))
    lines = (r.stdout or "").strip().splitlines()
    count = lines[-1].strip() if lines else "0"
    assert count == "0", (
        f"orphan-reply не должно быть в notmuch, но notmuch count={count!r} (id_term={id_term})"
    )


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_email_bridge_orphan_skip(deployed_stack: str) -> None:
    """Reply на неизвестный родитель → ``orphan_skip``; письмо ушло из INBOX и не вошло в FSM."""
    project = deployed_stack
    rt = discover_runtime(project, repo_root=REPO_ROOT)

    orphan_parent = f"orphan-parent-{uuid.uuid4().hex}@localhost"
    raw_id = f"orphan-reply-{uuid.uuid4().hex}@localhost"
    raw_inner = raw_id
    nm_inner = email_ingress_notmuch_id_inner(raw_id)

    try:
        smtp_inject_inbound(
            project,
            checkout="/unused",
            repo_root=REPO_ROOT,
            message_id=raw_id,
            in_reply_to=orphan_parent,
            body="e2e orphan reply body",
        )
        wait_for_greenmail_inbox_message_gone_host(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            message_id=raw_id,
            timeout=TIMEOUT_POLL_SHORT,
        )
        _wait_bridge_orphan_skip(project, raw_inner=raw_inner)
        _assert_not_indexed(project, nm_inner=nm_inner)
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
