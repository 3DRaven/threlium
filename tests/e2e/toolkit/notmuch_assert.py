"""Notmuch remote asserts on SUT."""
from __future__ import annotations

import json
import shlex
from pathlib import Path
from typing import Any, Sequence

from threlium.types import NotmuchTag

from .bridges.email import notmuch_id_search_term
from .constants import (
    E2E_FSM_MAILBOX_STAGE_IDS,
    E2E_REMOTE_POSIX_HOME,
    E2E_REMOTE_REPO_PATH,
    E2E_REMOTE_THRELIUM_HOME,
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
)
from .diag import mailflow_pipeline_diag
from .poll import poll_until, poll_until_backoff
from .remote_boot import REMOTE_PROBE_LOGGER_BOOT
from .runtime import service_exec

def poll_notmuch_positive(project_name: str, *, repo_root: Path | None = None) -> str:
    root = repo_root or REPO_ROOT

    def _count() -> str | None:
        r = service_exec(
            project_name,
            "sut",
            ["bash", "-lc", f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count '*' 2>/dev/null || echo 0"],
            repo_root=root,
            timeout=30,
        )
        if r.returncode != 0:
            return None
        try:
            n = int((r.stdout or "").strip())
        except ValueError:
            return None
        if n > 0:
            return (r.stdout or "").strip()
        return None

    return poll_until(_count, timeout=TIMEOUT_POLL_SHORT, desc="notmuch count > 0")


def poll_lightrag_indexed_positive(
    project_name: str, *, repo_root: Path | None = None, timeout: float | None = None
) -> None:
    """Wait until LightRAG drain has actually inserted data into vectordb.

    Polls ``vdb_chunks.json`` file size inside the SUT: a file > 4 bytes means
    nano-vectordb has at least one stored vector (empty JSON list ``[]`` is 2-3 bytes).
    This is more reliable than checking ``tag:lightrag_indexed`` which gets applied by
    fdm on archive copies before the drain runs.
    """
    root = repo_root or REPO_ROOT
    w = float(timeout) if timeout is not None else float(TIMEOUT_POLL_SHORT)
    cmd = [
        "bash", "-lc",
        "stat --printf='%s' /home/threlium/threlium/data/lightrag/vdb_chunks.json 2>/dev/null || echo 0",
    ]

    def _probe() -> str | None:
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=30)
        if r.returncode != 0:
            return None
        try:
            sz = int((r.stdout or "").strip())
        except ValueError:
            return None
        return str(sz) if sz > 10 else None

    poll_until(_probe, timeout=w, interval=2.0, desc="vdb_chunks.json size > 10 bytes")


def wait_for_notmuch_message(
    project_name: str,
    *,
    message_id: str,
    subject: str | None = None,
    repo_root: Path | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Дождаться появления письма в notmuch по ``id:…``.

    Если передан ``subject``, запрос сужается ``and subject:'…'`` (устаревший режим).
    Для e2e-дезамбiguaции достаточно ``message_id`` (inner id / якорь треда).
    """
    root = repo_root or REPO_ROOT
    id_term = notmuch_id_search_term(message_id)
    query = f"{id_term} and subject:'{subject}'" if subject is not None else id_term
    py_body = (
        "import os, subprocess, sys\n"
        + REMOTE_PROBE_LOGGER_BOOT
        + f"os.environ.setdefault('HOME', {E2E_REMOTE_POSIX_HOME!r})\n"
        f"os.environ.setdefault('NOTMUCH_CONFIG', {E2E_REMOTE_POSIX_HOME + '/.notmuch-config'!r})\n"
        f"query = {query!r}\n"
        "r = subprocess.run(['notmuch', 'count', query], capture_output=True, text=True)\n"
        "raw = (r.stdout or '0').strip() or '0'\n"
        "_probe_out.info('NOTMUCH_QUERY_COUNT=' + raw)\n"
        "try:\n"
        "    n = int(raw)\n"
        "except ValueError:\n"
        "    n = 0\n"
        "raise SystemExit(0 if n > 0 else 1)\n"
    )
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py_body + "\nPY\n"]

    snap: dict[str, str] = {"q": "?", "rc": "?"}

    def _count() -> bool | None:
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=30)
        snap["rc"] = str(r.returncode)
        snap["q"] = "?"
        for line in (r.stdout or "").splitlines():
            if line.startswith("NOTMUCH_QUERY_COUNT="):
                snap["q"] = line.split("=", 1)[1].strip()
                break
        return True if r.returncode == 0 else None

    def _extra() -> str:
        r2 = service_exec(
            project_name,
            "sut",
            ["bash", "-lc", f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count '*' 2>/dev/null || echo notmuch_err"],
            repo_root=root,
            timeout=30,
        )
        total = (r2.stdout or "").strip().replace("\n", " ")[:200]
        return (
            f"query={query!r} matched_count={snap['q']} last_probe_exit={snap['rc']} "
            f"notmuch_count_star={total}"
        )

    poll_until_backoff(
        _count,
        timeout=timeout,
        desc=f"notmuch query {query!r} > 0",
        progress_extra=_extra,
    )


def assert_notmuch_thread_fully_in_stages(
    project_name: str,
    *,
    anchor_message_id: str,
    repo_root: Path | None = None,
    settle_timeout: float | None = None,
) -> None:
    """Все сообщения notmuch-треда якорного id должны быть видны в union index.

    `docs/INDEX.md` §1, §10 решение 7: union-notmuch root = ``stages/``;
    отдельного ``archive/Maildir`` нет. Каждое настроенное письмо должно быть
    в ``stages/<stage>/Maildir/cur`` (после ``nm_settle()``) или ``new`` (между
    ``insert`` и стартом worker'а).

    Опрос: после reasoning → egress цепочка может ещё обрабатываться секунды;
    ждём ``count(thread) == count(thread and not tag:unread)`` до ``settle_timeout``.
    """
    root = repo_root or REPO_ROOT
    if settle_timeout is not None:
        w = float(settle_timeout)
    else:
        w = float(TIMEOUT_POLL_SHORT)
    _unread_term = NotmuchTag.UNREAD.as_tag_query_term()
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    py = f"""import json, os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
anchor = {anchor_message_id!r}
id_q = {_id_q_lit}
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p.stdout)
if not tid:
    _probe_out.info("NOTMUCH_THREAD_RESOLVE_FAIL stdout=" + repr(p.stdout) + " stderr=" + repr(p.stderr))
    sys.exit(2)
c_full = subprocess.run(["notmuch", "count", tid], capture_output=True, text=True)
q_settled = f"{{tid}} and not {_unread_term}"
c_settled = subprocess.run(["notmuch", "count", q_settled], capture_output=True, text=True)
try:
    n_full = int((c_full.stdout or "0").strip() or "0")
except ValueError:
    n_full = 0
try:
    n_settled = int((c_settled.stdout or "0").strip() or "0")
except ValueError:
    n_settled = 0
_probe_out.info(
    "NOTMUCH_THREAD_COUNTS tid="
    + repr(tid)
    + " full="
    + str(n_full)
    + " settled="
    + str(n_settled)
)
if n_full <= 0:
    sys.exit(3)
if n_full != n_settled:
    sys.exit(4)
sys.exit(0)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]

    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch thread settled (anchor={anchor_message_id!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=root)
        raise AssertionError(
            "notmuch thread did not fully settle in union stages index within "
            f"{w}s (anchor id={anchor_message_id!r}).{out}"
        ) from None


def assert_notmuch_thread_has_messages_in_folders(
    project_name: str,
    *,
    anchor_message_id: str,
    stage_folder_ids: Sequence[str],
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
) -> None:
    """В треде якорного ``Message-ID`` есть хотя бы одно сообщение в каждом ``folder:<id>/Maildir``."""
    if not stage_folder_ids:
        return
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    stages_json = json.dumps([str(s).strip() for s in stage_folder_ids if str(s).strip()])
    py = f"""import json, os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
stages = json.loads({repr(stages_json)})
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p.stdout)
if not tid:
    _probe_out.info("NOTMUCH_THREAD_RESOLVE_FAIL stdout=" + repr(p.stdout) + " stderr=" + repr(p.stderr))
    sys.exit(2)
missing = []
for sid in stages:
    q = f'{{tid}} AND folder:"{{sid}}/Maildir"'
    c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
    try:
        n = int((c.stdout or "0").strip() or "0")
    except ValueError:
        n = 0
    if n < 1:
        missing.append(sid)
        _probe_out.info("NOTMUCH_STAGE_FOLDER_EMPTY stage=" + repr(sid) + " q=" + repr(q))
if missing:
    sys.exit(5)
sys.exit(0)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]
    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch thread stage folders (anchor={anchor_message_id!r}, stages={list(stage_folder_ids)!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=root)
        raise AssertionError(
            "notmuch thread missing expected stage Maildir folder hits within "
            f"{w}s (anchor id={anchor_message_id!r}, wanted folders={list(stage_folder_ids)!r}).{out}"
        ) from None


def poll_notmuch_thread_in_stage_folder(
    project_name: str,
    *,
    anchor_message_id: str,
    stage_folder_id: str,
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
) -> None:
    """Poll до появления сообщения из треда *anchor_message_id* в ``folder:<stage>/Maildir``.

    Изолированная замена journalctl-проверок маршрутизации: привязка по ``Message-ID``
    конкретного теста, без глобального grep по журналу.
    """
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    sid_lit = repr(str(stage_folder_id).strip())
    py = f"""import json, os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
sid = {sid_lit}
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p.stdout)
if not tid:
    _probe_out.info("NOTMUCH_THREAD_RESOLVE_FAIL stdout=" + repr(p.stdout) + " stderr=" + repr(p.stderr))
    sys.exit(2)
q = f'{{tid}} AND folder:"{{sid}}/Maildir"'
c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((c.stdout or "0").strip() or "0")
except ValueError:
    n = 0
if n < 1:
    _probe_out.info("NOTMUCH_STAGE_FOLDER_EMPTY stage=" + repr(sid) + " q=" + repr(q))
    sys.exit(5)
sys.exit(0)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        return True if r.returncode == 0 else None

    poll_until(
        _probe,
        timeout=w,
        interval=2.0,
        desc=(
            f"notmuch thread in stage folder "
            f"(anchor={anchor_message_id!r}, stage={stage_folder_id!r})"
        ),
    )


def assert_notmuch_folder_contains_body_token(
    project_name: str,
    *,
    stage_folder_id: str,
    body_token: str,
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
    min_count: int = 1,
) -> None:
    """Хотя бы ``min_count`` сообщений в ``folder:<stage>/Maildir`` с подстрокой ``body_token`` в теле (notmuch)."""
    tok = str(body_token).strip()
    if not tok:
        raise ValueError("assert_notmuch_folder_contains_body_token: empty body_token")
    sid = str(stage_folder_id).strip()
    if not sid:
        raise ValueError("assert_notmuch_folder_contains_body_token: empty stage_folder_id")
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    tok_lit = repr(tok)
    sid_lit = repr(sid)
    mc = int(min_count)
    py = f"""import os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
tok = {tok_lit}
sid = {sid_lit}
want = {mc}
q = f'folder:"{{sid}}/Maildir" "{{tok}}"'
c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((c.stdout or "0").strip() or "0")
except ValueError:
    n = 0
_probe_out.info("NOTMUCH_FOLDER_TOKEN_COUNT n=" + str(n) + " q=" + repr(q))
sys.exit(0 if n >= want else 6)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]
    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch folder={stage_folder_id!r} body contains {tok[:48]!r}…",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        raise AssertionError(
            f"notmuch folder {stage_folder_id!r} had fewer than {min_count} hits for body token "
            f"within {w}s (token={tok!r}).{out}"
        ) from None


def assert_notmuch_thread_tag_count(
    project_name: str,
    *,
    anchor_message_id: str,
    tag: str,
    min_count: int = 1,
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
) -> None:
    """В треде якорного ``Message-ID`` не меньше ``min_count`` сообщений с ``tag:<tag>``."""
    tag_val = str(tag).strip().removeprefix("tag:")
    if not tag_val:
        raise ValueError("assert_notmuch_thread_tag_count: empty tag")
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    tag_lit = repr(tag_val)
    mc = int(min_count)
    py = f"""import json, os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
tag = {tag_lit}
want = {mc}
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
try:
    payload = json.loads((p.stdout or "").strip() or "[]")
except json.JSONDecodeError:
    payload = []
tid = ""
if isinstance(payload, list) and payload:
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or "").strip()
if not tid:
    sys.exit(2)
if not tid.startswith("thread:"):
    tid = f"thread:{{tid}}"
q = f'{{tid}} tag:{{tag}}'
c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((c.stdout or "0").strip() or "0")
except ValueError:
    n = 0
_probe_out.info("NOTMUCH_THREAD_TAG_COUNT n=" + str(n) + " q=" + repr(q))
sys.exit(0 if n >= want else 6)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]
    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch thread tag:{tag_val!r} count>={min_count} (anchor={anchor_message_id!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        raise AssertionError(
            f"notmuch thread had fewer than {min_count} messages with tag:{tag_val!r} within {w}s "
            f"(anchor id={anchor_message_id!r}).{out}"
        ) from None


def assert_notmuch_thread_has_no_unread(
    project: str,
    *,
    anchor_message_id: str,
    repo_root: Path | None = None,
) -> None:
    """После успешного прогона в треде нет ``tag:unread`` (``nm_settle``)."""
    root = repo_root or REPO_ROOT
    id_term = notmuch_id_search_term(anchor_message_id)
    q = f"thread:{anchor_message_id} and tag:unread"
    cmd = [
        "bash",
        "-lc",
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count {shlex.quote(q)}",
    ]
    r = service_exec(project, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
    count = (r.stdout or "0").strip().splitlines()[-1].strip()
    assert count == "0", (
        f"expected no unread in thread (anchor={anchor_message_id!r}), count={count!r}"
    )
