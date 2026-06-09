"""Notmuch remote asserts on SUT."""
from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

from .bridges.email import notmuch_id_search_term
from .constants import (
    E2E_REMOTE_POSIX_HOME,
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
)
from .poll import poll_until
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
    project_name: str,
    *,
    correlation_key: str,
    repo_root: Path | None = None,
    timeout: float | None = None,
) -> None:
    """Wait until LightRAG embedded chunks **for this thread** — via WireMock state, not the global file.

    Индексация целиком завязана на вызовы стабов WireMock: эмбеддинг-стаб сценария (``006_embeddings_*``
    и т.п.) на каждом обслуживании пишет ``recordState`` флаг ``lightrag_embedded`` в контекст,
    ключёванный ЧИСТО по ``X-Threlium-Thread-Root`` (tag-free). Ждём именно этот флаг через probe-стаб
    (HTTP), а не ``docker exec stat`` ГЛОБАЛЬНОГО ``faiss_index_chunks.index.meta.json``.

    Почему так (урок ``-n2``): прежний ``stat`` (a) бил по ОДНОМУ общему faiss-файлу (не изолирован по
    треду), (b) шёл через ``service_exec`` = ``docker exec``, который под ``-n2`` конкурирует/голодает →
    poll выгорал по таймауту, хотя индексация шла (faiss рос). Флаг по thread-root: per-test изоляция,
    дёшево (HTTP к WireMock), без зависимости от объёма журнала/faiss и без ``docker exec``. См. §3.6.
    """
    from tests.e2e.wiremock_client import (  # noqa: PLC0415
        wiremock_public_base,
        wiremock_state_thread_root_call_sites,
    )

    from .runtime import discover_runtime  # noqa: PLC0415

    w = float(timeout) if timeout is not None else float(TIMEOUT_POLL_SHORT)
    rt = discover_runtime(project_name, repo_root=repo_root or REPO_ROOT)
    wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)

    def _probe() -> str | None:
        cs = wiremock_state_thread_root_call_sites(wm, correlation_key)
        return "1" if "lightrag_index" in cs else None

    poll_until(
        _probe, timeout=w, interval=2.0, desc="lightrag_index call-site (thread-root state)"
    )


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


def assert_notmuch_thread_stage_message_count_at_least(
    project_name: str,
    *,
    anchor_message_id: str,
    stage_folder_id: str,
    min_count: int = 2,
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
) -> None:
    """В треде якоря не меньше ``min_count`` сообщений в ``folder:<stage>/Maildir``."""
    if min_count < 1:
        raise ValueError("min_count must be >= 1")
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    sid_lit = repr(str(stage_folder_id).strip())
    want = int(min_count)
    py = f"""import json, os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
sid = {sid_lit}
want = {want}
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
    sys.exit(2)
q = f'{{tid}} AND folder:"{{sid}}/Maildir"'
c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((c.stdout or "0").strip() or "0")
except ValueError:
    n = 0
if n < want:
    _probe_out.info("NOTMUCH_THREAD_STAGE_COUNT n=" + str(n) + " want=" + str(want) + " q=" + repr(q))
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
            desc=(
                f"notmuch thread stage count >= {min_count} "
                f"(anchor={anchor_message_id!r}, stage={stage_folder_id!r})"
            ),
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        raise AssertionError(
            f"notmuch thread had fewer than {min_count} messages in folder "
            f"{stage_folder_id!r} within {w}s (anchor={anchor_message_id!r}).{out}"
        ) from None


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
