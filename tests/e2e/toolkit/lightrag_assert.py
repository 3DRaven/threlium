"""LightRAG indexing asserts."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from threlium.lightrag_drain_query import lightrag_drain_pending_search
from threlium.types import FsmStage, NotmuchQueryField, NotmuchTag

from .bridges.email import notmuch_id_search_term
from .constants import (
    E2E_REMOTE_POSIX_HOME,
    E2E_REMOTE_THRELIUM_HOME,
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
)
from .diag import mailflow_pipeline_diag
from .poll import poll_until
from .remote_boot import REMOTE_PROBE_LOGGER_BOOT
from .runtime import service_exec

def assert_notmuch_mailflow_thread_has_lightrag_indexed(
    project_name: str,
    *,
    anchor_message_id: str,
    repo_root: Path | None = None,
    settle_timeout: float | None = None,
) -> None:
    """В треде якорного id есть хотя бы одно сообщение с ``tag:lightrag_indexed`` (индексация LightRAG прошла)."""
    root = repo_root or REPO_ROOT
    if settle_timeout is not None:
        w = float(settle_timeout)
    else:
        w = float(TIMEOUT_POLL_SHORT)
    rag_term = NotmuchTag.LIGHTRAG_INDEXED.as_tag_query_term()
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    py = f"""import json, os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
anchor = {anchor_message_id!r}
id_q = {_id_q_lit}
rag_term = {rag_term!r}
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
q_rag = tid + " and " + rag_term
c_rag = subprocess.run(["notmuch", "count", q_rag], capture_output=True, text=True)
try:
    n_rag = int((c_rag.stdout or "0").strip() or "0")
except ValueError:
    n_rag = 0
_probe_out.info("LIGHTRAG_INDEXED_IN_THREAD=" + str(n_rag) + " tid=" + repr(tid))
if n_rag < 1:
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
            desc=f"notmuch thread has lightrag_indexed (anchor={anchor_message_id!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=root)
        raise AssertionError(
            "notmuch thread has no messages tagged lightrag_indexed within "
            f"{w}s (anchor id={anchor_message_id!r}).{out}"
        ) from None


def assert_notmuch_thread_lightrag_index_filter(
    project_name: str,
    *,
    anchor_message_id: str,
    indexed_stages: tuple[FsmStage, ...],
    excluded_stages: tuple[FsmStage, ...],
    repo_root: Path | None = None,
    settle_timeout: float | None = None,
) -> None:
    """Селективная индексация LightRAG в треде якоря (push-down whitelist).

    Для каждой ``indexed_stages``: в треде есть письмо ``to:<stage>`` И оно
    ``tag:lightrag_indexed`` (content-indexable стадия попала в drain).
    Для каждой ``excluded_stages``: письма ``to:<stage>`` в треде есть, но НИ
    одно НЕ ``tag:lightrag_indexed`` (SERVICE/не-whitelist стадия отсечена
    селектором :func:`threlium.lightrag_drain_query.lightrag_drain_pending_search`
    и в граф не попала). Drain при этом доходит до idle — отсечённые письма не
    «застревают» в pending.
    """
    root = repo_root or REPO_ROOT
    w = float(settle_timeout) if settle_timeout is not None else float(TIMEOUT_POLL_SHORT)
    rag_term = NotmuchTag.LIGHTRAG_INDEXED.as_tag_query_term()
    indexed_to_terms = [
        NotmuchQueryField.TO.term(s.rfc822_mailbox) for s in indexed_stages
    ]
    excluded_to_terms = [
        NotmuchQueryField.TO.term(s.rfc822_mailbox) for s in excluded_stages
    ]
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    py = f"""import json, os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
rag_term = {rag_term!r}
indexed_terms = {indexed_to_terms!r}
excluded_terms = {excluded_to_terms!r}
def _count(q: str) -> int:
    c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
    try:
        return int((c.stdout or "0").strip() or "0")
    except ValueError:
        return 0
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
problems = []
for term in indexed_terms:
    n_all = _count(tid + " and " + term)
    n_idx = _count(tid + " and " + term + " and " + rag_term)
    _probe_out.info("INDEXED_STAGE term=" + repr(term) + " all=" + str(n_all) + " indexed=" + str(n_idx))
    if n_all < 1:
        problems.append("missing stage " + term)
    elif n_idx < 1:
        problems.append("not indexed " + term)
for term in excluded_terms:
    n_all = _count(tid + " and " + term)
    n_idx = _count(tid + " and " + term + " and " + rag_term)
    _probe_out.info("EXCLUDED_STAGE term=" + repr(term) + " all=" + str(n_all) + " indexed=" + str(n_idx))
    if n_all < 1:
        problems.append("missing stage " + term)
    elif n_idx > 0:
        problems.append("unexpectedly indexed " + term)
if problems:
    _probe_out.info("INDEX_FILTER_PROBLEMS=" + repr(problems) + " tid=" + repr(tid))
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
            desc=f"lightrag index filter (anchor={anchor_message_id!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=root)
        raise AssertionError(
            "lightrag selective indexing invariant violated within "
            f"{w}s (anchor id={anchor_message_id!r}).{out}"
        ) from None
