"""Mailflow diagnostics and failure dumps."""
from __future__ import annotations

import subprocess
from pathlib import Path

from tests.e2e.sut_user_systemd import (
    E2E_SUT_THRELIUM_USER_UNIT_JOURNAL,
    E2E_THRELIUM_USER,
    e2e_threlium_user_unit_journalctl_bash,
)

from .bridges.email import notmuch_id_search_term
from .constants import (
    E2E_FSM_MAILBOX_STAGE_IDS,
    E2E_WIREMOCK_CONTAINER_PORT,
    E2E_REMOTE_POSIX_HOME,
    E2E_REMOTE_REPO_PATH,
    E2E_REMOTE_THRELIUM_HOME,
    E2E_SUT_MAIL_ARCHIVE_SYSTEM_DIAG_SCRIPT,
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
)
from .poll import mailflow_diag_block, poll_until_backoff
from .remote_boot import REMOTE_PROBE_LOGGER_BOOT
from .runtime import (
    _compose_container,
    _mapped_port,
    compose_logs,
    service_exec,
)
from .smtp_ingress import _email_bridge_systemd_diag_script

def mailflow_wait_fsm_maildir_activity(
    project_name: str,
    *,
    repo_root: Path | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
    message_id: str | None = None,
    subject: str | None = None,
) -> None:
    """Ждём признаков FSM: файлы в ``new`` у ingress/enrich/reasoning **или** письмо в notmuch по якорю.

    Если задан только ``message_id``, notmuch-ветка использует ``id:…`` без subject.
    Если заданы оба — запрос ``id:… and subject:'…'``. Если ``message_id`` нет — только Maildir.
    """
    root = repo_root or REPO_ROOT
    if message_id is None:
        query_py = "None"
    elif subject is None:
        query_py = repr(notmuch_id_search_term(message_id))
    else:
        query_py = repr(f"{notmuch_id_search_term(message_id)} and subject:'{subject}'")
    py = f"""import os, pathlib, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}for _envf in ("{E2E_REMOTE_REPO_PATH}/env/threlium.env",):
    try:
        with open(_envf) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except OSError:
        pass
TH = os.environ.get("THRELIUM_HOME", "{E2E_REMOTE_THRELIUM_HOME}")
tot = 0
for sid in ("ingress", "enrich", "reasoning"):
    p = pathlib.Path(TH) / "stages" / sid / "Maildir" / "new"
    if p.is_dir():
        tot += sum(1 for x in p.iterdir() if x.is_file() and not x.name.startswith("."))
_probe_out.info("FSM_NEW_TOTAL=" + str(tot))
if tot > 0:
    sys.exit(0)
q = {query_py}
if q is None:
    sys.exit(1)
os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
r = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((r.stdout or "0").strip() or "0")
except ValueError:
    n = 0
_probe_out.info("NOTMUCH_MATCH=" + str(n))
sys.exit(0 if n > 0 else 1)
"""

    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY\n"]

    def _probe() -> bool | None:
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
        return True if r.returncode == 0 else None

    def _extra() -> str:
        r2 = service_exec(project_name, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
        return (r2.stdout or "").strip().replace("\n", " ")[:400]

    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc="FSM Maildir activity or notmuch anchor",
        progress_extra=_extra,
    )


def mailflow_fsm_maildir_systemd_snapshot(project_name: str, *, repo_root: Path | None = None) -> str:
    """Снимок Maildir (stages/) + журнал threlium user-юнитов (``runuser`` + ``--user-unit``) и широкий хвост."""
    root = repo_root or REPO_ROOT
    ids_bash = " ".join(E2E_FSM_MAILBOX_STAGE_IDS)
    script = f"""
set +e
if [ -f {E2E_REMOTE_REPO_PATH}/env/threlium.env ]; then
  set -a
  # shellcheck disable=SC1091
  . {E2E_REMOTE_REPO_PATH}/env/threlium.env
  set +a
fi
TH="${{THRELIUM_HOME:-{E2E_REMOTE_THRELIUM_HOME}}}"
echo "THRELIUM_HOME=$TH"
echo "--- Maildir file counts (new/cur/tmp, non-hidden; stages/* only) ---"
for sub in $(for id in {ids_bash}; do echo "stages/$id/Maildir"; done); do
  base="$TH/$sub"
  for box in new cur tmp; do
    d="$base/$box"
    if [ -d "$d" ]; then
      n=$(find "$d" -maxdepth 1 -type f ! -name '.*' 2>/dev/null | wc -l)
    else
      n=0
    fi
    echo "$sub/$box: $n"
  done
done
echo "--- threlium user-unit journals (runuser {E2E_THRELIUM_USER}, --user-unit) ---"
{E2E_SUT_THRELIUM_USER_UNIT_JOURNAL}
echo "--- journal broad tail (all units) ---"
journalctl -n 200 --no-pager 2>&1 || true
echo "--- LightRAG working_dir (INDEX §5b) ---"
if [ -d "$TH/lightrag" ]; then
  ls -la "$TH/lightrag" 2>&1 | head -n 40 || true
else
  echo "(no $TH/lightrag)"
fi
"""
    r = service_exec(project_name, "sut", ["bash", "-lc", script], repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
    return (r.stdout or "") + (r.stderr or "") + f"\n(exit {r.returncode})\n"


def dump_failure_artifacts(project_name: str, *, repo_root: Path | None = None) -> str:
    """Собирает вывод диагностик в одну строку для stderr теста.

    Широкий хвост журнала — **от root** (``journalctl -n`` без ``--user``).
    Стабильные threlium user-юниты — через ``runuser`` + ``--user-unit``
    (:data:`E2E_SUT_THRELIUM_USER_UNIT_JOURNAL` в ``sut_user_systemd``), затем широкий хвост.
    Системный стек mail-archive (Cockpit):
    :data:`E2E_SUT_MAIL_ARCHIVE_SYSTEM_DIAG_SCRIPT`. Снимок Maildir —
    ``mailflow_fsm_maildir_systemd_snapshot``.
    Если после стабилизации enrich остаются аномалии в заголовках — см. ``journalctl``
    для failed ``threlium-work@`` / ``threlium-bridge@`` и ``~/.fdm.conf``.
    """
    chunks: list[str] = []
    root = repo_root or REPO_ROOT

    chunks.append("=== fsm maildir + systemd snapshot ===\n")
    chunks.append(mailflow_fsm_maildir_systemd_snapshot(project_name, repo_root=root))

    chunks.append("=== container logs ===\n")
    chunks.append(compose_logs(project_name, repo_root=root))

    for label, cmd in [
        (
            "mail-archive system units (cockpit)",
            ["bash", "-lc", E2E_SUT_MAIL_ARCHIVE_SYSTEM_DIAG_SCRIPT],
        ),
        (f"loginctl {E2E_THRELIUM_USER}", ["bash", "-lc", f"loginctl show-user {E2E_THRELIUM_USER} 2>&1 || true"]),
        (
            "journal threlium user-units + broad tail",
            ["bash", "-lc", E2E_SUT_THRELIUM_USER_UNIT_JOURNAL + "\necho '--- journal broad tail ---'\njournalctl -n 200 --no-pager 2>&1 || true"],
        ),
        (
            "notmuch count",
            ["bash", "-lc", f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count '*' 2>&1 || true"],
        ),
        (
            "fdm.conf rendered in sut",
            [
                "bash",
                "-lc",
                f"echo '--- readlink ~/.fdm.conf ---'; readlink -f {E2E_REMOTE_POSIX_HOME}/.fdm.conf 2>&1 || true; "
                f"echo '--- {E2E_REMOTE_POSIX_HOME}/.fdm.conf ---'; sed -n '1,260p' {E2E_REMOTE_POSIX_HOME}/.fdm.conf 2>&1 || true; "
                f"echo '--- repo config/fdm.conf ---'; "
                f"sed -n '1,260p' {E2E_REMOTE_REPO_PATH}/config/fdm.conf 2>&1 || true",
            ],
        ),
        ("threlium.yaml", ["bash", "-lc", f"cat {E2E_REMOTE_THRELIUM_HOME}/config/threlium.yaml 2>&1 || true"]),
        ("threlium.env", ["bash", "-lc", f"cat {E2E_REMOTE_REPO_PATH}/env/threlium.env 2>&1 || true"]),
        (
            "bridge-email systemd service",
            ["bash", "-lc", _email_bridge_systemd_diag_script()],
        ),
        ("msmtprc", ["bash", "-lc", f"cat {E2E_REMOTE_POSIX_HOME}/.msmtprc 2>&1 || true"]),
        (
            "journal threlium-bridge@email (fdm path)",
            [
                "bash",
                "-lc",
                f"{e2e_threlium_user_unit_journalctl_bash('threlium-bridge@email.service', 120)}",
            ],
        ),
    ]:
        chunks.append(f"\n=== {label} ===\n")
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
        chunks.append(r.stdout + r.stderr + f"\n(exit {r.returncode})\n")

    chunks.append("\n=== greenmail (if any) ===\n")
    try:
        greenmail = _compose_container(project_name, "greenmail")
        chunks.append(greenmail.logs(stdout=True, stderr=True, tail=500).decode("utf-8", errors="replace"))
    except Exception as e:  # pragma: no cover
        chunks.append(f"(failed to fetch greenmail logs: {e!r})\n")

    chunks.append("\n=== wiremock (OpenAI/Matrix stubs) ===\n")
    try:
        from tests.e2e.wiremock_client import describe_wiremock_admin_state

        wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
        chunks.append(describe_wiremock_admin_state(wm_host, wm_port, project_name=project_name))
    except Exception as e:  # pragma: no cover
        chunks.append(f"(failed to describe wiremock state: {e!r})\n")
    return "".join(chunks)


def reset_mda_pipeline_diag(project_name: str, *, repo_root: Path | None = None) -> None:
    """Перед mailflow: ранее очищался ``maildrop.log``; с fdm отдельного LDA-лога нет — no-op."""
    root = repo_root or REPO_ROOT
    service_exec(project_name, "sut", ["bash", "-lc", "true"], repo_root=root, timeout=30)


def reset_maildrop_debug_log(project_name: str, *, repo_root: Path | None = None) -> None:
    """Совместимость: см. :func:`reset_mda_pipeline_diag`."""
    reset_mda_pipeline_diag(project_name, repo_root=repo_root)


def mailflow_pipeline_diag(
    project_name: str,
    *,
    anchor_message_id: str,
    repo_root: Path | None = None,
) -> None:
    """Диагностика после mailflow: notmuch (тред в архиве), хвост LLM-лога, journal threlium."""
    root = repo_root or REPO_ROOT
    snap = mailflow_fsm_maildir_systemd_snapshot(project_name, repo_root=root)
    mailflow_diag_block("mailflow: fsm maildir + systemd snapshot", snap, max_chars=30000)
    mf = service_exec(
        project_name,
        "sut",
        [
            "bash",
            "-lc",
            f"echo '--- readlink ~/.fdm.conf ---'; readlink -f {E2E_REMOTE_POSIX_HOME}/.fdm.conf 2>&1 || true; "
            f"echo '--- {E2E_REMOTE_POSIX_HOME}/.fdm.conf ---'; sed -n '1,260p' {E2E_REMOTE_POSIX_HOME}/.fdm.conf 2>&1 || true; "
            f"echo '--- repo config/fdm.conf ---'; sed -n '1,260p' {E2E_REMOTE_REPO_PATH}/config/fdm.conf 2>&1 || true",
        ],
        repo_root=root,
        timeout=30,
    )
    mailflow_diag_block("mailflow: rendered fdm.conf in sut", mf.stdout + mf.stderr, max_chars=30000)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    py = f"""import json, os, subprocess, sys
{REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
anchor = {anchor_message_id!r}
id_q = {_id_q_lit}
def _paths(raw: str) -> list[str]:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return []
    out: list[str] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, str):
                s = item.strip()
            elif isinstance(item, dict):
                s = str(item.get("path") or item.get("file") or item.get("filename") or "").strip()
            else:
                s = ""
            if s:
                out.append(s)
    return out

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
    return tid if tid.startswith("thread:") else f"thread:{tid}"
p = subprocess.run(
    ["notmuch", "search", "--limit=30", "--output=files", "--format=json", id_q],
    capture_output=True,
    text=True,
)
_probe_out.info("--- notmuch search id anchor (files, up to 30) ---")
_probe_out.info(json.dumps(_paths(p.stdout), ensure_ascii=False, indent=2) or "(empty)")
p2 = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p2.stdout)
if tid:
    p3 = subprocess.run(
        ["notmuch", "search", "--limit=50", "--output=files", "--format=json", tid],
        capture_output=True,
        text=True,
    )
    _probe_out.info("--- notmuch thread in union index (files, up to 50) ---")
    _probe_out.info(json.dumps(_paths(p3.stdout), ensure_ascii=False, indent=2) or "(empty)")
sys.exit(0)
"""
    r = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"],
        repo_root=root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    mailflow_diag_block("mailflow: notmuch thread / stages slice", r.stdout + r.stderr, max_chars=25000)

    try:
        from tests.e2e.wiremock_client import describe_wiremock_admin_state

        wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
        wm_state = describe_wiremock_admin_state(wm_host, wm_port, project_name=project_name)
    except Exception as e:  # pragma: no cover
        wm_state = f"(failed to describe wiremock state: {e!r})"
    mailflow_diag_block("mailflow: wiremock admin + journal tail", wm_state, max_chars=12000)

    md_log_r = service_exec(
        project_name,
        "sut",
        [
            "bash",
            "-lc",
            f"{e2e_threlium_user_unit_journalctl_bash('threlium-bridge@email.service', 200)}",
        ],
        repo_root=root,
        timeout=30,
    )
    mailflow_diag_block("mailflow: journal threlium-bridge@email (fdm)", md_log_r.stdout + md_log_r.stderr, max_chars=20000)

    j = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", E2E_SUT_THRELIUM_USER_UNIT_JOURNAL + "\necho '--- journal broad tail ---'\njournalctl -n 120 --no-pager 2>&1 || true"],
        repo_root=root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    mailflow_diag_block("mailflow: journal (threlium user-units + broad tail)", j.stdout + j.stderr, max_chars=20000)


def _iter_notmuch_mbox_show_messages(mbox_text: str) -> Iterator[EmailMessage]:
    """RFC822-письма из ``notmuch show --format=mbox`` (полные заголовки, в т.ч. ``X-Threlium-*``).

    ``--format=json`` отдаёт урезанный ``headers`` без служебных полей Threlium; mbox — полный конверт.
    """
    for block in re.split(r"(?=^From )", mbox_text, flags=re.MULTILINE):
        block = block.strip()
        if not block:
            continue
        lines = block.splitlines(keepends=True)
        if lines and lines[0].startswith("From "):
            rfc822 = "".join(lines[1:])
        else:
            rfc822 = block
        if rfc822.strip():
            yield e2e_parse_rfc822(rfc822.encode("utf-8", errors="replace"))


def _notmuch_mbox_show_route_b62_for_message(
    mbox_text: str,
    *,
    message_id_inner: str,
) -> str | None:
    """``X-Threlium-Route`` (b62 wire) для письма с данным inner ``Message-ID`` в выводе mbox."""
    from threlium.mail_header_names import MailHeaderName

    needle = NotmuchMessageIdInner.parse(message_id_inner)
    hdr = MailHeaderName.ROUTE.value
    for msg in _iter_notmuch_mbox_show_messages(mbox_text):
        mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID)
        mid_inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
        if mid_inner is None or not mid_inner.equals_case_insensitive(needle):
            continue
        route = msg.get(hdr)
        if route is None:
            route = msg.get("X-Threlium-Route")
        if route is None:
            continue
        s = str(route).strip()
        return s if s else None
    return None
