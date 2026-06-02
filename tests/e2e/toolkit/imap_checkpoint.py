"""IMAP processed-folder / email bridge checkpoint."""
from __future__ import annotations

import shlex
from pathlib import Path

from threlium.types import EmailIngressRoute, IngressRouteB62Wire

from .bridges.email import notmuch_id_search_term
from .constants import E2E_SUT_NOTMUCH_BASH_EXPORT, REPO_ROOT, TIMEOUT_POLL_SHORT
from .diag import _notmuch_mbox_show_route_b62_for_message
from .runtime import service_exec

def email_ingress_imap_checkpoint_from_notmuch(
    project: str,
    *,
    nm_inner: str,
    repo_root: Path | None = None,
) -> tuple[int | None, int]:
    """``(imap_uidvalidity, imap_uid)`` из ``X-Threlium-Route`` ingress-письма в notmuch."""
    root = repo_root or REPO_ROOT
    id_term = notmuch_id_search_term(nm_inner)
    cmd = [
        "bash",
        "-lc",
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch show --format=mbox {shlex.quote(id_term)}",
    ]
    r = service_exec(project, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
    text = (r.stdout or "").strip()
    if not text:
        return None, 0
    route_b62 = _notmuch_mbox_show_route_b62_for_message(text, message_id_inner=nm_inner)
    if not route_b62:
        return None, 0
    route_w = IngressRouteB62Wire.decode_b62_wire(route_b62)
    if not isinstance(route_w, EmailIngressRoute):
        return None, 0
    uid = route_w.imap_uid
    uiv = route_w.imap_uidvalidity
    return (int(uiv) if uiv is not None else None, int(uid) if uid is not None else 0)


def restart_email_bridge_service(project: str, *, repo_root: Path | None = None) -> None:
    """``systemctl --user restart threlium-bridge@email`` на SUT."""
    from .sut_user_systemd import E2E_THRELIUM_USER

    root = repo_root or REPO_ROOT
    cmd = [
        "bash",
        "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user restart threlium-bridge@email.service",
    ]
    r = service_exec(project, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
    assert r.returncode == 0, f"bridge restart failed: {(r.stderr or r.stdout)!r}"
