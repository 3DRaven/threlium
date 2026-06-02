#!/usr/bin/env python3
"""Зонд IRT + резолв маршрута egress и сверка с CLI notmuch.

**U0** — письмо в стадии ``ingress``, с которого начался тред (самое старое в треде
с путём под ``…/ingress/Maildir/…`` в union notmuch).

Запуск на агенте::

  export NOTMUCH_CONFIG=$HOME/.notmuch-config
  export PYTHONPATH=/home/threlium/threlium/agent/scripts
  /home/threlium/threlium/agent/.venv/bin/python scripts/debug_egress_irt_probe.py \\
      [/path/to/mail/file]
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import notmuch2  # pyright: ignore[reportMissingImports]

from threlium import nm
from threlium.mail import email_message_from_bytes
from threlium.ingress_route_resolve import (
    egress_fsm_start_inner_from_email,
    resolve_route_for_egress_fsm_from_email,
)
from threlium.irt_chain import iter_in_reply_to_ancestors_from_inner_id

DEFAULT_MAIL = Path(
    "/home/threlium/threlium/data/stages/egress_router/Maildir/new/"
    "1779044305.M983606P193466.th-agent"
)


def _id_term(inner: str) -> str:
    return f"id:{inner}"


def _notmuch_cli(argv: list[str]) -> tuple[int, str, str]:
    env = os.environ.copy()
    env.setdefault("NOTMUCH_CONFIG", str(Path.home() / ".notmuch-config"))
    p = subprocess.run(
        ["notmuch", *argv],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return p.returncode, p.stdout, p.stderr


def _print_notmuch_mbox(mid_inner: str, *, max_bytes: int = 12000) -> None:
    term = _id_term(mid_inner)
    rc, out, err = _notmuch_cli(["show", "--format=mbox", term])
    print(f"--- notmuch show --format=mbox {term!r} rc={rc} ---")
    if err.strip():
        print(err.rstrip())
    body = out if len(out) <= max_bytes else out[:max_bytes] + "\n… [truncated]\n"
    print(body.rstrip() or "(empty stdout)")


def _print_notmuch_tags(mid_inner: str) -> None:
    term = _id_term(mid_inner)
    rc, out, err = _notmuch_cli(["search", "--output=tags", term])
    line = out.strip().replace("\n", " ") or "(no tags line)"
    print(f"--- notmuch search --output=tags {term!r} rc={rc} → {line}")


def _find_u0_path_and_mid(leaf_inner) -> tuple[Path | None, str | None]:
    """Самое старое письмо треда, лежащее в Maildir стадии ``ingress`` (начало треда в ingress)."""
    with nm.notmuch_database(write=False) as db:
        tid = nm.thread_id_for_header_message_id_in_db(db, leaf_inner)
        if tid is None:
            return None, None
        q = tid.as_notmuch_thread_term()
        for nm_msg in db.messages(q, sort=notmuch2.Database.SORT.OLDEST_FIRST):
            p = Path(str(nm_msg.path))
            if "/ingress/Maildir/" in str(p).replace("\\", "/"):
                mid = nm.require_inner_message_id_from_notmuch_message(nm_msg)
                return p, mid.value
    return None, None


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_MAIL
    print("=== file ===", path, "exists=", path.is_file())

    msg = email_message_from_bytes(path.read_bytes())
    leaf = egress_fsm_start_inner_from_email(msg)
    print("=== leaf inner ===", leaf.as_angle_bracket_header())

    with nm.notmuch_database(write=False) as db:
        tid = nm.thread_id_for_header_message_id_in_db(db, leaf)
        print("=== notmuch thread id (nm) ===", tid.value if tid else None)

    u0_path, u0_mid = _find_u0_path_and_mid(leaf)
    print("=== U0 (oldest in thread under …/ingress/…) ===")
    if u0_mid is None:
        print("  (not found — no message in this thread has path …/ingress/Maildir/…)")
    else:
        print("  path:", u0_path)
        print("  Message-ID inner:", u0_mid)
        _print_notmuch_tags(u0_mid)
        tags_line = _notmuch_cli(["search", "--output=tags", _id_term(u0_mid)])[1].strip()
        print("  tag:route present:", "route" in tags_line.split())
        rc_r, out_r, _ = _notmuch_cli(["search", "--output=files", _id_term(u0_mid)])
        hdr = ""
        if rc_r == 0 and out_r.strip():
            try:
                p0 = Path(out_r.strip().split("\n")[0])
                raw = p0.read_bytes()[:8000]
                from threlium.mail import parse_rfc822

                em = parse_rfc822(raw)
                hdr = em.get("X-Threlium-Route")
            except OSError:
                hdr = "(read header failed)"
        print("  X-Threlium-Route (first bytes of file on disk):", repr(hdr))

    print("=== IRT ancestors: Python snapshots vs notmuch CLI ===", flush=True)
    chain = iter_in_reply_to_ancestors_from_inner_id(leaf)
    print("=== IRT chain len (notmuch2) ===", len(chain))
    for i, s in enumerate(chain):
        mid = s.message_id_inner.value
        print(
            f"\n--- [{i}] snapshot mid={mid!r}",
            f"path={s.path}",
            f"tags={sorted(s.tags)}",
            f"In-Reply-To={s.header_in_reply_to!r}",
            f"X-Threlium-Route={s.header_route!r}",
            flush=True,
        )
        _print_notmuch_tags(mid)
        _print_notmuch_mbox(mid)

    print("\n=== resolve_route_for_egress_fsm_from_email ===")
    try:
        r = resolve_route_for_egress_fsm_from_email(msg)
        print("OK", r)
    except Exception as e:
        print(type(e).__name__ + ":", e)


if __name__ == "__main__":
    main()
