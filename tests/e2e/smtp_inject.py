#!/usr/bin/env python3
"""Вызывается с хоста pytest: одно письмо в GreenMail по SMTP.

Usage::

    python smtp_inject.py <host> <port> [--message-id ID] [--subject SUBJECT] [--body TEXT]

Env overrides (lower priority than CLI)::

    THRELIUM_E2E_INJECT_MESSAGE_ID   — Message-ID without angle brackets
    THRELIUM_E2E_INJECT_SUBJECT      — Subject header
    THRELIUM_E2E_INJECT_IN_REPLY_TO  — In-Reply-To / References (inner MID, без скобок)
    THRELIUM_E2E_FETCHMAIL_USER      — local-part или полный адрес получателя (по умолчанию ``test`` → ``test@localhost``)
"""
from __future__ import annotations

import os
import smtplib
import sys
from email.message import EmailMessage

from threlium.logutil import logger, setup_logging, shutdown_logging

_DEFAULT_MESSAGE_ID = "e2e-inbound@localhost"
_DEFAULT_SUBJECT = "e2e inbound"


def main() -> None:
    setup_logging(os.environ.get("THRELIUM_LOG_LEVEL", "INFO"))
    log = logger.bind(stage="e2e", component="smtp_inject")
    args = sys.argv[1:]
    host = "greenmail"
    port = 3025
    message_id: str | None = None
    subject: str | None = None
    body_text: str | None = None
    in_reply_to: str | None = None

    positional: list[str] = []
    i = 0
    while i < len(args):
        if args[i] == "--message-id" and i + 1 < len(args):
            message_id = args[i + 1]
            i += 2
        elif args[i] == "--subject" and i + 1 < len(args):
            subject = args[i + 1]
            i += 2
        elif args[i] == "--body" and i + 1 < len(args):
            body_text = args[i + 1]
            i += 2
        elif args[i] == "--in-reply-to" and i + 1 < len(args):
            in_reply_to = args[i + 1]
            i += 2
        else:
            positional.append(args[i])
            i += 1

    if len(positional) >= 1:
        host = positional[0]
    if len(positional) >= 2:
        port = int(positional[1])

    message_id = message_id or os.environ.get("THRELIUM_E2E_INJECT_MESSAGE_ID") or _DEFAULT_MESSAGE_ID
    subject = subject or os.environ.get("THRELIUM_E2E_INJECT_SUBJECT") or _DEFAULT_SUBJECT
    in_reply_to = in_reply_to or os.environ.get("THRELIUM_E2E_INJECT_IN_REPLY_TO")
    to_addr = os.environ.get("THRELIUM_E2E_FETCHMAIL_USER", "test").strip()
    if not to_addr:
        raise SystemExit("THRELIUM_E2E_FETCHMAIL_USER must be non-empty when set")
    if "@" not in to_addr:
        to_addr = f"{to_addr}@localhost"

    msg = EmailMessage()
    msg["From"] = "pytest@localhost"
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Message-ID"] = f"<{message_id.strip('<>')}>"
    if in_reply_to:
        irt = f"<{in_reply_to.strip('<>')}>"
        msg["In-Reply-To"] = irt
        msg["References"] = irt
    msg.set_content(body_text if body_text is not None else "e2e body")
    # Thread-root MID в SUT: e2e_thread_root_mid_for_message_id(message_id)
    with smtplib.SMTP(host, port) as s:
        s.send_message(msg)
    log.info("smtp_inject_ok", host=host, port=port, message_id=message_id.strip("<>"))
    shutdown_logging()


if __name__ == "__main__":
    main()
