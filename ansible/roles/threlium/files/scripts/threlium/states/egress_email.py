#!/usr/bin/env python3
"""egress_email@localhost: SMTP через msmtp, затем запись отправленного в ``archive``."""
from __future__ import annotations

import shutil
import subprocess
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path

import threlium.nm as nm
from threlium.delivery import run_fdm
from threlium.settings import ThreliumSettings
from threlium.egress_self_archive import (
    build_egress_sent_record_to_archive,
    find_existing_egress_archive,
)
from threlium.ingress_route_resolve import resolve_egress_task_route_ancestor
from threlium.logutil import logger
from threlium.mime_reform import (
    RFC822_FOR_INSERT,
    email_message_from_bytes,
    system_part_text,
)
from threlium.types import (
    EmailIngressRoute,
    EmailNativeId,
    ExternalRfcMidWire,
    FsmStage,
    RfcMessageIdWire,
    RfcReferencesWire,
    MailHeaderName,
    references_angle_bracket_tokens,
    truncate_rfc_references_wire,
)

_HDR = MailHeaderName

log = logger.bind(stage="egress_email")


def _references_append_smtp_tail(refs: str | None, tail: ExternalRfcMidWire | None) -> str | None:
    """§M4: к RFC-цепочке ``References`` добавить хвост = внешний ``In-Reply-To``, если ещё нет."""
    base = (refs or "").strip()
    if tail is None:
        return base if base else None
    t = tail.value.strip()
    if not t:
        return base if base else None
    tail_token = t if t.startswith("<") and t.endswith(">") else f"<{t.strip('<>')}>"
    if not base:
        return tail_token
    if tail_token in set(references_angle_bracket_tokens(base)):
        return base
    return f"{base} {tail_token}".strip()


def _strip_internal_before_smtp(em: EmailMessage) -> None:
    for h in list(em.keys()):
        hl = h.lower()
        if hl.startswith("x-threlium-"):
            del em[h]


def _run_msmtp_stdin(data: bytes) -> None:
    """RFC822 на stdin → ``msmtp -t``; код ≠ 0 → ``RuntimeError``."""
    msmtp = shutil.which("msmtp") or "/usr/bin/msmtp"
    if not Path(msmtp).is_file():
        raise RuntimeError("msmtp not found (install msmtp)")
    r = subprocess.run([msmtp, "-t"], input=data)
    if r.returncode != 0:
        raise RuntimeError(f"msmtp exited with code {r.returncode}")


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    existing = find_existing_egress_archive(msg)
    if existing is not None:
        log.info("archive_found_resend")
        glue_native = RfcMessageIdWire.native_from_canonical_str(
            existing.glue_message_id.value, native_type=EmailNativeId,
        )
        ext_mid = f"<{glue_native.message_id}>"
        resend_msg = _build_smtp_message(msg, ext_mid, config=config)
        smtp_bytes = resend_msg.as_bytes(policy=RFC822_FOR_INSERT)
        _run_msmtp_stdin(smtp_bytes)
        return None

    leaf_inner = nm.require_inner_message_id_from_fsm_email(msg)
    nat = RfcMessageIdWire.native_from_canonical_str(
        leaf_inner.as_angle_bracket_header(), native_type=EmailNativeId
    )
    log.info("canonical_message_id", version=nat.v)

    outbound_mid = make_msgid(domain="localhost")
    smtp_msg = _build_smtp_message(msg, outbound_mid, config=config)
    smtp_bytes = smtp_msg.as_bytes(policy=RFC822_FOR_INSERT)

    ext_inner = outbound_mid.strip().strip("<>")
    glue_native = EmailNativeId(v=1, message_id=ext_inner)
    glue_mid = RfcMessageIdWire.from_native(glue_native)
    sent_raw = smtp_bytes.decode("utf-8", errors="replace")

    archive_email = build_egress_sent_record_to_archive(
        msg, stage=stage, sent_raw=sent_raw, glue_message_id_wire=glue_mid,
        settings=config,
    )
    run_fdm(archive_email.as_bytes(policy=RFC822_FOR_INSERT))
    log.info("archive_written")

    log.info("msmtp_sending")
    _run_msmtp_stdin(smtp_bytes)

    return None


def _build_smtp_message(
    msg: EmailMessage,
    outbound_mid: str,
    *,
    config: ThreliumSettings,
) -> EmailMessage:
    """Собрать SMTP-письмо (адреса, References, IRT) из FSM task."""
    ing, ancestor_snap = resolve_egress_task_route_ancestor(
        msg,
        EmailIngressRoute,
        wrong_route_type_message=lambda r: (
            f"egress_email: expected EmailIngressRoute, got {type(r).__name__}"
        ),
    )
    dest = ing.origin
    if not dest:
        raise RuntimeError("egress_email: empty X-Threlium-Route origin")

    smtp_msg = email_message_from_bytes(msg.as_bytes(policy=RFC822_FOR_INSERT))
    _strip_internal_before_smtp(smtp_msg)
    # Внешнему получателю уходит чистое text/plain тело из <system>, без внутренней
    # MIME-структуры FSM (<system>/<history>-части, их Content-ID и inline-дисп.).
    # set_content схлопывает multipart обратно в одиночную text/plain-часть.
    smtp_msg.set_content(system_part_text(msg), subtype="plain", charset="utf-8")

    dm = ExternalRfcMidWire(value=outbound_mid)
    for h in list(smtp_msg.keys()):
        if h.lower() == _HDR.MESSAGE_ID.lower():
            del smtp_msg[h]
    smtp_msg[_HDR.MESSAGE_ID] = dm.value

    irt_ext = ing.reply_target_rfc_message_id
    for h in list(smtp_msg.keys()):
        if h.lower() == _HDR.IN_REPLY_TO.lower():
            del smtp_msg[h]
    if irt_ext is not None:
        smtp_msg[_HDR.IN_REPLY_TO] = irt_ext.value

    refs_w = ancestor_snap.header_references
    refs_base = refs_w.value if refs_w is not None else None
    if refs_base and refs_base.strip():
        refs_base = RfcReferencesWire.threlium_decanonicalize_refs(
            refs_base, EmailNativeId
        ).value
    refs_combined = _references_append_smtp_tail(refs_base, irt_ext)
    for h in list(smtp_msg.keys()):
        if h.lower() == _HDR.REFERENCES.lower():
            del smtp_msg[h]
    if refs_combined:
        rs = truncate_rfc_references_wire(
            RfcReferencesWire.parse(refs_combined),
            max_len=config.egress.references_max_chars,
        ).value.strip()
        if rs:
            smtp_msg[_HDR.REFERENCES] = rs

    for h in list(smtp_msg.keys()):
        if h.lower() == _HDR.TO.value.lower():
            del smtp_msg[h]
    smtp_msg[_HDR.TO] = dest
    for h in list(smtp_msg.keys()):
        if h.lower() == _HDR.FROM.value.lower():
            del smtp_msg[h]
    smtp_msg[_HDR.FROM] = config.egress.email_from

    subj_w = ancestor_snap.header_subject
    if subj_w is not None:
        smtp_subj = subj_w.value.replace("\n", " ").replace("\r", "")[:900]
        if smtp_subj:
            for h in list(smtp_msg.keys()):
                if h.lower() == _HDR.SUBJECT.value.lower():
                    del smtp_msg[h]
            smtp_msg[_HDR.SUBJECT] = smtp_subj

    return smtp_msg
