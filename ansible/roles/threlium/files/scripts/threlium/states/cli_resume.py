#!/usr/bin/env python3
"""cli_resume@localhost: ответ после HITL → cli_exec или enrich_fast@ (ARCHITECTURE §6.2, §4.3)."""
import email as _email_mod
from email import policy as _email_policy
from email.message import EmailMessage

from threlium.cli_fsm import (
    cli_payload_as_json,
    parse_cli_intent_payload,
    parse_yes_no,
)
from threlium.ingress_hitl_resolve import find_cli_intent_maildir_path_from_in_reply_to_ancestors
from threlium.fsm_emit import build_fsm_plain_to_stage, build_fsm_step_to_stage
from threlium.mime_reform import extract_plain_body, system_part_text
from threlium.nm import require_inner_message_id_from_fsm_email
from threlium.settings import ThreliumSettings
from threlium.types import FsmStage, FsmTransitionPlainBody, FsmTransitionPlainSubjectLine


def _extract_decoded_body_from_maildir_file(path) -> str:
    """Read a Maildir file and properly decode its body (handles QP, base64, etc.)."""
    raw_bytes = path.read_bytes()
    intent_msg = _email_mod.message_from_bytes(raw_bytes, policy=_email_policy.default)
    return extract_plain_body(intent_msg)


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    # FSM-стадии не индексируют (docs/INDEX.md §8): fdm/notmuch insert уже сделали
    # терминирующий `notmuch insert` при доставке этого письма.
    yn = parse_yes_no(system_part_text(msg))

    intent_path = find_cli_intent_maildir_path_from_in_reply_to_ancestors(
        require_inner_message_id_from_fsm_email(msg)
    )
    if intent_path is None:
        note = (
            "Threlium cli_resume: could not find a CLI intent message along In-Reply-To ancestors "
            "(notmuch). Ensure the chain is indexed and a prior cli_intent step exists on this branch."
        )
        return build_fsm_step_to_stage(
            msg,
            to_addr=FsmStage.ENRICH_FAST,
            from_stage=stage,
            history=note,
            system=note,
            subject_line=FsmTransitionPlainSubjectLine.parse("CLI resume: intent not found"),
            settings=config,
        )

    body_text = _extract_decoded_body_from_maildir_file(intent_path)
    payload = parse_cli_intent_payload(body_text)
    if not payload:
        note = "Threlium cli_resume: could not parse stored CLI intent JSON in thread."
        return build_fsm_step_to_stage(
            msg,
            to_addr=FsmStage.ENRICH_FAST,
            from_stage=stage,
            history=note,
            system=note,
            subject_line=FsmTransitionPlainSubjectLine.parse("CLI resume: bad intent"),
            settings=config,
        )

    canon = cli_payload_as_json(payload)
    if yn is True:
        return build_fsm_plain_to_stage(
            msg, to_addr=FsmStage.CLI_EXEC, from_stage=stage, body=FsmTransitionPlainBody.parse(canon),
            settings=config,
        )
    note = (
        "Threlium cli_resume: user did not confirm the CLI command (no or ambiguous reply)."
    )
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=stage,
        history=note,
        system=note,
        subject_line=FsmTransitionPlainSubjectLine.parse("CLI command not confirmed"),
        settings=config,
    )
