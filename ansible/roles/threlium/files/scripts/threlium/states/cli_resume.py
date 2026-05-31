#!/usr/bin/env python3
"""cli_resume@localhost: ответ после HITL → cli_exec или enrich_fast@ (ARCHITECTURE §6.2, §4.3)."""
import email as _email_mod
import shlex
from email import policy as _email_policy
from email.message import EmailMessage

from threlium.cli_fsm import cli_payload_as_json, parse_cli_intent_payload
from threlium.cli_hitl_tool_bridge import parse_confirm_cli_hitl
from threlium.fsm_emit import build_fsm_plain_to_stage, build_fsm_step_to_stage
from threlium.ingress_hitl_resolve import find_cli_intent_maildir_path_from_in_reply_to_ancestors
from threlium.litellm_correlation_headers import build_litellm_correlation_headers
from threlium.litellm_route_context import get_litellm_http_correlation
from threlium.litellm_tool_completion import completion_required_tool_sync
from threlium.litellm_tool_response import (
    LiteLlmToolResponseError,
    require_tool_calls_response,
)
from threlium.litellm_tool_spec import load_tool_spec
from threlium.mime_reform import extract_plain_body, system_part_text
from threlium.nm import require_inner_message_id_from_fsm_email
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings, resolve_llm_endpoint
from threlium.types import (
    CliHitlBridgeError,
    FsmStage,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    LiteLlmAcompletionKwargs,
    LiteLlmChatMessage,
    LitellmCallSite,
    LitellmRoutingSite,
    PromptPath,
)
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader


def _extract_decoded_body_from_maildir_file(path) -> str:
    """Read a Maildir file and properly decode its body (handles QP, base64, etc.)."""
    raw_bytes = path.read_bytes()
    intent_msg = _email_mod.message_from_bytes(raw_bytes, policy=_email_policy.default)
    return extract_plain_body(intent_msg)


def _command_line_from_payload(payload) -> str:
    line = " ".join(shlex.quote(a) for a in payload.argv)
    if payload.cwd:
        line = f"(cwd={shlex.quote(payload.cwd)}) {line}"
    return line


def _e2e_litellm_correlation(
    msg: EmailMessage, config: ThreliumSettings
) -> dict[str, str] | None:
    if not config.e2e.litellm_route_correlation:
        return None
    snap = get_litellm_http_correlation()
    if snap is not None:
        corr = dict(snap)
    else:
        corr = build_litellm_correlation_headers(
            msg, call_site=LitellmCallSite.CLI_HITL_RESUME
        )
    corr[LitellmCorrelationHeader.CALL_SITE.value] = (
        LitellmCallSite.CLI_HITL_RESUME.value
    )
    return corr


def _classify_hitl_reply(
    msg: EmailMessage,
    *,
    command_line: str,
    user_reply: str,
    config: ThreliumSettings,
) -> bool:
    """LLM classifier (score 0); fail-closed → False on any bridge/LLM error."""
    ep = resolve_llm_endpoint(config.litellm, LitellmRoutingSite.CLI_HITL_RESUME)
    mr = ep.max_retries if ep.max_retries is not None else config.litellm.max_retries
    system = render_prompt(PromptPath.CLI_RESUME_CLASSIFY_SYSTEM).strip()
    user = render_prompt(
        PromptPath.CLI_RESUME_CLASSIFY_USER,
        command_line=command_line,
        user_reply=user_reply,
    ).strip()
    tool_spec = load_tool_spec(PromptPath.CLI_RESUME_CONFIRM_CLI_HITL_TOOL_SPEC)
    call = LiteLlmAcompletionKwargs(
        model=ep.model,
        messages=[
            LiteLlmChatMessage(role="system", content=system),
            LiteLlmChatMessage(role="user", content=user),
        ],
        timeout=float(ep.timeout),
        max_retries=mr,
        api_key=ep.api_key,
        api_base=ep.api_base,
        max_tokens=ep.max_tokens,
        chat_template_kwargs=ep.chat_template_kwargs or None,
    )
    try:
        resp = completion_required_tool_sync(
            settings=config,
            call=call,
            tools=[tool_spec],
            correlation_override=_e2e_litellm_correlation(msg, config),
        )
        assistant = require_tool_calls_response(resp, context="cli_hitl_resume")
        args = parse_confirm_cli_hitl(assistant)
        return args.confirmed is True
    except (CliHitlBridgeError, LiteLlmToolResponseError, RuntimeError, ValueError, TypeError):
        return False


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    # FSM-стадии не индексируют (docs/INDEX.md §8): fdm/notmuch insert уже сделали
    # терминирующий `notmuch insert` при доставке этого письма.
    user_reply = system_part_text(msg).strip()

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
    command_line = _command_line_from_payload(payload)
    if _classify_hitl_reply(
        msg, command_line=command_line, user_reply=user_reply, config=config
    ):
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
