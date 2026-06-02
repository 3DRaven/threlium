#!/usr/bin/env python3
"""cli_resume@localhost: ответ после HITL → cli_exec или enrich_fast@ (ARCHITECTURE §6.2, §4.3)."""
from email.message import EmailMessage

import jsonschema

from threlium.cli_fsm import (
    cli_command_line_for_intent,
    cli_payload_as_json,
    parse_cli_intent_payload,
)
from threlium.cli_hitl_tool_bridge import parse_confirm_cli_hitl_assistant
from threlium.fsm_emit import build_fsm_plain_to_stage, build_fsm_step_to_stage
from threlium.ingress_hitl_resolve import find_cli_intent_maildir_path_from_in_reply_to_ancestors
from threlium.litellm_correlation_headers import build_litellm_correlation_headers
from threlium.litellm_required_tool import invoke_required_tool
from threlium.litellm_route_context import get_litellm_http_correlation
from threlium.litellm_tool_response import LiteLlmToolResponseError
from threlium.litellm_tool_spec import load_tool_spec
from threlium.logutil import clip_log_text, logger
from threlium.mime_reform import system_part_text, system_part_text_from_path
from threlium.nm import require_inner_message_id_from_fsm_email
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings, resolve_llm_endpoint
from threlium.types import (
    CliHitlBridgeError,
    ConfirmCliHitlToolArgs,
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

log = logger.bind(stage="cli_resume")

_MAX_CLI_HITL_CLASSIFY_RETRIES = 2


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
            msg, call_site=LitellmCallSite.CONFIRM_CLI_HITL
        )
    corr[LitellmCorrelationHeader.CALL_SITE.value] = (
        LitellmCallSite.CONFIRM_CLI_HITL.value
    )
    return corr


def _emit_user_not_confirmed(
    msg: EmailMessage,
    stage: FsmStage,
    *,
    config: ThreliumSettings,
    interpretation: str | None = None,
) -> EmailMessage:
    note = render_prompt(
        PromptPath.CLI_RESUME_NOT_CONFIRMED,
        interpretation=interpretation or "",
    ).strip()
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=stage,
        history=note,
        system=note,
        subject_line=FsmTransitionPlainSubjectLine.parse("CLI command not confirmed"),
        settings=config,
    )


def _classify_hitl_reply(
    msg: EmailMessage,
    *,
    command_line: str,
    user_reply: str,
    config: ThreliumSettings,
) -> ConfirmCliHitlToolArgs:
    """LLM classifier (score 0); retry на bridge/tool_response; затем raise."""
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
    correlation = _e2e_litellm_correlation(msg, config)

    last_error: BaseException | None = None
    for attempt in range(_MAX_CLI_HITL_CLASSIFY_RETRIES + 1):
        try:
            assistant = invoke_required_tool(
                settings=config,
                call=call,
                tool_spec=tool_spec,
                correlation_snap=correlation,
                context="cli_hitl_resume",
            )
            args = parse_confirm_cli_hitl_assistant(assistant)
            log.info(
                "cli_hitl_classify_ok",
                confirmed=args.confirmed,
                user_reply_len=len(user_reply),
                command_line=clip_log_text(command_line),
            )
            return args
        except (
            CliHitlBridgeError,
            LiteLlmToolResponseError,
            jsonschema.ValidationError,
        ) as exc:
            last_error = exc
            if attempt >= _MAX_CLI_HITL_CLASSIFY_RETRIES:
                raise
            log.warning(
                "cli_hitl_classify_retry",
                attempt=attempt + 1,
                error=str(exc),
            )
    if last_error is not None:
        raise last_error
    raise RuntimeError("cli_hitl_resume: unreachable classify retry loop exit")


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    # FSM-стадии не индексируют (docs/INDEX.md §8): fdm/notmuch insert уже сделали
    # терминирующий `notmuch insert` при доставке этого письма.
    user_reply = system_part_text(msg).strip()

    if not user_reply:
        log.info("cli_hitl_empty_reply")
        return _emit_user_not_confirmed(msg, stage, config=config)

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

    try:
        intent_payload_text = system_part_text_from_path(intent_path).strip()
    except RuntimeError:
        # Письмо cli_intent по контракту несёт payload в <system>; отсутствие части —
        # деградировавшая цепочка после долгого HITL-разрыва. Сохраняем graceful-маршрут.
        log.warning("cli_resume_intent_no_system", path=str(intent_path))
        intent_payload_text = ""
    payload = parse_cli_intent_payload(intent_payload_text)
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
    command_line = cli_command_line_for_intent(payload)
    args = _classify_hitl_reply(
        msg, command_line=command_line, user_reply=user_reply, config=config
    )
    if args.confirmed:
        return build_fsm_plain_to_stage(
            msg,
            to_addr=FsmStage.CLI_EXEC,
            from_stage=stage,
            body=FsmTransitionPlainBody.parse(canon),
            settings=config,
        )
    log.info(
        "cli_hitl_user_declined",
        interpretation=clip_log_text(args.interpretation or ""),
    )
    return _emit_user_not_confirmed(
        msg,
        stage,
        config=config,
        interpretation=args.interpretation,
    )
