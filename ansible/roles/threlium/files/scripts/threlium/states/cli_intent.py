#!/usr/bin/env python3
"""cli_intent@localhost: решение роутера через ``classify_cli_intent`` (ARCHITECTURE §6.2).

route-collision → enrich_fast@localhost (observation-note: имя маршрута — это tool,
                  а не CLI-команда; модель вызовет настоящий tool на следующем хопе).
allow           → cli_exec@localhost (новое письмо с канонизированным JSON).
deny            → ingress@localhost (отказ; далее enrich → reasoning).
hitl            → cli_hitl_out@localhost (подтверждение у пользователя).
"""
import shlex
from email.message import EmailMessage

from threlium.cli_fsm import (
    classify_cli_intent,
    cli_payload_as_json,
    parse_cli_intent_payload,
)
from threlium.fsm_emit import build_fsm_plain_to_stage, build_fsm_step_to_stage
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    CliExecDecision,
    CliIntentPolicy,
    CliRouteCollision,
    FsmStage,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    PromptPath,
)


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    body = system_part_text(msg).strip()
    payload = parse_cli_intent_payload(body)
    if not payload:
        return build_fsm_plain_to_stage(
            msg,
            to_addr=FsmStage.INGRESS,
            from_stage=stage,
            body=FsmTransitionPlainBody.parse(
                render_prompt(PromptPath.CLI_INTENT_INVALID, prior=body)
            ),
            subject_line=FsmTransitionPlainSubjectLine.parse(
                render_prompt(PromptPath.CLI_INTENT_INVALID_SUBJECT).strip()
            ),
            settings=config,
        )

    canon = cli_payload_as_json(payload)

    match classify_cli_intent(payload, config):
        case CliRouteCollision(route=route, cmd=cmd):
            note = render_prompt(
                PromptPath.CLI_INTENT_ROUTE_COLLISION, route=route.value, cmd=cmd
            ).strip()
            return build_fsm_step_to_stage(
                msg,
                to_addr=FsmStage.ENRICH_FAST,
                from_stage=stage,
                history=note,
                settings=config,
            )
        case CliExecDecision(policy=CliIntentPolicy.DENY):
            denied_line = " ".join(shlex.quote(a) for a in payload.argv)
            if payload.cwd:
                denied_line = f"(cwd={shlex.quote(payload.cwd)}) {denied_line}"
            return build_fsm_plain_to_stage(
                msg,
                to_addr=FsmStage.INGRESS,
                from_stage=stage,
                body=FsmTransitionPlainBody.parse(
                    render_prompt(PromptPath.CLI_INTENT_DENIED, command_line=denied_line)
                ),
                subject_line=FsmTransitionPlainSubjectLine.parse(
                    render_prompt(PromptPath.CLI_INTENT_DENIED_SUBJECT).strip()
                ),
                settings=config,
            )
        case CliExecDecision(policy=CliIntentPolicy.ALLOW):
            return build_fsm_plain_to_stage(
                msg, to_addr=FsmStage.CLI_EXEC, from_stage=stage,
                body=FsmTransitionPlainBody.parse(canon),
                settings=config,
            )
        case CliExecDecision(policy=CliIntentPolicy.HITL):
            return build_fsm_plain_to_stage(
                msg,
                to_addr=FsmStage.CLI_HITL_OUT,
                from_stage=stage,
                body=FsmTransitionPlainBody.parse(canon),
                settings=config,
            )
