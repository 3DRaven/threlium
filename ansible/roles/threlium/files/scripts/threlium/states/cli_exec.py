#!/usr/bin/env python3
"""cli_exec → ingress@localhost: исполнение команды в transient ``systemd-run --scope`` (ARCHITECTURE §6).

Capability-профиль = хвост ``X-Threlium-Capabilities`` на входящем письме.
Ресурсные лимиты (``MemoryMax``, ``CPUQuota``, ``TasksMax``) из ``Config``
(env: ``THRELIUM_CLI_EXEC_*``). ``cli_exec`` только читает capabilities.
"""
import subprocess
from email.message import EmailMessage

from threlium.cli_fsm import parse_cli_intent_payload, resolve_cli_exec_argv
from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    FsmStage,
    MailHeaderName,
    PromptPath,
    ThreliumCapabilitiesBudgetLine,
)

log = logger.bind(stage="cli_exec")


def _peek_cap_top(
    line: ThreliumCapabilitiesBudgetLine | None,
) -> str | None:
    """Вершина стека Capabilities без POP (peek, не мутация).

    POP делает ``egress_router`` при возврате из субагента;
    ``cli_exec`` только читает для выбора ресурсного профиля.
    """
    if line is None or not line.value.strip():
        return None
    parts = line.value.strip().split()
    return parts[-1] if parts else None


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    prior = system_part_text(msg).strip()
    cli = parse_cli_intent_payload(prior)

    if not cli:
        log.warning("no_parseable_payload")
        body = render_prompt(PromptPath.CLI_EXEC_OBSERVATION, cmd_line="", prior=prior)
        # observation → <history> (результат tool в долгую память, origin=cli_exec) +
        # <system> (payload, который ingress прочитает как продолжение хода).
        return build_fsm_step_to_stage(
            msg, to_addr=FsmStage.INGRESS, from_stage=stage,
            history=body, system=body, settings=config,
        )

    # Peek capability profile from X-Threlium-Capabilities stack top
    cap_line = ThreliumCapabilitiesBudgetLine.parse(
        msg.get(MailHeaderName.CAPABILITIES.value)
    )
    cap_name = _peek_cap_top(cap_line) or "default"

    exec_argv = resolve_cli_exec_argv(cli.argv)
    cmd_line = " ".join(exec_argv)
    log.info("executing", cap=cap_name, cmd_line=cmd_line)

    # Build systemd-run --scope command with resource limits from Config
    scope_cmd = [
        "systemd-run", "--user", "--scope", "--quiet",
        f"--property=MemoryMax={config.cli.exec_memory_max}",
        f"--property=CPUQuota={config.cli.exec_cpu_quota}",
        f"--property=TasksMax={config.cli.exec_tasks_max}",
        "--",
        *exec_argv,
    ]

    try:
        result = subprocess.run(
            scope_cmd,
            capture_output=True,
            timeout=config.cli.exec_timeout,
            text=True,
            cwd=cli.cwd or None,
        )
        observation = (
            f"exit_code={result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    except subprocess.TimeoutExpired:
        observation = f"TIMEOUT after {config.cli.exec_timeout}s"
        log.error("timeout", timeout_seconds=config.cli.exec_timeout)
    except Exception as e:
        observation = f"exec error: {e}"
        log.error("exec_error", error=str(e))

    body = render_prompt(
        PromptPath.CLI_EXEC_OBSERVATION,
        cmd_line=cmd_line,
        prior=observation,
    )
    # observation (cmd_line + stdout/stderr/exit) → <history> (origin=cli_exec) + <system>.
    # cmd_line уже встроен в рендер, отдельный request_echo не нужен (был бы дублем).
    return build_fsm_step_to_stage(
        msg, to_addr=FsmStage.INGRESS, from_stage=stage,
        history=body, system=body, settings=config,
    )
