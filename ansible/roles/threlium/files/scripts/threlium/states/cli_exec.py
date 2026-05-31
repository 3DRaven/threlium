#!/usr/bin/env python3
"""cli_exec → enrich_fast@localhost: исполнение через transient ``systemd-run``.

SANDBOX (``privileged: false``): ``systemd-run --user --wait --pipe`` + ProtectSystem=strict, …
PRIVILEGED: ``systemd-run --wait --pipe --uid=0`` (Polkit на хосте при необходимости).
"""
import os
import subprocess
from email.message import EmailMessage

from threlium.cli_fsm import parse_cli_intent_payload, resolve_cli_exec_argv
from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import FsmStage, PromptPath

log = logger.bind(stage="cli_exec")


def _subprocess_env_for_systemd_user() -> dict[str, str]:
    """Ensure ``XDG_RUNTIME_DIR`` for ``systemd-run --user`` (user session bus)."""
    env = dict(os.environ)
    if env.get("XDG_RUNTIME_DIR"):
        return env
    try:
        uid = os.getuid()
    except AttributeError:
        return env
    runtime = f"/run/user/{uid}"
    if os.path.isdir(runtime):
        env["XDG_RUNTIME_DIR"] = runtime
    return env


def _build_scope_cmd(
    exec_argv: list[str],
    config: ThreliumSettings,
    *,
    privileged: bool,
) -> list[str]:
    props = [
        f"--property=MemoryMax={config.cli.exec_memory_max}",
        f"--property=CPUQuota={config.cli.exec_cpu_quota}",
        f"--property=TasksMax={config.cli.exec_tasks_max}",
    ]
    if privileged:
        return [
            "systemd-run",
            "--wait",
            "--pipe",
            "--uid=0",
            *props,
            "--",
            *exec_argv,
        ]
    sandbox_props = [
        "--property=ProtectSystem=strict",
        "--property=ProtectHome=read-only",
    ]
    for rw in config.cli.sandbox_read_write_paths.split(","):
        path = rw.strip()
        if path:
            sandbox_props.append(f"--property=ReadWritePaths={path}")
    if config.cli.sandbox_private_network:
        sandbox_props.append("--property=PrivateNetwork=yes")
    return [
        "systemd-run",
        "--user",
        "--wait",
        "--pipe",
        "--quiet",
        *sandbox_props,
        *props,
        "--",
        *exec_argv,
    ]


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    prior = system_part_text(msg).strip()
    cli = parse_cli_intent_payload(prior)

    if not cli:
        log.warning("no_parseable_payload")
        body = render_prompt(PromptPath.CLI_EXEC_OBSERVATION, cmd_line="", prior=prior)
        return build_fsm_step_to_stage(
            msg,
            to_addr=FsmStage.ENRICH_FAST,
            from_stage=stage,
            history=body,
            system=body,
            settings=config,
        )

    privileged = cli.privileged
    mode = "privileged" if privileged else "sandbox"
    exec_argv = resolve_cli_exec_argv(cli.argv)
    cmd_line = " ".join(exec_argv)
    log.info(
        "executing",
        mode=mode,
        privileged=privileged,
        cmd_line=cmd_line,
    )

    scope_cmd = _build_scope_cmd(exec_argv, config, privileged=privileged)

    try:
        result = subprocess.run(
            scope_cmd,
            capture_output=True,
            timeout=config.cli.exec_timeout,
            text=True,
            cwd=cli.cwd or None,
            env=_subprocess_env_for_systemd_user(),
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
        mode=mode,
        privileged=privileged,
    )
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=stage,
        history=body,
        system=body,
        settings=config,
    )
