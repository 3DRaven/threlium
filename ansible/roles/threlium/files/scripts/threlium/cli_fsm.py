"""Парсинг и политика CLI FSM (cli_intent / cli_resume / cli_hitl_out)."""
from __future__ import annotations

import json
import os
import re
import shlex

import msgspec

from threlium.types import (
    CliExecDecision,
    CliIntentDecision,
    CliIntentPayload,
    CliIntentPolicy,
    CliRouteCollision,
    FsmStage,
    REASONING_TARGET_STAGES,
)

_SHELL_OP_TOKENS = frozenset({"&&", "||", ";", "|"})
_SHELL_BINARIES = frozenset({"sh", "bash"})


def parse_json_loose(text: str) -> object:
    text = text.strip()
    m = re.search(r"\{[\s\S]*\}\s*$", text)
    if m:
        text = m.group(0)
    return json.loads(text)


def parse_cli_intent_payload(text: str) -> CliIntentPayload | None:
    """Ожидается {\"cli\": {\"argv\": [str, ...], \"cwd\"?, \"privileged\"?}}."""
    obj = parse_json_loose(text)
    if not isinstance(obj, dict):
        return None
    cli = obj.get("cli")
    if not isinstance(cli, dict):
        return None
    argv = cli.get("argv")
    if not isinstance(argv, list) or not argv:
        return None
    if not all(isinstance(x, str) for x in argv):
        return None
    cwd_raw = cli.get("cwd")
    cwd_norm: str | None
    if cwd_raw is None:
        cwd_norm = None
    elif isinstance(cwd_raw, str):
        t = cwd_raw.strip()
        cwd_norm = t if t else None
    else:
        return None
    priv_raw = cli.get("privileged", False)
    if isinstance(priv_raw, bool):
        privileged = priv_raw
    else:
        return None
    try:
        return msgspec.convert(
            {"argv": argv, "cwd": cwd_norm, "privileged": privileged},
            type=CliIntentPayload,
        )
    except msgspec.ValidationError:
        return None


def cli_payload_as_json(cli: CliIntentPayload) -> str:
    inner: dict[str, object] = {"argv": cli.argv, "privileged": cli.privileged}
    if cli.cwd:
        inner["cwd"] = cli.cwd
    return json.dumps({"cli": inner}, ensure_ascii=False)


def argv_to_shell_line(argv: list[str]) -> str:
    """Склейка argv в строку для ``sh -c`` (операторы без кавычек)."""
    parts: list[str] = []
    for tok in argv:
        if tok in _SHELL_OP_TOKENS:
            parts.append(tok)
        else:
            parts.append(shlex.quote(tok))
    return " ".join(parts)


def argv_is_sh_c_wrapper(argv: list[str]) -> bool:
    if len(argv) < 3:
        return False
    base = os.path.basename(argv[0].strip() or " ").lower()
    return base in _SHELL_BINARIES and argv[1] == "-c"


def argv_uses_shell_chaining(argv: list[str]) -> bool:
    if any(t in _SHELL_OP_TOKENS for t in argv):
        return True
    joined = " ".join(argv)
    return bool(re.search(r"\s(&&|\|\||\|)\s", joined))


def shell_command_line_for_argv(argv: list[str]) -> str:
    """Строка для ``sh -c`` (явная обёртка или склейка argv с операторами)."""
    if argv_is_sh_c_wrapper(argv):
        return argv[2]
    if argv_uses_shell_chaining(argv):
        return argv_to_shell_line(argv)
    return shlex.join(argv)


def resolve_cli_exec_argv(argv: list[str]) -> list[str]:
    """Argv для ``systemd-run -- …`` (прямой exec или ``sh -c`` при цепочке)."""
    if argv_is_sh_c_wrapper(argv):
        base = os.path.basename(argv[0].strip() or " ").lower()
        shell = base if base in _SHELL_BINARIES else "sh"
        return [shell, "-c", argv[2]]
    if argv_uses_shell_chaining(argv):
        return ["sh", "-c", argv_to_shell_line(argv)]
    return list(argv)


def cli_argv_route_collision(argv: list[str]) -> FsmStage | None:
    """Имя FSM-маршрута в позиции ``argv[0]`` → коллизия (не ``sh -c`` / цепочка).

    Reasoning-маршруты — tools, не CLI-бинари. ``rg memory_query …`` не ловится.
    """
    if argv_is_sh_c_wrapper(argv) or argv_uses_shell_chaining(argv):
        return None
    route_by_name = {s.value: s for s in REASONING_TARGET_STAGES}
    base = os.path.basename(argv[0].strip() or " ").lower()
    return route_by_name.get(base)


def classify_cli_intent(cli: CliIntentPayload) -> CliIntentDecision:
    """Единая граница-фабрика решения роутера ``cli_intent``.

    Сперва — коллизия имени маршрута (семантический misroute), иначе sandbox/privileged.
    """
    collision = cli_argv_route_collision(cli.argv)
    if collision is not None:
        return CliRouteCollision(
            route=collision, cmd=shell_command_line_for_argv(cli.argv)
        )
    return CliExecDecision(policy=classify_cli_policy(cli))


def classify_cli_policy(cli: CliIntentPayload) -> CliIntentPolicy:
    """``privileged`` в payload → system scope; иначе sandbox (user scope + ProtectSystem)."""
    if cli.privileged:
        return CliIntentPolicy.PRIVILEGED
    return CliIntentPolicy.SANDBOX


