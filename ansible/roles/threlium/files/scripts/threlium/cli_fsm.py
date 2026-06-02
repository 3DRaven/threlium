"""Парсинг и политика CLI FSM (cli_intent / cli_resume / cli_hitl_out).

Вход ``parse_cli_intent_payload`` — строго тело ``<system>``-части письма
``reasoning → cli_intent`` (канон ``{"cli": {...}}`` из ``cli_intent/email_body.j2 | tojson``),
а не «текст с мусором»: разбор через ``msgspec.json.decode`` без salvage-regex
(``docs/CONTEXT_CONTRACT.md`` §2). Невалидный JSON / схема → ``None``.
"""
from __future__ import annotations

import json
import os
import re
import shlex

import msgspec

from threlium.types import (
    CliExecDecision,
    CliIntentDecision,
    CliIntentEnvelope,
    CliIntentPayload,
    CliIntentPolicy,
    CliRouteCollision,
    FsmStage,
    REASONING_TARGET_STAGES,
)

_SHELL_OP_TOKENS = frozenset({"&&", "||", ";", "|"})
_SHELL_BINARIES = frozenset({"sh", "bash"})


def parse_cli_intent_payload(text: str) -> CliIntentPayload | None:
    """Строгий разбор тела ``<system>`` → ``CliIntentPayload``.

    Ожидается каноничный ``{"cli": {"argv": [str, ...], "cwd"?, "privileged"?}}``.
    Невалидный JSON, нарушение схемы или пустой ``argv`` → ``None`` (стадии трактуют как
    invalid intent → ``enrich_fast`` / ingress, см. вызывающие). Без regex-извлечения.
    """
    raw = text.strip()
    if not raw:
        return None
    try:
        env = msgspec.json.decode(raw.encode("utf-8"), type=CliIntentEnvelope)
    except (msgspec.DecodeError, msgspec.ValidationError):
        return None
    cli = env.cli
    if not cli.argv:
        return None
    cwd = cli.cwd.strip() if cli.cwd is not None else None
    if cwd == "":
        cwd = None
    if cwd == cli.cwd:
        return cli
    return CliIntentPayload(argv=cli.argv, cwd=cwd, privileged=cli.privileged)


def cli_payload_as_json(cli: CliIntentPayload) -> str:
    inner: dict[str, object] = {"argv": cli.argv, "privileged": cli.privileged}
    if cli.cwd:
        inner["cwd"] = cli.cwd
    return json.dumps({"cli": inner}, ensure_ascii=False)


def cli_command_line_for_intent(cli: CliIntentPayload) -> str:
    """Строка команды для HITL-вопроса и classify (shlex-quoted argv, optional cwd)."""
    line = " ".join(shlex.quote(a) for a in cli.argv)
    if cli.cwd:
        line = f"(cwd={shlex.quote(cli.cwd)}) {line}"
    return line


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


