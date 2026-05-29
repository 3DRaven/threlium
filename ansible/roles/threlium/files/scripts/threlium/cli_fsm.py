"""Парсинг и политика CLI FSM (cli_intent / cli_resume / cli_hitl_out)."""
from __future__ import annotations

import json
import os
import re
import shlex

import msgspec

from threlium.settings import ThreliumSettings
from threlium.types import (
    CliIntentPayload,
    CliIntentPolicy,
)

_SHELL_OP_TOKENS = frozenset({"&&", "||", ";", "|"})
_SHELL_BINARIES = frozenset({"sh", "bash"})
_CHAIN_SPLIT_RE = re.compile(r"\s*(?:&&|\|\||;|\|)\s*")


def parse_json_loose(text: str) -> object:
    text = text.strip()
    m = re.search(r"\{[\s\S]*\}\s*$", text)
    if m:
        text = m.group(0)
    return json.loads(text)


def parse_cli_intent_payload(text: str) -> CliIntentPayload | None:
    """Ожидается {\"cli\": {\"argv\": [str, ...], \"cwd\": str?}}."""
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
    try:
        return msgspec.convert(
            {"argv": argv, "cwd": cwd_norm},
            type=CliIntentPayload,
        )
    except msgspec.ValidationError:
        return None


def cli_payload_as_json(cli: CliIntentPayload) -> str:
    inner: dict[str, object] = {"argv": cli.argv}
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


def _deny_substrings(settings: ThreliumSettings) -> tuple[str, ...]:
    """Подстроки, запрещённые в командной строке (subshell / command substitution).

    Цепочки ``&&``, ``;``, ``|`` разрешены; блокируются ``$(``, backticks и переводы строк.
    Дополнительные паттерны — ``settings.cli.deny_patterns`` (через запятую).
    """
    raw = settings.cli.deny_patterns.strip()
    if raw:
        return tuple(s.strip() for s in raw.split(",") if s.strip())
    return ("`", "$(", "${", "\n", "\r")


def _allowlist_basenames(settings: ThreliumSettings) -> set[str]:
    raw = settings.cli.allowlist
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def _binaries_in_shell_line(line: str) -> list[str]:
    """Имена бинарников в каждом сегменте shell-цепочки."""
    out: list[str] = []
    for part in _CHAIN_SPLIT_RE.split(line.strip()):
        part = part.strip()
        if not part:
            continue
        try:
            tokens = shlex.split(part)
        except ValueError:
            return []
        if tokens:
            out.append(os.path.basename(tokens[0]).lower())
    return out


def _policy_for_shell_line(line: str, settings: ThreliumSettings) -> CliIntentPolicy:
    for sub in _deny_substrings(settings):
        if sub in line:
            return CliIntentPolicy.DENY
    bins = _binaries_in_shell_line(line)
    if not bins:
        return CliIntentPolicy.HITL
    allow = _allowlist_basenames(settings)
    if all(b in allow for b in bins):
        return CliIntentPolicy.ALLOW
    return CliIntentPolicy.HITL


def classify_cli_policy(cli: CliIntentPayload, settings: ThreliumSettings) -> CliIntentPolicy:
    """Политика исполнения CLI: ``allow`` | ``deny`` | ``hitl``.

    Одиночная команда: ``argv[0]`` в allowlist → ``allow``, иначе ``hitl``.
    Цепочка (``&&``, ``;``, ``|`` в argv или ``sh -c``): все сегменты в allowlist → ``allow``.
    Subshell / ``$(`` / backticks → ``deny``.
    """
    argv = cli.argv
    if argv_is_sh_c_wrapper(argv) or argv_uses_shell_chaining(argv):
        return _policy_for_shell_line(shell_command_line_for_argv(argv), settings)

    joined = " ".join(argv)
    for sub in _deny_substrings(settings):
        if sub in joined:
            return CliIntentPolicy.DENY
    base = os.path.basename(argv[0].strip() or " ").lower()
    if base in _allowlist_basenames(settings):
        return CliIntentPolicy.ALLOW
    return CliIntentPolicy.HITL


def parse_yes_no(text: str) -> bool | None:
    """True = да, False = нет, None = неоднозначно (обрабатываем как отказ)."""
    line = text.strip().split("\n", 1)[0].strip().lower()
    if re.match(r"^(yes|y|да|д)\s*\.?$", line):
        return True
    if re.match(r"^(no|n|нет|н)\s*\.?$", line):
        return False
    return None
