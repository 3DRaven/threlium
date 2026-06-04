#!/usr/bin/env python3
"""Собрать payload reasoning и вызвать vLLM напрямую (replay инцидента).

Извлечение «что думала модель» (Qwen3 + vLLM --reasoning-parser qwen3):
  * без stream: ``choices[0].message.reasoning`` (строка);
  * stream: чанки ``choices[0].delta.reasoning`` (SSE), затем ``delta.content`` / tool_calls.

Переменные:
  REPLAY_STREAM=1       — stream=true, писать reasoning по ходу в *.reasoning.stream.txt
  REPLAY_SKIP_CURL=1    — только собрать payload
  REPLAY_MID=...        — Message-ID письма в reasoning Maildir
  VLLM_URL, REPLAY_*    — пути и таймаут
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_REPO = Path(os.environ.get("THRELIUM_REPO", "/home/threlium/threlium/agent"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from threlium.mail import parse_rfc822
from threlium.mail import canonicalize_mime
from threlium.prompts import init_prompts_root, render_prompt
from threlium.settings import load_settings, resolve_llm_endpoint
from threlium.states.reasoning import _render_user_prompt
from threlium.states.reasoning_tool_spec import load_tools_for_routes
from threlium.types import (
    HopBudgetLine,
    LitellmRoutingSite,
    MailHeaderName,
    PromptPath,
    REASONING_TARGET_STAGES,
)

_HDR = MailHeaderName
MID = os.environ.get(
    "REPLAY_MID",
    "2SBBidg4Ln3EgIhpJmbWLTqAq2gxsKl8HATUvnEoAEHjxc6Wcdxk2IlFySDv6oeP2j8Z9FSoB6TEfrD3xGvlfdYhf9dzkXyn@localhost",
)
OUT_PAYLOAD = Path(os.environ.get("REPLAY_PAYLOAD", "/tmp/reasoning_replay_payload.json"))
OUT_RESPONSE = Path(os.environ.get("REPLAY_RESPONSE", "/tmp/reasoning_replay_response.json"))
VLLM_URL = os.environ.get("VLLM_URL", "http://127.0.0.1:8000/v1/chat/completions")
API_KEY = os.environ.get("VLLM_API_KEY", "sk-dummy")
CURL_TIMEOUT = int(os.environ.get("REPLAY_CURL_TIMEOUT", "900"))
USE_STREAM = os.environ.get("REPLAY_STREAM", "").strip().lower() in ("1", "true", "yes")
# По умолчанию tool spec собираются в скрипте (без Jinja, без maxLength).
USE_BUILTIN_REPLAY_TOOLS = os.environ.get(
    "REPLAY_USE_TEMPLATE_TOOLS", ""
).strip().lower() not in ("1", "true", "yes")
# Путь к vllm-start.sh (рядом с compose на spark): --max-model-len=...
_DEFAULT_VLLM_START_SH = Path.home() / "vllm/qwen-3.6-35b/vllm-start.sh"


def _str_prop(description: str, *, min_length: int = 1) -> dict[str, object]:
    """Строковое поле tool schema без maxLength (только replay)."""
    return {"type": "string", "minLength": min_length, "description": description}


def build_replay_tool_specs() -> tuple[list[dict[str, object]], dict[str, dict[str, object]]]:
    """Tool spec для replay: описания в коде, без лимитов длины в schema.

    Семантика маршрутов совпадает с ``ROUTE_TO_ADDRESS`` / прод FSM, но текст
    не копируется из ``tool_spec.j2`` и не упоминает числовые потолки.
    """
    specs: list[dict[str, object]] = [
        {
            "type": "function",
            "function": {
                "name": "egress_router",
                "description": (
                    "Отправить готовый ответ пользователю во внешний канал (email, Matrix, "
                    "Telegram). Выбирай, когда ответ можно отдать как есть: не нужен CLI, "
                    "ещё один цикл enrich/reasoning или подагент. Если пользователь просит "
                    "показать полный JSON или большой фрагмент контекста — включи его в body "
                    "дословно, без плейсхолдеров вроде «{ ... }» и без сокращений."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "subject": _str_prop(
                            "Тема исходящего письма пользователю. Короткая одна строка, "
                            "без перевода строк; обычно Re: к текущему треду."
                        ),
                        "body": _str_prop(
                            "Полный текст ответа пользователю в UTF-8. Сюда входят результаты "
                            "команд, структурированные данные (в том числе JSON из enrich/LightRAG), "
                            "пояснения. Передавай фактическое содержимое целиком, если его "
                            "запросили для проверки структуры."
                        ),
                    },
                    "required": ["subject", "body"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "cli_intent",
                "description": (
                    "Запустить shell-команду через CLI-конвейер Threlium (политика "
                    "allow/deny/HITL на стороне cli_intent). Только когда нужен внешний "
                    "побочный эффект, который нельзя получить из памяти или текущего контекста."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "argv": {
                            "type": "array",
                            "items": _str_prop("Один элемент argv (без shell-эскейпа)."),
                            "minItems": 1,
                            "description": (
                                "Команда как argv: argv[0] — имя программы, далее аргументы."
                            ),
                        },
                        "cwd": {
                            "type": "string",
                            "description": "Рабочая директория для запуска (опционально).",
                        },
                        "reasoning": _str_prop(
                            "Краткое объяснение, зачем нужна эта команда (для логов и HITL)."
                        ),
                    },
                    "required": ["argv", "reasoning"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "thread_memory",
                "description": (
                    "Сохранить заметку в память текущего диалогового треда (LightRAG). "
                    "Для фактов и решений, важных в этом треде, не для глобальных знаний."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "note": _str_prop(
                            "Заметка в третьем лице, самодостаточная, без «я/мы» — её "
                            "проиндексирует LightRAG для этого треда."
                        ),
                    },
                    "required": ["note"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "global_memory",
                "description": (
                    "Сохранить заметку в глобальную память агента (между тредами). "
                    "Для устойчивых фактов о среде, политиках, предпочтениях пользователя."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "note": _str_prop(
                            "Глобальная заметка в третьем лице, самодостаточная."
                        ),
                    },
                    "required": ["note"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "subagent_intent",
                "description": (
                    "Делегировать подзадачу подагенту с отдельным hop-budget. Когда нужен "
                    "независимый reasoning-цикл, а не один CLI-вызов."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task": _str_prop(
                            "Формулировка задачи: цель, критерии успеха, ограничения. "
                            "Подагент не видит контекст родителя."
                        ),
                    },
                    "required": ["task"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "reflect",
                "description": (
                    "Запросить ещё один цикл enrich → reasoning с обновлённым LightRAG. "
                    "Стоит ~3 hop. Не дублируй транскрипт — нужны summary и явный запрос, "
                    "что уточнить в графе знаний."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "summary": _str_prop(
                            "Кратко: что уже выяснено и к какому выводу идём."
                        ),
                        "clarification_request": _str_prop(
                            "Что именно должно появиться в следующем LightRAG-контексте: "
                            "вопрос, гипотеза, недостающие сущности/связи."
                        ),
                    },
                    "required": ["summary", "clarification_request"],
                    "additionalProperties": False,
                },
            },
        },
    ]
    schemas: dict[str, dict[str, object]] = {}
    for spec in specs:
        fn = spec["function"]
        assert isinstance(fn, dict)
        name = fn["name"]
        params = fn["parameters"]
        assert isinstance(params, dict)
        schemas[str(name)] = params
    return specs, schemas


def _load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k, v)


def _find_mail(mid: str) -> Path:
    env = os.environ.copy()
    env.setdefault("HOME", str(Path.home()))
    env.setdefault("NOTMUCH_CONFIG", str(Path.home() / ".notmuch-config"))
    out = subprocess.check_output(
        ["notmuch", "search", "--output=files", f"id:{mid}"],
        env=env,
        text=True,
    ).strip()
    if not out:
        raise SystemExit(f"notmuch: no message for {mid!r}")
    return Path(out.splitlines()[0])


def _reasoning_text(msg: dict[str, Any]) -> str:
    """vLLM Qwen3: ``reasoning``; иные провайдеры — ``reasoning_content``."""
    for key in ("reasoning", "reasoning_content"):
        v = msg.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def _summarize_response(resp: dict[str, Any]) -> dict[str, Any]:
    choice = resp["choices"][0]
    msg_o = choice["message"]
    tc = (msg_o.get("tool_calls") or [None])[0]
    reasoning = _reasoning_text(msg_o)
    body_parsed: str | None = None
    if tc:
        try:
            args = json.loads(tc["function"]["arguments"])
            body_parsed = args.get("body") if isinstance(args, dict) else None
        except json.JSONDecodeError:
            body_parsed = None
    return {
        "finish_reason": choice.get("finish_reason"),
        "usage": resp.get("usage"),
        "tool_name": tc["function"]["name"] if tc else None,
        "arguments_len": len(tc["function"]["arguments"]) if tc else 0,
        "arguments_preview": (tc["function"]["arguments"][:500] if tc else None),
        "arguments_tail": (tc["function"]["arguments"][-300:] if tc else None),
        "body_len": len(body_parsed) if isinstance(body_parsed, str) else None,
        "body_full": body_parsed,
        "reasoning_len": len(reasoning),
        "reasoning_preview": reasoning[:400] if reasoning else None,
        "reasoning_tail": reasoning[-400:] if reasoning else None,
        "content_len": len(msg_o.get("content") or ""),
    }


def _call_vllm_curl(bare: Path) -> None:
    cmd = [
        "curl",
        "-sS",
        "-m",
        str(CURL_TIMEOUT),
        VLLM_URL,
        "-H",
        f"Authorization: Bearer {API_KEY}",
        "-H",
        "Content-Type: application/json",
        "-d",
        f"@{bare}",
        "-o",
        str(OUT_RESPONSE),
        "-w",
        "\nhttp_code=%{http_code} time_total=%{time_total}\n",
    ]
    print("curl:", " ".join(cmd), file=sys.stderr)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    print(proc.stdout, file=sys.stderr)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(proc.returncode)


def _call_vllm_stream(payload: dict[str, object]) -> dict[str, Any]:
    """POST stream=true; собрать reasoning/content; вернуть псевдо-completion для summary."""
    body = {**payload, "stream": True}
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        VLLM_URL,
        data=data,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    reasoning_parts: list[str] = []
    content_parts: list[str] = []
    tool_arg_parts: list[str] = []
    finish_reason: str | None = None
    stream_log = OUT_RESPONSE.with_suffix(".reasoning.stream.txt")
    stream_log.write_text("")

    print(f"stream → {stream_log}", file=sys.stderr)
    with urllib.request.urlopen(req, timeout=CURL_TIMEOUT) as resp:
        for raw_line in resp:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line.startswith("data:"):
                continue
            chunk_s = line[5:].strip()
            if chunk_s == "[DONE]":
                break
            chunk = json.loads(chunk_s)
            delta = chunk["choices"][0].get("delta") or {}
            if delta.get("reasoning"):
                reasoning_parts.append(str(delta["reasoning"]))
                with stream_log.open("a", encoding="utf-8") as f:
                    f.write(str(delta["reasoning"]))
            if delta.get("content"):
                content_parts.append(str(delta["content"]))
            # tool_calls могут приходить по частям в stream
            tcs = delta.get("tool_calls")
            if isinstance(tcs, list):
                for tc in tcs:
                    fn = (tc or {}).get("function") or {}
                    if fn.get("arguments"):
                        tool_arg_parts.append(str(fn["arguments"]))
            fr = chunk["choices"][0].get("finish_reason")
            if fr:
                finish_reason = str(fr)

    reasoning = "".join(reasoning_parts)
    reasoning_path = OUT_RESPONSE.with_suffix(".reasoning.txt")
    reasoning_path.write_text(reasoning, encoding="utf-8")
    print(f"reasoning saved: {reasoning_path} ({len(reasoning)} chars)", file=sys.stderr)

    tool_args = "".join(tool_arg_parts)
    assembled: dict[str, Any] = {
        "choices": [
            {
                "finish_reason": finish_reason or "stop",
                "message": {
                    "role": "assistant",
                    "content": "".join(content_parts),
                    "reasoning": reasoning,
                    "tool_calls": (
                        [
                            {
                                "function": {
                                    "name": "egress_router",
                                    "arguments": tool_args,
                                }
                            }
                        ]
                        if tool_args
                        else None
                    ),
                },
            }
        ],
        "usage": None,
        "_stream_note": "usage omitted in stream mode; see stream log",
    }
    OUT_RESPONSE.write_text(json.dumps(assembled, ensure_ascii=False, indent=2))
    return assembled


def main() -> None:
    _load_env_file(_REPO / "env" / "threlium.env")
    os.chdir(_REPO)
    cfg = load_settings()
    init_prompts_root(cfg.home)

    path = _find_mail(MID)
    raw = path.read_bytes()
    msg = canonicalize_mime(parse_rfc822(raw))
    hop = HopBudgetLine.parse(msg.get(_HDR.HOP_BUDGET))
    ep = resolve_llm_endpoint(cfg.litellm, LitellmRoutingSite.REASONING)
    if USE_BUILTIN_REPLAY_TOOLS:
        tools, _schemas = build_replay_tool_specs()
        tool_spec_source = "builtin_replay_no_max_length"
    else:
        routes = sorted(REASONING_TARGET_STAGES, key=lambda s: s.value)
        tools, _schemas = load_tools_for_routes(routes)
        tool_spec_source = "template_j2"
    system = render_prompt(PromptPath.REASONING_SYSTEM).strip()
    # replay: контекст как в проде — из MIME-частей enrich (без отдельного body trim)
    user_content = _render_user_prompt(msg, hop)

    model = ep.model
    if model.startswith("openai/"):
        model = model.split("/", 1)[1]

    payload: dict[str, object] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_content},
        ],
        "tools": tools,
        "tool_choice": "required",
    }
    if ep.chat_template_kwargs:
        payload["chat_template_kwargs"] = ep.chat_template_kwargs

    meta = {
        "mail_path": str(path),
        "user_content_len": len(user_content),
        "model": model,
        "api_base": ep.api_base,
        "chat_template_kwargs": ep.chat_template_kwargs,
        "n_tools": len(tools),
        "stream": USE_STREAM,
        "tool_spec_source": tool_spec_source,
    }
    OUT_PAYLOAD.write_text(
        json.dumps({"meta": meta, "payload": payload}, ensure_ascii=False, indent=2)
    )
    print(json.dumps(meta, indent=2), file=sys.stderr)

    bare = OUT_PAYLOAD.parent / (OUT_PAYLOAD.stem + "_bare.json")
    bare.write_text(json.dumps(payload, ensure_ascii=False))

    if os.environ.get("REPLAY_SKIP_CURL", "").strip() in ("1", "true", "yes"):
        print(f"payload written: {bare}", file=sys.stderr)
        return

    if USE_STREAM:
        try:
            resp = _call_vllm_stream(payload)
        except urllib.error.URLError as e:
            raise SystemExit(f"stream request failed: {e}") from e
    else:
        _call_vllm_curl(bare)
        if not OUT_RESPONSE.is_file():
            raise SystemExit("no response file")
        resp = json.loads(OUT_RESPONSE.read_text())
        reasoning = _reasoning_text(resp["choices"][0]["message"])
        if reasoning:
            rp = OUT_RESPONSE.with_suffix(".reasoning.txt")
            rp.write_text(reasoning, encoding="utf-8")
            print(f"reasoning saved: {rp} ({len(reasoning)} chars)", file=sys.stderr)

    summary = _summarize_response(resp)
    summary_path = OUT_RESPONSE.with_suffix(".summary.json")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
