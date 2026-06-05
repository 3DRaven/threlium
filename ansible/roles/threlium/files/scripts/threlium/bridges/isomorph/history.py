"""Разбор присланной истории клиента: ``In-Reply-To`` из водяного знака + хвост (чистый compute).

Структурный разбор ``messages`` (НЕ поиск подстроки):
  1. найти последний ``role=="assistant"`` (= прошлый ответ Threlium); ассистента нет → первый ход (orphan);
  2. ``In-Reply-To`` = glue-MID, ЗАКОДИРОВАННЫЙ egress'ом в невидимый водяной знак этого ответа
     (`decode_glue_snowflake` → snowflake → `snowflake_to_mid`). Знака нет → orphan. БЕЗ notmuch-чтения и
     БЕЗ голосования: знак несёт ТОЧНЫЙ MID хвоста, он и есть IRT нового сообщения (docs/THREAD_MODEL §isomorph);
  3. хвост = ВСЁ присланное после последнего якоря-ассистента (system + user'ы + tool-результаты), слитое в
     один ``<system>``-body. Якоря — только наши ответы; ничего из клиентского не отбрасываем. Первый ход
     (нет ассистента) → сливается вся история (включая Anthropic top-level ``system``).

Финальный ``Message-ID`` нового ingress (``hash(parent=IRT, tail)``) — :func:`ingress_message_id`.
"""
from __future__ import annotations

import msgspec

from threlium.types import (
    IsomorphApiSurface,
    IsomorphContentHashWire,
    IsomorphContentId,
    RfcMessageIdWire,
)

from .snowflake_mid import decode_glue_snowflake, snowflake_to_mid


class ParsedHistory(msgspec.Struct, frozen=True):
    """Чистый разбор присланной истории (без notmuch): IRT из водяного знака + хвост."""

    #: ``In-Reply-To`` нового хода = glue-MID из водяного знака last-assistant; ``None`` ⟺ первый ход (orphan).
    in_reply_to: RfcMessageIdWire | None
    #: Plain-текст хвоста (после last-assistant) → ``<system>``-body для FSM.
    tail_body: str


class _Msg(msgspec.Struct, frozen=True):
    role: str
    #: Plain-рендер для FSM-body (любая роль); для assistant — текст (с возможным водяным знаком).
    render: str


def _coerce_text(content: object) -> str:
    """OpenAI/Anthropic ``content``: строка или массив блоков → плоский текст (водяной знак в тексте сохраняется)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                btype = block.get("type")
                if btype in ("text", "input_text", "output_text"):
                    parts.append(str(block.get("text", "")))
                elif btype == "tool_result":
                    parts.append(_coerce_text(block.get("content")))
        return "\n".join(p for p in parts if p)
    return str(content)


def _render_user_or_tool(role: str, m: dict[str, object]) -> str:
    text = _coerce_text(m.get("content")).strip()
    return f"[{role}] {text}" if text else ""


def _parse_messages(surface: IsomorphApiSurface, body: dict[str, object]) -> list[_Msg]:
    raw = body.get("messages")
    if not isinstance(raw, list):
        raise ValueError("isomorph: request body has no 'messages' array")
    out: list[_Msg] = []
    # Anthropic держит system отдельным top-level полем (НЕ в messages). Это такое же сообщение
    # клиента, как прочие, — вносим первым (до anchor'ов) → сольётся в хвост turn-1. OpenAI шлёт
    # system обычным элементом messages (role=="system") → попадает в общий проход ниже.
    if surface is IsomorphApiSurface.ANTHROPIC_MESSAGES:
        sys_text = _coerce_text(body.get("system")).strip()
        if sys_text:
            out.append(_Msg(role="system", render=f"[system] {sys_text}"))
    for m in raw:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role", "")).strip()
        if role == "assistant":
            # Якорь-ответ Threlium: текст (с невидимым водяным знаком glue) для декода IRT.
            out.append(_Msg(role=role, render=_coerce_text(m.get("content"))))
        else:
            out.append(_Msg(role=role, render=_render_user_or_tool(role, m)))
    return out


def _content_addressed_mid(content_hash: str) -> RfcMessageIdWire:
    return RfcMessageIdWire.from_native(IsomorphContentId(v=1, content_hash=content_hash))


def _render_body(tail_msgs: list[_Msg]) -> str:
    return "\n".join(m.render for m in tail_msgs if m.render).strip()


def parse_history(surface: IsomorphApiSurface, body: dict[str, object]) -> ParsedHistory:
    """Полная история клиента → ``In-Reply-To`` (из водяного знака last-assistant) + хвост. Чистый compute.

    Хвост = сообщения после последнего assistant (или вся история, если ассистента нет) — сливаются в один
    ``<system>``-body. IRT = glue-MID, который egress закодировал в невидимый знак последнего ответа; знака нет
    (или это первый ход) → ``None`` (orphan, новый тред).
    """
    msgs = _parse_messages(surface, body)

    last_assistant_idx = -1
    for i in range(len(msgs) - 1, -1, -1):
        if msgs[i].role == "assistant":
            last_assistant_idx = i
            break

    in_reply_to: RfcMessageIdWire | None = None
    if last_assistant_idx >= 0:
        sf = decode_glue_snowflake(msgs[last_assistant_idx].render)
        if sf is not None:
            in_reply_to = snowflake_to_mid(sf)

    return ParsedHistory(
        in_reply_to=in_reply_to,
        tail_body=_render_body(msgs[last_assistant_idx + 1:]),
    )


def ingress_message_id(*, parent_value: str, tail_body: str) -> RfcMessageIdWire:
    """Контент-адресуемый ``Message-ID`` нового ingress = ``hash(parent=IRT, tail)``.

    ``parent_value`` = resolved ``In-Reply-To`` (glue-MID из знака) или ``""`` для orphan. Идемпотентность
    ретраев + позиционная уникальность (тот же хвост под разным родителем → разные MID).
    """
    return _content_addressed_mid(
        IsomorphContentHashWire.from_ingress_tail(parent=parent_value, tail=tail_body).value
    )
