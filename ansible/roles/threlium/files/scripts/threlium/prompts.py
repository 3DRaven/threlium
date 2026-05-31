"""Единый Jinja2-рендерер шаблонов писем (`docs/INDEX.md` §7.3 / §8 / §5b.3).

Все user/LLM-видимые тексты, FSM wire-format маркеры и шаблонные тела
писем живут в раскладке по стадиям FSM
``$THRELIUM_HOME/prompts/<stage>/<purpose>.j2`` (например,
``ingress/orphan_notice.j2``, ``reasoning/<route>/tool_spec.j2``).
Cross-cutting артефакты, не привязанные ни к
одной стадии (например, ``lightrag/ingest_body.j2``), лежат под ``prompts/lightrag/``.
Стадии (`states/*.py`), раннеры (`runners/*.py`) и мосты (`bridges/*.py`)
вызывают :func:`render_prompt`,
передавая только :class:`~threlium.types.prompt_path.PromptPath`.

Пакетные агрегирующие модули FSM не импортируются; путь к каталогу промптов
задаётся через ``init_prompts_root(home)`` из ``ThreliumSettings.home``.

Штатный ``jinja2.filters.do_tojson`` после ``json.dumps`` **всегда** вызывает
``htmlsafe_json_dumps`` (экран ``<`` → ``\\u003c`` и т.д.; см. исходники Jinja2).
Политики ``Environment.policies`` (``json.dumps_function``, ``json.dumps_kwargs``)
настраивают только сам ``json.dumps`` — без замены фильтра HTML-шум в
``text/plain`` не убрать. Здесь политики выставляются как в документации Jinja2,
а фильтр ``tojson`` подменяется на вызов ``dumps_function`` + ``dumps_kwargs``
без ``htmlsafe_json_dumps``.
"""
from __future__ import annotations

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from threlium.types.prompt_path import PromptPath

_PROMPTS_ROOT: Path | None = None
_PROMPTS_ENV: Environment | None = None


def _plain_text_tojson(value: object, indent: int | None = None) -> str:
    """``json.dumps`` по ``Environment.policies``, без ``htmlsafe_json_dumps``."""
    env = _PROMPTS_ENV
    if env is None:
        raise RuntimeError("prompts: Environment not initialized for tojson")
    dumps_fn = env.policies["json.dumps_function"] or json.dumps
    kwargs: dict[str, object] = dict(env.policies["json.dumps_kwargs"])
    if indent is not None:
        kwargs = dict(kwargs)
        kwargs["indent"] = indent
    return dumps_fn(value, **kwargs)


def _history_text(msg: object) -> str:
    """Jinja-фильтр: содержательное тело письма = конкатенация его ``<history>``-частей.

    После унификации тело контекста живёт в ``<history>``-частях (а не в первом попавшемся
    text/plain, которым может оказаться ``<system>``-payload). Граница чтения — VO-хелперы
    ``mime_reform``; импорт ленивый, чтобы не тянуть MIME-слой в низкоуровневый рендерер.
    """
    from email.message import EmailMessage

    from threlium.mime_reform import history_part_text, iter_history_parts

    if not isinstance(msg, EmailMessage):
        return ""
    chunks = [
        text
        for _cid, part in iter_history_parts(msg)
        if (text := history_part_text(part).strip())
    ]
    return "\n\n---\n\n".join(chunks)


def _last_history_text(msg: object) -> str:
    """Jinja: canonical user turn = последняя непустая ``<history>`` (ingress distill)."""
    from email.message import EmailMessage

    from threlium.mime_reform import last_history_part_text

    if not isinstance(msg, EmailMessage):
        return ""
    return last_history_part_text(msg)


def init_prompts_root(home: Path) -> None:
    global _PROMPTS_ROOT, _PROMPTS_ENV
    _PROMPTS_ROOT = home / "prompts"
    _PROMPTS_ENV = None


def _prompts_env() -> Environment:
    global _PROMPTS_ENV
    if _PROMPTS_ENV is None:
        if _PROMPTS_ROOT is None:
            raise RuntimeError(
                "prompts: init_prompts_root(home) must be called before render_prompt()"
            )
        _PROMPTS_ENV = Environment(
            loader=FileSystemLoader(str(_PROMPTS_ROOT)),
            autoescape=select_autoescape(default=False),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )
        _kw: dict[str, object] = dict(_PROMPTS_ENV.policies["json.dumps_kwargs"])
        _kw["ensure_ascii"] = False
        _PROMPTS_ENV.policies["json.dumps_kwargs"] = _kw
        _PROMPTS_ENV.policies["json.dumps_function"] = json.dumps
        _PROMPTS_ENV.filters["tojson"] = _plain_text_tojson
        _PROMPTS_ENV.filters["history_text"] = _history_text
        _PROMPTS_ENV.filters["last_history_text"] = _last_history_text
    return _PROMPTS_ENV


def render_prompt(template_name: PromptPath, /, **vars: object) -> str:
    """Отрендерить шаблон из ``$THRELIUM_HOME/prompts/<template_name>``.

    Загрузчик кешируется на процесс; ``StrictUndefined`` ловит забытые
    переменные на этапе рендера. Один шаблон = один user-editable артефакт
    (тело письма, Subject, system-prompt LLM, EML-конверт моста и т.п.).
    """
    return _prompts_env().get_template(str(template_name)).render(**vars)


__all__ = ["PromptPath", "init_prompts_root", "render_prompt"]
