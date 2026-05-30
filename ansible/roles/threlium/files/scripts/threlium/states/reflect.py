#!/usr/bin/env python3
"""reflect@localhost → ingress@localhost (docs/MEMORY_TABLE.md §3).

Tool «думать ещё один цикл»: рендерит Jinja2-шаблон из подкаталога
``$THRELIUM_HOME/prompts/reflect/`` (``continue.j2`` пока бюджета хватает на
ещё один цикл ``reflect → ingress → enrich → reasoning``, иначе ``final.j2``)
и отправляет результирующий text/plain в ``ingress@localhost``. Бюджет
вычисляется по хвосту ``X-Threlium-Hop-Budget`` (см.
``threlium.fsm_emit`` / hop-токены (``N`` → ``N-0``, ``N-k`` → остаток
``N - k``).
"""
from __future__ import annotations

import re
from email.message import EmailMessage

from threlium.fsm_emit import HDR_HOP_BUDGET, build_fsm_plain_to_stage
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    HopBudgetLine,
    MailHeaderName,
    PromptPath,
    ReflectJinjaSubjectContext,
)

_HDR = MailHeaderName

log = logger.bind(stage="reflect")

CYCLE_COST = 3
SAFETY_MARGIN = 1


def _remaining_hops(line: HopBudgetLine) -> int:
    """Остаток последнего токена ``X-Threlium-Hop-Budget``: ``N`` → ``N``;
    ``N-k`` → ``max(0, N-k)``. Пустой/нераспознанный токен → 0 (терминируем
    через ``reflect/final.j2``).
    """
    s = line.value
    if not s:
        return 0
    last = s.split()[-1]
    if re.fullmatch(r"\d+", last):
        return int(last)
    m = re.fullmatch(r"(\d+)-(\d+)", last)
    if not m:
        return 0
    return max(0, int(m.group(1)) - int(m.group(2)))


def _select_template(remaining: int) -> PromptPath:
    return (
        PromptPath.REFLECT_CONTINUE
        if remaining >= CYCLE_COST + SAFETY_MARGIN
        else PromptPath.REFLECT_FINAL
    )


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    raw_budget = HopBudgetLine.parse_present_from_email(msg, HDR_HOP_BUDGET)
    line = raw_budget or HopBudgetLine.parse(None)
    remaining = _remaining_hops(line)
    template = _select_template(remaining)
    subj_ctx = ReflectJinjaSubjectContext.parse_present_from_email(msg, _HDR.SUBJECT)
    subj_vo = subj_ctx or ReflectJinjaSubjectContext.parse(None)
    body = render_prompt(
        template,
        remaining_hops=remaining,
        cycle_cost=CYCLE_COST,
        previous_reasoning=system_part_text(msg),
        subject=subj_vo.value,
    )
    log.info("selected_template", template=str(template), remaining=remaining)
    return build_fsm_plain_to_stage(
        msg,
        to_addr=FsmStage.INGRESS,
        from_stage=stage,
        body=FsmTransitionPlainBody.parse(body),
        settings=config,
    )
