#!/usr/bin/env python3
"""thread_memory@localhost → enrich_fast@localhost (docs/MEMORY_TABLE.md §1).

Записывает note в Maildir (durable, settled при fdm insert) и передаёт его в
enrich_fast как ЗАПРОС-эхо ``<hash@history>`` для мгновенного отражения в контексте
reasoning. Для памяти ценен именно запрос: само письмо-запрос и есть то, что агент
решил запомнить, поэтому origin предзаштампован = ``reasoning`` (автор факта), а
отдельный «recorded»-ответ в историю не идёт. Полная RAG-индексация — async,
доступна на следующем полном enrich-цикле.
"""
from email.message import EmailMessage

from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.mime_reform import system_part_text
from threlium.settings import ThreliumSettings
from threlium.types import FsmStage


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    note = system_part_text(msg).strip()
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=stage,
        request_echo=note,
        settings=config,
    )
