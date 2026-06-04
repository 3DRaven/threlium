"""memory_query@localhost → enrich_fast@localhost.

Направленный запрос к LightRAG (knowledge base / memory) с формулировкой модели.
Дешевле reflect (2 хопа vs 3), возвращает только ответ RAG без пересборки контекста.
"""
from email.message import EmailMessage

from threlium.fsm_emit_semantic import emit_to_enrich_fast
from threlium.knowledge_fsm import parse_memory_query_payload
from threlium.litellm_route_context import get_litellm_http_correlation
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt

from threlium.runners.lightrag.aquery import build_lightrag_query_param, run_lightrag_aquery
from threlium.settings import ThreliumSettings
from threlium.types import EnrichCalleeHistoryText, EnrichRequestEchoText, FsmStage, LitellmCallSite, PromptPath
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    payload = parse_memory_query_payload(system_part_text(msg))
    if payload is None:
        raise RuntimeError("memory_query: invalid payload")

    rag_correlation: dict[str, str] | None = None
    if config.e2e.litellm_route_correlation:
        snap = get_litellm_http_correlation()
        rag_correlation = dict(snap) if snap else None
        if rag_correlation is not None:
            rag_correlation[LitellmCorrelationHeader.CALL_SITE.value] = (
                LitellmCallSite.LIGHTRAG_QUERY.value
            )

    query_param = build_lightrag_query_param(config)
    result = run_lightrag_aquery(
        payload.query,
        settings=config,
        correlation=rag_correlation,
        param=query_param,
        query_api=config.lightrag.query_api,
    )

    answer = str(result) if result else ""

    observation = render_prompt(
        PromptPath.MEMORY_QUERY_OBSERVATION,
        reasoning=payload.reasoning,
        no_results=not answer.strip(),
        answer=answer,
    ).strip()

    # Callee владеет историей: в память едут ЗАПРОС (что искали: payload.query, эхо с
    # предштампом origin=reasoning) и ОТВЕТ (observation: RAG-результат, origin=memory_query).
    # Иначе сама формулировка запроса терялась бы из истории (reasoning шлёт её только в
    # <system>). Разные тела → разные <hash@history>.
    return emit_to_enrich_fast(
        msg,
        stage,
        history=EnrichCalleeHistoryText.parse(observation),
        request_echo=EnrichRequestEchoText.parse(payload.query),
        settings=config,
    )
