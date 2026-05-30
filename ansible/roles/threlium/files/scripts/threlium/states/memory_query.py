"""memory_query@localhost → enrich_fast@localhost.

Направленный запрос к LightRAG (knowledge base / memory) с формулировкой модели.
Дешевле reflect (2 хопа vs 3), возвращает только ответ RAG без пересборки контекста.
"""
from email.message import EmailMessage

from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.knowledge_fsm import parse_memory_query_payload
from threlium.litellm_route_context import get_litellm_http_correlation
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.runners.lightrag import daemon_lightrag, run_rag_coroutine
from threlium.settings import ThreliumSettings
from threlium.types import FsmStage, LitellmCallSite, PromptPath
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    payload = parse_memory_query_payload(system_part_text(msg))
    if payload is None:
        raise RuntimeError("memory_query: invalid payload")

    rag = daemon_lightrag()
    if rag is None:
        raise RuntimeError("memory_query: LightRAG not ready")

    rag_correlation: dict[str, str] | None = None
    if config.e2e.litellm_route_correlation:
        snap = get_litellm_http_correlation()
        rag_correlation = dict(snap) if snap else None
        if rag_correlation is not None:
            rag_correlation[LitellmCorrelationHeader.CALL_SITE.value] = (
                LitellmCallSite.LIGHTRAG_QUERY.value
            )

    from lightrag import QueryParam

    query_param = QueryParam(mode="hybrid", top_k=config.lightrag.query_top_k)
    result = run_rag_coroutine(
        rag.aquery(payload.query, param=query_param),
        settings=config,
        correlation=rag_correlation,
    )

    answer = str(result) if result else ""
    max_chars = config.knowledge.observation_max_chars
    truncated = answer[:max_chars] if answer else ""

    observation = render_prompt(
        PromptPath.MEMORY_QUERY_OBSERVATION,
        reasoning=payload.reasoning,
        no_results=not truncated,
        answer=truncated,
    ).strip()

    # Callee владеет историей: в память едут ЗАПРОС (что искали: payload.query, эхо с
    # предштампом origin=reasoning) и ОТВЕТ (observation: RAG-результат, origin=memory_query).
    # Иначе сама формулировка запроса терялась бы из истории (reasoning шлёт её только в
    # <system>). Разные тела → разные <hash@history>.
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=stage,
        history=observation,
        request_echo=payload.query,
        settings=config,
    )
