"""LightRAG для ``threlium-engine``: один фоновый asyncio-loop + батч-индексация.

Индексация settled-сообщений (селектор ``SELECTOR``: без ``unread``, без
``lightrag_indexed``) планируется из FSM после ``nm_settle`` через
:func:`schedule_index_pending` **без** блокировки сокета. ``enrich`` дергает ``aquery``
через :func:`run_rag_coroutine` на том же инстансе. Все ``await`` к LightRAG —
только на выделенном loop (см. ``start_rag_loop_thread``), чтобы
``asyncio.Lock`` в ``lightrag.kg.shared_storage`` не привязывались к
разным event loop'ам потоков ``ThreadingUnixStreamServer``.

Письма в ``stages/archive/Maildir`` при insert получают ``+lightrag_indexed`` (fdm)
и не попадают в селектор pending. Остальные сообщения после ``await rag.ainsert(...)``
получают ``lightrag_indexed`` через :func:`threlium.nm.batch_tag_add`.
"""
from threlium.runners.lightrag._lifecycle import (
    daemon_lightrag,
    run_rag_coroutine,
    schedule_bootstrap_knowledge,
    schedule_index_pending,
    start_rag_loop_thread,
    stop_rag_loop_thread,
)
from threlium.runners.lightrag.aquery import (
    build_lightrag_query_param,
    run_lightrag_aquery,
)

__all__ = [
    "build_lightrag_query_param",
    "daemon_lightrag",
    "run_lightrag_aquery",
    "run_rag_coroutine",
    "schedule_bootstrap_knowledge",
    "schedule_index_pending",
    "start_rag_loop_thread",
    "stop_rag_loop_thread",
]
