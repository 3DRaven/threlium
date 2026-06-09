"""RAG instance construction and e2e correlation bridge installation."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from lightrag import LightRAG
from lightrag.llm_roles import RoleLLMConfig
from lightrag.utils import EmbeddingFunc

from threlium.lightrag_chunking import threlium_email_chunking_func
from threlium.lightrag_prompts import install_overlay
from threlium.settings import (
    ThreliumSettings,
    resolve_llm_endpoint,
    resolve_embedding_endpoint,
    resolve_rerank_endpoint,
)
from threlium.types import (
    LitellmCallSite,
    LitellmRoutingSite,
)

from threlium.litellm_route_context import get_litellm_correlation_from_ctxvar
from threlium.runners.lightrag._adapters import (
    LIGHTRAG_CORRELATION_KWARG,
    CallSiteResolver,
    build_embedding_func,
    build_llm_func,
    build_rerank_func,
    extract_call_site,
    fixed_call_site,
)
from threlium.logutil import logger

log = logger.bind(stage="lightrag")


def _chunk_dims(settings: ThreliumSettings) -> tuple[int, int]:
    body_max = max(64, settings.lightrag.chunk_body_tokens)
    pct = max(0, min(99, settings.lightrag.chunk_body_overlap_pct))
    overlap = max(0, min(body_max - 1, int(body_max * pct / 100)))
    return body_max, overlap


def _working_dir(settings: ThreliumSettings) -> Path:
    raw = settings.lightrag.working_dir.strip()
    if not raw:
        raw = str(settings.home / "lightrag")
    p = Path(raw).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _embed_dim(settings: ThreliumSettings) -> int:
    raw = settings.lightrag.embed_dim.strip()
    return int(raw or "1536")


def _embed_max_tokens(settings: ThreliumSettings) -> int:
    raw = settings.lightrag.embed_max_tokens.strip()
    return int(raw or "8192")


_DEFAULT_ENTITY_TYPES = (
    "person,organization,location,concept,event,technology,document"
)


def _addon_params(settings: ThreliumSettings) -> dict[str, object]:
    language = settings.lightrag.language.strip() or "Russian"
    raw = settings.lightrag.entity_types.strip() or _DEFAULT_ENTITY_TYPES
    entity_types = [t.strip() for t in raw.split(",") if t.strip()]
    if not entity_types:
        entity_types = [t.strip() for t in _DEFAULT_ENTITY_TYPES.split(",")]
    return {"language": language, "entity_types": entity_types}


def _register_lancedb_storage() -> None:
    """Зарегистрировать threlium-адаптер ``LanceDBVectorDBStorage`` в реестре lightrag — БЕЗ патча вендора.

    Мутируем три module-level dict'а ``lightrag.kg``: allowlist реализаций (`verify_storage_implementation`
    гейтит по нему), env-требования (пусто — стор файловый) и name→module (``get_storage_class`` лениво
    импортит этот путь). Идемпотентно. Модуль ``lancedb_impl`` тянет тяжёлый ``import lancedb`` только при
    реальном старте стора (в ``initialize``), не на регистрации.
    """
    from lightrag.kg import (  # noqa: PLC0415
        STORAGE_ENV_REQUIREMENTS,
        STORAGE_IMPLEMENTATIONS,
        STORAGES,
    )

    impls = STORAGE_IMPLEMENTATIONS["VECTOR_STORAGE"]["implementations"]
    if "LanceDBVectorDBStorage" not in impls:
        impls.append("LanceDBVectorDBStorage")
    STORAGE_ENV_REQUIREMENTS.setdefault("LanceDBVectorDBStorage", [])
    STORAGES["LanceDBVectorDBStorage"] = "threlium.runners.lightrag.lancedb_impl"


def _register_cozo_storage() -> None:
    """Зарегистрировать threlium-адаптер ``CozoGraphStorage`` (GRAPH_STORAGE) в реестре lightrag — без патча
    вендора (как LanceDB). ``cozo_impl`` тянет ``import pycozo`` только в ``initialize``."""
    from lightrag.kg import (  # noqa: PLC0415
        STORAGE_ENV_REQUIREMENTS,
        STORAGE_IMPLEMENTATIONS,
        STORAGES,
    )

    impls = STORAGE_IMPLEMENTATIONS["GRAPH_STORAGE"]["implementations"]
    if "CozoGraphStorage" not in impls:
        impls.append("CozoGraphStorage")
    STORAGE_ENV_REQUIREMENTS.setdefault("CozoGraphStorage", [])
    STORAGES["CozoGraphStorage"] = "threlium.runners.lightrag.cozo_impl"


def build_rag(settings: ThreliumSettings) -> LightRAG:
    """Construct LightRAG instance from settings (not yet initialized)."""
    install_overlay(settings)
    _register_lancedb_storage()
    _register_cozo_storage()

    body_max, overlap_toks = _chunk_dims(settings)
    llm_ep = resolve_llm_endpoint(settings.litellm, LitellmRoutingSite.LIGHTRAG_LLM)
    embed_ep = resolve_embedding_endpoint(settings.litellm)
    log.info(
        "litellm_routing",
        site=LitellmRoutingSite.LIGHTRAG_LLM.value,
        score=llm_ep.score,
        embedding_score=embed_ep.embedding_score,
    )
    rerank_ep = resolve_rerank_endpoint(settings.litellm)
    if rerank_ep is not None:
        log.info("litellm_routing_rerank", rerank_score=rerank_ep.rerank_score)

    # Отдельная llm-функция на каждую роль-точку вызова LightRAG (1.5 role_llm_configs). call-site и tool-spec
    # детерминированы РЕЗОЛВЕРОМ точки (без сниффинга формата): keyword/query → константа, extract → структурный
    # (одна роль LightRAG = entity/gleaning/summarize). База = extract. max_async/timeout наследуют базовые.
    def _role_llm(resolve_call_site: CallSiteResolver) -> Any:
        return build_llm_func(
            settings,
            llm_ep=llm_ep,
            default_max_retries=settings.litellm.max_retries,
            chat_template_kwargs=llm_ep.chat_template_kwargs or None,
            resolve_call_site=resolve_call_site,
        )

    extract_llm = _role_llm(extract_call_site)
    rag_kwargs: dict[str, Any] = {
        "working_dir": str(_working_dir(settings)),
        "llm_model_func": extract_llm,
        "role_llm_configs": {
            "extract": RoleLLMConfig(func=extract_llm),
            "keyword": RoleLLMConfig(func=_role_llm(fixed_call_site(LitellmCallSite.EXTRACT_QUERY_KEYWORDS))),
            "query": RoleLLMConfig(func=_role_llm(fixed_call_site(LitellmCallSite.GENERATE_RAG_ANSWER))),
        },
        "embedding_func": EmbeddingFunc(
            embedding_dim=_embed_dim(settings),
            max_token_size=_embed_max_tokens(settings),
            func=build_embedding_func(
                settings,
                embed_ep=embed_ep,
                default_max_retries=settings.litellm.max_retries,
            ),
        ),
        "addon_params": _addon_params(settings),
        # RedisKVStorage вместо JsonKVStorage: Json пере-сериализует ВЕСЬ файл на каждый flush
        # (kv_store_*.json, в т.ч. llm_response_cache ~2.9MB) — растущий json.dump. Redis — построчные
        # upsert'ы, без полной перезаписи. localhost-only (bind 127.0.0.1, protected-mode yes), REDIS_URI
        # по умолчанию redis://localhost:6379. doc_status тоже в Redis (Json писал его на КАЖДЫЙ upsert).
        "kv_storage": "RedisKVStorage",
        # LanceDBVectorDBStorage (threlium-адаптер ``lancedb_impl``, регистрируется в рантайме): встраиваемый
        # Lance-стор с MVCC (конкурентные чтения+записи безопасны — нет faiss concurrent-write segfault, нет
        # single-process сериализации Milvus Lite) и НАТИВНЫМ async API (не морозит rag-loop синхронным gRPC,
        # как Milvus). Lock-free: адаптер без ``_storage_lock``, конкуренцию арбитрит MVCC. Эмбеддинг на upsert,
        # без отложенного flush. Это разблокирует будущее снятие единого rag-loop (независимые aquery/ainsert).
        # Trade-off: per-query latency LanceDB выше faiss на малом корпусе (фреймворк-оверхед) — приемлемо,
        # приоритет стабильность/конкурентность, не скорость. Память: n4-rag-loop-stall-pyspy.
        "vector_storage": "LanceDBVectorDBStorage",
        # CozoGraphStorage (threlium-адаптер cozo_impl, rocksdb): встраиваемый MVCC граф вместо in-memory
        # NetworkX. NetworkX держался на единственном asyncio-loop для взаимного исключения над self._graph;
        # cozo (RocksDB MVCC) безопасен при конкурентной записи в разные узлы (lightrag фанит граф-upsert через
        # Semaphore(8) + per-entity keyed-lock) → разблокирует Stage-2 (снятие единого rag-loop). Память:
        # n4-rag-loop-stall-pyspy.
        "graph_storage": "CozoGraphStorage",
        "doc_status_storage": "RedisDocStatusStorage",
        "chunk_token_size": body_max,
        "chunk_overlap_token_size": overlap_toks,
        "chunking_func": threlium_email_chunking_func,
        "tiktoken_model_name": settings.lightrag.tiktoken_model_name,
        # JSON-режим извлечения сущностей (1.5): LightRAG шлёт entity_extraction_json_* промпт и
        # парсит ответ как нативный JSON {entities, relationships}. Наш tool-bridge форсит ровно эту
        # схему (tool spec = JSON LightRAG) → constrained decoding vLLM даёт валидный JSON.
        "entity_extraction_use_json": True,
    }
    if rerank_ep is not None:
        rag_kwargs["rerank_model_func"] = build_rerank_func(
            settings,
            rerank_ep=rerank_ep,
            default_max_retries=settings.litellm.max_retries,
        )
    # Параллельность из settings (e2e и прод одинаково): корреляция per-call (ctxvar call-site + thread-root
    # на каждый запрос), стабы матчатся по X-Threlium-Call-Site + hasContext(thread-root) — БЕЗ зависимости от
    # порядка вызовов. LanceDB MVCC безопасен для конкурентной записи → ``max_parallel_insert`` можно держать >1
    # (drain-singleton батчит → ainsert переиспользует внутреннюю параллельность lightrag через семафор).
    rag_kwargs["llm_model_max_async"] = settings.lightrag.llm_model_max_async
    rag_kwargs["embedding_func_max_async"] = settings.lightrag.embedding_func_max_async
    rag_kwargs["max_parallel_insert"] = settings.lightrag.max_parallel_insert
    rag = LightRAG(**rag_kwargs)
    if settings.e2e.litellm_route_correlation:
        _install_query_correlation_bridge(rag)
    return rag


def _wrap_pooled_with_correlation(pooled: Any) -> Any:
    """Обернуть ПУЛ-функцию lightrag так, чтобы на submission-границе (контекст запроса/индексации,
    где ctxvar корректен) впрыснуть корреляцию в kwargs. Воркеры lightrag заморозили контекст при
    создании пула (bootstrap), поэтому ctxvar внутри воркера протух — единственный пер-вызовный канал
    к воркеру это args/kwargs очереди (см. _adapters.LIGHTRAG_CORRELATION_KWARG)."""

    async def _inject(*args: Any, **kwargs: Any) -> Any:
        if LIGHTRAG_CORRELATION_KWARG not in kwargs:
            corr = get_litellm_correlation_from_ctxvar()
            if corr:
                kwargs[LIGHTRAG_CORRELATION_KWARG] = dict(corr)
        return await pooled(*args, **kwargs)

    return _inject


def _install_query_correlation_bridge(rag: Any) -> None:
    """Поставить kwarg-мост корреляции поверх пул-обёрток lightrag (embed/rerank/llm/role-funcs).

    Вызывается ПОСЛЕ ``LightRAG(...)``: к этому моменту lightrag уже обернул наши функции
    ``priority_limit_async_func_call`` (предсозданные воркеры). Наш мост сидит снаружи пула и читает
    ctxvar в правильном контексте до постановки задачи в очередь."""
    ef = getattr(rag, "embedding_func", None)
    if ef is not None and getattr(ef, "func", None) is not None:
        # МУТИРУЕМ тот же объект EmbeddingFunc на месте: хранилища lightrag (text_chunks_db и др.)
        # захватили ССЫЛКУ на него при конструировании; replace() создал бы новый объект, который
        # query-путь (operate.py: text_chunks_db.embedding_func) не увидит. EmbeddingFunc — frozen
        # dataclass, поэтому через object.__setattr__.
        object.__setattr__(ef, "func", _wrap_pooled_with_correlation(ef.func))
    if getattr(rag, "rerank_model_func", None) is not None:
        rag.rerank_model_func = _wrap_pooled_with_correlation(rag.rerank_model_func)
    if getattr(rag, "llm_model_func", None) is not None:
        rag.llm_model_func = _wrap_pooled_with_correlation(rag.llm_model_func)
    # Роль-LLM (extract/keyword/query) — мост ВНУТРЬ ``_role_llm_states[role].wrapped``: query-путь
    # читает именно отсюда (lightrag.py: _build_global_config → role_llm_funcs[role] = state.wrapped,
    # пересобирается на КАЖДЫЙ запрос). ``role_llm_configs`` — input-only (читается лишь в __post_init__),
    # его подмена постфактум НЕ влияет на вызов (был баг: keyword/query несли протухший thread-root первого
    # треда из замороженного воркер-пула → второй тест ловил 0 query-вызовов под своим stub_tag). Оборачиваем
    # уже-пулезированную ``wrapped`` снаружи: ctxvar читается на submission-границе и едет в воркер kwarg-ом.
    states = getattr(rag, "_role_llm_states", None)
    if isinstance(states, dict):
        for _name, state in states.items():
            wrapped = getattr(state, "wrapped", None)
            if wrapped is not None:
                state.wrapped = _wrap_pooled_with_correlation(wrapped)
