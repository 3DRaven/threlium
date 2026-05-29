"""Единая конфигурация Threlium на pydantic-settings.

Единственная точка входа: :func:`load_settings`. Все defaults, валидация и
приоритет источников (OS env > YAML > defaults в коде) определяются здесь.

Источники (все опциональны):
    1. OS env / systemd ``EnvironmentFile=`` — префикс ``THRELIUM_``, вложенность ``__``
    2. YAML-файл (``config/threlium.yaml``) — если существует
    3. Defaults в полях ``ThreliumSettings`` и вложенных моделей

Без YAML и без env — defaults достаточны для запуска.

При ошибке валидации сообщения Pydantic содержат имя поля и ``description`` из
:class:`Field` — ориентируйтесь на них и на этот модуль как на источник правды.
"""
from __future__ import annotations

import math
import os
import re
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict, YamlConfigSettingsSource

from threlium.types.litellm_routing_site import LitellmRoutingSite

# Режимы LightRAG QueryParam.mode (см. ``states/enrich.py``).
_LIGHTRAG_QUERY_MODES: Final[frozenset[str]] = frozenset(
    {"local", "global", "hybrid", "naive", "mix", "bypass"}
)
# Допустимые значения LightragSettings.query_api (метод LightRAG для retrieval в enrich).
_LIGHTRAG_QUERY_APIS: Final[frozenset[str]] = frozenset(
    {"aquery", "aquery_data", "aquery_llm"}
)
# cgroup/systemd-run: память процесса, напр. 256M, 1G
_MEM_MAX_RE: Final[re.Pattern[str]] = re.compile(r"^\d+([KMGT][iI]?[bB]?)?$", re.IGNORECASE)
# CPUQuota: процент или infinity
_CPU_QUOTA_RE: Final[re.Pattern[str]] = re.compile(r"^(\d+%|infinity)$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# LiteLLM routing models (pydantic, replaces msgspec structs from
# litellm_routing_catalog.py)
# ---------------------------------------------------------------------------


class LlmEndpoint(BaseModel):
    """Один LLM-эндпоинт (completion). Меньший ``score`` = дешевле при выборе."""

    model_config = ConfigDict(str_strip_whitespace=True)

    score: float = Field(
        default=0.0,
        description="Вес стоимости эндпоинта; при выборе минимизируется расхождение с target_score сайта.",
    )
    enabled: bool = Field(default=True, description="False — эндпоинт не участвует в выборе.")
    model: str = Field(
        default="gpt-4o-mini",
        description="Идентификатор модели для LiteLLM (OpenAI-совместимый провайдер). Пример: gpt-4o-mini.",
    )
    api_base: str = Field(
        default="",
        description="Базовый URL API без суффикса /chat/completions. Пусто — дефолт провайдера. Пример: https://api.openai.com/v1",
    )
    api_key: str | None = Field(
        default=None,
        description="Ключ API; null/пусто — из env OPENAI_API_KEY и т.п. Не логировать.",
    )
    timeout: float = Field(
        default=120.0,
        gt=0,
        description="Таймаут HTTP/SDK одного вызова completion (секунды), > 0.",
    )
    max_tokens: int | None = Field(
        default=None,
        ge=1,
        description="Лимит токенов ответа для этого эндпоинта; null — без отдельного лимита.",
    )
    thinking_token_budget: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Жёсткий лимит токенов на CoT/reasoning (vLLM top-level thinking_token_budget). "
            "null — не передавать. Имеет смысл при enable_thinking, чтобы оставить max_tokens "
            "на tool_calls / content."
        ),
    )
    length_recovery_max_attempts: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Сколько раз вызывать completion при finish_reason=length (включая recovery с "
            "системным hint); null — брать litellm.length_recovery_max_attempts. "
            "Используется стадией reasoning."
        ),
    )
    max_retries: int | None = Field(
        default=None,
        ge=0,
        description="Ретраи LiteLLM для эндпоинта; null — брать litellm.max_retries.",
    )
    chat_template_kwargs: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Произвольный dict доп. параметров шаблона чата, передаётся в LiteLLM как "
            "chat_template_kwargs (для провайдера openai уходит в extra_body). "
            "null / отсутствие / пустой {} — не передавать. "
            "Полный перечень vLLM-ключей и примеры по моделям — в комментариях "
            "к threlium_litellm.llm_endpoints в ansible/roles/threlium/defaults/main.yml "
            "и ansible/host_vars/th-agent.yml."
        ),
    )

    @field_validator("score", mode="after")
    @classmethod
    def _score_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("litellm.llm_endpoints[].score: должно быть конечным числом")
        return v

    @model_validator(mode="after")
    def _enabled_requires_model(self) -> LlmEndpoint:
        if self.enabled and not self.model.strip():
            raise ValueError(
                "litellm.llm_endpoints[]: при enabled=true поле model не может быть пустым "
                "(укажите идентификатор модели, напр. gpt-4o-mini)."
            )
        return self


class EmbeddingEndpoint(BaseModel):
    """Один embedding-эндпоинт. Меньший ``embedding_score`` = дешевле при выборе."""

    model_config = ConfigDict(str_strip_whitespace=True)

    embedding_score: float = Field(
        default=0.0,
        description="Вес для выбора эндпоинта; сравнивается с target_embedding_score.",
    )
    enabled: bool = Field(default=True, description="False — эндпоинт не используется.")
    model: str = Field(
        default="text-embedding-3-small",
        description="Идентификатор embedding-модели. Пример: text-embedding-3-small.",
    )
    api_base: str = Field(default="", description="Базовый URL embeddings API. Пусто — дефолт провайдера.")
    api_key: str | None = Field(default=None, description="Ключ API; null — из окружения.")
    timeout: float = Field(default=120.0, gt=0, description="Таймаут вызова embedding (секунды), > 0.")
    max_retries: int | None = Field(default=None, ge=0, description="Ретраи; null — litellm.max_retries.")
    encoding_format: Literal["float", "base64", "bytes", "bytes_only"] | None = Field(
        default=None,
        description=(
            "OpenAI-compatible embeddings: encoding_format для SDK. "
            "null/отсутствие — ключ не передаётся в LiteLLM (e2e/WireMock). "
            "Для vLLM OpenAI-server обычно нужно float."
        ),
    )

    @field_validator("encoding_format", mode="before")
    @classmethod
    def _encoding_format_empty_to_none(cls, v: object) -> object:
        if v is None or v == "":
            return None
        return v

    @field_validator("embedding_score", mode="after")
    @classmethod
    def _emb_score_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("litellm.embedding_endpoints[].embedding_score: должно быть конечным числом")
        return v

    @model_validator(mode="after")
    def _enabled_requires_model(self) -> EmbeddingEndpoint:
        if self.enabled and not self.model.strip():
            raise ValueError(
                "litellm.embedding_endpoints[]: при enabled=true поле model не может быть пустым."
            )
        return self


class RerankEndpoint(BaseModel):
    """Один rerank-эндпоинт. Меньший ``rerank_score`` = дешевле при выборе."""

    model_config = ConfigDict(str_strip_whitespace=True)

    rerank_score: float = Field(
        default=0.0,
        description="Вес для выбора эндпоинта; сравнивается с target_rerank_score.",
    )
    enabled: bool = Field(default=True, description="False — эндпоинт не используется.")
    model: str = Field(
        default="",
        description="Идентификатор rerank-модели. Пример: hosted_vllm/bge-rerank.",
    )
    api_base: str = Field(default="", description="Базовый URL rerank API. Пусто — дефолт провайдера.")
    api_key: str | None = Field(default=None, description="Ключ API; null — из окружения.")
    timeout: float = Field(default=120.0, gt=0, description="Таймаут вызова rerank (секунды), > 0.")
    max_retries: int | None = Field(default=None, ge=0, description="Ретраи; null — litellm.max_retries.")
    top_n: int | None = Field(default=None, ge=1, description="top_n для rerank; null — дефолт модели.")

    @field_validator("rerank_score", mode="after")
    @classmethod
    def _rerank_score_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("litellm.rerank_endpoints[].rerank_score: должно быть конечным числом")
        return v

    @model_validator(mode="after")
    def _enabled_requires_model(self) -> RerankEndpoint:
        if self.enabled and not self.model.strip():
            raise ValueError(
                "litellm.rerank_endpoints[]: при enabled=true поле model не может быть пустым."
            )
        return self


class LlmSiteTarget(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    target_score: float = Field(
        default=0.0,
        description="Желаемый score LLM-эндпоинта для этого сайта; выбирается ближайший среди enabled.",
    )

    @field_validator("target_score", mode="after")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("litellm.targets.*.target_score: должно быть конечным числом")
        return v


class EmbeddingSiteTarget(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    target_embedding_score: float = Field(
        default=0.0,
        description="Желаемый embedding_score; выбирается ближайший enabled embedding-эндпоинт.",
    )

    @field_validator("target_embedding_score", mode="after")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("litellm.targets.lightrag_embedding.target_embedding_score: конечное число")
        return v


class RerankSiteTarget(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    target_rerank_score: float = Field(
        default=0.0,
        description="Желаемый rerank_score; выбирается ближайший enabled rerank-эндпоинт.",
    )

    @field_validator("target_rerank_score", mode="after")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("litellm.targets.lightrag_rerank.target_rerank_score: конечное число")
        return v


class RoutingTargets(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    reasoning: LlmSiteTarget = Field(default_factory=LlmSiteTarget, description="Маршрут стадии reasoning.")
    enrich_plan: LlmSiteTarget = Field(default_factory=LlmSiteTarget, description="Маршрут LLM плана enrich.")
    response_observe: LlmSiteTarget = Field(default_factory=LlmSiteTarget, description="Маршрут LLM суммаризации response_observe.")
    summarize_context: LlmSiteTarget = Field(default_factory=LlmSiteTarget, description="Маршрут LLM суммаризации контекста (score 0).")
    lightrag_llm: LlmSiteTarget = Field(
        default_factory=LlmSiteTarget,
        description="Маршрут LLM внутри LightRAG (не embedding).",
    )
    lightrag_embedding: EmbeddingSiteTarget = Field(
        default_factory=EmbeddingSiteTarget,
        description="Маршрут embedding для LightRAG.",
    )
    lightrag_rerank: RerankSiteTarget = Field(
        default_factory=RerankSiteTarget,
        description="Маршрут rerank для LightRAG.",
    )


# ---------------------------------------------------------------------------
# resolve helpers (moved from litellm_routing_catalog.py)
# ---------------------------------------------------------------------------


def resolve_llm_endpoint(
    settings: LitellmSettings,
    site: LitellmRoutingSite,
) -> LlmEndpoint:
    """Один выбранный LLM-эндпоинт; исключение при отсутствии enabled."""
    if site == LitellmRoutingSite.LIGHTRAG_EMBEDDING:
        raise ValueError("use resolve_embedding_endpoint for lightrag_embedding")

    enabled = [e for e in settings.llm_endpoints if e.enabled]
    if not enabled:
        raise RuntimeError("litellm routing: no enabled llm_endpoints")

    if site == LitellmRoutingSite.REASONING:
        target = settings.targets.reasoning.target_score
    elif site == LitellmRoutingSite.ENRICH_PLAN:
        target = settings.targets.enrich_plan.target_score
    elif site == LitellmRoutingSite.RESPONSE_OBSERVE:
        target = settings.targets.response_observe.target_score
    elif site == LitellmRoutingSite.SUMMARIZE_CONTEXT:
        target = settings.targets.summarize_context.target_score
    else:
        target = settings.targets.lightrag_llm.target_score

    return min(enabled, key=lambda e: (abs(e.score - target), e.score))


def resolve_embedding_endpoint(settings: LitellmSettings) -> EmbeddingEndpoint:
    """Один выбранный embedding-эндпоинт для ``lightrag_embedding``."""
    enabled = [e for e in settings.embedding_endpoints if e.enabled]
    if not enabled:
        raise RuntimeError("litellm routing: no enabled embedding_endpoints")

    target = settings.targets.lightrag_embedding.target_embedding_score
    return min(enabled, key=lambda e: (abs(e.embedding_score - target), e.embedding_score))


def resolve_rerank_endpoint(settings: LitellmSettings) -> RerankEndpoint | None:
    """Один выбранный rerank-эндпоинт для ``lightrag_rerank``; ``None`` если rerank не настроен."""
    enabled = [e for e in settings.rerank_endpoints if e.enabled]
    if not enabled:
        return None
    target = settings.targets.lightrag_rerank.target_rerank_score
    return min(enabled, key=lambda e: (abs(e.rerank_score - target), e.rerank_score))


# ---------------------------------------------------------------------------
# Nested settings sections
# ---------------------------------------------------------------------------


class LitellmSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    max_retries: int = Field(
        default=3,
        ge=0,
        description="Глобальный лимит ретраев LiteLLM/SDK (≥0). В e2e часто 0 для WireMock.",
    )
    length_recovery_max_attempts: int = Field(
        default=2,
        ge=1,
        description=(
            "Дефолт числа completion-попыток при finish_reason=length для reasoning "
            "(первая + recovery); переопределяется на llm_endpoints[]."
        ),
    )
    llm_endpoints: list[LlmEndpoint] = Field(
        default_factory=lambda: [LlmEndpoint()],
        min_length=1,
        description="Каталог LLM-эндпоинтов; нужен минимум один элемент.",
    )
    embedding_endpoints: list[EmbeddingEndpoint] = Field(
        default_factory=lambda: [EmbeddingEndpoint()],
        min_length=1,
        description="Каталог embedding-эндпоинтов; нужен минимум один элемент.",
    )
    rerank_endpoints: list[RerankEndpoint] = Field(
        default_factory=list,
        description="Каталог rerank-эндпоинтов; пустой список — rerank отключён.",
    )
    targets: RoutingTargets = Field(
        default_factory=RoutingTargets,
        description="Целевые score по сайтам маршрутизации (reasoning, enrich_plan, lightrag_*, …).",
    )

    @model_validator(mode="after")
    def _at_least_one_enabled_llm(self) -> LitellmSettings:
        if not any(e.enabled for e in self.llm_endpoints):
            raise ValueError(
                "litellm.llm_endpoints: нужен хотя бы один элемент с enabled=true "
                "(иначе resolve_llm_endpoint не сможет выбрать эндпоинт)."
            )
        return self

    @model_validator(mode="after")
    def _at_least_one_enabled_embedding(self) -> LitellmSettings:
        if not any(e.enabled for e in self.embedding_endpoints):
            raise ValueError(
                "litellm.embedding_endpoints: нужен хотя бы один элемент с enabled=true "
                "(иначе LightRAG embedding не запустится)."
            )
        return self


class LightragSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    language: str = Field(
        default="Russian",
        min_length=1,
        description="Язык промптов/извлечения сущностей для LightRAG. Пример: Russian, English.",
    )
    entity_types: str = Field(
        default="person,organization,location,concept,event,technology,document",
        min_length=1,
        description="Список типов сущностей через запятую (конфиг LightRAG).",
    )
    working_dir: str = Field(
        default="",
        description="Каталог данных графа; пусто — $THRELIUM_HOME/lightrag.",
    )
    chunk_body_tokens: int = Field(default=1200, ge=1, description="Размер чанка тела письма в токенах.")
    chunk_body_overlap_pct: int = Field(
        default=10,
        ge=0,
        le=100,
        description="Перекрытие чанков в процентах 0–100.",
    )
    insert_batch: int = Field(default=16, ge=1, description="Размер батча ainsert в RAG-loop.")
    max_parallel_insert: int = Field(
        default=2,
        ge=1,
        description="Потолок LightRAG max_parallel_insert (не e2e).",
    )
    llm_model_max_async: int = Field(
        default=4,
        ge=1,
        description="Потолок LightRAG llm_model_max_async при индексации (не e2e).",
    )
    embedding_func_max_async: int = Field(
        default=4,
        ge=1,
        description="Потолок LightRAG embedding_func_max_async (не e2e).",
    )
    embed_dim: str = Field(
        default="",
        description="Размерность вектора (строка для провайдера); пусто — авто из модели.",
    )
    embed_max_tokens: str = Field(default="", description="Лимит токенов на embedding; пусто — дефолт модели.")
    rag_loop_shutdown_timeout_sec: float = Field(
        default=30.0,
        gt=0,
        description=(
            "Таймаут cancel+finalize RAG-loop при остановке threlium-engine (сек). "
            "Не путать с litellm LLM timeout."
        ),
    )
    bootstrap_timeout_sec: float = Field(
        default=1800.0,
        gt=0,
        description=(
            "Дедлайн всей фоновой bootstrap-индексации knowledge/ при старте engine (сек). "
            "Это wall-clock на весь корпус (десятки LLM/embed вызовов), а НЕ таймаут одного "
            "completion (llm_endpoints[].timeout). Истечение не валит engine — задача фоновая, "
            "остаток доиндексируется на следующем старте (дедуп через doc_status)."
        ),
    )
    prompts_overlay: bool = Field(
        default=True,
        description="Включить оверлей Jinja-промптов LightRAG из репозитория.",
    )
    aquery_hints: str = Field(default="", description="Доп. текст в enrich/aquery (может быть пустым).")
    query_api: str = Field(
        default="aquery_llm",
        description=(
            "Метод LightRAG для retrieval в enrich: "
            "aquery (str, финальный RAG-LLM), "
            "aquery_data (dict, только retrieval без RAG-LLM), "
            "aquery_llm (dict, retrieval + llm_response — максимально полный ответ). "
            "Ansible: threlium_lightrag.query_api."
        ),
    )
    query_mode: str = Field(
        default="hybrid",
        description="Режим LightRAG aquery: local|global|hybrid|naive|mix|bypass (см. enrich.py).",
    )
    query_top_k: int = Field(default=40, ge=1, description="QueryParam.top_k.")
    query_chunk_top_k: int = Field(default=20, ge=1, description="QueryParam.chunk_top_k.")
    query_max_total_tokens: int = Field(default=30_000, ge=1, description="Верхняя граница токенов на запрос.")
    query_max_entity_tokens: int = Field(default=6000, ge=1, description="Лимит токенов на сущности.")
    query_max_relation_tokens: int = Field(default=8000, ge=1, description="Лимит токенов на отношения.")
    query_response_type: str = Field(
        default="Concise Bullet Points",
        description="QueryParam.response_type — формат ответа LightRAG LLM.",
    )
    enable_rerank: bool = Field(default=True, description="QueryParam.enable_rerank — использовать rerank чанков.")
    llm_max_tokens: int = Field(
        default=0,
        ge=0,
        description="Лимит токенов LLM внутри LightRAG; 0 — без лимита (семантика раннера).",
    )

    @field_validator("query_api", mode="after")
    @classmethod
    def _query_api_known(cls, v: str) -> str:
        key = (v or "").strip().lower()
        if key not in _LIGHTRAG_QUERY_APIS:
            raise ValueError(
                f"lightrag.query_api: недопустимо {v!r}. Допустимые значения: {sorted(_LIGHTRAG_QUERY_APIS)}"
            )
        return key

    @field_validator("query_mode", mode="after")
    @classmethod
    def _query_mode_known(cls, v: str) -> str:
        key = (v or "").strip().lower()
        if key not in _LIGHTRAG_QUERY_MODES:
            raise ValueError(
                f"lightrag.query_mode: недопустимо {v!r}. Допустимые значения: {sorted(_LIGHTRAG_QUERY_MODES)}"
            )
        return key


class EmailBridgeSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    imap_host: str = Field(default="", description="IMAP сервер fetchmail/IMAP-bridge. Пусто — мост не настраивается.")
    imap_user: str = Field(default="", description="Логин IMAP.")
    imap_pass: str = Field(default="", description="Пароль IMAP (секрет).")
    imap_port: int = Field(
        default=0,
        ge=0,
        le=65535,
        description="Порт IMAP; 0 — авто (993 при SSL, 143 без) в коде моста.",
    )
    imap_use_ssl: bool = Field(default=True, description="IMAPS vs plain IMAP.")
    imap_ssl_verify: bool = Field(default=True, description="Проверять TLS-сертификат сервера.")
    imap_idle_max_sec: int = Field(
        default=1740,
        ge=60,
        le=29 * 60,
        description="Интервал IDLE/опроса (сек); RFC 2177 — меньше 29 минут, у нас 60…1740.",
    )
    imap_processed_folder: str = Field(
        default="",
        description=(
            "IMAP-папка/label, куда мост переносит обработанные письма из INBOX (UID MOVE). "
            "Пусто — legacy-поведение: только флаг \\Seen, без переноса. "
            "Gmail: имя вложенного label через '/' (напр. 'Threlium/Processed'), завести вручную в UI."
        ),
    )
    imap_ensure_processed_folder: bool = Field(
        default=True,
        description=(
            "Создавать imap_processed_folder при старте моста, если её нет (CREATE). "
            "Gmail: false — label создаётся вручную, CREATE по IMAP не поддержан."
        ),
    )

    @model_validator(mode="after")
    def _imap_host_requires_user(self) -> EmailBridgeSettings:
        if self.imap_host.strip():
            if not self.imap_user.strip():
                raise ValueError(
                    "bridges.email: задан imap_host, но imap_user пуст — укажите учётную запись IMAP."
                )
        return self


class MatrixBridgeSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    homeserver: str = Field(
        default="",
        description="Matrix homeserver (домен или URL). Пример: matrix.example.com или https://matrix.example.com",
    )
    user: str = Field(default="", description="MXID бота, напр. @threlium:example.com")
    token: str = Field(default="", description="Access token клиента Matrix (секрет).")


class TelegramBridgeSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    bot_token: str = Field(default="", description="Токен BotFather (секрет). Пусто — Telegram-мост не используется.")
    bot_api_base: str = Field(
        default="",
        description="Необязательный override API Telegram; пусто — https://api.telegram.org",
    )

    @field_validator("bot_api_base", mode="after")
    @classmethod
    def _bot_api_base_shape(cls, v: str) -> str:
        s = v.strip()
        if not s:
            return ""
        if not s.startswith(("http://", "https://")):
            raise ValueError(
                "bridges.telegram.bot_api_base: если задано, должно начинаться с http:// или https:// "
                f"(получено {v!r})."
            )
        return s.rstrip("/")


class BridgesSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    email: EmailBridgeSettings = Field(default_factory=EmailBridgeSettings)
    matrix: MatrixBridgeSettings = Field(default_factory=MatrixBridgeSettings)
    telegram: TelegramBridgeSettings = Field(default_factory=TelegramBridgeSettings)

    @field_validator("email", "matrix", "telegram", mode="before")
    @classmethod
    def _bridge_subsection_not_null(cls, v: Any) -> Any:
        """YAML с только комментариями под ключом даёт ``null`` — трактуем как пустой объект."""
        return {} if v is None else v


class EnrichSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    context_thread_n: int = Field(default=200, ge=1, description="Глубина треда для unified-контекста.")
    context_thread_memory_n: int = Field(default=100, ge=1, description="Глубина для thread memory.")
    context_global_n: int = Field(default=100, ge=1, description="Глубина глобальной памяти.")
    context_max_chars: int = Field(
        default=180_000,
        ge=0,
        description="Лимит символов контекста: enrich-промпты, исходящее MIME-тело, reasoning user prompt.",
    )

    tier1_full: int = Field(default=5, ge=1, description="Макс. сообщений с full body.")
    tier2_summary: int = Field(default=15, ge=0, description="Макс. сообщений с summary.")
    tier_preview_chars: int = Field(default=200, ge=50, description="Символов preview в tier2.")
    plan_recent_n: int = Field(default=5, ge=1, description="Кол-во последних сообщений для query plan.")

    priority_user: float = Field(default=10.0, description="Вес user_message.")
    priority_graph: float = Field(default=6.0, description="Вес knowledge_graph.")
    priority_unified: float = Field(default=8.0, description="Вес conversation_history.")
    priority_thread_mem: float = Field(default=4.0, description="Вес thread_memory.")
    priority_global_mem: float = Field(default=3.0, description="Вес global_memory.")
    priority_extra: float = Field(default=5.0, description="Вес response_state + task_state + response_observation.")

    tool_observation_estimate_cap_chars: int = Field(
        default=500, ge=1,
        description="Потолок символов тела tool_observation при оценке веса MCKP (не рендеринг).",
    )
    tier1_medium_ratio: int = Field(
        default=2, ge=1,
        description="Делитель tier1_full для MEDIUM-варианта unified MCKP (tier1_med = tier1_full // ratio).",
    )

    summarize_enabled: bool = Field(default=True, description="Включить LLM-суммаризацию при overflow unified.")
    summarize_batch_max_messages: int = Field(
        default=10, ge=1,
        description="Макс. писем в одном батче суммаризации (JSON payload → summarize_context).",
    )
    summarize_trigger_min_excess_chars: int = Field(
        default=500, ge=0,
        description="Минимальный избыток символов для запуска суммаризации (порог overflow).",
    )

    type_weight_user_input: float = Field(default=1.0, description="Вес ContextMessageType.USER_INPUT.")
    type_weight_agent_response: float = Field(default=0.7, description="Вес ContextMessageType.AGENT_RESPONSE.")
    type_weight_context_summary: float = Field(
        default=1.0,
        description="Вес ContextMessageType.CONTEXT_SUMMARY (summarize_memory@ итог хвоста треда).",
    )
    type_weight_tool_observation: float = Field(default=0.5, description="Вес ContextMessageType.TOOL_OBSERVATION.")
    type_weight_system: float = Field(default=0.3, description="Вес ContextMessageType.SYSTEM.")
    type_weight_service: float = Field(default=0.1, description="Вес ContextMessageType.SERVICE.")

    def message_type_weights(self) -> "ContextMessageTypeWeights":
        """Typed container из flat-полей для передачи в scorer."""
        from threlium.context_budget import ContextMessageTypeWeights
        return ContextMessageTypeWeights(
            user_input=self.type_weight_user_input,
            agent_response=self.type_weight_agent_response,
            context_summary=self.type_weight_context_summary,
            tool_observation=self.type_weight_tool_observation,
            system=self.type_weight_system,
            service=self.type_weight_service,
        )


class CliSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    exec_timeout: int = Field(default=30, ge=1, description="Таймаут systemd-run для cli_exec (сек).")
    exec_memory_max: str = Field(
        default="256M",
        description="MemoryMax для песочницы (systemd). Пример: 256M, 1G.",
    )
    exec_cpu_quota: str = Field(
        default="100%",
        description="CPUQuota для песочницы. Пример: 50%, 100%, infinity.",
    )
    exec_tasks_max: int = Field(default=16, ge=1, description="Максимум параллельных cli-задач.")
    allowlist: str = Field(
        default="ls,cat,pwd,echo,true,head,tail,wc,find,rg,grep,git,python3,test",
        min_length=1,
        description="Список разрешённых имён бинарников через запятую (без путей).",
    )
    deny_patterns: str = Field(
        default="",
        description="Подстроки argv через запятую для запрета (дополнительная политика).",
    )

    @field_validator("exec_memory_max", mode="after")
    @classmethod
    def _memory_max_format(cls, v: str) -> str:
        s = v.strip()
        if not _MEM_MAX_RE.match(s):
            raise ValueError(
                f"cli.exec_memory_max: ожидается cgroup-стиль вроде 256M или 1G (получено {v!r})."
            )
        return s

    @field_validator("exec_cpu_quota", mode="after")
    @classmethod
    def _cpu_quota_format(cls, v: str) -> str:
        s = v.strip()
        if not _CPU_QUOTA_RE.match(s):
            raise ValueError(
                f"cli.exec_cpu_quota: ожидается процент (50%) или infinity (получено {v!r})."
            )
        return s


class KnowledgeSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    formal_report_max_chars: int = Field(
        default=4000,
        ge=1,
        description="Макс. символов pySHACL-отчёта / syntax error в observation formal_reason.",
    )
    formal_derived_max_chars: int = Field(
        default=6000,
        ge=1,
        description="Макс. символов entailed-дельты (derived_triples) в observation formal_reason.",
    )
    formal_query_max_chars: int = Field(
        default=4000,
        ge=1,
        description="Макс. символов SPARQL-результата (query_result) в observation formal_reason.",
    )
    observation_max_chars: int = Field(
        default=180_000,
        ge=0,
        description="Макс. символов тела observation-note (memory_query answer и др.).",
    )


class HopSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    budget_root: int = Field(default=256, ge=1, description="Максимум hop FSM для root-агента.")
    budget_sub: int = Field(default=256, ge=1, description="Максимум hop для subagent-веток.")


class CapSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    root: str = Field(
        default="",
        description="Необязательная capability-метка root (заголовок X-Threlium-Capabilities); пусто — не задана.",
    )
    sub: str = Field(default="", description="Capability-метка для subagent; пусто — не задана.")


class EgressSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    email_from: str = Field(
        default="agent@localhost",
        min_length=1,
        description="RFC5322 From для исходящей почты агента (egress_email). Пример: agent@localhost",
    )
    references_max_chars: int = Field(
        default=8000, ge=100,
        description="Макс. длина заголовка References (RFC truncation) в символах.",
    )


class E2eSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    litellm_route_correlation: bool = Field(
        default=False,
        description="E2e: форсировать размер батча индексации 1 для корреляции HTTP с одним документом.",
    )


class MsmtpSettings(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    host: str = Field(default="", description="SMTP relay для msmtp; пусто — msmtp не настроен на отправку наружу.")
    port: int = Field(default=587, ge=1, le=65535, description="Порт SMTP submission.")
    user: str = Field(default="", description="AUTH пользователь (может быть пустым без AUTH).")
    password: str = Field(default="", description="AUTH пароль (секрет).")
    from_addr: str = Field(
        default="threlium@localhost",
        min_length=1,
        description="Envelope From для msmtp по умолчанию.",
    )
    tls: bool = Field(default=True, description="Использовать STARTTLS/TLS.")
    auth: bool = Field(default=True, description="Включить SMTP AUTH.")


# ---------------------------------------------------------------------------
# Top-level settings
# ---------------------------------------------------------------------------


class ThreliumSettings(BaseSettings):
    """Единая конфигурация процесса Threlium.

    Приоритет: OS env > YAML > defaults.
    Env-переменные: ``THRELIUM_`` prefix, ``__`` для вложенности.
    Пример: ``THRELIUM_BRIDGES__EMAIL__IMAP_HOST=mail.example.com``
    """

    model_config = SettingsConfigDict(
        env_prefix="THRELIUM_",
        env_nested_delimiter="__",
        extra="ignore",
        str_strip_whitespace=True,
    )

    home: Path = Field(
        default=Path(""),
        description="Корень данных (Maildir stages, config/, lightrag). Пустой Path — только из defaults/env без YAML.",
    )
    repo: str = Field(
        default="",
        description="Путь к checkout репозитория с кодом и prompts/ (на хосте). Пример: /home/threlium/threlium/agent",
    )
    venv: str = Field(
        default="",
        description="Каталог Python venv с зависимостями. Пример: …/agent/.venv",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="DEBUG",
        description="Уровень логирования: DEBUG, INFO, WARNING, ERROR",
    )

    # Внутренний путь YAML для YamlConfigSettingsSource (exclude=True — не из env).
    yaml_path_for_load: str | None = Field(default=None, exclude=True, repr=False)

    litellm: LitellmSettings = Field(default_factory=LitellmSettings)
    lightrag: LightragSettings = Field(default_factory=LightragSettings)
    bridges: BridgesSettings = Field(default_factory=BridgesSettings)
    enrich: EnrichSettings = Field(default_factory=EnrichSettings)
    knowledge: KnowledgeSettings = Field(default_factory=KnowledgeSettings)
    cli: CliSettings = Field(default_factory=CliSettings)
    hop: HopSettings = Field(default_factory=HopSettings)
    cap: CapSettings = Field(default_factory=CapSettings)
    egress: EgressSettings = Field(default_factory=EgressSettings)
    e2e: E2eSettings = Field(default_factory=E2eSettings)
    msmtp: MsmtpSettings = Field(default_factory=MsmtpSettings)

    @field_validator("home", mode="after")
    @classmethod
    def _home_absolute_if_set(cls, v: Path) -> Path:
        if v == Path():
            return v
        p = v.expanduser()
        if not p.is_absolute():
            raise ValueError(
                f"THRELIUM_HOME / home: ожидается абсолютный путь (получено {str(v)!r}). "
                "Пример: /home/threlium/threlium/data"
            )
        return p

    @field_validator(
        "litellm",
        "lightrag",
        "bridges",
        "enrich",
        "knowledge",
        "cli",
        "hop",
        "cap",
        "egress",
        "e2e",
        "msmtp",
        mode="before",
    )
    @classmethod
    def _top_nested_section_not_null(cls, v: Any) -> Any:
        """Секция YAML вида ``cap:`` без полей парсится как ``null`` — подставляем пустой mapping."""
        return {} if v is None else v

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any = None,
        env_settings: Any = None,
        dotenv_settings: Any = None,
        file_secret_settings: Any = None,
    ) -> tuple[Any, ...]:
        sources: list[Any] = []
        if env_settings is not None:
            sources.append(env_settings)
        yaml_file = (
            init_settings.init_kwargs.get("yaml_path_for_load")
            if init_settings is not None
            else None
        )
        if yaml_file and Path(yaml_file).is_file():
            sources.append(YamlConfigSettingsSource(settings_cls, yaml_file=yaml_file))
        if init_settings is not None:
            sources.append(init_settings)
        return tuple(sources)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def load_settings(
    *,
    yaml_path: str | Path | None = None,
    _auto_discover: bool = True,
) -> ThreliumSettings:
    """Единственная точка создания конфигурации.

    Args:
        yaml_path: Явный путь к YAML. Если не задан и ``_auto_discover=True``,
            пытается ``$THRELIUM_HOME/config/threlium.yaml``.
        _auto_discover: Искать YAML автоматически по ``THRELIUM_HOME``.

    Returns:
        Frozen snapshot конфигурации.
    """
    if yaml_path is None and _auto_discover:
        thome = os.environ.get("THRELIUM_HOME", "").strip()
        if thome:
            candidate = Path(thome) / "config" / "threlium.yaml"
            if candidate.is_file():
                yaml_path = candidate

    if yaml_path is not None:
        return ThreliumSettings(yaml_path_for_load=str(yaml_path))
    return ThreliumSettings()
