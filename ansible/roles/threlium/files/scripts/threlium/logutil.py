"""Structured async-safe logging: structlog + stdlib QueueHandler + JSON → journald.

Форматирование (ProcessorFormatter + JSON) выполняется в потоке-источнике на QueueHandler,
до enqueue. В очередь попадает LogRecord с готовой строкой; фоновый StreamHandler
только пишет ``%(message)s`` в stderr — без повторного ProcessorFormatter и без
кастомного prepare() для dict в record.msg.

Публичный API:
    from threlium.logutil import clip_log_text, logger, setup_logging, shutdown_logging
"""
from __future__ import annotations

import logging
import queue
import sys
from logging.handlers import QueueHandler, QueueListener

import structlog

_listener: QueueListener | None = None

_FOREIGN_PRE_CHAIN = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_log_level,
    structlog.stdlib.add_logger_name,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.format_exc_info,
    structlog.stdlib.ExtraAdder(),
]

# Внешние библиотеки/инструменты — логируем на **INFO**, продукт (threlium) остаётся на корневом DEBUG.
# НЕ глушим в WARNING и НЕ отключаем: это резало полезную диагностику LLM/HTTP/индексации. На INFO
# HTTP-клиенты дают по ОДНОЙ строке на запрос (без DEBUG-флуда тел/заголовков, который раньше переполнял
# QueueHandler); lightrag — pipeline-статус/worker init/shutdown/прогресс; litellm — маршрутизация/стоимость
# вызовов. Если какой-то внешний логгер окажется шумным/битым на INFO — точечное исключение, не общий обрез.
_EXTERNAL_LOGGER_NAMES: tuple[str, ...] = (
    "httpx",
    "httpcore",
    "httpcore.connection",
    "httpcore.http11",
    "urllib3",
    "docker",
    "requests",
    "charset_normalizer",
    "lightrag",
    "LiteLLM",
    "LiteLLM Proxy",
    "LiteLLM Router",
)

_LOG_CLIP_SUFFIX = "…"
_DEFAULT_LOG_TEXT_MAX_LEN = 128


def clip_log_text(value: str, *, max_len: int = _DEFAULT_LOG_TEXT_MAX_LEN) -> str:
    """Обрезать строку для structlog/journald; при превышении — суффикс ``…``."""
    if max_len < 1:
        raise ValueError(f"max_len must be >= 1, got {max_len!r}")
    if len(value) <= max_len:
        return value
    if max_len <= len(_LOG_CLIP_SUFFIX):
        return _LOG_CLIP_SUFFIX[:max_len]
    return f"{value[: max_len - len(_LOG_CLIP_SUFFIX)]}{_LOG_CLIP_SUFFIX}"


def _configure_external_loggers() -> None:
    """Внешние логгеры (HTTP-клиенты, lightrag, litellm) → INFO с propagate в root.

    Продукт остаётся на корневом уровне (DEBUG); внешние — на INFO (полезный поток без DEBUG-флуда).
    Собственные хендлеры внешних логгеров снимаем, чтобы запись шла через единый root QueueHandler
    (JSON→journald), а не дублировалась/обходила формат.
    """
    for name in _EXTERNAL_LOGGER_NAMES:
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.disabled = False
        lg.propagate = True
        lg.setLevel(logging.INFO)


def setup_logging(log_level: str = "DEBUG") -> None:
    """Инициализировать logging. Идемпотентно: повторный вызов перезапускает listener."""
    global _listener

    shutdown_logging()

    _LOG_LEVEL_MAP: dict[str, int] = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    key = log_level.upper()
    if key not in _LOG_LEVEL_MAP:
        raise ValueError(f"unknown log level {log_level!r}, expected one of {sorted(_LOG_LEVEL_MAP)}")
    numeric_level = _LOG_LEVEL_MAP[key]
    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(10000)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(logging.Formatter("%(message)s"))

    queue_handler = QueueHandler(log_queue)
    queue_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
            foreign_pre_chain=_FOREIGN_PRE_CHAIN,
        )
    )

    _listener = QueueListener(log_queue, stderr_handler, respect_handler_level=False)
    _listener.start()

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(queue_handler)
    root.setLevel(numeric_level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # Безопасно с lazy ``logger`` ниже: модульный ``log = logger.bind(...)`` НЕ материализует
        # bound-логгер на импорте (до ``setup_logging``), поэтому кэш фиксирует уже сконфигурированный
        # JSON-логгер, а не дефолтный ``PrintLogger``. Даёт пер-вызовный выигрыш на rag-loop/FSM-потоках.
        cache_logger_on_first_use=True,
    )

    _configure_external_loggers()


def shutdown_logging() -> None:
    """Graceful stop. Вызывать при SIGTERM / engine stop."""
    global _listener
    if _listener is not None:
        _listener.stop()
        _listener = None


_structlog_logger = structlog.get_logger()


class _LazyBoundLogger:
    """Ленивая обёртка над ``structlog``-bound-логгером: реальный ``.bind(**binds)`` откладывается до
    ПЕРВОГО использования (первый ``log.info``/``debug``/…), а не на импорте модуля.

    Зачем: ``structlog`` BoundLoggerLazyProxy материализуется именно на ``.bind()`` — против активной
    конфигурации в этот момент. 40+ модулей делают ``log = logger.bind(stage=...)`` на уровне модуля; если
    ``.bind()`` выполнить ДО ``setup_logging()``, он зафиксирует дефолтный ``PrintLogger`` (текст в stdout
    мимо JSON/journald и мимо уровней внешних логгеров). Откладывая ``.bind()`` до первого вызова (всегда
    после ``setup_logging`` в обоих entrypoint-ах), снимаем это скрытое import-order-условие. Результат
    кэшируется → накладные только на первый вызов.
    """

    def __init__(self, **binds: object) -> None:
        self.__dict__["_binds"] = binds
        self.__dict__["_real"] = None

    def __getattr__(self, item: str):  # noqa: ANN204 — делегирование произвольных атрибутов bound-логгера
        real = self.__dict__.get("_real")
        if real is None:
            real = _structlog_logger.bind(**self.__dict__["_binds"])
            self.__dict__["_real"] = real
        return getattr(real, item)


class _LazyLoggerFactory:
    """Модульный ``logger``: ``.bind(**)`` → ленивая обёртка; прямой ``logger.info(...)`` тоже ленив."""

    def bind(self, **binds: object) -> _LazyBoundLogger:
        return _LazyBoundLogger(**binds)

    def __getattr__(self, item: str):  # noqa: ANN204
        return getattr(_LazyBoundLogger(), item)


# Публичный ``logger``: ленивый, чтобы модульные ``log = logger.bind(...)`` не материализовались на импорте.
logger = _LazyLoggerFactory()
