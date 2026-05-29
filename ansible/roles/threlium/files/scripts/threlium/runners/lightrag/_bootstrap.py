"""Bootstrap-индексация файлов из $THRELIUM_HOME/knowledge/ в LightRAG при старте engine.

Корпус стримится батчами: метаданные файлов собираются лениво, содержимое читается
по одному батчу за раз (память ограничена размером батча, а не всего корпуса).

Дедупликация — ответственность LightRAG (``apipeline_enqueue_documents`` вызывает
``doc_status.filter_keys`` внутри ``ainsert``). Повторная загрузка при рестарте —
no-op на стороне RAG, без лишних LLM/embed вызовов.
"""
from __future__ import annotations

import asyncio
import hashlib
import itertools
import logging
import time
from collections.abc import Iterator, Sequence
from email import policy
from email.message import EmailMessage
from pathlib import Path

from lightrag import LightRAG

from threlium.settings import ThreliumSettings
from threlium.systemd_notify import notify_status
from threlium.types.lightrag_document_header import LightragDocumentHeader
from threlium.types.systemd_status import SystemdStatusBody

log = logging.getLogger(__name__)

_ALLOWED_SUFFIXES = frozenset((".md", ".txt", ".ttl", ".json", ".yaml", ".yml"))


def _doc_id_for_path(rel_path: str) -> str:
    h = hashlib.md5(rel_path.encode(), usedforsecurity=False).hexdigest()[:16]
    return f"knowledge:bootstrap:{h}"


def _wrap_as_rfc822(content: str, *, doc_id: str, filename: str) -> str:
    """Wrap raw file content in RFC822 with X-Threlium-Thread-Id for chunking compatibility."""
    msg = EmailMessage()
    msg[LightragDocumentHeader.THREAD_ID] = doc_id
    msg["Subject"] = filename
    msg.set_content(content.rstrip("\n"), subtype="plain", charset="utf-8")
    return msg.as_string(policy=policy.default).strip() + "\n"


def _iter_eligible_files(knowledge_dir: Path) -> Iterator[tuple[str, str]]:
    """Lazily yield (rel_path, doc_id) для подходящих файлов — без чтения содержимого."""
    for path in sorted(knowledge_dir.rglob("*")):
        if not path.is_file() or path.suffix not in _ALLOWED_SUFFIXES:
            continue
        rel = str(path.relative_to(knowledge_dir))
        yield rel, _doc_id_for_path(rel)


def _read_batch_documents(
    knowledge_dir: Path, batch: Sequence[tuple[str, str]]
) -> tuple[list[str], list[str], list[str]]:
    """(texts, ids, file_paths) для одного батча; пустые/нечитаемые файлы пропускаются."""
    texts: list[str] = []
    ids: list[str] = []
    file_paths: list[str] = []
    for rel, doc_id in batch:
        path = knowledge_dir / rel
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            log.warning("bootstrap_knowledge: cannot read %s: %s", path, e)
            continue
        if not content.strip():
            continue
        texts.append(_wrap_as_rfc822(content, doc_id=doc_id, filename=rel))
        ids.append(doc_id)
        file_paths.append(rel)
    return texts, ids, file_paths


async def bootstrap_knowledge_dir(
    rag: LightRAG,
    settings: ThreliumSettings,
    *,
    lock: asyncio.Lock | None = None,
) -> int:
    """Index knowledge files батчами по ``lightrag.insert_batch`` (как drain).

    Стримим корпус: ``_iter_eligible_files`` отдаёт только метаданные ``(rel, doc_id)``,
    а содержимое читается и оборачивается в RFC822 (``_read_batch_documents``) уже внутри
    одного батча — в памяти живёт максимум один батч контента, а не весь корпус.

    Возвращает число документов, фактически переданных в ``ainsert`` (0 — если каталога
    нет / нет подходящих или непустых файлов). Сам ``ainsert`` идемпотентен: уже
    проиндексированные документы отфильтровываются ``doc_status``, новых LLM/embed-вызовов
    не будет.

    Параллельность — на уровне инстанса ``rag`` (``build_rag``): в проде
    ``max_parallel_insert`` / ``*_max_async`` > 1 (LightRAG распараллеливает документы
    внутри одного ``ainsert``), в e2e (``litellm_route_correlation``) — 1 (сериализация).
    Здесь не дублируем эту логику: бьём корпус на батчи фиксированного размера и
    отдаём каждый батч целиком в LightRAG.

    ``lock`` (общий ``_drain_lock``) берётся **на каждый батч**, а не на весь bootstrap:
    так фоновый drain входящих сообщений может чередоваться с bootstrap-батчами и не
    голодает на длинном корпусе. Два параллельных ``ainsert`` на одном инстансе при этом
    исключены (общий ``shared_storage``-lock).
    """
    knowledge_dir = Path(settings.home) / "knowledge"
    if not knowledge_dir.is_dir():
        log.info("bootstrap_knowledge: directory not found, skipping: %s", knowledge_dir)
        return 0

    candidate_files = list(_iter_eligible_files(knowledge_dir))
    if not candidate_files:
        log.info("bootstrap_knowledge: no eligible files in %s", knowledge_dir)
        return 0

    batch_size = max(1, settings.lightrag.insert_batch)
    total = len(candidate_files)
    notify_status(SystemdStatusBody.lightrag_bootstrap_indexing(doc_count=total))
    done = 0
    for batch in itertools.batched(candidate_files, batch_size):
        texts, ids, file_paths = _read_batch_documents(knowledge_dir, batch)
        if not texts:
            continue
        t0 = time.monotonic()
        if lock is not None:
            async with lock:
                await rag.ainsert(texts, ids=ids, file_paths=file_paths)
        else:
            await rag.ainsert(texts, ids=ids, file_paths=file_paths)
        done += len(texts)
        log.info(
            "bootstrap_knowledge: batch indexed (%d/%d, %.1fs) from %s",
            done,
            total,
            time.monotonic() - t0,
            knowledge_dir,
        )
    return done
