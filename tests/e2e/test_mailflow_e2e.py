"""Полный почтовый контур на **уже поднятом** e2e-стеке: без compose, без bake, без Ansible в этом модуле.

**Тест-кейс: сквозной почтовый happy-path (SMTP → бридж → notmuch → WireMock (OpenAI-стабы) → ответ).** Проверки
выполняются после того, как входящее письмо уже прошло цепочку до FSM.

**Цель.** Внешнее письмо принято, проиндексировано, обработано конечным автоматом с вызовом подмены
LLM через общий сервис WireMock, тред дошёл до ожидаемых стадий, пользователю ушёл ответ обратно на
тестовый почтовый сервер.

1. **Стек.** Фикстура ``deployed_stack`` зависит от session ``compose_stack`` (autouse в ``conftest.py``):
   стек поднимается или переиспользуется как в ``docs/TESTING.md``. Этот модуль **никогда** не вызывает
   ``ansible-playbook`` и bake сам по себе.

2. **WireMock.** Лидер compose регистрирует ``compose_bootstrap`` (State + readiness); стабы
   ``wiremock_stubs/test_mailflow_e2e/``; изоляция — **WireMock State** по ключу из заголовка
   ``X-Threlium-Thread-Root`` (ключ контекста State через ``state-matcher`` и шаблон заголовка запроса).
   Узкие ``bodyPatterns`` — модель и фрагменты промптов продукта.

3. **Инъекция входящего письма.** Уникальные ``Message-ID``; вычисляемый ``correlation_key``
   (= ``X-Threlium-Thread-Root``);
   тема письма не используется как якорь теста (дефолт SMTP — см. ``smtp_inject.py``). Тело несёт маркеры для маршрута
   ингресса (не для матчинга стабов WireMock). Письмо отправляется в тот же SMTP-контур,
   что и реальный входящий трафик в e2e. Дальше три последовательных ожидания: на стороне GreenMail
   письмо ушло из INBOX (UID MOVE в processed-папку) после заборки IMAP-бриджем; снимается диагностический снимок почтовых
   каталогов и пользовательских служб на системе под тестом; в Maildir фиксируется активность
   конечного автомата по внутреннему идентификатору письма в notmuch (без subject в запросах).

4. **Проверки успешного прохождения.** По внутреннему идентификатору письмо видно в индексе notmuch.
   Диагностика пайплайна по якорю. Убеждаются, что в журнале WireMock есть POST
   ``/chat/completions`` с ``stub_tag`` и тем же ``X-Threlium-Thread-Root`` (или теле журнала).
   Проверяется, что весь тред с этим якорем прошёл ожидаемые стадии автомата. Наконец, во входящих
   тестового пользователя на GreenMail появляется ответ, сопоставимый с исходным входящим по внешнему
   идентификатору письма. При сбое перед повторным выбросом ошибки в журнал выводится сводка артефактов для разборки.

Маркер ``mailflow`` — логический тег полного контура; выборку по умолчанию не фильтрует.
Подготовка образа и ``site.yml`` — вне этого файла (см. ``docs/TESTING.md`` §5,
``tests/e2e/wipe_sync.py``, ручной compose).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .helpers import (
    MailflowScenarioSpec,
    assert_full_mailflow_pipeline,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    REPO_ROOT,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"

MAILFLOW_SPEC = MailflowScenarioSpec(
    label="mailflow_e2e",
    raw_id_prefix="mf-ing-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_mailflow_e2e",
    stub_tag="stub-mailflow-e2e-01",
    body_head="e2e body",
    min_chat_completion_posts=1,
    min_embedding_posts=7,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
)


@pytest.fixture()
def mailflow_processed_stack(deployed_stack: str) -> object:
    """WireMock upsert → inject → \\Seen → FSM activity.

    Yields ``(project_name, raw_message_id, canonical_message_id, notmuch_id_inner, stub_tag,
    correlation_key)``.
    """
    with mailflow_inject_and_wait(MAILFLOW_SPEC, deployed_stack) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_full_mailflow_deploy_and_pipeline(
    mailflow_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """Живой стек → SMTP → IMAP bridge (IDLE) → notmuch → WireMock → архив (тред) → ответ в GreenMail."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        mailflow_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            MAILFLOW_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
