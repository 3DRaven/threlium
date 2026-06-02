"""Полный почтовый контур на **уже поднятом** e2e-стеке: без compose, без bake, без Ansible в этом модуле.

**Тест-кейс: сквозной почтовый happy-path (SMTP → бридж → notmuch → WireMock (OpenAI-стабы) → ответ).** Проверки
выполняются после того, как входящее письмо уже прошло цепочку до FSM.

**Цель.** Внешнее письмо принято, проиндексировано, обработано конечным автоматом с вызовом подмены
LLM через общий сервис WireMock, тред дошёл до ожидаемых стадий, пользователю ушёл ответ обратно на
тестовый почтовый сервер. Happy-path использует **response_finalize Mode 1** (пустой response buffer,
только ``content`` из tool call → ``egress_email`` без сборки из буфера).

1. **Стек.** Session ``compose_stack`` + per-test ``e2e_runtime`` (autouse в ``conftest.py``):
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

Подготовка образа и ``site.yml`` — вне этого файла (см. ``docs/TESTING.md`` §5,
``tests/e2e/wipe_sync.py``, ручной compose).
"""
from __future__ import annotations

from pathlib import Path

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .toolkit import (
    E2EComposeRuntime,
    MailflowScenarioSpec,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    poll_until,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"

MAILFLOW_SPEC = MailflowScenarioSpec(
    label="mailflow_e2e",
    raw_id_prefix="mf-ing-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_mailflow_e2e",
    stub_tag="stub-mailflow-e2e-01",
    body_head="e2e body",
    min_chat_completion_posts=3,
    min_embedding_posts=5,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="ok from llm-mock",
)


def _assert_egress_reply_excludes_internal_mime(project: str, *, raw_id: str) -> None:
    """External SMTP reply must not leak ``@history`` / ``@system`` MIME parts (egress_email purity)."""
    import imaplib
    from .toolkit import E2E_FETCHMAIL_PASS, E2E_GREENMAIL_REPLY_USER
    from .mail_wire import e2e_parse_rfc822

    rt = discover_runtime(project, repo_root=REPO_ROOT)
    user_inner = raw_id.strip().strip("<>").lower()

    def _fetch_body() -> str | None:
        with imaplib.IMAP4(
            rt.greenmail_imap_host, rt.greenmail_imap_port, timeout=int(TIMEOUT_POLL_SHORT)
        ) as imap:
            imap.login(E2E_GREENMAIL_REPLY_USER, E2E_FETCHMAIL_PASS)
            typ, _ = imap.select("INBOX", readonly=True)
            if typ != "OK":
                return None
            typ, data = imap.search(None, "ALL")
            if typ != "OK" or not data or not data[0]:
                return None
            for num in reversed(data[0].split()):
                typ, msg_data = imap.fetch(num, "(RFC822)")
                if typ != "OK" or not msg_data:
                    continue
                raw = msg_data[0][1]
                if not isinstance(raw, bytes):
                    continue
                msg = e2e_parse_rfc822(raw)
                irt = (msg.get("In-Reply-To") or "").lower()
                if user_inner not in irt:
                    continue
                if msg.is_multipart():
                    parts = []
                    for p in msg.walk():
                        if p.get_content_type() == "text/plain":
                            pl = p.get_payload(decode=True)
                            parts.append(
                                pl.decode("utf-8", errors="replace")
                                if isinstance(pl, bytes)
                                else str(pl or "")
                            )
                    return "\n".join(parts)
                pl = msg.get_payload(decode=True)
                return (
                    pl.decode("utf-8", errors="replace")
                    if isinstance(pl, bytes)
                    else str(pl or "")
                )
        return None

    body = poll_until(
        _fetch_body,
        timeout=TIMEOUT_POLL_SHORT,
        desc="fetch GreenMail agent reply body",
    )
    assert body is not None
    lowered = body.lower()
    for forbidden in ("@history", "@system", "content-id:", "multipart/mixed"):
        assert forbidden not in lowered, (
            f"external SMTP reply leaked internal MIME marker {forbidden!r}"
        )
    log.info("egress_email_purity_verified", body_len=len(body))


def test_full_mailflow_deploy_and_pipeline(e2e_runtime: E2EComposeRuntime) -> None:
    """Mode 1 happy-path: SMTP → FSM → WireMock → egress without response buffer."""
    with mailflow_inject_and_wait(MAILFLOW_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                MAILFLOW_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            _assert_egress_reply_excludes_internal_mime(project, raw_id=raw_id)
            assert FsmStage.RESPONSE_APPEND.value not in MAILFLOW_SPEC.expect_notmuch_stage_folders
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
