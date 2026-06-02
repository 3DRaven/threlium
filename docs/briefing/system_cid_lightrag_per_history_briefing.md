# Брифинг: FSM `<system>` + строгий CLI + LightRAG per-history chunking

Документ для передачи контекста в другую сессию. Нормативные контракты остаются в
[`CONTEXT_CONTRACT.md`](../CONTEXT_CONTRACT.md) §2, §7 и [`INDEX.md`](../INDEX.md) §5b;
здесь — что сделано, зачем и что не трогали. См. также
[`summarize_context_overflow_e2e_briefing.md`](summarize_context_overflow_e2e_briefing.md)
(enrich/summarize и `concat_history_parts_text` — не LightRAG ingest),
[`e2e_toolkit_refactor_briefing.md`](e2e_toolkit_refactor_briefing.md) (`toolkit/lightrag_assert.py`, `toolkit/notmuch_assert.py`).

**Дата работы:** 2026-06-02  
**План (не редактировать):** `.cursor/plans/system_cid_maildir_read_bd86a493.plan.md`  
**Smoke e2e:** `test_mailflow_e2e.py::test_full_mailflow_deploy_and_pipeline` — **PASSED** (~38 с), лог `test-runs/.smoke_system_cid_lightrag.log`

---

## Цель эпика

Довести код до конца [`CONTEXT_CONTRACT.md`](../CONTEXT_CONTRACT.md):

1. **Фаза 1:** payload между FSM-стадиями и ретроспективное чтение из Maildir — только через `<system>`-CID, не через первый `text/plain`. CLI JSON — строгий `msgspec`, без salvage-regex.
2. **Фаза 2:** LightRAG ingest/chunking — по отдельным `<history>`-частям, без слияния в одно plain-тело и без `extract_plain_body` в chunking.

Меняется **слой чтения MIME** и **формат ingest-строки**, граф FSM и маршруты ошибок — без изменений.

---

## Фаза 1 — FSM payload и CLI JSON

### Проблема

| Модуль | Было | Стало |
|--------|------|--------|
| `states/cli_resume.py` | `extract_plain_body` с диска (intent по IRT) | `system_part_text_from_path` |
| `task/collect.py` | `extract_plain_body` для `tasks_upsert` | `system_part_text_from_path` (fail-fast) |
| `response/collect.py` | `extract_plain_body` для append/edit | `system_part_text_from_path` (fail-fast) |
| `cli_fsm.py` | `parse_json_loose` (regex `{...}`) | `msgspec.json.decode` → `CliIntentEnvelope` |

### Ключевые изменения в коде

**`mime_reform.py`**

- Добавлен `system_part_text_from_path(path)` — обёртка `email_message_from_path` + `system_part_text`, fail-fast как у `system_part_text`.

**`types/cli_mail.py`**

- `CliIntentEnvelope` — обёртка `{"cli": CliIntentPayload}` для strict decode.

**`cli_fsm.py`**

- Удалён `parse_json_loose`.
- `parse_cli_intent_payload(text)` → `msgspec.json.decode(..., type=CliIntentEnvelope)`; невалидный JSON/схема/пустой `argv` → `None` (те же ветки стадий).

**`cli_resume.py`**

- `try/except RuntimeError` при чтении intent: отсутствие `<system>` → пустая строка → `parse_cli_intent_payload` → `None` → **сохранён** graceful-маршрут `enrich_fast` после долгого HITL-разрыва.
- Удалена `_extract_decoded_body_from_maildir_file`.

**`task/collect.py`**, **`response/collect.py`**

- Без `try/except` на уровне MIME — как у handler'ов `tasks_upsert` / `response_edit`.
- Ветка `<task-init>` в `task/collect` **не тронута** (`extract_part_by_content_id(EnrichPartId.TASK_INIT)`).

**Доки (точечно):** `ingress_hitl_resolve.py` module doc, `tasks_upsert.py` module doc, `CONTEXT_CONTRACT.md` строка `cli_resume` в таблице CLI/HITL.

### Инварианты FSM (не ломались)

- `cli_resume → enrich_fast` при bad/not-found intent.
- `response_edit → ingress`, `tasks_upsert → ingress` при invalid args — в handler'ах.
- Канонический JSON из `cli_intent/email_body.j2` (`| tojson`) валиден для `msgspec`.

### Не трогали

- `states/ingress.py` bridge: `extract_plain_body` для **внешнего** тела → `<system>` (граница ingress).

---

## Фаза 2 — LightRAG ingest и chunking

### Было

```
Maildir → concat_history_parts_text → ingest_body.j2 → одно text/plain
       → chunking: extract_plain_body → одно окно токенов
```

Границы distill-частей (`## User intent`, `## User reply language`, …) терялись.

### Стало

```
Maildir → render_lightrag_ingest_document
       → synthetic multipart/mixed: N × inline text/plain, CID <{sha256(body)}@history>
       → chunking: iter_history_parts → per-part (малый part = 1 chunk, большой = window/overlap)
```

### Ключевые изменения в коде

**`lightrag_ingest.py`**

- Убраны `concat_history_parts_text`, `render_prompt(LIGHTRAG_INGEST_BODY, ...)`, `set_content` одного plain.
- Synthetic: `_copy_graph_headers` + `X-Threlium-Thread-Id`; для каждой непустой части `iter_history_parts(msg)` → `_make_inline_text_part(EnrichContentId.from_history_body(text), text)`.

**`lightrag_chunking.py`**

- Удалён импорт и использование `extract_plain_body`.
- Цикл по `iter_history_parts(em)`; сквозная нумерация `X-Threlium-LightRAG-Chunk` 1..N по документу.
- Нет непустых `<history>` → `ValueError` (fail-fast; drain-gate `message_has_history` должен был отфильтровать раньше).

**`runners/lightrag/_bootstrap.py`**

- `_wrap_as_rfc822`: `multipart/mixed` с одной `<history>`-частью (тот же путь chunking, без fallback на корневой `text/plain`).

**Удалено**

- `ansible/roles/threlium/files/prompts/lightrag/ingest_body.j2`
- `PromptPath.LIGHTRAG_INGEST_BODY` в `types/prompt_path.py`
- запись в `ansible/roles/threlium/vars/main.yml` → `threlium_required_prompts`

**Доки (точечно):** `CONTEXT_CONTRACT.md` §7, `INDEX.md` §5b (3 места), `TYPES.md`, `ARCHITECTURE.md`, `MEMORY_TABLE.md`, `prompts.py` (пример в docstring).

**ADR:** каталог `docs/adr/` удалён из репо — ссылки на `0001-lightrag-ingest-chunking-enrich.md` в INDEX/TYPES заменены на `CONTEXT_CONTRACT.md` §7 где правили; полный проход по всем битым ссылкам на ADR не делался.

---

## Проверка

| Что | Результат |
|-----|-----------|
| `python -m py_compile` изменённых модулей | OK |
| Smoke `test_full_mailflow_deploy_and_pipeline` | **PASSED** 38.13 s |
| Полный набор e2e из плана (cli_resume HITL, response_buffer, task_ledger, cli_discovery, knowledge_bootstrap, …) | **не гонялся** (по запросу — один smoke) |

Обновление SUT для smoke: `docker cp` 14 файлов в `/home/threlium/threlium/agent/scripts/threlium/…`, удаление `ingest_body.j2` в `/home/threlium/threlium/data/prompts/lightrag/`, без ansible `refresh`.

---

## Файлы (чеклист для ревью / cherry-pick)

```
ansible/roles/threlium/files/scripts/threlium/
  mime_reform.py              # +system_part_text_from_path
  cli_fsm.py                  # strict msgspec, −parse_json_loose
  types/cli_mail.py           # +CliIntentEnvelope
  types/__init__.py           # export CliIntentEnvelope
  types/prompt_path.py        # −LIGHTRAG_INGEST_BODY
  states/cli_resume.py
  states/tasks_upsert.py      # doc only
  task/collect.py
  response/collect.py
  ingress_hitl_resolve.py     # doc only
  lightrag_ingest.py
  lightrag_chunking.py
  runners/lightrag/_bootstrap.py
  prompts.py                  # docstring example

ansible/roles/threlium/vars/main.yml   # −ingest_body.j2 из required_prompts
ansible/roles/threlium/files/prompts/lightrag/ingest_body.j2  # DELETED

docs/CONTEXT_CONTRACT.md    # cli_resume row + §7
docs/INDEX.md                 # §5b ingest contract (3 edits)
docs/TYPES.md, ARCHITECTURE.md, MEMORY_TABLE.md
```

---

## Out of scope (явно)

- `find_cli_intent_maildir_path_from_in_reply_to_ancestors` → `EmailMessage` (остаётся path-based).
- Unit-тесты (в проекте только e2e).
- Перенос `X-Threlium-Origin` / score history-частей в synthetic ingest.
- Полный прогон e2e-матрицы из плана; пересборка WireMock embedding-стабов при сдвиге границ чанков (риск отмечён в плане, smoke не выявил).

---

## Следующие шаги (если продолжать)

1. **Bake/deploy:** закрепить изменения в образе SUT (`wipe_bake` или `wipe_sync`), не только `docker cp`.
2. **E2e из плана:** по желанию — `test_mailflow_live_only` (cli_resume HITL), `test_response_buffer_e2e`, task_ledger, `test_cli_discovery_chain_e2e`, `test_cli_route_collision`, `test_lightrag_index_filter_e2e`, `test_knowledge_bootstrap_live_e2e`.
3. **Поиск битых ссылок:** `rg 'adr/0001|ingest_body\.j2'` по `docs/` и тестам.
4. **`concat_history_parts_text`:** остаётся для enrich/summarize/Jinja `history_text` — это **не** LightRAG ingest; см. [`summarize_context_overflow_e2e_briefing.md`](summarize_context_overflow_e2e_briefing.md) (фикс `history_body_chars`).

---

## Быстрые отсылки по контракту

| Вопрос | Ответ |
|--------|--------|
| Где payload FSM? | `<system>` — `system_part_text` / `system_part_text_from_path` |
| Где память для LLM / граф? | `<history>` — `iter_history_parts`, `history_part_text` |
| Как читается `user_reply_language` downstream? | Текст в `<history>` (`## User reply language`), не повторный парсинг |
| LLM routing | Только `tool_calls`; сырой текст → ошибка |
| LightRAG drain gate | `message_has_history` в `_drain.py` |
