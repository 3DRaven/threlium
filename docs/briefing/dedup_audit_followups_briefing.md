# Брифинг: dedup-refactor audit follow-ups (память, CRDT, MID-guard, hygiene)

Документ для передачи контекста в другую сессию — особенно при работе со **стадиями FSM**, которые не были центром эпика, но затронуты общими контрактами (emit, CRDT, MIME, bridges).

Нормативные контракты — в [`CONTEXT_CONTRACT.md`](../CONTEXT_CONTRACT.md) §2–§3, §7–§8,
[`MEMORY_TABLE.md`](../MEMORY_TABLE.md) §1–2, [`TYPES.md`](../TYPES.md);
план (не редактировать): `.cursor/plans/dedup_audit_follow-ups_959fb822.plan.md`.

См. также параллельные брифинги (соседние эпики, не смешивать scope):

- [`system_cid_lightrag_per_history_briefing.md`](system_cid_lightrag_per_history_briefing.md) — `<system>` / per-history LightRAG ingest
- [`e2e_toolkit_refactor_briefing.md`](e2e_toolkit_refactor_briefing.md) — harness `tests/e2e/toolkit/`
- [`enrich_task_hypotheses_briefing.md`](enrich_task_hypotheses_briefing.md) — второй LLM в enrich

**Дата работы:** 2026-06-03  
**Статус кода:** в репозитории (коммиты `de7a7d4` … `2cff9bc`, `eba9072`; далее — см. § «После эпика»)  
**Audit smoke e2e:** **6/6 PASS** (~3:27) — `memory_query`, `formal_reason_chain`, `response_observe`, `greenmail_inbox_delivery`, `live_memory_table` ×2

---

## Цель эпика

Закрыть хвосты параллельного аудита dedup-refactor (4 субагента): **C0.4 durable memory** через модель **request_echo**, унификация CRDT/MID-guard/config kwarg, hygiene типов и e2e-стабильность — **без** смены графа FSM и маршрутов ошибок.

| Приоритет | Суть | Стадии / модули |
|-----------|------|-----------------|
| **P0** | Память: callee владеет `<history>` на L_M2; L_M1 system-only | `reasoning`, `thread_memory`, `global_memory`, `_memory_write`, `enrich_fast`, LightRAG drain |
| **P0** | `memory_query` через общий LightRAG query API | `memory_query`, `runners/lightrag/aquery.py` |
| **P0** | Ingress preserving relay без stale `<system>` | `ingress` (см. § «После эпика» — переписан) |
| **P1** | `require_fsm_message_id` вместо ручного parse | enrich, reasoning, subagent_end, egress_*, cli_resume, response_*, tasks_upsert, enrich_fast |
| **P1** | CRDT facade `crdt_ledger_state` | `tasks_upsert`, `response_finalize`, `response_observe`, `enrich_fast` |
| **P1** | Internal helpers ingress/egress → `config=` | `ingress`, `egress_telegram`, `egress_matrix` |
| **P2** | Types / bootstrap hygiene | `context_token_count`, Matrix structs, `EnrichContentId`, `_bootstrap.py` |

---

## P0 — C0.4: durable memory через request_echo

### Что было неверно в первоначальном audit-fix (отменено)

| Ошибка | Почему неверно |
|--------|----------------|
| «Парная `<history>` на L_M1» / rewrite Maildir | [`INDEX.md` §5.5.3](../INDEX.md): **bytes входящего файла не модифицируются** при settle |
| `history=durable_history` в `reasoning → memory` | [`CONTEXT_CONTRACT.md` §3](../CONTEXT_CONTRACT.md): tool-target emit **только `<system>`** |
| «L_M1 должно индексироваться LightRAG» | L_M1 = system-only → **`lightrag_skipped` ожидаем** |
| «Индексируем только L_M1» | Индексируемый факт — settled **L_M2** (исходящее memory-стадии с request_echo) |

### Каноническая модель (для всех memory-стадий)

```text
reasoning --L_M1(system note)--> thread_memory|global_memory
           --L_M2(request_echo <history>, origin=reasoning)--> enrich_fast
           --L_M3(relay)--> reasoning
LightRAG drain: skip L_M1, index L_M2 в enrich_fast/Maildir/cur/
```

**Ключевые изменения:**

| Модуль | Было | Стало |
|--------|------|--------|
| `states/reasoning.py` | (ошибочно) durable history на memory-target | `build_fsm_step_to_stage(..., system=...)` без `history=` |
| `states/_memory_write.py` | Дублирование в `thread_memory` / `global_memory` | Общий `emit_memory_note_to_enrich_fast`: `system_part_text` → `render_prompt(base.j2)` → `emit_to_enrich_fast(request_echo=...)` |
| `states/thread_memory.py`, `global_memory.py` | Полные handler'ы | Re-export `main` из `_memory_write` |
| `prompts/thread_memory/base.j2`, `global_memory/base.j2` | — | Шаблон тела для request_echo (`{{ note }}`) |
| `docs/MEMORY_TABLE.md`, `CONTEXT_CONTRACT.md` | Legacy «парная history на L_M1» | L_M2 indexed, L_M1 system-only OK |

**Инварианты для соседних стадий:**

- `enrich_fast` **не** перештамповывает `X-Threlium-Origin` на memory-echo (origin уже `reasoning`).
- Drain gate — **`message_has_history`**, не `To:`-стадия ([`lightrag_drain_query.py`](../../ansible/roles/threlium/files/scripts/threlium/lightrag_drain_query.py)).
- Fast-cycle видимость note — через relay L_M3 в `reasoning`; полнотекстовый RAG — со следующего полного `enrich`.

---

## P0 — `memory_query`

| Было | Стало |
|------|--------|
| Hardcoded `QueryParam(...)` в handler | `build_lightrag_query_param(config)` + `config.lightrag.query_api` |
| Дублирование dispatch | Общий `run_lightrag_aquery` из `runners/lightrag/aquery.py` (тот же путь, что `enrich`) |

Стадия по-прежнему: `memory_query → enrich_fast` с observation в request_echo / callee history ([`memory_query.py`](../../ansible/roles/threlium/files/scripts/threlium/states/memory_query.py)).

---

## P0 — Ingress preserving path (историческая ветка)

В коммите `160b88b` на **preserving relay** (internal `From:` → enrich без distill) добавлен strip stale `<system>`:

```python
relay = email_without_system_parts(msg) if message_has_system(msg) else msg
emit_transition_simple_step_preserving_payload(relay, ...)
```

**После эпика** ingress переписан (`2de0795`): gateway **только для bridge** + `user_intent` distill; preserving relay для internal стадий убран. Хелпер `email_without_system_parts` остаётся в `mime_reform.py`, **call-site сейчас нет** — при следующем preserving-relay паттерне использовать его, не дублировать.

---

## P1 — `require_fsm_message_id` (единый MID-guard)

Централизован в [`nm.py`](../../ansible/roles/threlium/files/scripts/threlium/nm.py):

```python
mid_w, inner = require_fsm_message_id(msg, "<stage>")
```

**Стадии с guard (inner id нужен для CRDT / egress / логов):**

`enrich`, `enrich_fast`, `reasoning`, `subagent_end`, `tasks_upsert`, `response_observe`, `response_edit`, `response_finalize`, `egress_email`, `egress_matrix`, `cli_resume`.

**Стадии без guard** (inner id не используется в handler): `ingress`, `reflect`, `formal_reason`, `memory_query`, `subagent_intent`, `cli_intent`, `cli_exec`, `egress_router`, `egress_telegram`, `summarize_*`, memory re-exports — **это нормально**, не «недоделка» миграции.

При добавлении новой стадии с CRDT-collect или egress leaf-id — **сразу** `require_fsm_message_id`, не `RfcMessageIdWire.parse_present_from_email` вручную.

---

## P1 — CRDT facade

[`ledger_context_parts.py`](../../ansible/roles/threlium/files/scripts/threlium/ledger_context_parts.py):

- `crdt_ledger_state(inner)` → `CrdtLedgerState(response_ops, task_ops, task_ledger)`
- `trimmed_crdt_state_texts`, `ledger_context_parts` — для enrich context buckets

**Потребители (было дублирование collect/reduce):**

| Стадия | Использование |
|--------|----------------|
| `tasks_upsert` | parent frame через `crdt_ledger_state(parent_inner)` |
| `response_finalize` | `task_ledger` для finalize body |
| `response_observe` | trimmed state texts |
| `enrich_fast` | `response_ops` + relay patch |

---

## P1 — `config=` vs `settings=` в internal helpers

| Слой | Kwarg |
|------|--------|
| `states/*/main(..., *, config: ThreliumSettings)` | **`config=`** — публичный контракт стадии |
| Internal helpers ingress/egress | **`config=`** (мигрировано в `160b88b`) |
| `fsm_emit` / `build_fsm_step_to_stage` / semantic emit | **`settings=config`** — API emit-слоя **не меняли** |

Новые internal helper'ы стадий — **`config=`**; на границе с emit передавать `settings=config`.

---

## P1 — Bridges (optional, deferred factories)

Matrix/Telegram ingress loops **не** переведены на `matrix_client()` / `telegram_bot()` factories — разный lifecycle (long-lived `/sync` vs one-shot egress). Задокументировано комментариями в [`bridges/matrix.py`](../../ansible/roles/threlium/files/scripts/threlium/bridges/matrix.py), [`bridges/telegram.py`](../../ansible/roles/threlium/files/scripts/threlium/bridges/telegram.py).

**Deferred отдельным PR:** `egress_channel_deliver` template (план §3.3).

---

## P2 — Types / bootstrap hygiene

| Область | Изменение |
|---------|-----------|
| `context_budget.py` | `part_origin_stage(part)` для веса history-частей |
| Matrix wire | Typed `msgspec.Struct` для inbound room message body ([`types/matrix_client_room_message.py`](../../ansible/roles/threlium/files/scripts/threlium/types/matrix_client_room_message.py)) |
| `EnrichContentId` | Sets / `from_history_body` — единый CID для `<history>` |
| `runners/lightrag/_bootstrap.py` | `threlium.logutil.logger` вместо ad-hoc logging |
| Dead routing helper | Удалён неиспользуемый helper из bridges routing |

---

## E2e и стабильность shared-сессии

**Проблема:** `test_greenmail_inbox_delivery_smoke` оставляет письмо, которое email-bridge забирает **после** teardown → FSM без `ingress_distill` stub → crash-loop → засорение WireMock journal для следующих тестов.

**Fix:** `tests/e2e/wiremock_stubs/test_greenmail_delivery_e2e/075_chat_ingress_distill.json` (`eba9072`).

Audit smoke (6 тестов, одна pytest-сессия, shared stack): **6/6 PASS**.

---

## После эпика (смежные коммиты, не путать с audit scope)

| Коммит | Суть | Влияние на «другие стадии» |
|--------|------|----------------------------|
| `2de0795` | Ingress: bridge-only + `user_intent` distill | Internal preserving relay **снят**; см. [`ingress.py`](../../ansible/roles/threlium/files/scripts/threlium/states/ingress.py) |
| `cf22a51` | IRT preserve на enrich relay | SUBAGENT_TABLE §4 — managed patch на envelope, не на stripped relay |
| `c3ab37c` | Shared `aquery` + adapter bridge retries | `enrich`, `memory_query`, formal_reason tool paths |

---

## Чеклист файлов (ревью / cherry-pick)

```text
ansible/roles/threlium/files/scripts/threlium/
  states/_memory_write.py          # shared memory write + request_echo
  states/thread_memory.py          # re-export
  states/global_memory.py          # re-export
  states/reasoning.py              # memory-target: system only
  states/memory_query.py           # build_lightrag_query_param
  states/tasks_upsert.py           # crdt_ledger_state + mid guard
  states/response_finalize.py      # crdt_ledger_state + mid guard
  states/response_observe.py       # crdt facade
  states/enrich_fast.py            # crdt facade + mid guard
  states/enrich.py                 # mid guard
  states/subagent_end.py           # mid guard
  states/egress_email.py           # mid guard (2cff9bc)
  states/egress_matrix.py          # mid guard + config=
  states/cli_resume.py             # mid guard (2cff9bc)
  ledger_context_parts.py          # NEW crdt facade
  nm.py                            # require_fsm_message_id
  mime_reform.py                   # +email_without_system_parts (no call-site)
  context_budget.py                # part_origin_stage
  runners/lightrag/aquery.py       # build_lightrag_query_param
  bridges/matrix.py, telegram.py   # lifecycle comments

ansible/roles/threlium/files/prompts/
  thread_memory/base.j2, global_memory/base.j2

tests/e2e/wiremock_stubs/test_greenmail_delivery_e2e/
  075_chat_ingress_distill.json

docs/MEMORY_TABLE.md, CONTEXT_CONTRACT.md, TYPES.md
```

---

## Out of scope (явно deferred)

- **`egress_channel_deliver`** — общий шаблон egress (план §3.3).
- **Ingress poll → client factories** — только документация lifecycle.
- **§1.6 enrich_context bucket `<thread-memory>`** для system-only L_M1 — optional, e2e не проверяет явно.
- **Unit-тесты** — в проекте только e2e.
- **Полная унификация `settings=`** на emit API — сознательно не трогали.

---

## Быстрые отсылки для работы со стадиями

| Вопрос | Ответ |
|--------|--------|
| Куда reasoning шлёт memory note? | Только `<system>` на L_M1; **не** `history=` |
| Кто формирует `<history>` для LightRAG по памяти? | Callee (`_memory_write`) через `request_echo` на L_M2 |
| L_M1 в drain? | `lightrag_skipped` (нет `<history>`) — **OK** |
| L_M2 в drain? | Index в `enrich_fast/` после settle |
| CRDT на стадии | `crdt_ledger_state(inner)` — не дублировать collect/reduce |
| Message-ID на стадии | `require_fsm_message_id(msg, "stage")` если нужен inner |
| Kwarg settings в стадии | `main(..., config=)`; emit: `settings=config` |
| Preserving relay + stale system | `email_without_system_parts` (если снова появится relay без distill) |

---

## Следующие шаги (если продолжать)

1. Удалить или подключить **dead code** `email_without_system_parts` (после стабилизации ingress distill).
2. **`egress_channel_deliver`** — отдельный PR по плану §3.3.
3. Полный `run_individual_e2e.sh` после крупных ingress/enrich изменений (не только audit smoke 6/6).
