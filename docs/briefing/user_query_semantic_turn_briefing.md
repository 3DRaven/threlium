# Брифинг: семантический user-query vs IRT-relay

Документ для передачи контекста в другую сессию. Нормативные контракты — в
[`CONTEXT_CONTRACT.md`](../CONTEXT_CONTRACT.md) §3, §4.1, §5,
[`FSM.md`](../FSM.md), [`MEMORY_TABLE.md`](../MEMORY_TABLE.md) §3,
[`TYPES.md`](../TYPES.md).

**Дата работы:** 2026-06-03  
**План (не редактировать):** `.cursor/plans/user-query_semantic_turn_3ea8c8aa.plan.md`  
**Предшествующий эпик:** [`doc_types_ingress_cleanup_briefing.md`](doc_types_ingress_cleanup_briefing.md) (bridge-system-only, typed emit; частично superseded этим эпиком).

---

## Проблема, которую закрыли

До эпика [`enrich_user_query.py`](../../ansible/roles/threlium/files/scripts/threlium/enrich_user_query.py) реализовал **IRT-relay**: internal стадии (`reflect`, `subagent_end`, validation errors, …) искали в предках `To: enrich@` старый `<user-query>` (часто bridge-текст) и прикрепляли его снова. Семантически это путало «turn внешнего пользователя» с «turn текущего enrich-цикла».

**Целевая семантика (теперь в коде и docs):**

- `<user-query>` на `To: enrich@` = **смысловая нагрузка текущего хода** для enrich→reasoning (local turn callee), даже если это не внешний пользователь.
- **IRT-обход для user-query не используется** — стадия в момент emit сама формирует CID.
- **Bridge-path:** сырой bridge `<system>` дублируется в `<history>` (`## Original user message`); в `<user-query>` — преобразованный VO (`distill_body`, опц. orphan-prefix).

```
bridge → ingress:  только <system>
ingress → enrich:  <user-query>=distill_body + <history>: original user + distill parts
reflect / subagent_* / validation → enrich@:  <user-query>=local turn (без IRT)
enrich consumer:   require_enrich_user_query_text(msg) — без изменений (читает текущий лист)
```

---

## Фаза 1 — Bridge ingress: original user в history

| Файл | Изменение |
|------|-----------|
| `prompts/ingress/distill_history_original_user.j2` | **новый** — heading `## Original user message`, тело = сырой bridge system |
| `types/prompt_path.py` | `INGRESS_DISTILL_HISTORY_ORIGINAL_USER` |
| `ansible/roles/threlium/vars/main.yml` | deploy шаблона в списке prompts |
| `fsm_emit_semantic.py` | `emit_bridge_distill_to_enrich(..., original_user_message=…)` — **первая** history-часть, затем distill parts |
| `states/ingress.py` | `original_user_message=user_query` (без orphan); `user_query=distill_body` (с orphan для distill LLM) |

Порядок `<history>` на ingress→enrich: **`original_user_message`** → `user_reply_language` → `step_back_notes` → `open_gaps` → `user_intent`.

Осознанное дублирование: bridge system может быть и в `## Original user message`, и в `<user-query>` (distill_body) — разные роли (unified chronology vs LightRAG user-message budget).

---

## Фаза 2 — Удалён IRT loader; per-stage local turn

### Удалено

- Модуль **`enrich_user_query.py`** целиком (`load_enrich_user_query_from_thread_irt`, `require_enrich_user_query_for_reenrich`).

### Per-stage источник `<user-query>`

| Стадия | Источник `EnrichUserQueryText` |
|--------|--------------------------------|
| `reflect` | Рендер `reflect/continue.j2` или `final.j2` (budget: `remaining ≥ 4` → continue); `previous_reasoning` = `system_part_text(msg)` |
| `subagent_intent` (success) | `system_part_text(msg)` (task) — без изменений |
| `subagent_intent` (budget exhausted) | rendered `subagent_intent/budget_exhausted.j2` в **`<user-query>`**, не в history |
| `subagent_end` | `system_part_text(msg)` (результат субагента); `relay_history_from=msg` сохранён |
| `response_edit` / `tasks_upsert` | через `emit_enrich_validation_error` — notice = user-query |
| `response_finalize` (task-gate / mode 4) | notice (`build_task_incomplete_notice` / `response_not_formed.j2`) = user-query |
| `summarize_memory` | без изменений — relay из `<system>` payload overflow-цикла |

### `emit_enrich_validation_error`

**Файл:** `fsm_emit_semantic.py`

- Параметр `user_query` у caller **убран**.
- Внутри: `body = render_prompt(...)` → `EnrichUserQueryText.require(..., raw=body)` → `emit_to_enrich(..., callee_history=None)`.
- Инвариант enrich **не ослаблен**: `<user-query>` по-прежнему обязателен на исходящем письме.

### Reflect prompts

- `reflect/continue.j2`, `reflect/final.j2` — убраны формулировки «routed through **ingress** and enrich».

---

## Фаза 3 — Документация (точечные правки)

| Файл | Что обновлено |
|------|----------------|
| `CONTEXT_CONTRACT.md` §3 | reflect, subagent_end, validation, subagent budget — local turn, не IRT |
| `CONTEXT_CONTRACT.md` §4.1 | original user history, порядок parts, fallback без IRT-relay |
| `CONTEXT_CONTRACT.md` §5 | overflow cycle: canonical turn = `<user-query>` CID, не «последняя history» |
| `FSM.md` | reflect: rendered body → user-query |
| `MEMORY_TABLE.md` §3 | матрица reflect: user-query = continue/final render |
| `TYPES.md` | `original_user_message` в порядке history; `EnrichUserQueryText` = current enrich-turn VO |
| `doc_types_ingress_cleanup_briefing.md` | пометка superseded + ссылка на этот план |

---

## Фаза 4 — E2e smoke (статус на момент брифинга)

**Запускали:**

```bash
pytest -n0 -vv -s tests/e2e/wipe_sync.py 2>&1 | tee test-runs/.semantic_uq_wipe_sync.log
```

**Плановый smoke после wipe_sync:**

```bash
pytest -n0 -s \
  tests/e2e/test_mailflow_e2e.py::test_full_mailflow_deploy_and_pipeline \
  tests/e2e/test_subagent_frame_isolation_e2e.py::test_subagent_task_ledger_frame_isolation \
  tests/e2e/test_subagent_frame_isolation_e2e.py::test_subagent_response_buffer_frame_isolation \
  tests/e2e/test_summarize_context_e2e.py::test_summarize_overflow_full_pipeline \
  2>&1 | tee test-runs/.semantic_uq_e2e.log
```

**Что проверить в mailflow при первом enrich (ingress→enrich):**

- `<history>` содержит `## Original user message` + distill parts.
- `<user-query>` = `distill_body` (orphan + bridge system при orphan-path).

**При падении:** `docker exec` → сравнить MIME в `stages/ingress/Maildir` vs `stages/enrich/Maildir` (history parts + user-query CID).

На момент написания брифинга `wipe_sync` мог ещё идти или быть прерван; итог smoke — по хвосту логов выше.

---

## Что сознательно не менялось

- **Read-path на enrich:** `require_enrich_user_query_text`, `enrich_incoming_user_text.j2`, `user_query_text` filter — без изменений.
- **`ingress_bridge_user_query.enrich_user_query_from_bridge_system`** — bridge `<system>` → VO; используется и для distill envelope, и как `original_user_message` (без orphan).
- **`summarize_memory`** — локальный relay overflow user-query из payload.
- **WireMock stubs / e2e assertions** — не трогали; при смене MIME-формы первого enrich может потребоваться обновление wiremock или assert'ов в mailflow.

---

## Риски и follow-up для следующих сессий

1. **Reflect e2e:** отдельного reflect→enrich теста может не быть; при добавлении — assert local user-query (не bridge text), body содержит `[Threlium reflect — continue|final]`.
2. **Validation errors:** reasoning теперь видит ошибку как `<user-message>` turn, не как вторую history-часть — поведение LLM может сдвинуться; мониторить `response_edit` / `tasks_upsert` e2e.
3. **subagent_end:** user-query = результат субагента; если e2e ожидал bridge text в user-query — обновить ожидания.
4. **Дублирование bridge text** в original history + user-query — by design; не «чинить» без согласования контракта §4.1.

---

## Grep-audit (не должно остаться)

```bash
rg 'require_enrich_user_query_for_reenrich|load_enrich_user_query_from_thread_irt|enrich_user_query\.py' .
rg 'relay из IRT|<user-query> relay' docs/
```

Допустимо: упоминания IRT для **history/unified/threading** (не для user-query relay).

---

## Связанные файлы (быстрый index)

| Область | Пути |
|---------|------|
| Choke-points emit | `fsm_emit_semantic.py` |
| Bridge ingress | `states/ingress.py`, `ingress_bridge_user_query.py` |
| Internal callees | `states/reflect.py`, `subagent_intent.py`, `subagent_end.py`, `response_edit.py`, `tasks_upsert.py`, `response_finalize.py` |
| Prompts | `ingress/distill_history_original_user.j2`, `reflect/{continue,final}.j2` |
| Types | `types/prompt_path.py`, `types/fsm_strings.py` (`EnrichUserQueryText`) |
