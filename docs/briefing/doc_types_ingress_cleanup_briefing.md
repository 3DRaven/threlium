# Брифинг: doc/types cleanup после bridge-system-only

Документ для передачи контекста в другую сессию. Нормативные контракты — в
[`CONTEXT_CONTRACT.md`](../CONTEXT_CONTRACT.md) §2, §4.1, [`FSM.md`](../FSM.md),
[`SUBAGENT_TABLE.md`](../SUBAGENT_TABLE.md), [`TYPES.md`](../TYPES.md);
см. также [`system_cid_lightrag_per_history_briefing.md`](system_cid_lightrag_per_history_briefing.md)
(предыдущий эпик `<system>`/CLI), [`summarize_context_overflow_e2e_briefing.md`](summarize_context_overflow_e2e_briefing.md)
(overflow + `SummarizeContextStagePayload`).

**Дата работы:** 2026-06-03  
**Superseded (частично):** семантика `<user-query>` как local enrich-turn — см. [`user_query_semantic_turn_briefing.md`](user_query_semantic_turn_briefing.md) (план `.cursor/plans/user-query_semantic_turn_3ea8c8aa.plan.md`; IRT-relay `enrich_user_query.py` удалён).  
**План (не редактировать):** `.cursor/plans/doc_types_ingress_cleanup_7251a108.plan.md`  
**Предшествующие планы:** `system_user_query_enrich`, `bridge_system-only_ingress` (код bridge→system + direct X→enrich уже был; этот эпик закрывал doc/type хвосты).

**Smoke e2e (запуск):** после `wipe_sync` — mailflow + imap checkpoint + subagent isolation:

```bash
pytest -n0 -s tests/e2e/wipe_sync.py 2>&1 | tee test-runs/.doc_types_cleanup_wipe_sync.log
pytest -n0 -s \
  tests/e2e/test_mailflow_e2e.py::test_full_mailflow_deploy_and_pipeline \
  tests/e2e/test_imap_checkpoint_resume_e2e.py::test_imap_checkpoint_resume_and_duplicate_skip \
  tests/e2e/test_subagent_frame_isolation_e2e.py::test_subagent_task_ledger_frame_isolation \
  2>&1 | tee test-runs/.doc_types_cleanup_e2e.log
```

На момент написания брифинга прогон мог ещё идти (долгий ansible в `wipe_sync`); итог — по хвосту логов выше.

---

## Целевая архитектура (напоминание)

```
bridge → ingress:  только <system>
ingress → enrich:   distill <history> + attach <user-query> из bridge system
reflect/subagent_* / validation errors → enrich@ напрямую (без ingress relay)
cli_exec / memory / formal_reason / … → enrich_fast@
```

Ingress — **только** bridge distill + HITL router. Internal re-enrich **минует** ingress.

---

## Фаза 1 — Документация

### `docs/TYPES.md`

- Восстановлены обрезанные абзацы (были `…`):
  - **Ingress distill tool bridge** — полный контракт `ingress_distill_tool_spec.j2`, порядок `IngressDistillHistoryPartKind`, `<user-query>` не в distill tool, `enrich_user_query_from_bridge_system` + `emit_bridge_distill_to_enrich`.
  - **Enrich-input VO** — роли `EnrichUserQueryText` / `EnrichCalleeHistoryText` / `EnrichRequestEchoText`, choke-points emit; wire `SummarizeContextStagePayload.user_query` → decode boundary.

### `docs/FSM.md`

- §2 memory: `thread_memory` / `global_memory` → **`enrich_fast@`**, не `ingress`.
- §4.1: `cli_intent` отказ → **`enrich@`** (`emit_enrich_validation_error`), не `ingress → enrich`.
- §5 билдеры: разделены маршруты `ingress@` (bridge/HITL) vs `enrich@` / `enrich_fast@` (internal).
- §6.4: `reflect → enrich → reasoning`.

### `docs/SUBAGENT_TABLE.md`

- Intro + `egress_router` rule 2: `subagent_end → **enrich**` (не ingress).
- Матрица **30 шагов** (было 35): удалены 5 obsolete ingress-relay после `subagent_intent`, `cli_exec`, `subagent_end`.
- После `cli_exec` добавлен шаг **`enrich_fast → reasoning`** (не ingress relay).
- Пересчитаны MID/hop-budget по L1/L2/L0 цепочкам.

### `docs/MEMORY_TABLE.md`

- §1 intro: петля памяти → **`enrich_fast`**, не ingress.
- §3 reflect: `reflect → enrich → reasoning`; таблица 3 шага (убрана строка `ingress` между reflect и enrich).

### Прочее

- `docs/briefing/enrich_task_hypotheses_briefing.md`: `reflect → ingress` → `reflect → enrich`.

### Docstrings в коде

- `types/ingress_distill.py`: `IngressDistillResult`, `IngressDistillHistoryPartKind` — user query = **`<user-query>` CID**, не request_echo.

---

## Фаза 2 — Типы (emit-границы)

### `EnrichUserQueryText` → `_RequiredNonEmpty`

**Файл:** `types/fsm_strings.py`

- Базовый класс: `_RequiredNonEmpty` (поле `value: NonEmptyStr`).
- `require_value` — алиас `require`; `from_external_body` → `require(name="bridge system", …)`.
- `mime_reform.attach_user_query_part` — убран redundant empty-check; `require_enrich_user_query_text` → `.require`.

### `SummarizeContextStagePayload.user_query`

**Файлы:** `types/summarize_tool_args.py`, `states/summarize_context.py`, `states/summarize_memory.py`, `types/__init__.py`

- Wire-поле Struct **остаётся `str`** (JSON plain string).
- Добавлен `validated_user_query(payload) -> EnrichUserQueryText`.
- `_parse_payload` возвращает `tuple[..., EnrichUserQueryText]`; пустой/invalid user_query → `None`.
- `summarize_memory` использует `EnrichUserQueryText.require`.

### `emit_to_enrich_fast` — typed optional params

**Файл:** `fsm_emit_semantic.py` + call sites:

| Param | Тип |
|-------|-----|
| `history` | `EnrichCalleeHistoryText \| None` |
| `request_echo` | `EnrichRequestEchoText \| None` |
| `system` | `FsmTransitionPlainBody \| None` |

Обновлены: `cli_intent`, `cli_exec`, `cli_resume`, `formal_reason`, `memory_query`, `response_observe`, `_memory_write`.

Внутри — `.value` на границе с `build_fsm_step_to_stage` (низкоуровневый API по-прежнему `str`).

---

## Что сознательно не менялось

- **`build_fsm_step_to_stage` / `build_fsm_plain_to_stage`** — низкоуровневые билдеры с голым `str` (контраст typed enrich-path vs plain builders — осознанный).
- **Граф FSM в коде** — уже был после plan 2/3; этот эпик только синхронизировал docs/types.
- **`ingress_bridge_user_query` import `_iter_relay_leaf_parts`** из private API `mime_reform` — мелкий техдолг, не трогали.

---

## Grep-audit docs (остатки OK)

Допустимые упоминания `ingress → enrich`:

- `CONTEXT_CONTRACT.md` — **внешний** ход пользователя (bridge distill).
- `E2E_ISOLATION.md`, `THREAD_MODEL.md` — обобщённые схемы full pipeline.

Не должно остаться: `reflect → ingress`, `subagent_intent → ingress → enrich`, `subagent_end → ingress`, memory → ingress.

---

## Карта файлов (diff эпика)

| Область | Файлы |
|---------|--------|
| Docs | `TYPES.md`, `FSM.md`, `SUBAGENT_TABLE.md`, `MEMORY_TABLE.md`, `briefing/enrich_task_hypotheses_briefing.md` |
| Types | `types/fsm_strings.py`, `types/ingress_distill.py`, `types/summarize_tool_args.py`, `types/__init__.py` |
| MIME | `mime_reform.py` |
| Emit | `fsm_emit_semantic.py` |
| Stages | `cli_intent`, `cli_exec`, `cli_resume`, `formal_reason`, `memory_query`, `response_observe`, `_memory_write`, `summarize_context`, `summarize_memory` |

---

## Чеклист для следующей сессии

1. Убедиться, что smoke e2e **PASSED** (логи `test-runs/.doc_types_cleanup_*.log`).
2. При падении mailflow: сравнить MIME в `stages/ingress/Maildir` vs `stages/enrich/Maildir` — на ingress только `<system>`, на enrich есть `<user-query>` + distill `<history>`.
3. Опционально: solo `test_summarize_context_e2e.py::test_summarize_overflow_full_pipeline` (relay `user_query` через summarize cycle).
4. Не переписывать `.cursor/plans/*` — только точечные правки normative docs (`docs.mdc`).

---

## Связь с CONTEXT_CONTRACT §4.1

После эпика документация согласована с mermaid в CONTEXT_CONTRACT:

- bridge → system only;
- ingress создаёт UQ на ingress→enrich;
- internal stages идут напрямую в enrich/enrich_fast.

При расхождении **SUBAGENT_TABLE.md** и **CONTEXT_CONTRACT.md** §4.1 переопределяют устаревшие формулировки в FSM/ARCHITECTURE (см. disclaimer в SUBAGENT_TABLE intro).
