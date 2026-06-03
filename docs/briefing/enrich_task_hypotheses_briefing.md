# Брифинг: enrich late task hypotheses + reflect relay

Документ для передачи контекста в другую сессию. Нормативные контракты — в
[`CONTEXT_CONTRACT.md`](../CONTEXT_CONTRACT.md) §4, [`RESPONSE_TABLE.md`](../RESPONSE_TABLE.md) §8,
[`TYPES.md`](../TYPES.md) § LiteLLM / enrich tool bridge; план реализации (не редактировать):
`.cursor/plans/enrich_task_hypotheses_127bf143.plan.md`.

См. также параллельные брифинги (не смешивать эпики):
[`summarize_context_overflow_e2e_briefing.md`](summarize_context_overflow_e2e_briefing.md),
[`system_cid_lightrag_per_history_briefing.md`](system_cid_lightrag_per_history_briefing.md),
[`e2e_toolkit_refactor_briefing.md`](e2e_toolkit_refactor_briefing.md) (harness: `tests/e2e/toolkit/`, удалён `helpers.py`).

**Дата работы:** 2026-06-02  
**Статус кода:** реализовано в репозитории (не закоммичено на момент брифинга)  
**Smoke e2e:** `test_mailflow_e2e.py::test_full_mailflow_deploy_and_pipeline` — **PASSED** (~37 с, `--capture=no`), после `docker cp` + рестарт engine/bridge  
**Первый прогон mailflow после cold journal-reset:** **FAILED** за ~1 с на этапе RAG warmup (см. § «Гонки e2e»)

---

## Цель эпика

Ввести **второй LLM-проход в `enrich`** после LightRAG: проверяемые гипотезы (verify-subtasks) на полном контексте (graph + unified + memories + ledger), без новой FSM-стадии и без `tasks_upsert` на этом пути.

| Механизм | Когда | LLM | Куда пишет |
|----------|-------|-----|-----------|
| **`enrich_task_plan`** (early seed) | До RAG | Да | Тексты в graph query + позже в тот же `<task-init>` |
| **`enrich_task_hypotheses`** (late) | После RAG | Да | Только новые subtasks → **тот же** merged `<task-init>` |
| **`tasks_upsert`** | Reasoning tool | Reasoning | Статусы / follow-ups (без изменений) |
| **`reflect`** | Reasoning tool → `reflect → enrich` | **Нет** на стадии reflect | Полный re-enrich, не пополнение ledger |

Отменённая линия (старый план `context_reflection_llm`): LLM на стадии reflect, `ContextReflectSystemPayload`, `reflect → tasks_upsert → enrich_fast`.

---

## Архитектура (один hop enrich → reasoning)

```text
enrich.main (один процесс, один emit):
  seed LLM (enrich_task_plan)     → seed_defs in-memory
  RAG (aquery + MCKP)
  hyp LLM (enrich_task_hypotheses) → hyp_defs in-memory
  _finalize_task_mime_parts       → один TaskInitOp(seed+hyp) + <task-state>
  emit → reasoning
```

- **Нет** `tasks_upsert` / `enrich_fast` для гипотез.
- **Нет** tier-gate enrich→reflect.
- Гипотеза в данных = обычная subtask (`TaskSubtaskText` → `TaskSubtaskContentId`); отдельного типа `Hypothesis` **нет** ([`TYPES.md`](../TYPES.md)).
- Путь `summarize_overflow`: early return **до** seed/RAG/hyp — late hypotheses **не** вызываются.

---

## Реализованные изменения (код)

### Типы и LiteLLM ([`docs/TYPES.md`](../TYPES.md))

| Слой | Файл | Добавлено |
|------|------|----------|
| Tool args | `types/enrich_tool_args.py` | `EnrichTaskHypothesesToolArgs` (отдельный Struct, поля как у plan) |
| Function name | `types/enrich_tool_function.py` | `ENRICH_TASK_HYPOTHESES = "enrich_task_hypotheses"` |
| Call-site (e2e header) | `types/litellm_call_site.py` | `LitellmCallSite.ENRICH_TASK_HYPOTHESES` |
| Routing | `types/litellm_routing_site.py` | `LitellmRoutingSite.ENRICH_TASK_HYPOTHESES` |
| Prompt paths | `types/prompt_path.py` | `LIGHTRAG_ENRICH_TASK_HYPOTHESES` + `_TOOL_SPEC` |
| Bridge | `enrich_tool_bridge.py` | `parse_enrich_task_hypotheses_assistant` |
| Public API | `types/__init__.py` | экспорт `EnrichTaskHypothesesToolArgs` |

**Score ladder:** `enrich_task_hypotheses` в группе **score 1** (как `enrich_plan` / `enrich_task_plan`).

- `ansible/group_vars/e2e.yml`: `targets.enrich_task_hypotheses.target_score: 1.0`
- `ansible/roles/threlium/vars/main.yml`: prod default `1.0` + `threlium_required_prompts`
- `settings.py`: `RoutingTargets.enrich_task_hypotheses` + ветка в `resolve_llm_endpoint`

Асимметрия имён (как сейчас у seed): routing key `enrich_plan` ↔ call-site `enrich_task_plan`; для hypotheses routing = call-site = `enrich_task_hypotheses`.

### Промпты

| Файл | Назначение |
|------|------------|
| `prompts/lightrag/enrich_task_hypotheses.j2` | Late pass: verify-subtasks после graph/unified/memories; dedup ledger |
| `prompts/lightrag/tools/enrich_task_hypotheses_tool_spec.j2` | Автономный JSON, `# score 1`, `subtasks[]` max 8 |
| `prompts/reasoning/system.j2` | § `<reflect_strategy>`: reflect = re-enrich; гипотезы — следующий enrich + `tasks_upsert` |

Идеи для текста промпта (vendor, не копировать): Manus reflection/fact-check, Devin step-back, Anthropic «final verification step» — см. план §1.4.

### [`states/enrich.py`](../../ansible/roles/threlium/files/scripts/threlium/states/enrich.py)

Рефакторинг `_build_task_parts` → три функции:

| Функция | Роль |
|---------|------|
| `_parse_subtask_defs` | VO-only: `TaskSubtaskText.require`, `TaskSubtaskContentId.from_text`, dedup |
| `_build_task_seed_defs` | LLM до RAG; возвращает `(seed_defs, existing_ops, ledger_after_seed)` — **без** MIME |
| `_build_task_hypothesis_defs` | LLM после RAG; `LitellmRoutingSite.ENRICH_TASK_HYPOTHESES`; fail-open `[]` |
| `_finalize_task_mime_parts` | Один `TaskInitOp` на `seed_defs + hyp_defs`, один `<task-state>` |

`main`: seed → `_enrich_async` → hyp → finalize → `build_enriched_multipart` → emit reasoning.

### Reflect

[`states/reflect.py`](../../ansible/roles/threlium/files/scripts/threlium/states/reflect.py) — **без** LiteLLM (как было): Jinja → `reflect → enrich`.

---

## WireMock / e2e

### Bootstrap (глобально, все сценарии)

| Файл | Call-site | Ответ |
|------|-----------|--------|
| `compose_bootstrap/009_chat_completions_task_plan_default.json` | `enrich_task_plan` | `subtasks: []` |
| `compose_bootstrap/010_chat_completions_task_hypotheses_default.json` | `enrich_task_hypotheses` | `subtasks: []` |

При отсутствии сценарного стаба matчится bootstrap (priority 10) — fail-open пустой ledger.

### Сценарные каталоги (нужно доработать)

В [`test_mailflow_e2e/`](../../tests/e2e/wiremock_stubs/test_mailflow_e2e/) **нет** отдельных стабов:

- `enrich_task_plan` (есть только `080_chat_enrich_plan.json` → **`enrich_query_plan`**)
- `enrich_task_hypotheses` (есть `082_chat_enrich_rag_response.json` → **не** hypotheses)

Task-ledger сценарии уже имеют [`081_chat_enrich_task_plan.json`](../../tests/e2e/wiremock_stubs/test_task_ledger_bypass_e2e/081_chat_enrich_task_plan.json).

**Рекомендация для следующей сессии:** добавить в `test_mailflow_e2e/` (и при необходимости обновить `min_chat_completion_posts` в `MAILFLOW_SPEC` — сейчас `2`, после двух enrich-tool вызовов + reasoning может понадобиться `3+`):

- `081_chat_enrich_task_plan.json` — `X-Threlium-Call-Site: enrich_task_plan`, hasContext + thread-root
- `083_chat_enrich_task_hypotheses.json` — `enrich_task_hypotheses`, 1–2 verify subtasks (пустой `[]` тоже OK)

Шаблон — `081` из `test_task_ledger_bypass_e2e`; изоляция — `state-matcher` + `X-Threlium-Thread-Root`, не subject/body.

### Гонки e2e (важно)

После **cold journal-reset** в pytest-session:

1. Стирается `lightrag/` (`rm -rf`).
2. Engine стартует → `bootstrap_knowledge` → `vdb_chunks.json` быстро растёт (~8k+ байт).
3. `_inject_rag_warmup` (default `min_rerank_posts=1`): если `vdb_chunks` уже > 10 байт, идёт `_wait_rag_drain_idle` вместо полного warmup inject.

**Симптом:** первый прогон mailflow сразу после reset падает за ~1 с (до inject основного письма); повторный прогон ~37 с — **PASSED**.

**Не путать с багом hypotheses** — это инфраструктурная гонка reset/bootstrap vs rag warmup. Варианты: не считать bootstrap-doc за «vectordb ready», увеличить окно после engine start, или стабилизировать порядок в `mailflow_inject_and_wait`.

### Деплой на SUT без bake/sync

По запросу пользователя — только **`docker cp`** + рестарт user-units:

```bash
SUT=threlium_e2e_shared_613041-sut-1   # актуальное имя из docker ps
DST_PY=/home/threlium/threlium/agent/scripts/threlium
DST_PROMPTS=/home/threlium/threlium/data/prompts/lightrag

# Python (список файлов — см. git diff)
docker cp …/enrich.py $SUT:$DST_PY/states/
docker cp …/enrich_tool_bridge.py $SUT:$DST_PY/
docker cp …/types/{enrich_tool_*,litellm_*,prompt_path.py,__init__.py,settings.py} $SUT:$DST_PY/types/
docker cp …/settings.py $SUT:$DST_PY/

docker cp …/enrich_task_hypotheses.j2 $SUT:$DST_PROMPTS/
docker cp …/tools/enrich_task_hypotheses_tool_spec.j2 $SUT:$DST_PROMPTS/tools/
docker cp …/reasoning/system.j2 $SUT:/home/threlium/threlium/data/prompts/reasoning/

docker exec $SUT chown -R threlium:threlium $DST_PY $DST_PROMPTS …
docker exec -u threlium $SUT bash -lc 'export XDG_RUNTIME_DIR=/run/user/1001; systemctl --user restart threlium-engine.service threlium-bridge@email.service'
```

Проверка импорта в контейнере:

```bash
docker exec -u threlium -w /home/threlium/threlium/agent $SUT \
  .venv/bin/python -c "import threlium.states.enrich; from threlium.types.litellm_routing_site import LitellmRoutingSite; print(LitellmRoutingSite.ENRICH_TASK_HYPOTHESES.value)"
```

`targets.enrich_task_hypotheses` в `threlium.yaml` на SUT может **отсутствовать** — pydantic-default в `RoutingTargets` даёт `target_score: 1.0`; для явности — patch YAML или полный refresh vars.

Перезагрузка WireMock state: рестарт контейнера wiremock **или** session journal-reset (поднимает bootstrap, включая `010_…`).

---

## Документация (точечные правки, уже в diff)

| Документ | Что добавлено |
|----------|----------------|
| `INDEX.md` §7 | Два LLM в enrich: seed до графа, hypotheses после aquery |
| `ARCHITECTURE.md` | Абзац про late hypotheses |
| `CONTEXT_CONTRACT.md` | `<task-init>`: seed + late в одном `TaskInitOp` |
| `RESPONSE_TABLE.md` §8 | fail-open + один `<task-init>` |
| `TYPES.md` | score ladder + § enrich tool bridge + `EnrichTaskHypothesesToolArgs` |
| `E2E_ISOLATION.md` | Два call-site на enrich; stub pattern |

---

## Что **не** делали / out of scope

- Стадия `context_reflect`, tier-gate enrich→reflect, reflect-LLM.
- Гипотезы через `tasks_upsert` / `enrich_fast`.
- Unit-тесты (политика проекта — только e2e).
- Полный `wipe_bake` / `wipe_sync` в рамках сессии (пользователь запретил для быстрого цикла).
- Сценарные WireMock для **всех** каталогов с full enrich (только bootstrap + частично task_ledger).
- Опциональный kill-switch `enrich.task_hypotheses_enabled` в settings (в плане как optional, не реализован).

---

## Чеклист для следующей сессии

1. **Стабы:** `test_mailflow_e2e/081_enrich_task_plan.json`, `083_enrich_task_hypotheses.json`; при необходимости — зеркало в другие hot paths (`test_reasoning_litellm_mock_live`, `two_turn`).
2. **Проверки journal:** после прогона — call-site `enrich_task_plan` и `enrich_task_hypotheses` с тем же `X-Threlium-Thread-Root`; `GET /__admin/requests/unmatched` пуст.
3. **Счётчики:** пересмотреть `MAILFLOW_SPEC.min_chat_completion_posts` (и при желании assert по call-site в journal).
4. **Task ledger e2e:** `pytest -n0 -vv -s tests/e2e/test_task_ledger_e2e.py` — второй enrich-вызов не должен ломать phase reasoning stubs.
5. **Гонка warmup:** при падении сразу после session-reset — дождаться bootstrap / не требовать drain idle на свежем `vdb_chunks` от knowledge probe.
6. **Коммит:** только по явной просьбе пользователя; без `Co-authored-by: Cursor`.

---

## Команды smoke

```bash
# Mailflow (attach-only stack, полный вывод)
.venv/bin/python -m pytest -n0 -vv -s \
  tests/e2e/test_mailflow_e2e.py::test_full_mailflow_deploy_and_pipeline

# Task ledger matrix
.venv/bin/python -m pytest -n0 -vv -s tests/e2e/test_task_ledger_e2e.py

# Аудит tool stubs
.venv/bin/python scripts/audit_wiremock_tool_stubs.py
```

Логи успешного mailflow (после docker cp): `test-runs/.mailflow_debug.log` (фильтр `rg enrich_task` в journal SUT при отладке).

---

## Связь с reflect (для reasoning-промпта)

В [`reasoning/system.j2`](../../ansible/roles/threlium/files/prompts/reasoning/system.j2) уточнено:

- **`reflect`** = REMAP / полный re-enrich (новая формулировка graph query), не дописывание ledger.
- Новые verify-гипотезы в `<task_state>` — **следующий** `enrich` (late pass) и/или **`tasks_upsert`** от reasoning.

Это согласовано с кодом: reflect не вызывает `enrich_task_hypotheses`.
