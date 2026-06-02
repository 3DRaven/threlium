# RESPONSE_TABLE: инкрементальная сборка ответа

## 1. Обзор

Все ответы пользователю (короткие и длинные) проходят через `response_finalize`.
Длинные ответы собираются итеративно через `response_append` / `response_edit` /
`response_observe` с быстрым циклом `enrich_fast`. Прямой путь
`reasoning → egress_router` удалён.

Параллельно работает **task-ledger** (anti-drift, см. §8): отдельная стадия
`tasks_upsert` ведёт durable-список подзадач (план), а `response_finalize` **жёстко**
блокирует ответ, пока в ledger есть незавершённая работа (проверка по IRT, не по тексту
LLM). `response_observe` обозревает и буфер ответа, и состояние задач.

## 2. Стадии

| Стадия | Вход от | Выход в | Описание |
|---|---|---|---|
| `response_append` | `reasoning` | `enrich_fast` | Добавляет чанк текста в буфер ответа (durable в Maildir). |
| `response_edit` | `reasoning` | `enrich_fast` / `ingress` | Правит/удаляет чанк по 0-based position. Ошибка → `ingress`. |
| `response_observe` | `reasoning` | `enrich_fast` | Обзор буфера **+ task-ledger** → нарратив как `<{hash}@history>` обратно в reasoning. |
| `tasks_upsert` | `reasoning` | `enrich_fast` / `ingress` | Add новых подзадач + смена статусов существующих (см. §8). Ошибка валидации → `ingress`. |
| `enrich_fast` | `response_*` / `tasks_upsert` | `reasoning` | Быстрый цикл: `E_prev` (multipart) + recompute `<response-state>` / `<task-state>` → reasoning. |
| `response_finalize` | `reasoning` | `egress_router` / `ingress` | Финализация (4 режима, см. §3) + жёсткий task-gate (см. §8). |

## 3. Режимы `response_finalize`

| Mode | Buffer | Content | Действие |
|---|---|---|---|
| 1 | пуст | есть | Быстрый ответ: `content` → `egress_router` |
| 2 | есть | пуст | Ответ из буфера: `reduce(ops)` → `egress_router` |
| 3 | есть | есть | `reduce(ops)` + `content` → `egress_router` |
| 4 | пуст | пуст | `response_not_formed.j2` → `ingress` (продолжить reasoning) |

В режимах 1–3 перед отправкой срабатывает **task-gate** (§8, **fail-closed**): finalize
блокируется (`task_incomplete.j2` → `ingress`, ответ не уходит), если ledger **пуст**, либо
есть открытые (`pending` / `in_progress`) подзадачи, либо все `cancelled` без единой `done`.
Даже trivial-ответ обязан зафиксировать одну подзадачу `done` через `tasks_upsert`. Bypass —
только `allow_finalize_with_blocker` + непустой `blockers` при уже заведённом ledger.

## 4. Идентификация чанков

- `position` — 0-based индекс `AppendOp` в хронологическом порядке из `collect_ops`
- LLM видит `[0]`, `[1]`, `[2]` в observation / state summary
- `response_edit` адресует чанк по `position` из tool call
- Невалидная позиция → ошибка через `ingress` (graceful recovery)

## 5. CRDT-операции

```python
@dataclass(frozen=True)
class AppendOp:
    position: int                   # 0-based индекс в итоговом буфере
    content: str                    # из body письма
    message_id_inner: NotmuchMessageIdInner

@dataclass(frozen=True)
class EditOp:
    target_position: int            # 0-based позиция AppendOp
    new_content: str | None         # None = удаление; str = замена
    message_id_inner: NotmuchMessageIdInner
```

## 6. Алгоритм `collect_ops`

1. IRT-обход от leaf к root через `iter_in_reply_to_ancestors_from_inner_id`
2. Остановка на первом `tag:route` (граница interaction cycle)
3. Фильтр по `From:` — только `response_append`, `response_edit`
4. Reverse → хронологический порядок
5. Нумерация: каждый `AppendOp` получает инкрементный 0-based `position`
6. Для `EditOp` — `target_position` из JSON body

## 7. Multipart MIME (enrich → reasoning)

Письмо `enrich → reasoning` — `multipart/mixed` с MIME-частями по `Content-ID`:

Полный контракт письма (history/system, score/origin, дедуп, сбор) — в
[`CONTEXT_CONTRACT.md`](CONTEXT_CONTRACT.md). Ниже — только структурные core-CID
для буфера ответа/ledger.

| Content-ID | Источник | Описание |
|---|---|---|
| `<user-message>` | `enrich` | Текст пользователя |
| `<graph-answer>` | `enrich` | RAG-ответ (`rag.aquery`) |
| `<response-state>` | `enrich` / `enrich_fast` | Детерминированная сводка буфера ответа (recompute) |
| `<task-init>` | `enrich` | Стартовый набор подзадач (op `TaskInitOp`, durable для collect) |
| `<task-state>` | `enrich` / `enrich_fast` | Детерминированный reduced-ledger (кэш для промпта) |
| `<{hash}@history>` | любая стадия | Контент-адресная неисполняемая память; нарратив `response_observe` едет именно так |

Сырые чанки буфера (`response_append`/`response_edit`) и команды `tasks_upsert`
в `<history>` **НЕ** попадают: `reasoning` шлёт их как `<system>`-команды
(модель «callee владеет историей»), а сами мутаторы только пробрасывают payload
(`emit_*_preserving_payload`) без своей истории. Буфер виден reasoning'у как
детерминированный `<response-state>` + нарратив `response_observe`
(`<{hash}@history>`), а не как конкатенация всех чанков.

`enrich_fast` пересобирает (**replace**) `<response-state>` и `<task-state>`;
`<{hash}@history>`-части дописываются **аддитивно** и дедуплицируются по
контент-хешу тела (одинаковое тело → один CID), origin штампуется из конвертного
`From:` несущего письма. Остальные части `E_prev` не пересобираются.
Сбор дельты — сам `enrich_fast` (см. [FSM.md §5.2](FSM.md#52-контракт-тела-enrich--reasoning)).

## 8. Task-ledger CRDT (anti-drift)

Durable план задач треда: операции в Maildir сливаются по **content-addressed** identity
(`content_id = hash(normalize(text))`), статус — **монотонная решётка**
`pending(0) → in_progress(1) → done|cancelled(2)`, `merge = max(rank)` (ничья ранга 2 →
`done`). Reduce коммутативен/идемпотентен → не зависит от порядка писем в IRT.

```python
@dataclass(frozen=True)
class TaskInitOp:          # enrich → reasoning, MIME <task-init> (ensure-exists)
    subtasks: tuple[TaskSubtaskDef, ...]
    message_id_inner: NotmuchMessageIdInner

@dataclass(frozen=True)
class TasksUpsertOp:       # durable письмо → tasks_upsert (JSON tool-args)
    additions: tuple[NewSubtask, ...]          # add по тексту → content_id
    updates: tuple[SubtaskStatusUpdate, ...]   # status по content_id
    discovery_append / next_action / blockers / allow_finalize_with_blocker
    message_id_inner: NotmuchMessageIdInner
```

- **collect_task_ops** идёт по всему фрейму (IRT непрерывен) от листа до начала текущего
  субагента; хронология **root→leaf** (init/addition подзадачи всегда раньше её update —
  инвариант, на котором держится коммутативность reduce). Изоляция субагента — по глубине
  hop-стека (вложенные фреймы пропускаются, граница — письмо `subagent_intent` той же
  глубины). Источники op: `enrich` (`<task-init>`) и письма `→ tasks_upsert` (тело-JSON).
  Асимметрия с `collect_ops` буфера ответа: тот останавливается на `tag:route`, task —
  идёт по всему фрейму треда.
- **gate** (`ledger_has_open_work`, **fail-closed**): блокирует finalize если ledger **пуст**,
  есть `pending`/`in_progress`, **или** все `cancelled` без `done` (guard против escape-hatch
  «отменить всё и выйти»). Единственный bypass открытых подзадач —
  `allow_finalize_with_blocker` + непустой `blockers` (last-wins meta в reduced-ledger),
  и только при непустом ledger.
- `tasks_upsert` за один вызов и **добавляет** новые подзадачи (`new_subtasks`), и **меняет
  статусы** существующих (`subtask_updates` по `content_id`). Смена текста = новая подзадача +
  `cancelled` старой. Meta (`discovery_append` / `next_action` / `blockers` /
  `allow_finalize_with_blocker`) — last-wins в reduced-ledger. enrich сеет стартовый набор
  (LLM `enrich_task_plan`, fail-open на самом seed; пустой результат → ledger пуст → gate
  всё равно блокирует, пока модель не заведёт план) и после RAG добавляет проверяемые
  гипотезы (LLM `enrich_task_hypotheses`, тоже fail-open) — оба прохода пишут один
  `<task-init>` (один `TaskInitOp` на письмо enrich→reasoning), не через `tasks_upsert`.
