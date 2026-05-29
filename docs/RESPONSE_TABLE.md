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
| `response_observe` | `reasoning` | `enrich_fast` | Обзор буфера **+ task-ledger** → нарратив `<response-observation>` обратно в reasoning. |
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

В режимах 1–3 перед отправкой срабатывает **task-gate** (§8): если в ledger есть открытые
(`pending` / `in_progress`) подзадачи **или** все `cancelled` без единой `done` —
`task_incomplete.j2` → `ingress` (ответ не уходит). Пустой ledger → gate не мешает (fail-open).

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

| Content-ID | Источник | Описание |
|---|---|---|
| `<user-message>` | `enrich` | Текст пользователя |
| `<lightrag-context>` | `enrich` | RAG-контекст |
| `<response-state>` | `enrich_fast` | Сводка буфера ответа |
| `<task-init>` | `enrich` | Стартовый набор подзадач (op `TaskInitOp`, durable для collect) |
| `<task-state>` | `enrich` / `enrich_fast` | Детерминированный reduced-ledger (кэш для промпта) |
| `<response-observation>` | `response_observe` | Нарратив-обзор буфера + задач (бывш. `<plan-state>`) |

`enrich_fast` пересобирает (**replace**) `<response-state>` и `<task-state>`; relay-части (`<observation-note>` / `<response-observation>` / `<memory-note>`) дописываются **аддитивно** с уникальным `Content-ID` `<{family}@{inner-mid}>` — повторные хопы накапливаются, а не затирают друг друга. Остальные части `E_prev` не пересобираются.

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
  субагента; изоляция субагента — по глубине hop-стека (вложенные фреймы пропускаются,
  граница — письмо `subagent_intent` той же глубины). Источники op: `enrich` (`<task-init>`)
  и письма `→ tasks_upsert` (тело-JSON).
- **gate** (`ledger_has_open_work`): блокирует finalize если есть `pending`/`in_progress`
  **или** все `cancelled` без `done` (guard против escape-hatch «отменить всё и выйти»);
  пустой ledger → fail-open.
- `tasks_upsert` за один вызов и **добавляет** новые подзадачи (`new_subtasks`), и **меняет
  статусы** существующих (`subtask_updates` по `content_id`). Смена текста = новая подзадача +
  `cancelled` старой. enrich сеет стартовый набор (LLM `enrich_task_plan`, fail-open).
