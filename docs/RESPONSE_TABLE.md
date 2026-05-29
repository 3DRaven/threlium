# RESPONSE_TABLE: инкрементальная сборка ответа

## 1. Обзор

Все ответы пользователю (короткие и длинные) проходят через `response_finalize`.
Длинные ответы собираются итеративно через `response_append` / `response_edit` /
`response_observe` с быстрым циклом `enrich_fast`. Прямой путь
`reasoning → egress_router` удалён.

## 2. Стадии

| Стадия | Вход от | Выход в | Описание |
|---|---|---|---|
| `response_append` | `reasoning` | `enrich_fast` | Добавляет чанк текста в буфер ответа (durable в Maildir). |
| `response_edit` | `reasoning` | `enrich_fast` / `ingress` | Правит/удаляет чанк по 0-based position. Ошибка → `ingress`. |
| `response_observe` | `reasoning` | `enrich_fast` | Сводка буфера + полный текст → обратно в reasoning. |
| `enrich_fast` | `response_*` | `reasoning` | Быстрый цикл: `E_prev` (multipart) + `<response-state>` → reasoning. |
| `response_finalize` | `reasoning` | `egress_router` / `ingress` | Финализация ответа (4 режима, см. §3). |

## 3. Режимы `response_finalize`

| Mode | Buffer | Content | Действие |
|---|---|---|---|
| 1 | пуст | есть | Быстрый ответ: `content` → `egress_router` |
| 2 | есть | пуст | Ответ из буфера: `reduce(ops)` → `egress_router` |
| 3 | есть | есть | `reduce(ops)` + `content` → `egress_router` |
| 4 | пуст | пуст | `response_not_formed.j2` → `ingress` (продолжить reasoning) |

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

`enrich_fast` пересобирает (**replace**) только `<response-state>`; relay-части (`<observation-note>` / `<plan-state>` / `<memory-note>`) дописываются **аддитивно** с уникальным `Content-ID` `<{family}@{inner-mid}>` — повторные хопы накапливаются, а не затирают друг друга. Остальные части `E_prev` не пересобираются.
