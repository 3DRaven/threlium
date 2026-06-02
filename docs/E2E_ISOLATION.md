# E2E Test Isolation через WireMock State Extension

Документ описывает модель **полной изоляции** параллельных e2e-тестов
(`pytest -n N`) через WireMock State Extension — без `priority`, без общих
стабов, без `doesNotContain`-эксклюзий.

В корне репозитория `pyproject.toml` задаёт `testpaths = ["tests/e2e"]` — автоматизированный pytest-набор **только** e2e.

**Принципы.**

1. Каждый тест обитает в **полностью изолированном** State-окружении стабов.
2. Единственный механизм изоляции — `state-matcher` (context + properties).
3. `priority` запрещён — все стабы равноправны, однозначность матчинга
   обеспечивается State properties + URL/header discriminators.
4. Body-anchor `doesNotContain` для исключения чужих тестов запрещён.
5. Тест создаёт свой State при setup и **удаляет только свой** при teardown.

---

## 1. Архитектура: три оси изоляции

```
         ┌───────────────────────────────────────────────────────────────┐
         │                    WireMock State Store                       │
         │                                                              │
         │  context: stub-hitl-mx-01::<route_A>                         │
         │    active: "1", phase_1_done: "1"                            │
         │                                                              │
         │  context: stub-two-turn-01::<route_B>                        │
         │    active: "1"                                               │
         │                                                              │
         │  context: matrix_rooms  (shared list)                        │
         │    list: [{room_id: "!r1"}, {room_id: "!r2"}]               │
         │                                                              │
         │  context: telegram_updates  (shared list)                    │
         │    list: [{chat_id: "111"}, {chat_id: "222"}]                │
         └───────────────────────────────────────────────────────────────┘
```

| Канал    | Коррелятор запросов LiteLLM        | Коррелятор bridge-стабов             |
| -------- | ---------------------------------- | ------------------------------------ |
| Email    | `X-Threlium-Thread-Root` (MID старейшего ``tag:route`` в треде) | Тот же коррелятор / route wire |
| Matrix   | `X-Threlium-Thread-Root` (= `RfcMessageIdWire(MatrixNativeId(room_id,event_id))` корневого события) | `room_id` в shared list контексте (§3) |
| Telegram | `X-Threlium-Thread-Root` (из цепочки предков / тега route) | `chat_id` в shared list контексте |

После прохождения bridge все три канала конвертируются в email
(`build_bridge_ingress_email`) с уникальным `X-Threlium-Route`.
Дальше по pipeline (enrich → reasoning → egress) изоляция **одинакова**
для всех каналов — через `hasContext` по значению **того же** коррелятора, что в
`X-Threlium-Thread-Root` у исходящих запросов LiteLLM (в стабах — шаблон заголовка).

---

## 2. Email-канал

### 2.1. Модель State

Тест генерирует уникальный `Message-ID`, вычисляет correlator для LiteLLM (= канонический MID корня треда /
``X-Threlium-Thread-Root``), сидирует контекст:

```
composite_key = composite_context_key(stub_tag, correlation_key)
  # = "{stub_tag}::{thread_root_mid}"

POST /__threlium/e2e/state/setup
{"correlation_key": "<composite_key>"}
→ recordState: context=<composite_key>, state={active: "1"}
```

Между ходами одного треда (тот же `composite_key`) сброс защёлки reasoning:

```http
POST /__threlium/e2e/state/phase_reset
{"correlation_key": "<composite_key>"}
→ recordState: phase_tasks_ledger_done="null"  (удаляет только это свойство)
```

В pytest: `wiremock_state_reset_phase`. Admin `DELETE /__admin/state-extension/contexts/{name}`
для составного ключа — no-op (всегда 204, контекст остаётся); не использовать.

Имя контекста — составной ключ `{stub_tag}::{correlation_key}`.
Каждый набор стабов хардкодит свой `stub_tag` в `hasContext`, поэтому
стабы тестов A и B никогда не cross-match: контексты `stub-A::route_X`
и `stub-B::route_X` — разные записи в State Store.

### 2.2. Стабы LiteLLM: composite `hasContext` (без `live_lane`)

Каждый сценарий имеет **свой** набор стабов. Однозначность матчинга
обеспечивается **составным именем контекста** в `hasContext`:

```
hasContext = "{stub_tag}::{{request.headers.[x-threlium-thread-root]}}"
```

`stub_tag` — статическая строка, захардкоженная в JSON-стабе.
Handlebars-шаблон `{{request.headers.[x-threlium-thread-root]}}` подставляет
заголовок запроса. Результат — имя контекста, которое State Extension ищет
в своей in-memory карте. Контекст `stub-A::route_X` ≠ `stub-B::route_X`,
поэтому cross-matching физически невозможен.

**Enrich plan** (`080_chat_enrich_plan.json`, сценарий `hitl_matrix`):

```json
{
  "request": {
    "method": "POST",
    "urlPathPattern": "^(/v1/chat/completions|/chat/completions)$",
    "headers": {
      "X-Threlium-Call-Site": { "equalTo": "enrich_query_plan" }
    },
    "bodyPatterns": [
      { "contains": "\"tools\"" }
    ],
    "customMatcher": {
      "name": "state-matcher",
      "parameters": {
        "hasContext": "stub-mailflow-live-hitl-mx-01::{{request.headers.[x-threlium-thread-root]}}"
      }
    }
  },
  "response": { "status": 200, "...": "..." }
}
```

**Reasoning с фазовым автоматом** (subagent_shallow, первый вызов →
`tool_calls: subagent_intent`):

```json
{
  "request": {
    "method": "POST",
    "urlPathPattern": "^(/v1/chat/completions|/chat/completions)$",
    "headers": {
      "X-Threlium-Call-Site": { "equalTo": "reasoning" }
    },
    "bodyPatterns": [
      { "contains": "Message context (headers):" },
      { "contains": "\"tools\"" }
    ],
    "customMatcher": {
      "name": "state-matcher",
      "parameters": {
        "hasContext": "stub-mailflow-live-sat-shallow-01::{{request.headers.[x-threlium-thread-root]}}",
        "hasNotProperty": "phase_nested"
      }
    }
  },
  "response": {
    "status": 200,
    "jsonBody": {
      "choices": [{
        "finish_reason": "tool_calls",
        "message": {
          "tool_calls": [{
            "function": { "name": "subagent_intent", "arguments": "..." }
          }]
        }
      }]
    }
  },
  "serveEventListeners": [{
    "name": "recordState",
    "parameters": {
      "context": "stub-mailflow-live-sat-shallow-01::{{request.headers.[x-threlium-thread-root]}}",
      "state": { "phase_nested": "1" }
    }
  }]
}
```

**Reasoning** (subagent_shallow, второй вызов — `phase_nested` уже есть):

```json
{
  "request": {
    "...": "...",
    "customMatcher": {
      "name": "state-matcher",
      "parameters": {
        "hasContext": "stub-mailflow-live-sat-shallow-01::{{request.headers.[x-threlium-thread-root]}}",
        "hasProperty": "phase_nested"
      }
    }
  },
  "response": {
    "status": 200,
    "jsonBody": {
      "choices": [{
        "finish_reason": "stop",
        "message": { "content": "Final egress reply." }
      }]
    }
  }
}
```

Так же оформлены live-сценарии ``global_memory`` и ``reflect`` (почтовый контур): ``stub-mailflow-live-global-mem-01`` и ``stub-mailflow-live-reflect-cyc-01`` в каталогах ``tests/e2e/wiremock_stubs/test_mailflow_live_only_e2e/global_memory/`` и ``tests/e2e/wiremock_stubs/test_mailflow_live_only_e2e/reflect_cycle/`` — те же ``hasContext`` и фазовые ``recordState``, без ``priority``.

### 2.4. Teardown

Тест **не удаляет** контекст в `finally` — это делает `pytest_sessionfinish`
глобальным `DELETE /__admin/state-extension/contexts`. Причина: поздние
LiteLLM-запросы от SUT (after test body returns) должны по-прежнему матчить
стабы. Контексты от разных тестов **не конфликтуют** (уникальные route).

---

## 3. Matrix-канал

### 3.1. Shared list для `/sync` + вычисление коррелятора

Matrix bridge делает **один** `/sync` запрос ко всему homeserver (WireMock).
Ответ должен содержать события для **всех** активных тестов одновременно.

Для этого используется **общий контекст** `matrix_rooms` с **list**.

**Схема (в коде теста):**

1. Генерируем случайные `room_id` / `event_id`
   (`e2e_matrix_generate_room_ids()` — `!e2e_<hex>:mock`, `$evt_<hex>`).
2. Вычисляем `correlation_key` = `e2e_matrix_thread_root_mid_for_sync_event(room_id, event_id)`
   — это тот же `Message-ID`, что мост Matrix получит из `/sync` и положит в
   notmuch → `resolve_route_from_thread_oldest_route_tag` → `X-Threlium-Thread-Root`.
3. Регистрируем комнату — `POST /__threlium/e2e/matrix/register_room`
   (`wiremock_matrix_register_room`): кладём `room_id`, `event_id`, тело/имя/sender
   в shared list `matrix_rooms`.
4. Сидируем State для LiteLLM — `wiremock_state_seed_context(base, composite_context_key(stub_tag, correlation_key))`.
5. Bootstrap стаб `/sync` (`compose_bootstrap/020_matrix_sync.json`) через `response-template` +
   `{{#each (state context='matrix_rooms' …)}}` собирает ответ со **всеми** комнатами.
6. Bridge вычисляет MID → то же значение, что `correlation_key` → LiteLLM-стабы
   матчат по `hasContext: "{{request.headers.[x-threlium-thread-root]}}"`.
7. В `finally` — `wiremock_matrix_unregister_room(base, room_id=room_id)`.

**Пример двух параллельных тестов:**

```
# Тест A (setup):
POST /__threlium/e2e/matrix/register_room
{"room_id": "!e2e_a1b2c3d4:mock", "event_id": "$evt_a1b2c3d4e5f6",
 "event_body": "Test A message", "room_name": "Room A", "sender": "@user:mock"}
→ recordState:
    context: "matrix_rooms"
    list:
      addLast: { …above fields… }

# Тест B (параллельно):
POST /__threlium/e2e/matrix/register_room
{"room_id": "!e2e_f7e8d9c0:mock", "event_id": "$evt_f7e8d9c0a1b2",
 "event_body": "Test B message", "room_name": "Room B", "sender": "@user:mock"}
→ recordState:
    context: "matrix_rooms"
    list:
      addLast: { …above fields… }
```

Корреляторы (thread-root MID) **разные** — `hasContext` для LiteLLM-стабов
точно изолирует запросы тестов A и B.

### 3.2. Единый bootstrap стаб `/sync`

Для Matrix используется **один** bootstrap стаб (`compose_bootstrap/020_matrix_sync.json`),
который отвечает на **все** запросы `/sync` — независимо от наличия `since`.

Если `matrix_rooms` пуст — ответ содержит `"join": {}` (bridge спокойно
поллит без действий). Если тесты зарегистрировали комнаты — все они
появляются в ответе. Bridge дедуплицирует повторные события через
`Message-ID` в notmuch.

**Ключевое:** `fixedDelayMilliseconds: 2000` — bridge поллит каждые ~2 с.
Тест регистрирует комнату → максимум через 2 с bridge получает событие.
При 30 с таймауте теста остаётся 28 с на полный pipeline.

**Handlebars-шаблон (body field):**

```
"body": "...\"join\":{ {{#each (state context='matrix_rooms' ...)}} ... {{/each}} }..."
```

> **Gotcha: пробел между `{` и `{{#each`.**
> Без пробела Handlebars видит `{{{#each` как triple-stache (unescaped output)
> и бросает `HandlebarsException: found '#'`. Пробел `{ {{#each` делает `{`
> литеральным символом JSON, а `{{#each}}` — началом блока.

Стаб **не** использует `customMatcher` / `state-matcher` — он всегда отвечает.
Пустой state (`default='[]'`) → `{{#each}}` не генерирует ничего → `"join": { }`.

**FileLock для `matrix_rooms`:** при параллельном доступе к shared state
`wiremock_matrix_register_room` и `wiremock_matrix_unregister_room` сериализуются
тем же межпроцессным lock, что и WireMock Admin API (файл `e2e_wiremock_admin_api.lock` в каталоге
координатора compose, см. `tests.e2e.wiremock_client._wiremock_admin_api_exclusive`),
гарантируя атомарность list-операций даже из разных pytest-xdist workers.

### 3.4. Стаб `room_send` (egress)

Egress matrix отправляет ответ в конкретную комнату. `egress_matrix.py` передаёт
`X-Threlium-Thread-Root` через `nio.AsyncClientConfig(custom_headers=…)` —
тот же коррелятор, что и для LiteLLM-запросов (§3.5). Это позволяет
изолировать стаб через `state-matcher` с composite `hasContext`:

```json
{
  "request": {
    "method": "PUT",
    "urlPathPattern": "^/_matrix/client/(r0|v3)/rooms/[^/]+/send/m\\.room\\.message/[^/]+$",
    "customMatcher": {
      "name": "state-matcher",
      "parameters": {
        "hasContext": "stub-matrix-wiremock-live-e2e-01::{{request.headers.[x-threlium-thread-root]}}"
      }
    }
  },
  "response": {
    "status": 200,
    "transformers": ["response-template"],
    "jsonBody": {
      "event_id": "$send_{{randomValue length=22 type='ALPHANUMERIC'}}"
    }
  }
}
```

### 3.5. LiteLLM-стабы для Matrix

После bridge, письмо проходит pipeline (enrich → reasoning → egress)
с уникальным `X-Threlium-Route` (`MatrixIngressRoute`). Заголовок
`X-Threlium-Thread-Root` для LiteLLM — канонический MID того же корневого события:
`RfcMessageIdWire.from_native(MatrixNativeId(v=1, room_id, event_id))`.

Тест вычисляет **тот же** MID из тех же `room_id` / `event_id`, что передал
в `register_room` — и сидирует его в State (`wiremock_state_seed_context`).
LiteLLM-стабы матчат по `hasContext` + `property` — **точно так же**,
как для email-канала (§2.2). Параллельные тесты создают уникальные `room_id` →
уникальный `correlation_key` → нет коллизий.

### 3.6. Teardown

```python
# В finally теста:
wiremock_matrix_unregister_room(base, room_id=room_id)
# → POST /__threlium/e2e/matrix/unregister_room {"room_id": "!e2e_a1b2c3d4:mock"}
# → deleteState context="matrix_rooms" list.deleteWhere property="room_id" value="…"
```

`deleteWhere` удаляет **только свой** элемент из list, не затрагивая
комнаты параллельных тестов.

Контекст LiteLLM (по `correlation_key`) удаляется глобально в
`pytest_sessionfinish` **после** просушки workers и пустого `requests/unmatched`.

### 3.7. Реализация (файлы)

| Файл | Роль |
| ---- | ---- |
| `compose_bootstrap/010_e2e_matrix_register_room.json` | Стаб POST → `recordState addLast` в `matrix_rooms` |
| `compose_bootstrap/011_e2e_matrix_unregister_room.json` | Стаб POST → `deleteState deleteWhere` по `room_id` |
| `compose_bootstrap/020_matrix_sync.json` | Единый `/sync`: `response-template` + `#each` из State list `matrix_rooms` |
| `tests/e2e/wiremock_client.py` | `wiremock_matrix_register_room` / `wiremock_matrix_unregister_room` |
| `tests/e2e/helpers.py` | `e2e_matrix_generate_room_ids`, `e2e_matrix_thread_root_mid_for_sync_event` |

---

## 4. Telegram-канал

**Тот же каркас изоляции, что §3 (Matrix):** параллельные live e2e держатся на **WireMock State Extension** теми же базовыми правилами: **один bootstrap-стаб на транспорт** (здесь `getUpdates`, у Matrix — `/sync`), **один shared list** в своём имени контекста (`telegram_updates` ↔ `matrix_rooms`), **тот же коррелятор LiteLLM** — `X-Threlium-Thread-Root` (MID корня треда; в тесте считается заранее из `chat_id` / `message_id` / `message_thread_id` и сидится через `wiremock_state_seed_context` с composite key), **узкий teardown** — удаление только своей записи из list (`unregister_update` по `update_id` ↔ `unregister_room` по `room_id`). Запись в list и служебные POST на `/__threlium/e2e/telegram/…` сериализуются тем же **`_wiremock_admin_api_exclusive`**, что и Matrix (§3.2). **Отличие от Matrix:** PTB не вешает на `sendMessage`/`getMe` кастомные заголовки (нет `X-Threlium-Thread-Root` на wire), поэтому egress-стаб `sendMessage` **не** изолируется `state-matcher` по thread-root — только **узкие `bodyPatterns`** в сценарном каталоге и **разные каталоги / `stub_tag`** для фильтра журнала (§8.4).

### 4.1. Shared list для `getUpdates` + вычисление коррелятора

Telegram bridge — один `getUpdates` long poll. Контекст **`telegram_updates`**, поля элемента list — те же, что принимает `032_e2e_telegram_register_update` / шаблон `031` (в т.ч. `msg_date` для Unix-времени в JSON — не имя `date`, чтобы Handlebars в response-template не подменял значение).

**Схема (в коде теста), по шагам как §3.1:**

1. Генерируем уникальные `chat_id` / `message_id` / `update_id` (и при forum — `message_thread_id`) — `e2e_telegram_generate_update_bundle`.
2. `correlation_key` = `e2e_telegram_thread_root_mid_for_message(…)` — тот же MID, что мост положит в notmuch и в `X-Threlium-Thread-Root` для LiteLLM.
3. `wiremock_telegram_register_update` → `POST /__threlium/e2e/telegram/register_update` → `recordState addLast` в list `telegram_updates`.
4. `wiremock_state_seed_context(base, composite_context_key(stub_tag, correlation_key))`.
5. Bootstrap `031_telegram_get_updates.json` собирает `result` из **всех** элементов list (как `020_matrix_sync` из `matrix_rooms`).
6. LiteLLM-стабы сценария матчат по composite `hasContext` (`{stub_tag}::{{request.headers.[x-threlium-thread-root]}}`) с тем же thread-root, что и для email/Matrix (§2.2 / §3.5).
7. В `finally` — `wiremock_telegram_unregister_update(base, update_id=…)` по **своему** `update_id`.

```
# Тест (setup):
POST /__threlium/e2e/telegram/register_update
→ recordState:
    context: "telegram_updates"
    list:
      addLast:
        update_id: "100001"
        chat_id: "999001"
        message_id: "1"
        text: "Test message from user"
        msg_date: "1735689600"
        from_username: "e2e_user"
        from_id: "12345"
```

Параллельные тесты имеют разные `update_id` / `correlation_key` → элементы list и LiteLLM-контексты не пересекаются (аналогия комнат Matrix в §3.1).

### 4.2. Стаб `getUpdates`

Реализация — `compose_bootstrap/031_telegram_get_updates.json`: **без** `state-matcher` / `listSizeMoreThan`
(пустой list при отсутствии стаба дал бы unmatched и сломал бы guard по журналу). Один маппинг на все
`POST …/getUpdates`, ответ — `response-template` + `{{#each}}` по list `telegram_updates` (как
`020_matrix_sync.json` для `/sync`). Пустой list → `"result": []` (bridge поллит дальше); непустой —
все зарегистрированные update-ы. Дедупликация повторных ответов — на стороне bridge (notmuch по
`Message-ID`). Для forum в элементе list задаётся непустой `thread_kind` и `message_thread_id` в шаблоне
(ветка `supergroup` + поле в `message`); для лички — без `message_thread_id` в JSON сообщения.

### 4.3. Стаб `sendMessage` (egress)

```json
{
  "request": {
    "method": "POST",
    "urlPathPattern": "^/bot[^/]+/sendMessage$"
  },
  "response": {
    "status": 200,
    "transformers": ["response-template"],
    "jsonBody": {
      "ok": true,
      "result": {
        "message_id": "{{randomValue length=6 type='NUMERIC'}}",
        "chat": { "id": "{{jsonPath request.body '$.chat_id'}}" }
      }
    }
  }
}
```

В продакшене e2e не один «глобальный» маппинг: в каталоге `test_telegram_wiremock_live_e2e_*` несколько узких стабов с `bodyPatterns` и своим `stub_tag` (см. ввод §4 — без `state-matcher` на `sendMessage`). **PTB** шлёт `sendMessage` как `application/x-www-form-urlencoded` (`text=…` с `+` вместо пробела в значении поля), а не JSON: в `040_telegram_send_message.json` матчинг по подстроке — на `ok+telegram+…`, в ответе — `{{formData request.body 'fd' urlDecode=true}}` + `{{fd.chat_id}}` / `{{fd.text}}` (не `jsonPath` по `request.body`).

### 4.4. Teardown

```
POST /__threlium/e2e/telegram/unregister_update
→ deleteState:
    context: "telegram_updates"
    list:
      deleteWhere:
        property: "update_id"
        value: "<update_id теста>"
```

`update_id` глобально уникален на прогон; `chat_id` мог бы совпасть у разных сценариев (личка и forum).

### 4.5. LiteLLM-стабы

Как для email и Matrix — `X-Threlium-Route` кодирует
`TelegramIngressRoute` (chat_id, message_id, update_id).
Изоляция через `hasContext` + `property`.

### 4.6. Реализация (файлы)

| Файл | Роль |
| ---- | ---- |
| `compose_bootstrap/030_telegram_get_me.json` | `getMe` для PTB при старте бриджа |
| `compose_bootstrap/031_telegram_get_updates.json` | Единый `getUpdates`: `response-template` + `#each` из `telegram_updates` |
| `compose_bootstrap/032_e2e_telegram_register_update.json` | POST → `recordState addLast` в list |
| `compose_bootstrap/033_e2e_telegram_unregister_update.json` | POST → `deleteState deleteWhere` по `update_id` |
| `tests/e2e/wiremock_client.py` | `wiremock_telegram_register_update` / `wiremock_telegram_unregister_update`, `assert_wiremock_telegram_e2e_openai_coverage` |
| `tests/e2e/helpers.py` | `e2e_telegram_generate_update_bundle`, `e2e_telegram_thread_root_mid_for_message` |
| `tests/e2e/wiremock_stubs/test_telegram_wiremock_live_e2e_{private,forum_topic}/` | Полный набор LLM + `040_telegram_send_message.json` на сценарий |

---

## 5. Фазовый автомат внутри сценария

Для сценариев с несколькими LLM-вызовами одного типа (например, два
reasoning call: первый возвращает `tool_calls`, второй — финальный ответ)
используется `recordState` + `hasProperty`/`hasNotProperty`:

```
Запрос 1 → стаб матчит hasNotProperty: "phase_1_done"
           → ответ: tool_calls: subagent_intent
           → recordState: {phase_1_done: "1"}

Запрос 2 → стаб матчит hasProperty: "phase_1_done"
           → ответ: finish_reason: stop
```

Фазовые properties живут **в контексте route** (не глобально).
Параллельные тесты имеют разные route → разные контексты → фазы
не пересекаются.

---

## 6. Матрица: стаб × сценарий

| Стаб                  | Дискриминатор                                        |
| --------------------- | ---------------------------------------------------- |
| `/embeddings`         | `hasContext` (route)                                  |
| `/chat/completions` `085` entity | `hasContext` + `X-Threlium-Call-Site: extract_knowledge_graph` |
| `/chat/completions` `097` glean  | `hasContext` + `X-Threlium-Call-Site: extract_knowledge_graph_gleaning` |
| `/chat/completions` `095` summarize | `hasContext` + `X-Threlium-Call-Site: summarize_descriptions` |
| `/chat/completions` `090` keywords | `hasContext` + `X-Threlium-Call-Site: extract_query_keywords` |
| `/chat/completions` `082` rag response | `hasContext` + `X-Threlium-Call-Site: generate_rag_answer` |
| `/chat/completions` `071` entity chunk | `hasContext` + `extract_knowledge_graph` + body `X-Threlium-LightRAG-Chunk` |
| `/chat/completions` `055/056` naive chunk | `hasContext` + `generate_rag_answer` + body negative lookahead |
| `/chat/completions` `060/061` kg chunk | `hasContext` + `generate_rag_answer` + body `Knowledge Graph Data` |
| `/chat/completions` ingress distill (`075`) | composite `hasContext` + `X-Threlium-Call-Site: ingress_distill` + `"tools"`; ответ `finish_reason: tool_calls` |
| `/chat/completions` enrich plan (`080`) | composite `hasContext` + `X-Threlium-Call-Site: enrich_query_plan` + `"tools"`; ответ `finish_reason: tool_calls` |
| `/chat/completions` summarize_context | composite `hasContext` + `X-Threlium-Call-Site: summarize_thread_context` + `"tools"` |
| `/chat/completions` reasoning (`100`) | composite `hasContext` + phase properties + `X-Threlium-Call-Site: reasoning` (multi-tool) |
| `/sync` (matrix)      | Bootstrap, всегда отвечает; state `matrix_rooms` через `#each` |
| `/getUpdates` (tg)    | `telegram_updates` list (response-template, без `listSizeMoreThan`) |
| `room_send` (matrix)  | URL path (room_id)                                   |
| `sendMessage` (tg)    | Request body (chat_id)                               |

Запросы LightRAG к `/chat/completions` несут `"tools"` + `"tool_choice":"required"`; ответы стабов — `finish_reason: tool_calls`, аргументы в `tool_calls[].function.arguments` (delimiter/JSON/plain внутри JSON args). Изоляция по-прежнему: `hasContext` + granular `X-Threlium-Call-Site` (дополнительно в request можно `"contains": "\"tools\""`). Offline-проверка стабов: `python scripts/audit_wiremock_tool_stubs.py` (exit 0). Smoke сериализатора tool→delimiter: `.venv/bin/python scripts/verify_lightrag_tool_roundtrip.py`. Enrich-plan стабы (`enrich_query_plan` / `enrich_task_plan`) — отдельный маршрут (не `build_llm_func`), но тоже отвечают `finish_reason: tool_calls`. Контракт типов и bridge — [`TYPES.md`](TYPES.md) § «LightRAG tool bridge».

Гранулярные значения `X-Threlium-Call-Site` для LightRAG определяются в рантайме
функцией `threlium.types.lightrag_tool_phase.detect_lightrag_call_site_wire` по сигналам `llm_func`:
`keyword_extraction`, `history_messages`, `system_prompt` — без инспекции
prompt content; результат равен `function.name` единственного tool
(`extract_knowledge_graph` / `extract_knowledge_graph_gleaning` / `summarize_descriptions` /
`extract_query_keywords` / `generate_rag_answer`). Инвариант для всех chat-вызовов с одним
tool: `X-Threlium-Call-Site == tools[0].function.name` (исключение — reasoning multi-tool =
`reasoning`); проверяется в `merge_litellm_call_kwargs_and_log`. Pipeline-маркеры
`lightrag_index` / `lightrag_query` / `lightrag_query_rerank` / `fsm` остаются только для
не-tool вызовов (embedding / rerank / TLS-fallback). Enum — `LitellmCallSite` в
`types/litellm_call_site.py`; offline-аудит контракта стабов —
`python scripts/audit_wiremock_tool_stubs.py`.
Body patterns `071/055/056/060/061` — из структуры документа (`X-Threlium-LightRAG-Chunk`)
и KG контекста (`Knowledge Graph Data`), не из lightrag промптов — **безопасны**
при редактировании промптов.

Сценарий ``test_reasoning_litellm_context_trim_live``: в e2e ``context_max_chars=8000``,
``trim_context_text`` оставляет **хвост** user-тела. Стаб reasoning ``100`` матчит
``E2E-CTX-TRIM-TAIL-MARKER`` (не ``E2E-REASONING-LITELLM-BODY-MARKER`` из начала письма).

Ни один стаб не использует `priority`. Ни один стаб не использует
`doesNotContain` для исключения чужих тестов. Каждый тест владеет
**только своими** State-данными.

---

## 7. Жизненный цикл State в тесте

```
┌─ pytest session start (once, leader under FileLock) ───────┐
│  cold reset: stop pipeline → flush all SUT Maildirs/GreenMail│
│  → reset WM journal + all State + non-bootstrap mappings   │
│  → bootstrap stubs → start pipeline → idle → journal reset │
├─ per-test prepare (prepare_wiremock_scenario) ─────────────┤
│  e2e_clean_sut_messages_for_test(stub_tag, correlation_key) │
│  → upsert stubs → journal by stub_tag → seed context       │
│  → [matrix/tg] register_room / register_update             │
├─ test body ────────────────────────────────────────────────┤
│                                                            │
│  SUT обрабатывает сообщение:                               │
│  bridge → ingress → enrich → reasoning → egress            │
│  Каждый LiteLLM-запрос несёт X-Threlium-Route             │
│  state-matcher проверяет composite hasContext + phase       │
│  recordState продвигает фазовый автомат                    │
│                                                            │
├─ pytest teardown (finally) ────────────────────────────────┤
│                                                            │
│  5. [matrix/tg] unregister_room / unregister_update        │
│     → deleteWhere по room_id / update_id (только своё)     │
│  6. assert_wiremock_zero_unmatched_requests                │
│  7. Контекст route НЕ удалять — поздние запросы SUT       │
│                                                            │
├─ pytest_sessionfinish (controller, once) ──────────────────┤
│  wait idle + assert zero unmatched; pipeline stays up      │
│  при exitstatus≠0: укороченный drain (``THRELIUM_E2E_SESSIONFINISH_FAIL_DRAIN_SEC``, 30 с) │
└────────────────────────────────────────────────────────────┘
```

**Global memory при параллельном прогоне.** Prod намеренно подмешивает в enrich все
факты из `global_memory@` без фильтра по треду. E2e **не** требуют пустой global memory:
стабы матчят **свой** маркер (`contains: E2E-…-BODY`) и `hasContext`; `doesNotContain`
допустим только для фаз **одного** сценария (например act1 FSM без `RECOVERY`), не для
исключения чужих тестов.

---

## 8. Практические gotchas

### 8.1. Sessionfinish после FAIL (не «зависание» runner)

Тело теста может уже упасть на ``assert``, но ``pytest_sessionfinish`` при
``THRELIUM_E2E_LEAVE_STACK_RUNNING=1`` всё равно ждёт idle ``threlium-work@*`` /
``threlium-sweep@*`` и проверяет пустой ``GET …/requests/unmatched`` (инвариант
целостности стабов **не** отключается). Без укороченного лимита это до 120 с
(``THRELIUM_E2E_SESSIONFINISH_DRAIN_SEC``) — в логе видны ``poll(backoff) progress:
sut: threlium-work@ / threlium-sweep@``. После FAIL используется
``min(120, THRELIUM_E2E_SESSIONFINISH_FAIL_DRAIN_SEC)`` (по умолчанию 30 с);
``test-runs/run_individual_e2e.sh`` выставляет оба env и печатает подсказку в строке
итога. Параллельные smoke/pytest на том же compose с runner не запускать.

### 8.2. WireMock `matches` — full-match семантика

`bodyPatterns[].matches` в WireMock аналогичен Java `String.matches()`:
regex должен покрывать **всё** тело запроса целиком, а не подстроку.

```json
// НЕПРАВИЛЬНО — regex не покрывает хвост body:
{"matches": "(?s).*\\\"input\\\"\\s*:\\s*\\["}

// ПРАВИЛЬНО — `.*` в конце покрывает остаток:
{"matches": "(?s).*\\\"input\\\"\\s*:\\s*\\[.*"}
```

Для подстроковых проверок используйте `contains` или `matchesJsonPath`.

### 8.3. Handlebars: литеральная `{` перед `{{#block}}`

В `body` (string template) WireMock, если JSON-структура требует `{`
непосредственно перед Handlebars-блоком (`{{#each}}`, `{{#if}}`):

```
ОШИБКА:  "join":{{{#each ...}}    → Handlebars видит {{{ = triple-stache
FIX:     "join":{ {{#each ...}}   → пробел разделяет литерал и блок
```

Аналогично для закрытия: `{{/each}} }` вместо `{{/each}}}`.

### 8.4. Bootstrap стабы и selective reset

Cold reset (`reset_non_bootstrap_wiremock_mappings`) сохраняет стабы с тегом
`THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG`. На SUT после остановки pipeline выполняется ротация и vacuum user-journal `threlium` (`journalctl --user --rotate` + `--vacuum-time=1s`), чтобы в e2e-дампах не тянулись записи прошлых сессий с того же контейнера. Под этим тегом лежат:
- `000_e2e_state_setup.json`
- `001_e2e_state_phase_reset.json`
- `010_e2e_matrix_register_room.json` / `011_e2e_matrix_unregister_room.json`
- `020_matrix_sync.json`
- `030_telegram_get_me.json` / `031_telegram_get_updates.json`
- `032_e2e_telegram_register_update.json` / `033_e2e_telegram_unregister_update.json`
- `005_embeddings_batch_e2e_greenmail_readiness.json`

Тестовые стабы (`stub_tag != bootstrap`) удаляются при cold reset и
перерегистрируются каждым тестом через `upsert_wiremock_mapping_directory`.

Между тестами одной pytest-сессии полный flush **не** повторяется: `e2e_clean_sut_messages_for_test`
удаляет на SUT только письма прошлых запусков **этого** `stub_tag` (префиксы `Message-ID`,
Matrix `room_id` с `!e2e_`, Telegram `chat_id` в canonical id), сохраняя тред текущего
`correlation_key` для multi-turn.

### 8.5. test_id в теле сообщения для coverage assertion

`assert_wiremock_matrix_e2e_openai_coverage` проверяет, что `test_id`
буквально присутствует в теле LLM-запросов (для `_post_has_reasoning`).
Остальные фазы сверяются с маркерами из `bodyPatterns` JSON-стабов
(`tests/e2e/wiremock_stubs/test_matrix_wiremock_live_e2e/`, константы `_E2E_*` в `wiremock_client.py`), а не с произвольными фразами промптов.
Для email-тестов test_id естественно попадает через subject/body письма.
Для Matrix — нужно явно включить `test_id` в `event_body` при
`wiremock_matrix_register_room`, чтобы оно прошло через pipeline и
появилось в reasoning промпте.
Для Telegram — то же по `test_id` в тексте входящего сообщения (`wiremock_telegram_register_update`);
исходящий `sendMessage` проверяет `assert_wiremock_telegram_e2e_openai_coverage` (тело с `chat_id`,
фразой ответа из `100_chat_reasoning_egress_tool.json` и при forum — `message_thread_id`).

### 8.6. Notmuch-дедупликация при повторяющемся /sync

Bootstrap sync стаб отвечает одними и теми же событиями на каждый poll
(пока комната зарегистрирована). Bridge создаёт `Message-ID` из
`room_id + event_id` — при повторной вставке notmuch обнаруживает дубликат
и пропускает (`duplicate Message-ID in notmuch, skip`). Это штатное
поведение, не ошибка.

### 8.7. Запрет поля `priority` в JSON-стабах сценариев

В каталогах `tests/e2e/wiremock_stubs/**` (кроме `compose_bootstrap/`, где
`priority` может быть у bootstrap-маппингов инфраструктуры) **нельзя** задавать
ключ `"priority"` в mapping JSON.

**Почему:** при `priority` порядок матчинга определяется числом, а не
взаимоисключающими State properties; параллельные сценарии и фазы внутри одного
треда становятся непредсказуемыми, обходят модель §5.

**Вместо этого:** пара `hasProperty` / `hasNotProperty` на одном контексте
`{stub_tag}::{{request.headers.[x-threlium-thread-root]}}`, плюс дискриминаторы
`X-Threlium-Call-Site` (= `function.name` tool), URL и узкие `bodyPatterns` (chunk).
Пример: до HITL-классификации — `hasNotProperty: live_hitl_h03_hitl_classified`;
после — отдельный стаб с `hasProperty: live_hitl_h03_hitl_classified` и пустым
/minimal tool response (см. `hitl_matrix_resume_no/078*_post_classified_*.json`).

Проверка вручную: `rg '"priority"' tests/e2e/wiremock_stubs/test_` — ожидается
пустой вывод (bootstrap искать отдельно: `compose_bootstrap/`).

### 8.8. Эмуляция «долгого LLM»: цепочка **307 Temporary Redirect** (polling без длинного держания сокета)

Идея: WireMock **не держит** один HTTP-ответ открытым минутами (`fixedDelay` на огромное тело),
а по одному стабу на `POST /v1/chat/completions` (или узкий `state-matcher` под сценарий) **несколько раз подряд**
отвечает **307** с заголовком **`Location`** на **тот же** URL эндпоинта. Клиент (стек **LiteLLM → OpenAI SDK → httpx**)
**следует редиректам** внутри **одного** `send()`: каждый hop — новый **POST** с **тем же JSON-телом** (для **307** httpx **не**
превращает POST в GET, в отличие от **301/302/303**). Опционально на ответ 307 вешается **`fixedDelayMilliseconds`**
в стабе WireMock — пауза между hop’ами без удержания долгого сокета на одном ответе.

**Переключение «тест отпустил»:** второй стаб с более узким матчем при `hasProperty` / флаге в State Extension
(как фазы §5) или отдельный POST-триггер, обновляющий контекст, пока первый стаб ещё отдаёт 307; после смены состояния
тот же запрос попадает под стаб **200** с финальным `chat.completion`.

**Лимиты по умолчанию (важно для дизайна теста):**

- **httpx** (`DEFAULT_MAX_REDIRECTS = 20`): слишком длинная цепочка 307 даёт **`TooManyRedirects`**, а не бесконечный poll. При необходимости длиннее — отдельный `http_client` с увеличенным `max_redirects` (вне текущего пути Threlium).
- **Threlium `reasoning`:** в один вызов LiteLLM передаётся **`timeout`** из выбранной записи каталога `settings.litellm` (плейбук: по умолчанию ``threlium_reasoning_timeout_sec`` ≈ 120 с в `threlium.yaml`) — **вся** цепочка редиректов + чтение финального тела должна уложиться в этот бюджет, иначе `httpx.TimeoutException`.
- **`max_retries=0`** у FSM на LiteLLM **не отключает** следование редиректам: при нуле ретраев OpenAI **не** повторяет запрос после **429**, но **307** обрабатываются **внутри** первого запроса httpx (это не счётчик `max_retries`).

**Не путать с 429 + `Retry-After`:** при `max_retries=0` ответ **429** сразу приводит к ошибке без цикла;
для ретраев по 429 нужен `max_retries > 0` и заголовок `Retry-After` / backoff в OpenAI SDK — отдельная модель изоляции задержки.

Сквозной пример: live e2e ``test_live_telegram_wiremock_private_tail_307_second_message`` (каталог стабов
``tests/e2e/wiremock_stubs/test_telegram_wiremock_live_e2e_private_tail_307``, POST-триггер
``/__threlium/e2e/state/reasoning_release``, хелпер ``wiremock_state_reasoning_gate_release`` в ``tests/e2e/wiremock_client.py``).
