# Threlium — E2E: harness, изоляция, параллельность

Единый документ по сквозному (e2e) тестированию Threlium: **зачем** e2e — единственный
автоматизированный gate, **как** устроен harness (Docker Compose + Testcontainers + baked-образ SUT),
и — центрально — **как тесты изолированы** на одном общем WireMock при `pytest -n N`.

Этот документ заменяет прежние `TESTING.md` и `E2E_ISOLATION.md`: они слиты и переосмыслены вокруг одного
стержня — **коррелятора** (см. §2). Связанные контракты — в [INDEX.md](INDEX.md) (storage/fdm/nm_settle/
LightRAG-воркер), [ORCHESTRATION.md](ORCHESTRATION.md) (serial-per-thread, parallel-across-threads),
[PLAYBOOK.md](PLAYBOOK.md) (классы операций, тег `refresh`), [MESSAGES.md](MESSAGES.md) (канонизация MID),
[THREAD_MODEL.md](THREAD_MODEL.md) и [BRIDGE_ISOMORPH.md](BRIDGE_ISOMORPH.md) (тред-идентичность мостов).

> **Терминология «archive».** В коде встречаются два омонима: **bundle archive** (`*.tar.gz` post-deploy,
> артефакт установки) и **mail archive** (историческая выделенная `archive/Maildir` — её больше нет: union
> notmuch root указывает на `stages/`, каждое письмо durable в `stages/<stage>/Maildir/cur/<id>:2,S` после
> `nm_settle()`). Хелперы вида `*_archive*` означают именно «весь тред в union-индексе поверх `stages/`».

---

## 1. Философия: почему e2e — единственный gate

**Политика** ([ARCHITECTURE.md §1.3](ARCHITECTURE.md#13-политика-тестирования)): **единственный**
автоматизированный pytest-gate — e2e в `tests/e2e/`. **Юнит/интеграционных тестов в проекте нет**
(маркер `e2e` навешивается на все `test_*.py` collection-хуком; `tests/unit/` отсутствует намеренно).

**Почему.** Поведение Threlium эмерджентно — композиция `fdm` (`~/.fdm.conf`: `match` → `pipe` →
`notmuch insert … && threlium-dispatch.sh`), `notmuch`, FSM-стадий, RAG-loop **внутри** `threlium-engine`,
мостов и LLM. Единица изоляции в проде — связка submit `threlium-work@` ↔ долгоживущий `threlium-engine`
(handler стадии исполняется **in-process** в движке). Инварианты оркестрации (serial-per-thread, форк треда,
fdm `insert && dispatch`, `threlium-sweep@` backstop) достоверно видны только на живом `systemd --user` в SUT
— их нельзя замокать в юнит-тесте, не потеряв предмет проверки.

**Детерминизм.** e2e проверяет **контур**, а не качество модели: стаб → ответ → журнал/инвариант. Стохастика
реального LLM сломала бы контракт, поэтому **все** LLM/embeddings/мессенджер-API — WireMock-стабы.

**Политика честности (критично).** Тест **не** правит код продукта и **не** проталкивает данные в notmuch/
Maildir внутри SUT (никаких ручных `notmuch insert`, подкладки писем в `new/`, подмены бизнес-логики ради
зелёного assert). Поведенческие таймауты **не** повышают, чтобы «дождаться» медленного контура — чинят стабы/
вход/продукт. Разрешено только **чтение** диска/notmuch для сверки промежуточного состояния. Отсутствие
Docker/Linux/extras `[e2e]` — `pytest.fail(pytrace=False)`, **не** skip: дефект среды не прячут за зелёным.

---

## 2. Стержень: изоляция = коррелятор

Главный урок параллельного прогона (`pytest -n N` на одном WireMock): **изоляция тестов сводится к одному
понятию — коррелятору треда** (`thread-root`). Всё остальное (State-контексты, фазовые защёлки, shared-list,
журнальный guard) — обвязка вокруг него.

**Коррелятор** — это `X-Threlium-Thread-Root`: канонический `Message-ID` **корня** notmuch-треда (старейший
`tag:route`). Каждый исходящий LiteLLM-запрос стадии несёт его в заголовке; стаб матчится по составному имени
контекста `{stub_tag}::{thread-root}`. Два теста с разными thread-root физически не могут cross-match: контексты
`stub-A::root_X` и `stub-B::root_Y` — разные записи в Store.

### 2.1. Три требования к корректному коррелятору

Чтобы изоляция работала на `-n N`, коррелятор теста должен быть одновременно:

1. **Уникальным** — у каждого теста свой; иначе стабы/треды/фазы соседей пересекаются.
2. **Предсказуемым тесту** — тест должен знать значение **до** запроса, чтобы засидить
   `{stub_tag}::{thread-root}` (сид — до того, как SUT сделает первый LLM-вызов; гонки «ingress→первый LLM» нет).
3. **Collision-free по содержимому** — идентичные тела запросов НЕ должны схлопываться в один коррелятор.

Третий пункт — самый коварный и был первопричиной `-n2`-регрессии. Контент-адресуемый MID (`hash(тело)`) даёт
**одинаковый** коррелятор для одинаковых тел → notmuch дедупит/сливает треды → у соседнего теста «исчезает»
glue/тред → unmatched и/или зависание long-hold. Вывод: **при общей notmuch-БД и контент-адресуемых
коррелятах содержимое и запросов, И ответов должно быть test-уникальным** — либо коррелятор не должен зависеть
от содержимого вовсе (см. §2.3).

### 2.2. Стратегия A — контент-адресуемый коррелятор (precompute)

Тест **предвычисляет** thread-root тем же кодом, что и продукт, из тела, которым он владеет:
`ingress_message_id(parent="", tail=<tail>)`. Подходит, когда тест полностью владеет телом (прямой HTTP-клиент):
`thread_root_from_body(surface, body)` ([toolkit/isomorph_cline.py](../tests/e2e/toolkit/isomorph_cline.py)).

**Где ломается** (уроки сессии):
- **Коллизии идентичных тел** (см. §2.1.3): два теста/хода с одинаковым телом → один MID. Лечится только
  test-уникальным содержимым (разные тела + разные ответы-маркеры).
- **Хрупкость реконструкции.** Для реального клиента (Cline) тест **реконструирует** тело, чтобы предвычислить
  MID — и реконструкция зависит от даты/шаблона системного промпта клиента. На границе суток
  `cline_today_mdy()` разъезжается с датой, которую инжектит сам клиент → MID не совпадает → массовый unmatched.
  Это **не** баг продукта, а ловушка хрупкого precompute.

### 2.3. Стратегия B — явный инъецированный коррелятор (`E2E_MID:`) ⭐

Робастная замена precompute. Тест генерит **детерминированный уникальный** MID тем же кодом, что и egress
(`e2e_explicit_root_mid(marker)` → `snowflake_to_mid(hash(marker))` → `<b62@localhost>`), и **кладёт его прямо
в тело запроса** токеном `E2E_MID:<...@localhost>`. Мост в e2e-режиме (`settings.e2e.litellm_route_correlation`)
вынимает токен (`extract_e2e_explicit_mid`, [snowflake_mid.py](../ansible/roles/threlium/files/scripts/threlium/bridges/isomorph/snowflake_mid.py))
и берёт его как ingress thread-root — **без** content-hash. Тест сидит тот же MID. Совпадение гарантировано.

Свойства: уникален (по marker), предсказуем (тест сам выбрал), collision-free (не зависит от тела/даты).
Хелперы — `e2e_explicit_root_mid` / `e2e_root_prompt_token` / `e2e_explicit_root_corr` (inner-форма
pending↔push коррелятора). Это **только** для тестов; прод генерит уникальный MID сам (snowflake), без токена.

**Разделение test/prod.** Прод не предсказуем тесту (snowflake — случайно-уникален), поэтому e2e и прод
разводятся флагом источника MID: прод → snowflake; e2e → `E2E_MID:`/precompute. Механизм продолжения треда
(невидимый водяной знак glue в ответе → `In-Reply-To` следующего хода) **одинаков** в обоих режимах, поэтому
content-режим e2e всё равно прогоняет продакшн-путь — флаг обходит лишь сам генератор MID. Производственная
тред-идентичность мостов — [BRIDGE_ISOMORPH.md](BRIDGE_ISOMORPH.md) / [THREAD_MODEL.md](THREAD_MODEL.md).

### 2.4. Коррелятор LiteLLM на проде vs e2e

Снимок корреляции живёт в **ContextVar** ([litellm_route_context.py](../ansible/roles/threlium/files/scripts/threlium/litellm_route_context.py),
set/reset на границах стадий; дочерние async-задачи наследуют копию). На границе вызова
`merge_litellm_call_kwargs_and_log` ([litellm_client.py](../ansible/roles/threlium/files/scripts/threlium/litellm_client.py))
переносит снимок в `extra_headers`. В снимок входят **только** whitelist-заголовки конверта (`From`, `To`,
`Message-ID`, `In-Reply-To`) + `X-Threlium-Thread-Root` (из корня треда notmuch,
[resolve_route_from_thread_oldest_route_tag](../ansible/roles/threlium/files/scripts/threlium/ingress_route_resolve.py))
+ `X-Threlium-Call-Site` (enum [LitellmCallSite](../ansible/roles/threlium/files/scripts/threlium/types/litellm_call_site.py)).

**Различие.** На проде merge HTTP-заголовков **выключен** (`litellm_route_correlation=false` в
[defaults](../ansible/roles/threlium/defaults/main.yml)); call-site используется лишь для выбора фазы внутри
`llm_func`. В e2e (`group_vars/e2e.yml: threlium_e2e_litellm_route_correlation: true`) заголовки **дополнительно**
подмешиваются для WireMock `hasContext`. Базовый `lightrag_query` штампится **всегда** (прод+e2e) — иначе
`detect_lightrag_call_site_wire` дефолтнул бы к `lightrag_index` и вернул бы `extract_knowledge_graph` вместо
`generate_rag_answer` (wire-мусор в `## Answer`).

### 2.5. Гранулярный `X-Threlium-Call-Site`

Внутри одного thread-root разные LLM-вызовы различаются вторым дискриминатором — `X-Threlium-Call-Site`.
Для LightRAG он вычисляется в рантайме `detect_lightrag_call_site_wire` по сигналам `llm_func`
(`keyword_extraction` / `history_messages` / `system_prompt`, **без** инспекции prompt content) и равен
`function.name` единственного tool (`extract_knowledge_graph` / `…_gleaning` / `summarize_descriptions` /
`extract_query_keywords` / `generate_rag_answer`). Инвариант chat-вызова с одним tool:
`X-Threlium-Call-Site == tools[0].function.name` (исключение — reasoning multi-tool = `reasoning`); проверяется
в `merge_litellm_call_kwargs_and_log`. Offline-аудит контракта стабов — `python scripts/audit_wiremock_tool_stubs.py`.

**Граница гранулярности (важно):** call-site = имя tool-функции, поэтому одна и та же точка вызова в РАЗНЫХ
стадиях НЕ различается (`enrich` и `enrich_fast` зовут один enrich-LLM → оба `enrich_task_plan`). Вводить
синтетический `enrich_fast_*` не нужно: `enrich_fast` — стадия **без своего** LLM-вызова (быстрый rebuild
контекста, «reasoning без повторного RAG»), её «прогон» в стабах вообще не виден. Recovery-петлю
(gate→memory_query→enrich_fast→reasoning) проверяем по **содержимому reasoning** (§3.6.2): весь контекст,
включая recovery-артефакты, попадает в reasoning-промпт. Routing-стадии без LLM (`enrich_fast`,
`egress_router`, `archive`) в call-site списке не представлены — их эффект виден либо в reasoning-контенте,
либо в ответном письме GreenMail.

---

## 3. WireMock State Extension — механизм изоляции

Изоляция держится на [wiremock-state-extension](https://github.com/wiremock/wiremock-state-extension)
(исходники `vendor/wiremock/wiremock-state-extension`; в compose монтируется standalone-JAR в
`/var/wiremock/extensions/`, ServiceLoader, без `--extensions`; нужен `--global-response-templating`).
Расширение хранит **данные** между стабами (properties, shared list) и матчит запросы по состоянию.

### 3.1. Модель данных

| Термин | Описание |
| --- | --- |
| `context` | Имя ключа в Store. В Threlium — составной `{stub_tag}::{thread-root}` или shared (`matrix_rooms`, `telegram_updates`). |
| `state` | Карта `property → value` (строки) на контекст. Повторный `recordState` **мерджит**. |
| `property` | Поле в `state`. Значение `"null"` (строка) **удаляет** свойство. |
| `list` | Упорядоченный список карт в контексте. Только `addFirst`/`addLast`/delete — **in-place правки нет**. |
| `updateCount` | Счётчик изменений контекста (+1 за запрос с ≥1 write). |

Store — `CaffeineStore` (in-memory, lock на весь Store; TTL по умолчанию 60 мин). **Не** распределён; для
параллельных воркеров — **разные контексты** (§2). Шесть расширений одного `ExtensionFactory`:
`recordState`/`deleteState`/`stateTransaction` (ServeEventListener), `state-matcher` (RequestMatcher),
`state` (Handlebars helper), `stateAdminApi`.

### 3.2. `state-matcher` — матчинг по контексту

`customMatcher: {"name": "state-matcher", "parameters": {...}}`. Имя в `hasContext`/`hasNotContext` рендерится
Handlebars **до** проверки (можно `{{request.headers.[x-threlium-thread-root]}}`). Предикаты на контексте:
`hasContext`/`hasNotContext` (есть/нет), `hasProperty`/`hasNotProperty`, `property` (любой `StringValuePattern`),
`list`, `updateCount*`, `listSize*`. Несколько **разных** ключей в одном flat-объекте агрегируются через **AND**.
Логические `and`/`or`/`not` — массивами.

**Базовый стаб LiteLLM:**
```json
{ "request": {
    "urlPathPattern": "^(/v1/chat/completions|/chat/completions)$",
    "headers": { "X-Threlium-Call-Site": { "equalTo": "generate_rag_answer" } },
    "customMatcher": { "name": "state-matcher", "parameters": {
        "hasContext": "stub-<scenario>-01::{{request.headers.[x-threlium-thread-root]}}" } } },
  "response": { "status": 200, "...": "..." } }
```
`stub_tag` (`stub-<scenario>-01`) **захардкожен в JSON** — `upsert` его НЕ подставляет (метаданные ≠ матчер).
Поэтому при переиспользовании каталога стабов другим тестом **сидить нужно тем же зашитым `stub_tag`**, а
изоляция держится на СВОЁМ thread-root (урок SSE-тестов §7).

### 3.3. Фазовый автомат внутри треда — без `priority`

Для нескольких LLM-вызовов одного типа в одном ходу (reasoning: первый → `tool_calls`, второй → финал)
используются **взаимоисключающие** стабы на одном контексте: один с `hasNotProperty: phase_X`, другой — с
`hasProperty: phase_X`; первый по `recordState` ставит `phase_X`. Фазы живут **в контексте треда** → у
параллельных тестов разные → не пересекаются.

**`priority` запрещён** в сценарных стабах (`tests/e2e/wiremock_stubs/`, кроме `compose_bootstrap/`). Причина:
при `priority` порядок решает число, а не disjoint-state; параллельные фазы становятся непредсказуемы. Если
**два** стаба матчат один запрос одновременно — это ошибка проектирования: WireMock при равном default-priority
(=5) отдаёт **последний зарегистрированный** mapping (`upsert` = remove+add; файлы грузятся по имени — `102_`
позже `100_`). Модель держится на том, что state делает фазы disjoint, а не на `priority`. Также **запрещён**
`doesNotContain`-эксклюзий чужих тестов (допустим лишь для фаз **одного** сценария). Проверка:
`rg '"priority"' tests/e2e/wiremock_stubs/test_` → пусто.

> **Ловушка stale-латч (урок client-disconnect).** Если два теста делят **marker** → делят thread-root →
> делят State-контекст reasoning. `clean_isomorph_test_threads` чистит notmuch, но **не** фазовую защёлку в
> WireMock. Второй тест видит чужой `phase_tasks_ledger_done` → пропускает фазу закрытия задач → finalize-loop
> (open subtasks) → воркер не доходит до idle → teardown зависает. **Лечение: свой marker = свой thread-root =
> свой контекст** (а не reset защёлки задним числом). Multi-turn одного теста (общий контекст с happy-path)
> явно сбрасывает защёлку `wiremock_state_reset_phase` между ходами.

### 3.4. Helper `state` — чтение в ответах (shared list)

В `response-template` (только поле `body`-строка, **не** `jsonBody`): `{{#each (state context='matrix_rooms'
property='list' default='[]')}}…{{/each}}`. Между элементами — `{{#unless @last}},{{/unless}}`. Литеральная `{`
перед `{{#each}}` требует **пробела** (`{ {{#each`), иначе Handlebars видит triple-stache `{{{` →
`HandlebarsException`. Спец-properties: `updateCount`/`listSize`/`list` (весь массив).

### 3.5. Admin API + запись

База `/__admin/state-extension/`. GET `/contexts`, GET/DELETE `/contexts/{name}`, DELETE `/contexts` (все).
**Запись — только через стабы** (PUT/POST в Admin нет); сид — POST на публичный setup-стаб.

> **Gotcha:** `DELETE …/contexts/{name}` с `::`/`<`/`>`/`@` в имени (типичный составной ключ + MID) часто
> **no-op** (204, но контекст остаётся — имя в path не доходит до handler). Точечное снятие property —
> POST-триггеры (`phase_reset`, `recordState` с `"null"`), не Admin DELETE.

### 3.6. On-the-fly запись в state — asserts без зависимости от журнала ⭐

Расследование исходников (`vendor/wiremock/wiremock-state-extension`, `vendor/wiremock/wiremock`): стаб
может **на лету, во время обслуживания запроса**, считать и записать в state счётчики/флаги/выжимки —
так тест проверяет инвариант по **state** (читая его probe-стабом, см. §3.6.1/§3.6.2 — Admin
`GET /contexts/{name}` ломается на спецсимволах thread-root `::`/`<`/`>`/`@`, §3.5), а не сканируя журнал.
Это снимает зависимость от **объёма журнала** (кумулятивен за сессию, ring-buffer
`--max-request-journal-entries`, см. §9) и от per-tag чисток.

Как это работает (`RecordStateEventListener.beforeResponseSent`): значения `state`/`list` в `recordState`
**рендерятся Handlebars** по модели `request` + `response` (`{{jsonPath request.body …}}`,
`{{request.headers.…}}`), а `TemplateEngine` WireMock регистрирует jknack-хелперы `ConditionalHelpers`
(`eq`/`gt`/`and`/`or`/`not`), `NumberHelper`, `StringHelpers` + `contains`/`size`/`val (assign)` и сам
helper `state` (чтение текущего контекста). То есть прямо в `recordState` доступны и текущее состояние, и
данные запроса.

Идиомы (вместо «просканировать журнал по `stub_tag`»):
- **Счётчик попаданий** без арифметики: на каждый матч `list: { addLast: { hit: "1" } }` → число =
  `listSize` (special-property; читается probe-стабом `…/state/list_size`, не Admin path — см. §3.6.1).
- **Захват/выжимка**: `state: { last_chat_id: "{{jsonPath request.body '$.chat_id'}}" }`.
- **Assert на лету как флаг**: `state: { saw_needle: "{{#if (contains request.body 'NEEDLE')}}1{{/if}}" }`
  или сравнение через `eq`/`gt`; тест читает флаг из контекста.
- **Чтение в ответе/следующей фазе**: helper `{{state context='…' property='listSize'}}` (или `property`/
  `list`); спец-свойства — `updateCount`, `listSize`, `list`.

Ограничения: `recordState` — `serveEventListener` на **сматченном** стабе → срабатывает только когда стаб
матчится (unmatched в state не попадёт — для «чужого/неожиданного трафика» остаётся journal unmatched-guard
§5); запись идёт `beforeResponseSent` (после решения о матче) → влияет на **следующий** запрос, не на свой
(паттерн фаз §3.3). Рекомендация: где assert сейчас зависит от полноты журнала (подсчёт LLM-POST, поиск по
`needle`), переноси на state-счётчик/флаг — устойчивее на `-n2`.

### 3.6.1. Единый call-site recorder + state-asserts (итоговое состояние) ⭐

**Целевая архитектура проверок** (uniform, единый подход): весь жизненный цикл сценария наблюдаем по
вызовам моделей в WireMock, поэтому проверяем по **state**, а наружу ходим только в **GreenMail** (финальное
письмо). Никаких `docker exec` (`service_exec`) в SUT для ассертов, никакого скана журнала, никакой изоляции
по `stub_tag` — только коррелятор-заголовок `X-Threlium-Thread-Root` (§2).

- **Static call-site recorder.** Каждый сценарный LLM-стаб (`chat/completions` + `embeddings` со
  `state-matcher`) СТАТИЧЕСКИ несёт листенер
  `recordState → list.addLast { cs: "{{request.headers.[X-Threlium-Call-Site]}}" }` в контекст,
  ключёванный ЧИСТО по `{{request.headers.[X-Threlium-Thread-Root]}}` (tag-free). Так в state копится
  **упорядоченный список call-site всего треда** (`ingress_distill → enrich_* → lightrag_* → reasoning →
  summarize_thread_context → …`). Листенер — **в JSON стаба** (не инъекция в рантайме: динамическая
  правка/генерация стабов запрещена, §6.4; «динамика» = статический recordState).
- **Чтение — probe-стабы `compose_bootstrap/` (helper `state`, не Admin path):**
  `POST /__threlium/e2e/state/call_sites` → `{"call_sites":[…]}` (helper
  `wiremock_state_thread_root_call_sites`), `…/state/property`, `…/state/list_size`.
- **Все прежние journal/docker-exec проверки выводятся из списка:** число LLM-POST = `len`; покрытие стадии
  = `cs in call_sites`; summarize-count = `count('summarize_thread_context')`; lightrag-indexed =
  `'lightrag_index' in call_sites` (заменил `docker exec stat` глобального faiss — тот под `-n2` голодал на
  конкуренции docker-exec, см. §9). Терминальные стадии без LLM (`egress_router`/`egress_email`/`archive`)
  подтверждаются **ответным письмом GreenMail**, а не стабом.
- **Идеал (финальная фаза):** полный уход от `stub_tag` и `THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG`.
  `id` запекается статически в каждый стаб; cold-reset `reset_non_bootstrap` оставляет по **множеству id
  bootstrap-каталога** (а не по тегу); метаданные `stub_tag` исчезают вместе с journal-поиском. Итог: чистая
  статика стабов + state-extension, изоляция только по thread-root, наружу — только GreenMail+WireMock.

### 3.6.2. Проверка СОДЕРЖИМОГО — content-flags (а не журнал, не bodyPatterns+guard) ⭐

call-site список (§3.6.1) даёт **счёт/наличие** стадий. Проверку **содержимого** запроса (попал ли нужный
текст в промпт LLM) делаем **content-flag**: статический `recordState` на стабе на лету вычисляет
`contains` по телу и пишет флаг в state; тест читает флаг probe-стабом `/state/property`.

**Почему content-flag, а НЕ bodyPatterns+unmatched-guard** (рассмотрены оба):
- bodyPatterns+guard **скрывает «где упало»**: неверное содержимое → стаб не сматчился → unmatched → падает
  *guard* обобщённо (часто в другом тесте/в конце, каскадом), а не «маркер X отсутствует».
- правка Jinja2-промпта при bodyPatterns **ломает контур**: стаб перестаёт матчиться → у LLM-вызова нет
  ответа-заглушки → FSM висит/падает (а не просто ассерт). content-flag оставляет матчер мягким (контур жив),
  падает на конкретном ассерте — та же диагностируемость, что у прежнего journal-скана, но дёшево из state.

**Идиомы:**
- **Наличие:** `"saw_X": "{{#if (contains request.body 'MARKER')}}1{{else}}…{{/if}}"`; тест: `saw_X == "1"`.
- **Sticky** (для multi-hop: флаг не сбрасывать следующим вызовом без маркера) — в `{{else}}` читаем текущее
  значение: `{{state context=request.headers.[X-Threlium-Thread-Root] property='saw_X' default='0'}}`.
- **Несколько вариантов:** `(or (contains … 'H1') (contains … 'H2') …)` (jknack `or` — вариадик).
- **Отсутствие (нет negative matchers — они хрупки):** позитивный флаг «forbidden present» + ассерт `== "0"`.
  Посекционное отсутствие (контент запрещён в одной секции, допустим в другой) `contains` не выражает →
  если в тест реально инжектится сырой контент в обе роли, дать им **разные токены на стороне теста**
  (тест контролирует и инжект, и стабы) и проверять whole-body наличие/отсутствие; если сырой контент не
  инжектится вовсе (как в summarize, `pad_chars=0`) — whole-body флаг эквивалентен секционному.

**Time-independent чтение (не поллинг — поллинг = риск flaky):** читаем флаг ПОСЛЕ существующего
happens-before барьера. Контентные ассерты идут после `assert_full_mailflow_pipeline` (ждёт ответ GreenMail =
контур завершён), а reasoning/summarize отрабатывают причинно ДО egress→ответа (`recordState`
beforeResponseSent) → флаг уже записан → **прямое чтение**, без таймаута. Поллим только истинно
асинхронное (напр. `lightrag_index` drain, отстающий от ответа).

**Матрица «что чем проверяем» (итог):** счёт/наличие стадий → call-site список (§3.6.1); содержимое промпта →
content-flag; egress транспорта (telegram/matrix) — это тоже WireMock-вызов с thread-root → content-flag на
egress-стабе (как GreenMail для почты); финальная доставка почты → GreenMail; целостность (чужой трафик в
пустоту) → journal **unmatched-guard** (§5) — единственное оставшееся использование журнала. `docker exec`
(`service_exec`) — только setup/cleanup/deploy/restart и failure-diag, НЕ в проверках.

---

## 4. Каналы: коррелятор + транспорт

После моста все каналы → email (`build_bridge_ingress_email`) с уникальным `X-Threlium-Route`; дальше pipeline
(enrich → reasoning → egress) изолирован **одинаково** — `hasContext` по `X-Threlium-Thread-Root`. Различается
только **как получить коррелятор** и транспортный bootstrap.

| Канал | Коррелятор (thread-root) | Транспорт-bootstrap | Egress-стаб |
| --- | --- | --- | --- |
| **Email** | MID старейшего `tag:route` треда; тест: inner SMTP-инъекции (`e2e_smtp_inject_ingress_route_wire_for_message_id`) | SMTP→GreenMail→IMAP bridge | `sendMessage`/msmtp (не state-matcher) |
| **Matrix** | `RfcMessageIdWire(MatrixNativeId(room_id,event_id))` корневого события | один `/sync` + shared list `matrix_rooms` (`#each`) | `room_send`: state-matcher (nio custom_headers) |
| **Telegram** | MID из `chat_id`/`message_id`/`message_thread_id` | один `getUpdates` + shared list `telegram_updates` | `sendMessage`: bodyPatterns (PTB не шлёт thread-root на wire) |
| **Isomorph** | `E2E_MID:` (§2.3) или content-hash; прод — snowflake | прямой HTTP в мост (long-hold) | egress push в мост; водяной знак в ответе |

**Shared-list каналы (Matrix/Telegram).** Мост делает **один** `/sync` (или `getUpdates`) на весь homeserver,
поэтому ответ должен содержать события **всех** активных тестов. Решение — общий контекст с `list`:
тест в setup `register_room`/`register_update` (`addLast` своей записи), bootstrap-стаб собирает ответ `#each`
по list; в `finally` `unregister_*` (`deleteWhere` по `room_id`/`update_id` — **только своё**). Один
bootstrap-стаб **без** `state-matcher`/`listSizeMoreThan` (пустой list → пустой ответ, не unmatched).
List-операции из разных xdist-воркеров сериализуются тем же межпроцессным `_wiremock_admin_api_exclusive`
(FileLock `e2e_wiremock_admin_api.lock`), что и Admin GET. Дедуп повторных событий — на мосту (notmuch по MID).

**Isomorph (long-hold).** Прямой HTTP-мост: тест POST-ит тело сам → не нужен bootstrap-транспорт. Изоляция —
`E2E_MID:` thread-root (§2.3). Egress пушит ответ обратно в мост (`/internal/v1/push`); тред-непрерывность — не
голосование, а **невидимый водяной знак** glue-MID в content ответа (клиент возвращает его в истории →
`In-Reply-To` следующего хода). Детали — [BRIDGE_ISOMORPH.md](BRIDGE_ISOMORPH.md).

---

## 5. Параллельная безопасность (`pytest -n N`)

**Цель** — одновременно нагрузить и xdist-воркеры, и несколько notmuch-тредов в SUT (контракт
*serial-per-thread, parallel-across-threads*, [ORCHESTRATION.md](ORCHESTRATION.md)). `pytest.mark.
xdist_group("exclusive")` и любая exclusive-сериализация e2e **запрещены** — при гонках расширяют якоря, а не
отключают параллельность.

**Что параллельно-безопасно:** изолированный коррелятор на тест (§2) + узкие `bodyPatterns`/`X-Threlium-Call-Site`
+ свой каталог стабов/`stub_tag`. Несколько воркеров одновременно бьют в один WM — каждый запрос **обязан**
сматчиться своими стабами; иначе unmatched и любой воркер падает на guard.

**Журнальный guard** (нормативный инвариант целостности стабов): `GET /__admin/requests/unmatched` **глобально
пуст** — проверяется в `pytest_runtest_call` до и после тела каждого теста ([conftest.py](../tests/e2e/conftest.py)).
Фильтр по заголовкам **не** применяют (у unmatched-запроса может не быть `X-Threlium-Route`). Единственный
допустимый FileLock — вокруг самого Admin GET в `wiremock_unmatched_request_entries` (иначе 500 WM при
параллельных опросах); сам хук локами не сериализуют.

**Запрещено из кода сценариев** на общем WM: `wiremock_state_reset_all_contexts`, `reset_request_journal`
(`DELETE /__admin/requests`), глобальный `DELETE /__admin/mappings` — снесут чужие воркеры (bootstrap, State).
Исключение — **один** координированный cold reset на инвокацию pytest (`_e2e_wiremock_journal_reset_once`: под
FileLock лидер останавливает pipeline, `reset_non_bootstrap_wiremock_mappings`, журнал, Store, Maildir, upsert
`compose_bootstrap/`, запуск engine).

**Collision-at-root (центральный урок -n2).** Контент-адресуемые коррелятор/glue-MID **коллизируют** при
идентичном содержимом → notmuch сливает треды → каскад unmatched/зависаний. Под `-n2` лечится: (1) test-уникальные
тела И ответы, либо (2) явный `E2E_MID:` (§2.3). Прод снимает это в корне уникальными snowflake-MID.

**Изоляция журнала — по thread-root, НЕ по `stub_tag` (урок -n2-каскада).** `stub_tag` зашит в JSON каталога
стабов, поэтому **совпадает у тестов, переиспользующих один каталог** (telegram private + duplicate_skip;
summarize overflow + idempotent; task-ledger chain `*_chain_e2e` + параметрический `[task_ledger_chain]`).
`prepare_wiremock_scenario` раньше чистил журнал `remove_wiremock_journal_by_stub_tag` — на общем `-n2`-WM это
стирало matched-записи **параллельного** теста с тем же тегом → его journal-ассерт ловил ложный «0»/timeout, и
по глобальному guard каскадом падали все последующие. Чистка переведена на **свой thread-root**
(`remove_wiremock_journal_by_thread_root`: `POST /__admin/requests/remove` по заголовку `X-Threlium-Thread-Root`)
— тест трогает только свой тред, никогда чужой. По той же причине per-test journal-**поиск** должен скоупиться
по thread-root/уникальному ключу (chat_id), а не по одному `stub_tag` (иначе over-count соседа). Идеал —
вообще не зависеть от журнала, считать на лету в state (§3.6). `stub_tag` остаётся только для cleanup стабов и
диагностики, не для изоляции (см. §2: изоляция = коррелятор-заголовок).

**Журнал кумулятивен за сессию → ёмкость, не таймауты.** Журнал WireMock не чистится per-test (только cold reset
в начале сессии + свой thread-root); полный `-n2` суммарно даёт тысячи записей, упираясь в ring-buffer
`--max-request-journal-entries` (§9). Но вытесняются **старейшие** (ранних тестов), а тест ищет matched по
`matchingStub=<uuid>` свои **свежие** записи — поэтому вытеснение само по себе не роняет ассерты. Журнал-таймауты
под `-n2` — это **нагрузка** (см. ниже), а не объём; поднимать лимит как «фикс таймаутов» бесполезно (проверено:
2500 vs 20000 — тот же набор падений) и лишь добавляет память/GC WM.

**Нагрузочный потолок `-n2` полного набора.** ~70 тестов целиком под `-n2` на слабой машине: два тяжёлых контура
(reasoning + LightRAG entity extraction) конкурентно не успевают в фиксированный `TIMEOUT_POLL_SHORT=30c` →
~15-19 timeout'ов на тяжёлых тестах (reasoning/response_table/summarize/telegram-full-contour/matrix), коррелируя
с длительностью прогона (≈5 мин «налегке» vs ≈13 мин под нагрузкой). Это **ёмкость среды**, не логика и не
изоляция (таймаут не повышаем — §1). Снимается мощной машиной / меньшей конкуренцией (`-n` пониже для тяжёлых) /
ускорением контура; изоляционные баги (каскад, cross-wipe) при этом **уже** устранены.

**Serial-only тесты (skip-under-xdist).** Тест, который меняет **глобальную** конфигурацию моста (env +
рестарт) — несовместим с параллельными ходами и обязан:
```python
if os.environ.get("PYTEST_XDIST_WORKER"):
    pytest.skip("меняет глобальный конфиг моста → serial only (-n0)")
```
и **восстанавливать конфиг в `finally`** фикстуры (робастно при любом исходе). Пример — upstream-timeout→504
(§7.4): фикстура понижает `request_timeout_sec` до 8c и возвращает дефолт в teardown. Под `-n2` такой тест
показывается как `skipped`, не ломая остальных; валидируется в обычном `-n0`-прогоне.

---

## 6. Харнесс

### 6.1. Compose-стек

[`tests/e2e/compose/docker-compose.yml`](../tests/e2e/compose/docker-compose.yml): `sut`, `greenmail`, `wiremock`.

- **`sut`** — privileged + cgroup host + mount `/sys/fs/cgroup` (нужно для `loginctl enable-linger`,
  `systemctl --user`, `.path`-юнитов с inotify). Baked-образ `threlium/e2e-sut:baked`. Cockpit HTTPS :9090,
  Caddy HTTP :8080.
- **`greenmail`** (`standalone:latest`) — SMTP 3025 / IMAP 3143 (pytest с хоста) / IMAPS 3993 (мост в SUT);
  TLS PKCS#12 `greenmail.p12` (SAN `localhost`/`greenmail`/`127.0.0.1`). Динамический host-port (`"3025"`),
  pytest находит через `_mapped_port`.
- **`wiremock`** (`wiremock:latest`, host 9080→8080, `--global-response-templating`) — **единственный** HTTP-mock
  для OpenAI-совместимых вызовов (`/chat/completions`, `/embeddings` — без `/v1/`), Matrix (`/_matrix/…`),
  Telegram Bot API. State-extension JAR + classpath.

### 6.2. Baked-образ SUT

**Bake** — на bootstrap-образе (`geerlingguy/docker-ubuntu2404-ansible`) прогоняется тот же `site.yml`, что в
проде → `docker commit` в `threlium/e2e-sut:baked`. В образе — развёрнутая система, **источник правды —
`site.yml`, отдельного Dockerfile нет**. `ensure_e2e_sut_image_exists`: reuse по `docker image inspect`;
форс — `THRELIUM_E2E_REBUILD_BAKED_IMAGE=1` или `pytest -n0 tests/e2e/wipe_bake.py` (под локом
`/tmp/threlium_e2e_bake_image.lock`). Пересобирать при: правках `site.yml`/ролей/apt/pip/bootstrap-образа.
Правки только Python-кода Threlium/тестов/докум. — **не** повод.

### 6.3. Shared compose + filelock

Дефолт — **однопоточный** `pytest tests/e2e` (лидер = единственный участник). Параллельный контракт — **явно**
`-n N`; `addopts = -n N` в `pyproject.toml` **не** ставить. Все xdist-воркеры делят **один** compose-проект
`threlium_e2e_shared_{hex}`: первый под `FileLock` поднимает стек и пишет `ready.flag` + `runtime.json`
(`e2e_compose_coord_paths()`), остальные читают `project_name` / `discover_runtime`. «Мёртвый» координатор
(файлы есть, стек остановлен) — лидер проверяет running-контейнеры через Docker API и сбрасывает флаги.
`pytest_sessionfinish` **не** делает `compose down` (reuse; opt-in `THRELIUM_E2E_COMPOSE_DOWN=1`).

### 6.4. Фикстуры и toolkit

| Фикстура | Скоуп | Роль |
| --- | --- | --- |
| `compose_stack` | session | Attach-only к healthy стеку; journal reset WM, `runtime.json`. |
| `e2e_runtime` | function (autouse) | Per-test: координированный reset → pipeline → idle → reset WM. |

Toolkit ([`tests/e2e/toolkit/`](../tests/e2e/toolkit/)) — пакет harness: runtime/compose-обвязка, SUT image
strategy, polling через `tenacity` (`poll_until` fixed / `poll_until_backoff` exp, progress каждые 15c),
GreenMail/IMAP/notmuch waiters, WireMock-журнал, ansible, диагностика. Контракт-константы — `E2E_BAKED_SUT_IMAGE`,
`E2E_THRELIUM_USER`, `E2E_WIREMOCK_CONTAINER_PORT`, `E2E_REPLY_SUBJECT`/`E2E_REPLY_BODY_SNIPPET`, …

**Стабы — только статический закоммиченный JSON (нормативно).** Маппинги живут в git как
`wiremock_stubs/<тест>/*.json`; `compose_bootstrap/` — инфраструктурный (`recordState` setup/phase_reset,
matrix/telegram register, embeddings readiness, **state-readout probes** §3.6; тег
`THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG` переживает cold reset). **Запрещена любая динамическая
генерация/модификация стабов** — ни сборка/патч тел из pytest (временные каталоги, `replace`/Jinja2 по JSON,
Python-сборка `mapping`), ни инъекция `serveEventListeners`/полей в загрузчике на лету. «Динамика» делается
**внутри статического стаба**: `recordState`-листенер + `state`-helper (state-extension) считают/пишут
состояние во время обслуживания — это и есть state-asserts (§3.6), которые и позволяют не генерировать стабы
динамически. **Разрешено** в рантайме только: `wiremock_state_*` (сид/reset/чтение контекста),
`upsert_wiremock_mapping_directory` (грузит JSON как есть; стабильный `id` =
`wiremock_stub_id_for_e2e_stub_relpath`, в metadata — `stub_tag` для cleanup), `{{randomValue …}}` в ответах.
`stub_tag` **не** выбирает стаб на стороне WM и **не** основа изоляции (изоляция = thread-root, §2) — он только
для cleanup стабов.

### 6.5. Деплой в SUT + режимы прогона

Сценарные тесты **не** вызывают `ansible-playbook`. `site.yml` (полный или `--tags repo` для быстрого цикла
кода/конфигов; `--tags refresh` — сброс mail-state + рестарт user-units) — отдельный шаг до сценариев. Только
код `scripts/threlium`+`prompts/` на живом SUT без плейбука — [FSTS_SYNC.md](FSTS_SYNC.md).

| Команда | Что |
| --- | --- |
| `pytest tests/e2e` | Однопоточный прогон (дефолт), attach к baked-стеку. |
| `pytest tests/e2e -n 8` | Параллельный стресс — проверка thread-parallel контракта. |
| `pytest -n0 tests/e2e/wipe_bake.py` | Полный bake образа + сброс координаторов + `compose down`/`up`. |
| `pytest -n0 tests/e2e/wipe_sync.py` | Только harness (`--tags refresh`) на уже поднятом SUT. |

`wipe_*.py` **не** в дефолтной коллекции (имена вне `test_*.py`; не расширять `python_files` до `*.py`).

---

## 7. Паттерны: тестирование long-hold моста (isomorph)

Мост `isomorph` держит HTTP-соединение (long-hold) до egress-push. Эти паттерны переиспользуемы для любого
поведения долгоживущего соединения; все изолированы своим `E2E_MID:` thread-root (§2.3), стабы — переиспользуют
L0-цепочку json-вариантов (FSM-путь тот же; surface меняет лишь кодирование запроса/ответа моста).

### 7.1. Прямой SSE wire-shape (+ keep-alive)

Тест — прямой `stream:true` клиент (`bridge_post_sse` → `curl -N` изнутри SUT), читает **сырой** SSE-поток и
проверяет строгую wire-схему вендора **побайтово** (независимо от толерантности реального Cline):
- **Anthropic**: `message_start → content_block_start → content_block_delta → content_block_stop →
  message_delta → message_stop`; текст ответа — в дельтах.
- **OpenAI**: role в первом чанке, content-чанк, usage-чанк с пустым `choices`, терминатор `[DONE]`, каждый
  кадр `object == chat.completion.chunk`.

`parse_sse_events` разбирает кадры в `(event|None, data)`. **Keep-alive покрывается тем же тестом**: при
`keepalive_sec=20 < оборот FSM (~30c)` под `-n2` в потоке естественно появляется `event: ping` (Anthropic) /
`: keep-alive` (OpenAI) **до** ответа — поэтому `ping` исключают из проверки **порядка** (он валиден где угодно),
но требуют наличие каркасных событий.

### 7.2. Client-disconnect mid-hold

`bridge_post_sse(timeout=4)` обрывает клиента ПОСРЕДИ удержания (`exec_run` не бросает на `rc!=0` → возвращает
частичный поток). Проверка: мост чистит pending своего коннекта (generator `finally` → `forget`, поздний push =
no-op) и **переживает** (health отвечает), а in-flight ход FSM **не** обрывается (независим от коннекта) —
доходит до glue (ARCHIVE-FIRST). **Свой fresh marker обязателен** (иначе stale phase-latch §3.3 → finalize-loop →
teardown зависает).

### 7.3. FSM-error → error-envelope

`error_message` в push → мост отдаёт held-запросу вендорный error-envelope (HTTP 500 `{"error":{…}}`).
Тест: один `sut_exec` фоном держит `stream:false` запрос, через ~5c инъектит push в `/internal/v1/push`
(`bridge_post_json_with_pushed_error`, секрет `e2e-isomorph-push-secret`, `ingress_mid = e2e_explicit_root_corr`
— inner-форма ровно как мост) — push **опережает** FSM (~30c), мост резолвит held ошибкой. Стабы засижены → реальный
ход доходит чисто в фоне (late push = no-op), teardown idle без зависа.

### 7.4. Upstream-timeout → 504 (serial-only)

Мост отдаёт 504, если push не пришёл за `request_timeout_sec` (дефолт 180c). Чтобы не ждать — serial-only
фикстура (skip под xdist, §5) понижает таймаут до 8c (env `THRELIUM_BRIDGES__ISOMORPH__REQUEST_TIMEOUT_SEC` в
`/home/threlium/threlium/agent/env/threlium.env` + рестарт моста) и **восстанавливает в `finally`**. Запрос
держится → мост снимает pending → 504. `curl --max-time 40 > 8` → ловим именно мостовой 504, не клиентский обрыв.

---

## 8. Жизненный цикл State

```
┌─ pytest session start (лидер под FileLock, один раз) ──────────┐
│  cold reset: stop pipeline → flush Maildir/GreenMail            │
│  → reset WM journal + Store + non-bootstrap mappings           │
│  → bootstrap stubs → start engine → idle → journal reset       │
├─ per-test setup (фикстура сценария) ───────────────────────────┤
│  wait idle → wait bridge health → clean_*_test_threads(marker)  │
│  → upsert stubs (свой каталог/stub_tag) → seed context          │
│  → [matrix/tg] register_room / register_update                  │
├─ test body ────────────────────────────────────────────────────┤
│  SUT: bridge → ingress → enrich → reasoning → egress            │
│  каждый LiteLLM-запрос несёт X-Threlium-Thread-Root + Call-Site │
│  state-matcher: composite hasContext + phase; recordState        │
│  guard: GET /requests/unmatched пуст (до и после тела)          │
├─ test teardown (finally) ──────────────────────────────────────┤
│  [matrix/tg] unregister (deleteWhere — только своё)             │
│  контекст route НЕ удалять (поздний трафик SUT)                 │
│  matched-журнал НЕ чистить (остаётся для отладки)               │
├─ pytest_sessionfinish (один раз) ──────────────────────────────┤
│  wait idle + assert zero unmatched → wiremock_state_reset_all   │
│  при FAIL: укороченный drain (FAIL_DRAIN_SEC, 30c)              │
└────────────────────────────────────────────────────────────────┘
```

Контекст route в function-teardown **не** удаляют: поздние LiteLLM-запросы SUT (after test body) должны
по-прежнему матчиться. Полный сброс Store — только в `pytest_sessionfinish` (`wiremock_state_reset_all_contexts`)
**после** idle и пустого unmatched. `e2e_clean_sut_messages_for_test(stub_tag, correlation_key)` между тестами
удаляет на SUT только письма прошлых запусков **этого** marker'а, сохраняя тред текущего `correlation_key` для
multi-turn.

---

## 9. Практические gotchas

- **`bodyPatterns[].matches` — full-match** (как `String.matches()`): regex должен покрыть **всё** тело
  (`"(?s).*….*"`). Для подстрок — `contains`/`matchesJsonPath`.
- **Handlebars `{` перед блоком** — нужен пробел: `"join":{ {{#each …}}` (иначе `{{{` = triple-stache → exception);
  закрытие `{{/each}} }`.
- **LLM-кэш lightrag-hku** на долгоживущем SUT: повторный `aquery` с тем же текстом/keywords может **не** вызвать
  HTTP backend (`cache_type=keywords/query`) → ожидаемой фазы нет в журнале. Варьировать вход хэша — уникальный
  суффикс в seed-ответе/keywords-JSON/первом сообщении; для chat/embeddings — State-контекст.
- **WireMock OSS metadata** в шаблонах ответа не работает (`{{stub.metadata.…}}` → пусто); вариативность —
  `{{randomValue}}` в ответах, не второй слой шаблонизации до `upsert`.
- **307-цепочка для «долгого LLM»** (без удержания сокета): стаб несколько раз отвечает 307 `Location` на тот же
  URL (httpx следует редиректам внутри одного `send()`, POST→POST для 307); переключение «тест отпустил» — второй
  стаб по `hasProperty`/POST-триггеру. Лимиты: httpx `DEFAULT_MAX_REDIRECTS=20`, reasoning `timeout ≈120c` на всю
  цепочку, `max_retries=0` не отключает следование редиректам. **Требует `follow_redirects=True`** у HTTP-клиента:
  свой `openai_compatible_client.py` (замена litellm) шёл с httpx-дефолтом `follow_redirects=False` → 307 не
  следовался (не 4xx/5xx, не ретраябелен), reasoning падал на парсинге → весь 307-gate ломался. Пример —
  `test_live_telegram_wiremock_private_tail_307_second_message`.
- **LightRAG-стор — в Redis, не в файлах.** `doc_status`/`full_docs`/`text_chunks`/`*entities*`/
  `llm_response_cache` — ключи Redis (`$THRELIUM_HOME/lightrag/` хранит только faiss-индексы). Удаление
  `kv_store_doc_status.json` — **no-op** (файла нет): движок видит probe как `Duplicate document` и **пропускает
  embedding** → bootstrap-тест ловит «нет e2e-bootstrap embeddings». Для форс-переиндексации bootstrap нужен
  `redis-cli flushall` + снос faiss; doc_status читать из Redis (`redis-cli get doc_status:*`), не из файла.
  Reindex делает flushall + рестарт **общего** engine → модуль `test_knowledge_bootstrap_live_e2e` serial-only
  под xdist (§5).
- **Notmuch-дедуп при повторном `/sync`** — штатно (`duplicate Message-ID, skip`).
- **Sessionfinish после FAIL** — это **не** «зависание» runner: guard всё равно ждёт idle + пустой unmatched
  (укороченный `FAIL_DRAIN_SEC=30c`). Параллельные smoke на том же compose с runner не запускать.

---

## 10. Переменные окружения (ключевые)

Ни одна не обязательна для дефолта. Полный список — в коде conftest/toolkit; критичные:

| Переменная | Дефолт | Назначение |
| --- | --- | --- |
| `THRELIUM_E2E_REBUILD_BAKED_IMAGE` | unset | `1` → форс-bake лидером `compose_stack`. |
| `THRELIUM_E2E_SUT_IMAGE` | `threlium/e2e-sut:baked` | Образ `sut` (не-дефолт → off auto-bake). |
| `THRELIUM_E2E_LITELLM_ROUTE_CORRELATION` | e2e: on | Merge HTTP-заголовков корреляции для WM `hasContext`. |
| `THRELIUM_E2E_POLL_SHORT` | `30` | **Постоянный** таймаут poll'ов — не повышать ради медленного контура (чинят стабы/вход/продукт). |
| `THRELIUM_E2E_SESSIONFINISH_DRAIN_SEC` | `120` | Ожидание idle + пустой unmatched перед сбросом Store. |
| `THRELIUM_E2E_COMPOSE_DOWN` | unset | `1` → явный `compose down` после сессии. |
| `THRELIUM_E2E_ANSIBLE_TAGS` / `_SKIP_TAGS` | unset | `--tags`/`--skip-tags` для `ansible-playbook`. |

Команды:
```bash
.venv/bin/pip install -e ".[e2e,dev]"
pytest tests/e2e -vv                                   # дефолт (один процесс)
pytest tests/e2e -n 8 -vv                              # параллельный контракт
pytest -n0 tests/e2e/wipe_bake.py -vv -s && pytest tests/e2e   # полная подготовка
THRELIUM_E2E_COMPOSE_DOWN=1 pytest tests/e2e           # с явным down
```

---

## 11. Связь документов

| Документ | Роль |
| --- | --- |
| [INDEX.md](INDEX.md) | Master-контракт: storage (union root `stages/`), fdm `insert && dispatch`, `nm_settle()`, error handling, LightRAG-воркер. e2e-инварианты выводятся отсюда. |
| [ARCHITECTURE.md §1.3](ARCHITECTURE.md#13-политика-тестирования) | Политика: e2e — единственный quality gate. |
| [ORCHESTRATION.md](ORCHESTRATION.md) | serial-per-thread / parallel-across-threads — контракт, который проверяет `-n N`. |
| [PLAYBOOK.md §2.1](PLAYBOOK.md) | Классы операций (A/B), ограничения, тег `refresh` как тестовая надстройка. |
| [MESSAGES.md](MESSAGES.md) | Канонизация `Message-ID` на границах — основа уникальных коррелятов. |
| [THREAD_MODEL.md](THREAD_MODEL.md) / [BRIDGE_ISOMORPH.md](BRIDGE_ISOMORPH.md) | Производственная тред-идентичность мостов (snowflake-MID, водяной знак glue) — прод-аналог §2.3. |
| [FSTS_SYNC.md](FSTS_SYNC.md) | Синхронизация только кода `scripts/`+`prompts/` на живой SUT без плейбука. |
