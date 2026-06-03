# Спецификация FSM: Управление памятью

Данный документ дополняет базовую матрицу делегирования (`SUBAGENT_TABLE.md`) и описывает пошаговую логику переходов FSM для работы с долгосрочной памятью. 

Ключевой принцип Threlium: **Память — это не сайд-эффект в базу данных, а полноценные переходы графа FSM**. Сохранение факта в памяти означает генерацию нового RFC-письма (**fdm** `pipe` с `notmuch insert --folder=stages/<stage>/Maildir …` — файл сразу попадает в `new/` стадии и индексируется в union notmuch index, [`INDEX.md` §4](INDEX.md#4-mailfilter-terminating-insert)), что обеспечивает идеальную хронологическую наблюдаемость (temporal consistency) в `notmuch` и предоставляет LightRAG консистентный источник данных через **RAG-loop в `threlium-engine`** (single writer на `lightrag/working_dir/`, drain settled-сообщений после `nm_settle()`, [`INDEX.md` §5b](INDEX.md#5b-lightrag-worker)).

Имена служебных заголовков `X-Threlium-*` на wire — в [`MESSAGES.md` §8](MESSAGES.md#8-canonical-x-threlium-headers-glossary); делегирование и стеки — в [`SUBAGENT_TABLE.md`](SUBAGENT_TABLE.md); обработка ошибок без отдельной стадии — [`INDEX.md` §5.6](INDEX.md#56-universal-error-handling-в-runnersworkerpy) и [MESSAGES.md §5b](MESSAGES.md).

> **Двойное устаревание исторических формулировок (старая схема с archive → нынешняя; GraphRAG → LightRAG).** Матрицы переходов ниже — единственный нормативный артефакт этого документа и сохраняются 1:1; supporting prose адресует одновременно два слоя legacy: (1) «`cc "$ARCHIVE"` + выделенный `archive/Maildir`» → **fdm** `notmuch insert …` + durable `stages/<stage>/Maildir/cur/<id>:2,S` под единым union notmuch root; (2) «GraphRAG `input/`-каталоги + симлинк `input/global/` → `graphrag/global/` + мутабельный тег `graphrag_exported`» → один общий `LightRAG(working_dir=…)` + ingest: оболочка `EmailMessage`/`policy.default`, тело — по одной inline `text/plain` на каждую `<history>`-часть (без слияния), `X-Threlium-Thread-Id` только в синтетической строке `ainsert` + тег `lightrag_indexed` ([`INDEX.md` §5b](INDEX.md#5b-lightrag-worker), [§7.6](INDEX.md#76-per-thread-scoping-soft-через-маркеры), [`CONTEXT_CONTRACT.md` §7](CONTEXT_CONTRACT.md)).

---

## 1. Локальная память (`thread_memory`)

Локальная память используется агентом для фиксации промежуточных выжимок, важных только в рамках **текущего диалога**. 

Так как мы не покидаем текущего RFC-треда агента, кросс-тредовая маршрутизация не нужна. Это простая линейная петля с возвратом в **`enrich_fast`**.

### Матрица переходов `thread_memory`

| Шаг | Воркер | Состояние / Письмо | Описание |
| :--- | :--- | :--- | :--- |
| **1** | `reasoning` | **Отправляет:** `L_M1`<br>• `To: thread_memory@`<br>• `In-Reply-To: <Prev>` | LLM понимает, что нужно сохранить промежуточный факт для текущего контекста. Генерирует намерение сохранить локальную память. |
| **2** | `thread_memory` | **Получает:** `L_M1`<br>**Отправляет:** `L_M2`<br>• `To: enrich_fast@`<br>• `<hash@history>` = note (request-echo, `X-Threlium-Origin = reasoning`)<br>• `In-Reply-To: <L_M1>` | Скрипт читает payload из `<system>` (`system_part_text`), при необходимости форматирует тело и собирает RFC через `render_prompt("thread_memory/base.j2", …)` — без `[Scope:]` в теле и без `X-Threlium-Thread-Id` на диске ([`INDEX.md` §7.6](INDEX.md#76-per-thread-scoping-soft-через-маркеры)). Для памяти ценен именно **запрос**: в историю на **L_M2** едет note как request-echo с предштампом `origin=reasoning` (автор факта), отдельного «recorded»-ответа нет. `L_M1` (только `<system>`) уже атомарно записан **fdm**/**notmuch insert** в `stages/thread_memory/Maildir/new/`; воркер после emit `L_M2` делает `nm_settle(L_M1)` ([`INDEX.md` §5.5.3](INDEX.md#553-notmuch-consistency-через-notmuch2mutabletagset)) — архив переезжает `new/<id>` → `cur/<id>:2,S` **без изменения bytes** (drain: `lightrag_skipped`, т.к. нет `<history>`). Индексируемый факт — settled **L_M2** в `enrich_fast/Maildir/cur/` (async, синтетический ingest). См. [`CONTEXT_CONTRACT.md`](CONTEXT_CONTRACT.md). |
| **3** | `enrich_fast` | **Получает:** `L_M2`<br>**Отправляет:** `L_M3`<br>• `To: reasoning@` | Fast-cycle (без полного re-enrich/RAG): `enrich_fast` находит `E_prev` (предыдущее `To: reasoning`) по IRT, доклеивает `<history>`-заметку (origin уже предзаштампован `reasoning`, enrich_fast его не трогает), пересобирает `<response-state>`/`<task-state>` и возвращает поток в `reasoning`. Заметка мгновенно видна модели. |
| **—** | (RAG) | durable-факт | **L_M2** (request_echo) settled и проиндексирован LightRAG-воркером по `cur/`-триггеру в `enrich_fast/` (async, синтетический ingest с thread id). **L_M1** — system-only архив в `thread_memory/` (`lightrag_skipped`). Полнотекстовый retrieval доступен со следующего полного `enrich`-цикла. |

---

## 2. Глобальная память (`global_memory`)

Глобальная память хранит фундаментальные знания о пользователе (предпочтения, факты), которые должны быть доступны **во всех будущих диалогах**. Запись остаётся в той же модели — durable письмо в `stages/global_memory/Maildir/cur/<id>:2,S` под единым union notmuch index'ом, индексируется LightRAG-воркером **в тот же общий `lightrag/working_dir/`**, что и любые другие settled-сообщения; разведение от тредовых фактов — по адресу стадии (`global_memory@`) и содержимому писем, а thread id для чанков — только в синтетической ingest-строке ([`INDEX.md` §7.6](INDEX.md#76-per-thread-scoping-soft-через-маркеры)). Никаких физических подграфов, отдельных GraphRAG-проектов, симлинков `input/global/` и тегов `graphrag_exported` нет — это устаревший артефакт старой схемы.

С точки зрения FSM `global_memory` **полностью симметрична** `thread_memory`: запись остаётся в RFC-треде текущего агента, кросс-тредовых переходов нет. Единственное отличие — шаблон тела (`global_memory/base.j2` против `thread_memory/base.j2`) и адрес стадии `global_memory@localhost`; сам `To:` определяет Maildir и маршрутизацию, а не отдельный scope-маркер в wire-теле. На чтении в `enrich` глобальные факты доступны в том же графе; приоритет задаётся `lightrag/rag_response.j2` и сформулированным запросом. Специального тега `global_memory` или `lightrag_global` в `notmuch` **нет** — `To:` и текст письма самодостаточны.

### Матрица переходов `global_memory`

| Шаг | Воркер | Состояние / Письмо | Описание |
| :--- | :--- | :--- | :--- |
| **1** | `reasoning` | **Отправляет:** `L_G1`<br>• `To: global_memory@`<br>• `In-Reply-To: <Prev>` | LLM принимает решение зафиксировать фундаментальный факт о пользователе. Письмо остаётся в треде текущего агента (`In-Reply-To` указывает на предыдущее звено локального треда). |
| **2** | `global_memory` | **Получает:** `L_G1`<br>**Отправляет:** `L_G2`<br>• `To: enrich_fast@`<br>• `<hash@history>` = note (request-echo, `X-Threlium-Origin = reasoning`)<br>• `In-Reply-To: <L_G1>` | Скрипт читает payload из `<system>` (`system_part_text`), нормализует тело и собирает письмо через `render_prompt("global_memory/base.j2", …)` ([`INDEX.md` §7.6](INDEX.md#76-per-thread-scoping-soft-через-маркеры)). Симметрично `thread_memory`: в историю на **L_G2** едет note как request-echo с предштампом `origin=reasoning`. `L_G1` (только `<system>`) записан в `stages/global_memory/Maildir/`; после emit `L_G2` — `nm_settle(L_G1)` → `cur/` без изменения bytes (`lightrag_skipped`). Индексируемый факт — settled **L_G2** в `enrich_fast/`. См. [`CONTEXT_CONTRACT.md`](CONTEXT_CONTRACT.md). |
| **3** | `enrich_fast` | **Получает:** `L_G2`<br>**Отправляет:** `L_G3`<br>• `To: reasoning@` | Fast-cycle (симметрично `thread_memory`): `enrich_fast` находит `E_prev` по IRT, доклеивает `<history>`-заметку (origin уже предзаштампован `reasoning`), пересобирает `<response-state>`/`<task-state>` и возвращает в `reasoning`. |
| **—** | (RAG) | durable-факт | **L_G2** (request_echo) settled и проиндексирован в общий `lightrag/working_dir/` (async, cur/-триггер в `enrich_fast/`). **L_G1** — system-only архив (`lightrag_skipped`). |

### Свойства схемы

1. **Симметрия с `thread_memory`:** единый паттерн «локальная запись в стадийный Maildir под union notmuch index → возврат в `ingress`». Разница — шаблон тела и адрес `To:`; для LightRAG thread id попадает в граф через синтетическую ingest-строку, а не через маркер в wire-теле.
2. **Отсутствие кросс-тредовых переходов:** фреймы субагентов управляются маркерами `subagent_intent` / `subagent_end` в IRT-цепочке ([`SUBAGENT_TABLE.md`](SUBAGENT_TABLE.md)). Запись памяти не управляет фреймами — стадии памяти не открывают и не закрывают контекстов делегирования.
3. **Локальная наблюдаемость:** и намерение, и сам факт остаются в RFC-треде текущего агента. `notmuch show thread:<current>` (поверх union index'а `*`) даёт полную хронологию, включая точки записи в глобальную память.
4. **Нет гонок общего треда:** два параллельных треда, одновременно фиксирующих глобальный факт, пишут каждый в свой собственный тред (свои `In-Reply-To`-цепочки) — форк общего `<G_TIP>` в `notmuch`, возможный при кросс-тредовой схеме, невозможен по построению. На уровне LightRAG-воркера single-writer-инвариант ([`INDEX.md §5b`](INDEX.md#5b-lightrag-worker)) сериализует `rag.ainsert(...)` независимо от тред-параллелизма стадийных воркеров.
5. **Полная лента глобальной памяти:** запрос `notmuch search --sort=date 'to:global_memory@localhost AND folder:global_memory/Maildir AND NOT tag:unread'` даёт сквозную хронологию всех settled глобальных фактов по всем тредам — достаточно и для аудита, и для прогрева LightRAG-индекса при холодном старте.
6. **Отсутствие дублирования в LightRAG:** дедупликация — встроенная (`rag.ainsert(...)` дедуплицирует узлы по содержимому/идентификаторам, [`INDEX.md §5b`](INDEX.md#5b-lightrag-worker)) плюс фильтр селектора воркера `... AND NOT tag:lightrag_indexed`. Никаких ручных фильтров «исключить global из dialog/`input/`» не нужно — общий граф один, разведение — soft на чтении.

---

## 3. Reflect (продолжение рассуждения)

`reflect` — стадия-tool, доступная `reasoning` так же, как `thread_memory` и `global_memory`, но семантика принципиально иная: **это не запись факта в память, а явное намерение «думать ещё один цикл»** через свежий проход **`reflect → enrich → reasoning`** с обновлённым LightRAG-контекстом. С точки зрения графа FSM это одностадийная петля «`reflect → enrich`», без архивного смысла записи: попадание в durable `stages/reflect/Maildir/` через **fdm** `pipe` (`notmuch insert …`) — общесистемное свойство ([`INDEX.md §4`](INDEX.md#4-mailfilter-terminating-insert)), у `reflect` нет специальной интерпретации со стороны индексации (тот же общий граф и тот же ingest-пайплайн, что и для остальных settled писем треда).

Главное отличие от `thread_memory` — **budget-aware промпт**: `reflect` читает остаток `X-Threlium-Hop-Budget`, выбирает Jinja2-шаблон из подкаталога `$THRELIUM_HOME/prompts/reflect/` (`continue.j2`, пока бюджета хватает на ещё один цикл `reflect → enrich → reasoning`; `final.j2`, когда бюджет почти исчерпан) и подставляет конкретное число оставшихся hop'ов в результирующий текст.

Self-route `reasoning → reasoning` отсутствует сознательно: он минует `enrich`, LightRAG-граф не успевает пополниться (новые письма должны сначала пройти `nm_settle()` и попасть в drain `schedule_index_pending` на RAG-loop в `threlium-engine`, [`INDEX.md §5b`](INDEX.md#5b-lightrag-worker)), и «дополненного контекста» в принципе не появляется. Это нарушало бы и SoT-инвариант [`FSM.md §5`](FSM.md): «почта в очередь `stages/reasoning` попадает только с ребра `enrich → reasoning`». Любой «думать ещё» проходит через `reflect`; очередной цикл видит свежий `unified_messages` + текущий граф ([`INDEX.md` §7](INDEX.md#7-enrich-notmuch-context--query--lightrag)).

### Матрица переходов `reflect`

| Шаг | Воркер | Состояние / Письмо | Описание |
| :--- | :--- | :--- | :--- |
| **1** | `reasoning` | **Отправляет:** `R_R1`<br>• `To: reflect@`<br>• `In-Reply-To: <Prev>`<br>• `Body:` summary текущего рассуждения + явный запрос «что нужно уточнить/расширить» | LLM решает, что нужен ещё один цикл с обновлённым LightRAG-контекстом (свежий `rag.aquery(...)` в `enrich` после settled-цикла). `body` (ограничение `maxLength` JSON-Schema в `prompts/reasoning/reflect/tool_spec.j2`) — сжатое summary, **не** транскрипт предыдущих ходов; ограничитель митигирует поведенческое раздувание контекста между циклами. |
| **2** | `reflect` | **Получает:** `R_R1`<br>**Отправляет:** `R_R2`<br>• `To: enrich@`<br>• `In-Reply-To: <R_R1>`<br>• `<user-query>`: рендер `continue.j2` или `final.j2` с подстановкой `remaining_hops`, `cycle_cost`, `previous_reasoning`, `subject` | Стадия читает хвост `X-Threlium-Hop-Budget` (`N` → остаток `N`; `N-k` → остаток `max(0, N-k)`), выбирает шаблон по правилу `remaining ≥ cycle_cost (3) + safety_margin (1)` и эмитит через `emit_to_enrich` (без ingress/distill). После успешного emit `R_R2` воркер делает `nm_settle(R_R1)` — оригинал переезжает `new/<id>` → `cur/<id>:2,S`, ровно как у любой стадии ([`INDEX.md §5.5.3`](INDEX.md#553-notmuch-consistency-через-notmuch2mutabletagset)). |
| **3** | `enrich` | **Получает:** `R_R2`<br>**Отправляет:** `R_R3`<br>• `To: reasoning@`<br>• `In-Reply-To: <R_R2>` | Сбор контекста и цепочка `rag.aquery` как в [`INDEX.md` §7](INDEX.md#7-enrich-notmuch-context--query--lightrag) / [§7.5](INDEX.md#75-query-call-always-on). Контекст инжектируется поверх рендеренного reflect-промпта. На следующем reasoning-шаге модель видит и budget-инструкцию, и обновлённый блок обогащения из `enrich`. |

### Свойства схемы

1. **Не запись в память:** `reflect` не создаёт глобального или тредового факта в смысле LightRAG-индексации с особыми правилами. Settled-копии `R_R1`/`R_R2` в `stages/reflect/Maildir/cur/` подхватываются тем же общим селектором pending, что и остальные стадии (`* AND NOT tag:unread AND NOT tag:lightrag_indexed`, [`INDEX.md` §5b](INDEX.md#5b-lightrag-worker)) — никаких специальных подграфов или фильтров для `to:reflect@localhost` не существует.
2. **Budget-aware:** `reflect` единственная стадия, чьё тело параметризовано остатком `X-Threlium-Hop-Budget` через шаблоны `continue.j2`/`final.j2`. Это и есть причина выделять её в отдельный узел, а не нагружать `thread_memory` второй семантикой.
3. **Фреймы субагентов не трогаются:** `reflect` не открывает и не закрывает фреймов делегирования (hop-budget переносится билдерами как у остальных стадий). Делегирование остаётся прерогативой `subagent_intent` / `subagent_end` ([`SUBAGENT_TABLE.md`](SUBAGENT_TABLE.md)).
4. **Глобальный страховщик зацикливания:** `FSM_HOP_LIMIT` в `reasoning` (грубая оценка через `fsm_fatigue_from_hop_budget`) остаётся последним предохранителем. Если LLM продолжит звать `reflect` после `final.j2`, `reasoning` уйдёт в `egress_router` со штатным сообщением «processing limit».
5. **Нет утечки lightrag-контекста между циклами:** `reasoning` каждый раз сбрасывает `body` на свежий рендер `prompts/reasoning/reflect/email_body.j2` (из аргументов tool-call'а LLM) *до* следующего `enrich`, поэтому lightrag-блок от предыдущего цикла не дотягивается в новый. Лимит `maxLength` в JSON-Schema шаблона `prompts/reasoning/reflect/tool_spec.j2` отсекает поведенческий риск, что LLM начнёт копировать предыдущий ход в новое тело. Следующий проход `enrich` снова собирает `unified_messages` и актуальный (на момент вызова) `aquery` ([`INDEX.md` §7](INDEX.md#7-enrich-notmuch-context--query--lightrag)).
