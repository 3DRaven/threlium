# CONTEXT_CONTRACT: наполнение контекста и содержимого писем

> **Источник истины** по тому, *что лежит в FSM-письме* и *кто собирает это в контекст
> reasoning*. Итог двух рефакторингов: унификация `history`/`system` и модель «callee
> владеет историей». Остальные документы ([`FSM.md`](FSM.md), [`RESPONSE_TABLE.md`](RESPONSE_TABLE.md),
> [`MEMORY_TABLE.md`](MEMORY_TABLE.md), [`SUBAGENT_TABLE.md`](SUBAGENT_TABLE.md)) ссылаются
> сюда, а не дублируют. Цикл `formal_reason` и FSM gate — [`FORMAL_REASON_GATE.md`](FORMAL_REASON_GATE.md)
> (не дублировать здесь). Термины — [`INDEX.md` §глоссарий](INDEX.md). Типы/VO — [`TYPES.md`](TYPES.md).

Все имена/функции в этом документе соответствуют коду
`ansible/roles/threlium/files/scripts/threlium/` (не «по памяти»).

---

## 1. Инвариант контракта письма

Каждое FSM-письмо между стадиями — `multipart/mixed`:

```
канонический конверт (From/To/Subject/Message-ID/In-Reply-To/X-Threlium-*)
+ 0..N  <{sha256(body)}@history>   — неисполняемая ПАМЯТЬ
+ 0/1   <{sha256(body)}@system>    — исполняемая payload-КОМАНДА адресату
```

Голое message-level тело payload **больше не носит**: и память, и команда живут в
именованных MIME-частях по `Content-ID`. Это убирает зависимость от «первого text/plain»
и даёт единые инструменты чтения/сбора/дедупа для всех стадий.

**Формат CID — `local@domain`** (`EnrichContentId`, `mime_reform.py`):
- local-part = `sha256(тело-части)` (hex);
- domain = `history` или `system` (`EnrichPartId.HISTORY`/`SYSTEM`).

**Почему хеш только по телу** (`from_history_body` / `from_system_body`): `X-Threlium-Origin`
штампуется *постфактум* (`enrich_fast` или предштамп callee), поэтому у производителя и его
relay-копии **тела совпадают, а заголовки — нет**. Хеш по телу делает их одним CID → дедуп
схлопывает копии. Одинаковое тело в `history` и в `system` даёт **разные** CID (разный
domain) — поэтому пара «команда + её history-копия» не схлопывается: одна часть для механики,
одна для памяти.

Структурные core-CID полного enrich (`<user-message>` и т.д., §4) — **отдельный** механизм:
это пресобранный контекст enrich→reasoning, а не контракт `history`/`system`.

---

## 2. `<system>` — носитель команды

- **Ровно одна** `<system>`-часть на письмо (когда адресат потребляет payload).
- Читается строго `system_part_text(msg)` (`mime_reform.py`): **fail-fast** — отсутствие или
  >1 части → `RuntimeError` (инвариант «payload только в `<system>`»). Это замена
  `extract_plain_body` для всех внутренних чтений payload.
- **Не релеится и не индексируется**: `system` не входит в `RELAY_FAMILIES`; LightRAG-drain
  его игнорирует (память несёт парная `<history>`-копия, §7).
- Создаётся `build_fsm_step_to_stage(..., system=...)` или legacy `build_fsm_plain_to_stage`
  (тоже оборачивает body в `<system>`).
- **Внешняя граница**: мост (`bridge`) на входе извлекает тело внешнего письма
  (`extract_plain_body`) и оборачивает в `<system>` + кладёт `<history>`-копию хода
  пользователя. Терминальные `egress_email`/`egress_telegram`/`egress_matrix` строят чистое
  внешнее письмо **только из `<system>`** (`system_part_text`), не пробрасывая внутренний MIME.

---

## 3. `<history>` — память (модель «callee владеет историей»)

`<history>` — неисполняемая память агента. Per-part заголовки (граница через `MailHeaderName`,
без сырых строк, переживают `out.attach`):

- **`X-Threlium-Content-Score`** — базовый вес части, ставит **источник** из настроек
  (`settings.history.score_for(from_stage)`, `HistorySettings`: `score_default` +
  per-stage override `score_by_stage`). Финальный вес считает потребитель (recency × size ×
  score, `context_budget.py`).
- **`X-Threlium-Origin`** — стадия-автор части. По умолчанию **штампует `enrich_fast`** из
  конвертного `From:` несущего письма, только на частях **без** origin (единая точка origin).
  Исключение — эхо-запрос (ниже): его предзаштамповывает callee.

### Кто решает, что попадает в историю

Историю формирует **стадия, которая знает свою семантику (callee)**, а не вызывающий:

- `reasoning → tool`: **только `<system>`** (команда). Никакого `<history>`. Иначе сырой
  буфер ответа (`response_append`/`edit`/`tasks_upsert`) протекал бы в контекст.
- callee кладёт в исходящее письмо **0..2** `<hash@history>`-части:
  - **эхо-запрос** (что у меня спросили) — опционально, через `build_fsm_step_to_stage(
    request_echo=...)`; **предзаштампован** `X-Threlium-Origin = incoming From` (истинный
    автор запроса — вызывающий), score = `score_for(автор запроса)`;
  - **ответ** (результат) — `history=...`; origin проставит `enrich_fast` (= callee).
- Разные тела → разные `<hash@history>` → дедуп их не схлопывает.

```mermaid
flowchart LR
  R[reasoning] -->|"system=command (NO history)"| T[tool stage]
  T -->|"<hash@history> echo (origin=requester) + <hash@history> result"| EF[enrich_fast]
  EF -->|"collect all by IRT, dedup by content-CID, stamp origin"| R2[reasoning next]
  Mut["mutators: append/edit/tasks"] -->|"preserving payload, NO history"| EF
```

### Матрица по ВСЕМ стадиям (`threlium/states/*.py`)

`echo` = request-echo (`<history>` с предштампом `origin=incoming From`); `resp` = собственный
`<history>`-ответ (origin проставит enrich_fast); `sys` = `<system>`-payload; «релей» =
`emit_*_preserving_payload` (части входящего письма пробрасываются как есть). Пусто = не
применимо. Источник — фактические вызовы emit в коде.

**Вход / сборка контекста (инфраструктура, не tool-callee).**

| Стадия | To: | echo | resp | sys | Роль |
|---|---|:--:|:--:|:--:|---|
| `ingress` | `enrich` (или `cli_resume`) | — | да¹ | да | Единая точка входа. Ход пользователя → `<history>` + `<system>`; HITL-ответ → `cli_resume` только `<system>` (текст ответа для LLM-классификатора, ¹без history). |
| `enrich` | `reasoning` / `summarize_context` | — | — | да² | → `reasoning`: **`<system>` НЕТ** (reasoning не читает payload, см. ниже) — собирает core-CID §4 + `<unified-mail-context>` из `<history>`-частей треда; своих `<history>` не создаёт. ²Только → `summarize_context` есть `<system>`. |
| `enrich_fast` | `reasoning` | — | — | **релей** | Relay-сборщик дельты: `<history>` + `<system>` из окна (штампует `origin`); replace `<response-state>`/`<task-state>`. Старые `@system` из `E_prev` не копируются — только свежие из дельты. `reasoning` не кладёт их в LLM-промпт, но читает для FSM gate (`formal_reason`). |
| `reasoning` | tool | **нет** | **нет** | да | Чистый `<system>`-эмиттер tool-call (команда адресату). История — забота callee. На ВХОДЕ сам `<system>` не читает. |

> **`<system>` на входе `reasoning` не нужен.** `reasoning` собирает контекст из core-CID частей
> (`<user-message>`/`<graph-answer>`/… §4) и хронологии `<history>` (`ReasoningEnrichContext.from_email`),
> и **никогда** не вызывает `system_part_text`. Поэтому и `enrich`, и `enrich_fast` шлют в `reasoning`
> письмо **без** `<system>` — это разрешённый случай «0» из «0/1 `<system>`», а не нарушение контракта.
> `<system>` присутствует строго там, где адресат исполняет payload-команду (tool-входы, egress,
> `summarize_*`, `cli_*`, мутаторы через relay).

**Tool-callee (вызываются `reasoning`).**

| Стадия | To: | echo | resp | sys | Роль |
|---|---|:--:|:--:|:--:|---|
| `formal_reason` | `enrich_fast` | **да** (payload) | да | **да** | Echo + observation (`<history>`) + `FormalReasonResultPayload` в `<system>` (origin на relay — `enrich_fast`; gate — [`FORMAL_REASON_GATE.md`](FORMAL_REASON_GATE.md)). |
| `memory_query` | `enrich_fast` | **да** (query) | да | — | Запрос (echo, origin=reasoning) + RAG-ответ (observation, origin=memory_query). |
| `thread_memory` / `global_memory` | `enrich_fast` | **да** (note) | нет | — | Для памяти ценен запрос: note = что агент решил запомнить; origin=reasoning; «recorded»-ответа нет. |
| `response_observe` | `enrich_fast` | нет | да | — | Нарратив-обзор буфера + task-ledger. |
| `response_append` | `enrich_fast` | нет | нет | релей | Мутатор буфера; буфер виден как `<response-state>`. Сырой чанк в history не идёт. |
| `response_edit` | `enrich_fast` / `ingress` | нет | нет | релей / да | Мутатор буфера; ошибка валидации → `ingress` (`<system>`). |
| `tasks_upsert` | `enrich_fast` / `ingress` | нет | нет | релей / да | Мутатор ledger (`<task-state>`); ошибка → `ingress`. |
| `response_finalize` | `egress_router` / `ingress` | нет | да | да | Итоговый ответ (`<history>`+`<system>`); task-gate/ошибка → `ingress` (`<system>`). |
| `reflect` | `ingress` | нет | **нет** | да | Анти-раздувание (`MEMORY_TABLE.md §3`): рефреш-промпт только `<system>`. |
| `cli_intent` | `enrich_fast` / `cli_exec` / `cli_hitl_out` | нет | да³ | да | Роутер CLI. ³route-collision / invalid → `enrich_fast` (`<history>`-note); sandbox→`cli_exec`; privileged (+ HITL если включён)→`cli_hitl_out`→`cli_resume`→`cli_exec`. `cli_exec` / `cli_resume` (reject) → `enrich_fast`. |
| `subagent_intent` | `ingress` | **да** (task) | нет | релей | Делегирование субагенту. Фильтр фрейма (`iter_irt_ancestors_filtered`) изолирует внутренние письма субагента — родителю видны лишь границы intent/end. Задача жила в `<system>` и терялась из истории родителя → request-echo (origin=reasoning) кладёт её в `<history>`. |

**CLI / HITL цепочка.**

| Стадия | To: | echo | resp | sys | Роль |
|---|---|:--:|:--:|:--:|---|
| `cli_exec` | `enrich_fast` | —⁴ | да | да | Результат команды (observation: cmd_line+stdout/stderr/exit) → `<history>` (origin=cli_exec) + `<system>`. ⁴cmd_line уже в observation, отдельный echo избыточен. |
| `cli_hitl_out` | `egress_router` | — | да | да | Вопрос пользователю: `<system>` = тело отправки, `<history>` = копия вопроса. |
| `cli_resume` | `ingress` / `cli_exec` | — | нет | да | Возобновление после HITL: `<system>` = ответ пользователя; пустой ответ → `enrich_fast` без LLM; иначе sync LLM tool `confirm_cli_hitl` (score 0, retry bridge). Отказ пользователя (`confirmed=false`) → `enrich_fast`; ошибка classify после retry → падение стадии. |

**Субагент-возврат / сжатие / терминальные.**

| Стадия | To: | echo | resp | sys | Роль |
|---|---|:--:|:--:|:--:|---|
| `subagent_end` | `ingress` | — | релей | релей | Возврат результата субагента (preserving). `<history>`-финализация субагента пробрасывается и видна родителю на границе фрейма (симметрично request-echo в `subagent_intent`). |
| `summarize_context` | `summarize_memory` | — | да | — | Сводка хвоста контекста → `<history>` (durable, переживает дренаж). |
| `summarize_memory` | `enrich` | — | — | да | Дренаж сводки в полный enrich (`<system>`). |
| `egress_router` | `egress_*` / `subagent_end` | — | релей | релей | Маршрутизация по каналам; части письма не меняет. |
| `egress_email` / `egress_telegram` / `egress_matrix` | — (терминал) | — | — | читает | Строят внешнее письмо **только из `<system>`** (`system_part_text`); ничего не эмитят. |
| `archive` | — (терминал) | — | — | — | Оседает в Maildir/union-index; ничего не эмитит. |

---

## 4. Core-CID полного enrich → reasoning

Полный `enrich` собирает структурный контекст для reasoning как фиксированные секции
(`build_enriched_multipart`, `EnrichPartId`) — это **не** контракт `<system>` и **не**
`<history>`-память:

| Content-ID | Источник |
|---|---|
| `<user-message>` | canonical user text |
| `<graph-answer>` | JSON-envelope `rag.aquery` |
| `<unified-mail-context>` | хронология треда + memory из `<history>`-частей (`message_has_history`) |
| `<thread-memory>` / `<global-memory>` | memory-письма (намеренное дублирование маркеров) |
| `<response-state>` | детерминированный пересчёт CRDT-буфера |
| `<task-init>` / `<task-state>` | стартовый набор задач + reduced-ledger |

MCKP-бюджет / тиринг контекста (`context_budget.py`) живут **только в большом enrich**.
Деталь секций — [`FSM.md` §5.2](FSM.md#52-контракт-тела-enrich--reasoning).

---

## 5. Сбор и дедуп

Единый предикат «письмо содержательно» = `message_has_history(msg)` (≥1 непустая
`<history>`-часть). Заменяет старую классификацию по `To:`-стадии (`SERVICE`/`CONTEXT_ROLE`).

- **`enrich_fast`** (`splice_e_prev_with_history`, `mime_reform.py`): окно-дельта по IRT с
  прошлого `To: reasoning` (= `E_prev`, в multi-cycle — выход прошлого `enrich_fast`).
  Собирает все `<history>`-части окна **сырыми** (`collect_unified_delta_msgs`), штампует
  `X-Threlium-Origin` из `From:`, дописывает в хвост `E_prev`; **replace** `<response-state>`
  / `<task-state>` (пересчёт CRDT). Остальные части `E_prev` не трогает.
- **`enrich`** (`enrich_context.py`): полный обход IRT-предков (старые→новые), берёт
  `<history>`-части из **всех** писем (включая `To: enrich_fast`), исключая
  `tag:context_summarized` и memory-бакеты.
- **Дедуп — в обоих** по равенству `EnrichContentId` (контент-хеш тела): relay-копия
  схлопывается с оригиналом. Приоритет при коллизии — более раннее письмо (origin автора).

---

## 6. Презентация reasoning

`reasoning/user.j2` рендерит `<history>`-части **единым хронологическим потоком**:
- `<conversation_history>` — полный хвост (после полного enrich);
- `<conversation_delta>` — дельта с прошлого хода (после `enrich_fast`).

Каждая запись подписана `[from: <origin>]` (`X-Threlium-Origin`). **Видов-таксономии по
стадии** (`<observation>` / `<memory_note>` / `<plan_state>`) больше нет: семантику даёт
origin + tool spec, известный модели. Бюджет — tail-keep новейших (`context_max_chars`).

---

## 7. LightRAG-индексация

Drain (`runners/lightrag/_drain.py`, `lightrag_drain_query.py`) индексирует письмо тем же
предикатом `message_has_history`. notmuch не индексирует MIME по Content-ID, поэтому selector
даёт лишь tag-негативы (дешёвый pre-filter), а финальный `message_has_history` применяется
load-time. Тело графа — конкатенация `<history>`-частей (`lightrag_ingest.py`). `<system>`
**не индексируется**: его смысл несёт парная `<history>`-копия. Письма без history →
`lightrag_skipped` (не вечный pending).

---

## 8. Сквозные примеры по CID

**Ход пользователя (внешний → ingress).** Мост: внешнее тело → `<system>` + `<history>`-копия
→ `ingress`. `ingress._emit_to_enrich`: `<history>` (ход в память, origin=ingress) +
`<system>` (тело-команда для enrich) → `enrich`.

**Tool-цикл (reasoning → formal_reason → enrich_fast → reasoning).** Каноническое описание
(gate, `FormalReasonResultPayload`, relay `<system>`) — [`FORMAL_REASON_GATE.md`](FORMAL_REASON_GATE.md).
Кратко: `reasoning → formal_reason` — только `<system>`-команда; callee — echo + observation +
result JSON; `enrich_fast` релеит дельту (§5); prose в `<conversation_delta>`, gate по `<system>`.

**Буфер ответа (append×N + observe + finalize).** `reasoning → response_append`: `<system>` =
чанк. `response_append → enrich_fast`: preserving payload, **без** history (чанк не в памяти).
Буфер виден как `<response-state>`. `response_observe → enrich_fast`: `<history>` = нарратив.
`response_finalize → egress_router`: `<history>` (итог в память) + `<system>` (тело отправки).

**Egress (`<system>` тело + `<history>` копия).** `egress_router` пробрасывает обе части;
`egress_<chan>` строит внешнее письмо **только из `<system>`**; `<history>`-копия остаётся
в треде для контекста следующего хода.

**Память (thread/global).** `reasoning → thread_memory`: `<system>` = note. `thread_memory →
enrich_fast`: `<history>` = note как **request-echo** (предштамп `origin=reasoning` — автор
факта; «recorded»-ответа нет, для памяти ценен сам запрос). Fast-cycle даёт мгновенную
видимость; durable-факт async-индексируется LightRAG из `cur/` независимо от routing
([`MEMORY_TABLE.md` §1-2](MEMORY_TABLE.md)).

---

## 9. Инварианты (чек-лист)

- payload только в `<system>`, читается `system_part_text` (fail-fast);
- память только в `<hash@history>`; CID = хеш тела; origin/score — per-part заголовки;
- `reasoning → tool` никогда не несёт `<history>` (callee владеет историей);
- мутаторы буфера/ledger не несут `<history>`;
- сбор/дедуп — `message_has_history` + равенство `EnrichContentId` (enrich и enrich_fast);
- `<system>` не индексируется и не релеится; LightRAG = `message_has_history`;
- origin/score/CID — через VO (`FsmStage`-mailbox, `ThreliumContentScoreWire`,
  `EnrichContentId`), без сырых строк ([`TYPES.md`](TYPES.md)).
