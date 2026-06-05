# Отчёт: параллельный прогон e2e (`-n2`) — регрессия, расследование, варианты, предложения

Статус: расследование завершено; часть фиксов внедрена и проверена; финальный фикс изоляции тестов —
к согласованию (этот документ). Цель — восстановить параллельный прогон isomorph-suite (`pytest -n2`),
который ранее работал, и который сломался после серии рефакторингов (LightRAG 1.5 + JSON, per-role llm,
TLS→ContextVar, per-thread lock).

---

## 1. Симптомы

`pytest -n2` (4 isomorph-модуля = anthropic/openai × cline/json) деградировал от полностью рабочего к
каскадным падениям. По мере внедрения фиксов форма падения менялась:

| Прогон | Итог | Ошибки |
|---|---|---|
| исходный | 3 failed + каскад | `WireMock unmatched journal not empty` (×6) + `turn-2 409 timeout` (×2) |
| + `max_async=4` | 5 passed, 1 failed | `unmatched` = **0**; осталось `turn-2 in-work 409` |
| + distinct replies | 5 passed, 1 failed | `in-work` = **0**; осталось `curl (28)` turn-2 |
| + broadcast registry | 4 passed, 2 failed | осталось `timeout waiting for turn-1 glue indexed` |

`-n0` всё это время — **6/6** (детерминизм сохранён).

---

## 2. Расследование (что ИСКЛЮЧЕНО профилированием)

Добавлены профиль-логи (`PROF_stage` в `fsm._run_stage`, `PROF_rag*` в `run_rag_coroutine`,
`hold_register/resolve` в `pending.py`) + прямые замеры в SUT. Итог — **путь обработки НЕ узкое место**:

- **FSM-стадии**: sub-second (enrich ≤631 ms, reasoning ≤144 ms, egress ≤176 ms).
- **RAG-операции**: `aquery` coro ≤330 ms, `lock_wait=0` (per-thread lock не конфликтует).
- **Engine socket** (`ThreadingUnixStreamServer`): экспериментально ПАРАЛЛЕЛЕН (10 клиентов × 1 s задержки
  → wall 1.01 s, не 10 s). Глобального lock в handler нет.
- **systemd dispatch**: старт unit ~10 ms (одиночно), 16 параллельных стартов = 108 ms.
- **worker cold-start**: `engine_submit` — тонкий клиент, `import` = 34 ms, БЕЗ litellm/lightrag/numpy
  (`sys.modules`: litellm=False). Старая проблема «litellm в воркере» (полный `import litellm` = **2.39 s**)
  **исправлена** — тяжёлый импорт только в долгоживущих engine/bridge, не на стадию.
- **WireMock**: одиночный запрос ~5–13 ms; 24 параллельных — те же ~10–13 ms (глобальный `synchronized(store)`
  State-Extension не сериализует на этом масштабе).
- **notmuch settle** (`notmuch new`): ~10 ms (БД 211 сообщений).
- **engine во время столла**: max gap между FSM-событиями = 1.98 s — движок НЕ замирал на 120 s.

Вывод: 120-секундный стол — **не латентность и не throughput**, а **жёсткий стол маршрутизации** из-за
**коллизии контент-адресуемого коррелятора** при идентичных телах запросов.

---

## 3. Корневая причина (слоёная)

Контент-адресуемость — основа дизайна isomorph: `Message-ID` ингресса = `canon(hash(хвост))`, glue-MID
ответа = `canon(hash(reply))`, thread-root (= коррелятор `X-Threlium-Route`/`X-Threlium-Thread-Root` для
матча стабов) = старейший ingress-MID треда. **Уникальность коррелятора = уникальность контента.**

В suite happy-path и multiturn-turn-1 одного модуля POST-ят **идентичный `_BODY`** (и все 4 теста отвечали
идентичным `"ok from llm-mock"`). Под `-n2` (разные xdist-воркеры, одновременно) это даёт коллизии на
нескольких слоях:

1. **glue-MID коллизия** → два конкурентных теста дают одинаковый glue-MID → notmuch **сливает треды** →
   старейший ingress-MID слитого треда (thread-root) становится «чужим» → LLM-вызов несёт неверный
   thread-root → `{stub_tag}::{неверный-root}` не засижен → **unmatched** + `#ingress>#glue` → **in-work 409**.
   *(Подтверждает гипотезу о Route-заголовке: State-Extension работал верно — испорчен был его ВХОД.)*
   **Фикс:** distinct per-test reply marker → distinct glue-MID → нет слияния. → 0 unmatched, 0 in-work.

2. **ingress_mid коллизия в long-hold registry** → `IsomorphPendingRegistry` хранил
   `dict[ingress_mid] = pending`; идентичный ingress_mid → второй `register` **затирал** первый коннект →
   осиротевший коннект ждёт до timeout (120 s), ответы могли уйти не в свой запрос.
   **Фикс:** registry = СПИСОК ожидающих на `ingress_mid` (per-connection future, без затирания) + `resolve`
   делает **broadcast** одного идемпотентного ответа ВСЕМ ждущим этот MID.

3. **общий notmuch-тред/glue** → идентичный `_BODY` = один `Message-ID` = одно сообщение (notmuch дедуп) =
   один пайплайн = один тред/glue, разделяемый happy-path и multiturn-turn-1. Per-test glue-wait и
   thread-/ingress-count проверки multiturn ломаются под `-n2`.
   **Фикс (предлагается):** distinct тела запросов у happy-path vs multiturn (см. §5).

`max_async=1` (e2e) — отдельный усилитель: сериализовал ВСЕ LLM/embed на одном RAG-loop; JSON-извлечение
добавило вызовов → под `-n2` оба теста душили друг друга. Снят (→ `4`, безопасно: корреляция per-call,
стабы матчатся по call-site + `hasContext`, без зависимости от порядка).

---

## 4. Варианты коррелятора long-hold (анализ)

| Вариант | Доступен в момент hold? | Уникален на запрос? | Доезжает до egress-push? | Вердикт |
|---|---|---|---|---|
| **content-addressed `ingress_mid`** (тек.) | ✅ (из тела сразу) | ❌ (идентичные тела совпадают) | ✅ (egress знает его) | коллизия идентичных — РЕШАЕТСЯ broadcast'ом |
| **generated `request_id` (uuid)** | ✅ | ✅ | ❌ | идентичные тела → один `Message-ID` → notmuch дедуп → ОДИН пайплайн/egress → знает лишь один id → второй коннект осиротеет. Чтобы доехал — пришлось бы сделать `Message-ID` уникальным → ломает thread-root/voting/precompute-seed E2E_ISOLATION |
| **notmuch `thread_id`** | ❌ (только ПОСЛЕ индексации; hold нужен сразу) | ❌ (per-conversation + контент-derived → совпадает) | — | непригоден: тайминг (нужен submit→index→read до register) + та же коллизия |
| **voting (glue-MID)** | — | — | — | снижает коллизии для ПРОДОЛЖЕНИЙ (turn-2 находит тред turn-1 по hash ответа), но коллизия здесь — идентичные ПЕРВЫЕ запросы (нет прошлого ответа для голоса) → не покрывает |

**Ключевой вывод:** ни один контент-derived коррелятор (`ingress_mid`, `thread_id`) не различает идентичные
конкурентные запросы; сгенерированный `request_id` не переживает контент-адресуемый дедуп до egress. Поэтому
правильный путь — **bucket по `ingress_mid` + broadcast** (идемпотентно: идентичный запрос → идентичный
ответ → все ждущие получают его), а НЕ смена ключа коррелятора. Голосование ортогонально (для продолжений).

---

## 5. Предложения

**Внедрено и проверено (оставляем):**
1. **`max_async=4` в e2e** (`_construction.py`) — постоянно. Разлок сериализации RAG-loop под `-n2`.
2. **distinct per-test reply marker** в `100_chat_reasoning_egress_tool.json` каждого isomorph-стаба —
   distinct glue-MID, нет слияния тредов, корректный thread-root/Route.
3. **broadcast pending-registry** (`bridges/isomorph/pending.py`) — список ожидающих на `ingress_mid` +
   broadcast; снятие по своему future. Устраняет затирание коннекта независимо от тела (robustness и для
   реального мира: два идентичных конкурентных запроса получают один идемпотентный ответ).

**К внедрению (финальная изоляция тестов):**
4. **distinct тела запросов** у happy-path и multiturn в каждом модуле (например, multiturn turn-1 несёт свой
   суффикс-маркер в промпте) → distinct `ingress_mid` → distinct notmuch-тред/glue/registry-bucket → полная
   изоляция тестов друг от друга под `-n2`. Тест сам сидит свой thread-root (как в фикстуре).
   *Принцип изоляции:* при общей notmuch-БД и контент-адресуемых коррелятах **тела ингресса И тела ответов
   должны быть test-уникальны с обеих сторон** (промпты уже маркированы; ответы — фикс №2; первый запрос
   multiturn — этот пункт).

**Гигиена/харнесс (уже сделано/смежно):**
5. `wait_for_sut_threlium_user_workers_idle` — best-effort под xdist (глобальный idle недостижим при
   параллельных тестах; изоляция держится marker-scope + thread-root, не глобальным idle).
6. retry-on-409 turn-2 (in-work контракт «retry after its reply»).

**Опционально (будущее, для масштаба):**
7. Per-xdist-worker или per-request-kind WireMock-контейнеры (см. §WireMock investigation) — если State-Extension
   `synchronized(store)` станет узким местом на большем `-n` (сейчас не является, ~10 ms@24-concurrent).

---

## 5-bis. Водяной знак (invisible-watermark) — анализ (по идее пользователя)

**Протокол Cline НЕ несёт идентификатора сессии** (проверено в `vendor/cline/apps/vscode/src/core/api/
providers/{anthropic,openai*}.ts`): request body = `model` + `system` + `messages` (+ tools/max_tokens);
нет `metadata.user_id`, нет conversation/task id, нет стабильного session-заголовка.

**И в ОТВЕТЕ нет поля, которое Cline вернул бы обратно** (ключевой вопрос — проверено):
`ClineStorageMessage extends Anthropic.MessageParam` хранит response-`id?` и `model?` ЛОКАЛЬНО, но при сборке
СЛЕДУЮЩЕГО запроса шлёт `Anthropic.MessageParam` = `{role, content}` (а `convertToOpenAiMessages` →
`{role, content, tool_calls}`) — `id`/`model` НЕ валидные поля входного сообщения и **отбрасываются**, до
моста не доезжают. Итог: на обоих surfaces единственное, что Cline эхо-ит в историю, — assistant **content**.
Явного поля под notmuch thread-id НЕТ ⇒ **остаётся только невидимый водяной знак в content ответа**.

**Инструмент уже есть:** `invisible_task_mid.py` — `encode_mid_safe(int) -> невидимый суффикс` (алфавит
zero-width/ZWNJ/word-joiner/variation-selectors, 16-ричное кодирование, якорь `U+FFF9`+ZWSP) и
`decode_mid_safe(text) -> int | None`. **Прецедент:** Telegram bridge помечает placeholder-сообщения этим же
кодером (`is_egress_placeholder_message`, `PLACEHOLDER_TEXT`).

### Финальный дизайн (по уточнениям пользователя): snowflake-MID везде + знак в ответе egress, БЕЗ placeholder

**Главный сдвиг — отказ от контент-адресуемых Message-ID.** MID каждого письма генерируется ПРОИЗВОЛЬНО и
уникально: `base62(uid)@localhost`. Идентичные тела → РАЗНЫЕ MID → РАЗНЫЕ notmuch-сообщения/треды ⇒
**класс коллизий исчезает В КОРНЕ** (это и есть первопричина `-n2` из §3). Голосование по `hash(reply)` в
`thread_resolve` и контент-хеш MID больше не нужны.

**Источник `uid` — Snowflake (НЕ счётчик).** Монотонный счётчик ломается на рестарте (сброс → повторные MID →
коллизии тредов, и хуже — устаревший знак в истории клиента переуказывает на новый тред). Telegram-bridge
робастен потому, что метит ВНЕШНЕ выданный платформой `message_id`; у isomorph такой авторитетной нумерации нет.
Выбор — **Snowflake** (`snowflake-id` 1.0.2, `SnowflakeGenerator(instance)`). Проверено по исходнику
`snowflake/snowflake.py`: `value = timestamp << 22 | instance << 12 | seq` (41 бит время / 10 бит instance /
12 бит seq), `Snowflake.parse(int, epoch)` декодирует обратно `(timestamp, instance, seq)`, поддержан кастомный
`epoch`. k-сортируемый по времени; компонента времени растёт через рестарты (нет reset-коллизии счётчика).

**В водяной знак кладём САМ snowflake-int, не строку MID** (он короче): `MID = base62(snowflake)@localhost`,
знак = `encode_mid_safe(next(gen))`, декод `decode_mid_safe` → snowflake → `base62` → точный MID. С **кастомным
свежим epoch** старшие (временные) биты малы → int и невидимый знак остаются короткими годами.
**Бонус:** корень треда = МИНИМАЛЬНЫЙ snowflake в DAG → находится без обхода и даже без знака.

**Вместимость (по исходнику):** `MAX_TS=2^41−1` ≈ 69.7 лет, `MAX_INSTANCE=1023` (10 бит), `MAX_SEQ=4095`
(12 бит) → 1024×4096 = 4.19M id/мс. С запасом.

**Назначение instance — детерминированное, НЕ случайное (важно: это единственный collision-critical id).**
- *Разные мс* → отличается `time` → уникально при любом instance (интуиция «есть время» работает ТУТ). ✓
- *Одна мс* → уникальность держится на `instance`(+`seq`). Два минта в одну мс с ОДИНАКОВЫМ instance и `seq=0`
  (свежие генераторы на контекст) → КОЛЛИЗИЯ → слияние тредов = ровно тот `-n2`-баг, что устраняем.

**Случайный instance — birthday-проблема, не «1/1024 однажды».** Если контекст берёт instance случайно и держит,
вероятность что какие-то два из C контекстов совпали ≈ `1 − e^(−C²/2048)`: C=2 → 0.1%, C=16 → ~12%, C=32 → ~39%.
При совпадении instance ВСЕ same-ms минты между этими контекстами коллизируют. Т.е. случайность возвращает
именно ту неисправность, что мы убираем (из «невозможно» в «редко») — «есть время» спасает только cross-ms,
не concurrent.

**РЕШЕНИЕ: instance = in-process счётчик (по минту), wrap по mod 1024.** Каждый минт инкрементит
процесс-локальный счётчик; instance = counter % 1024. Внутри процесса это ZERO-collision: соседние минты дают
разные instance, поэтому даже два минта в одну мс различаются по instance (не опираясь на `seq`); чтобы
повторить instance В ОДНУ мс нужно >1024 минтов/мс (>1M/с) — нереально. Интуиция «время разное» работает
intra-process строго.
- **Мост** — asyncio-loop, минты сериализованы → счётчик без лока.
- **Egress движка** — многопоточный → счётчик через `itertools.count`/atomic (инкремент атомарен) либо лок.

**Один остаточный случай — cross-process.** Мост и egress ведут СВОИ счётчики с 0 → оба могут выдать
`instance=N` в одну мс → одинаковый `time|instance|seq=0` → коллизия. Закрывается one-liner'ом, оставаясь
«просто счётчиком» — **партиционировать instance по процессу** (старший бит = process-id): мост → `0..511`,
egress → `512..1023`. Разные диапазоны ⇒ cross-process коллизия невозможна, логика счётчика не меняется.
(Без партиционирования остаточный риск мал, но ненулевой; рекомендуется партиционировать.)

Остаточная оговорка — **часы не идут назад**: откат стенных часов (NTP *step*) может переиздать
`time|instance|seq` (uuid4 иммунен). NTP *slew* безопасен; на NTP-дисциплинированном сервере риск пренебрежим.

**Зависимость:** `snowflake-id` ДОБАВИТЬ в `pyproject.toml` (+ пересборка venv); пакет чистый python без
зависимостей, безопасен для engine/bridge (тонкий worker `engine_submit` MID не чеканит). `pybase62` — УЖЕ в
зависимостях (base62 готов).

**MID КАЖДОГО письма (включая ПЕРВОЕ) — через snowflake.** ingress-MID = `base62(snowflake)@localhost`;
egress (archive glue) MID = `base62(snowflake)@localhost`. Контент-хеша нет нигде. Идентичные конкурентные
ПЕРВЫЕ сообщения → РАЗНЫЕ ingress-MID → notmuch НЕ дедупит/не сливает → два РАЗНЫХ треда. Именно это делает
правило «первое сообщение в истории = всегда новый тред» истинным даже для идентичных тел (первопричина `-n2`
снята в корне).

**БЕЗ placeholder (отвергнут).** В HTTP без SSE (JSON) ответ ровно ОДИН — «работа начата» как ответ СКАЗАЛ БЫ
клиенту, что работа завершена. ⇒ placeholder для JSON не годится. И он НЕ нужен: знак кладём в САМ ответ агента
(egress archive glue). Egress при генерации ответа дописывает в его content невидимый знак =
`encode_mid_safe(snowflake_glue)` (САМ snowflake, не строка MID; MID = `base62(snowflake)` восстанавливается из
него). Клиент получает ответ — в нём уже однозначный якорь.

**Продолжение (минимум переделок).** Клиент кладёт ответ (со знаком) в историю. На новом ходу мост
`decode_mid_safe(last_assistant)` → `snowflake_glue` → `base62` → glue-MID ХВОСТА → ставит `In-Reply-To = glue-MID`
→ notmuch прицепляет новое сообщение к хвосту треда. Голосование по `hash(reply)` не нужно. **Первое сообщение**:
знака в истории нет → новый тред, свежий ingress snowflake-MID, без `In-Reply-To`. (Резерв: DAG-корень =
минимальный snowflake — если знак почему-то отсутствует, тред всё равно находится.)

**Осуществимость по Cline — ПРОВЕРЕНО (знак переживает round-trip):**
- `sanitizeAnthropicMessages` (`anthropic-format.ts`) — только cache-control на 2 последних user-сообщениях +
  `convertClineStorageToAnthropicMessage`; контент НЕ нормализует/не трогает.
- `convertClineStorageToAnthropicMessage` (`shared/messages/content.ts`) — string-контент возвращает дословно
  (fast-path), для массива блоков снимает лишь Cline-специфичные ПОЛЯ (не текст).
- `convertToOpenAiMessages` (`openai-format.ts`) — текст ассистента сохраняет.
- grep по всему `core/api` + `shared/messages`: НЕТ `normalize()`, нет вырезания zero-width
  (`\u200*`/`﻿`/`\u206*`), нет trim контента. ⇒ невидимый знак доезжает обратно на обоих surfaces.

**Работает для JSON И SSE одинаково:** знак — в content ответа; в JSON это единственный ответ, в SSE Cline
аккумулирует дельты в тот же финальный assistant-content. Раннего кадра/placeholder/keep-alive-трюка не нужно.
Контракт long-hold (один ответ = готовый результат) не меняется.

**Объём переделок — на порядок меньше placeholder-схемы:**
- ingress-MID: контент-хеш → `base62(snowflake)`;
- egress glue-MID: `hash(reply)` → `base62(snowflake)` + дописать невидимый знак `encode_mid_safe(snowflake)` в
  content ответа;
- bridge `thread_resolve`: голосование `hash(reply)` → `decode_mid_safe(last_assistant)` → `In-Reply-To`.
Никакой placeholder-машинерии, ранних SSE-кадров, telegram-style правок сообщения.

**E2E-сид — РЕШЕНО флагом стратегии MID (по идее пользователя).** Настройка переключает ТОЛЬКО ИСТОЧНИК int
для MID, не механизм:
- **Прод (default):** `base62(snowflake)@localhost` — уникально, без коллизий.
- **E2E:** `base62(hash(body))@localhost` (или MID прямо из JSON-входа теста) — ПРЕДСКАЗУЕМО: тест предвычисляет
  thread-root и сидит `{stub_tag}::{thread-root}` ДО запроса (как E2E_ISOLATION и делает сейчас) + distinct-тела
  (§5 п.4) → разные MID → нет cross-test коллизий. Предсказуемо И без коллизий.

**Минимальный зазор тест/прод:** механизм продолжения (знак → `In-Reply-To`) ОДИНАКОВ в обоих режимах — знак
несёт любой glue-MID-int (snowflake или content-hash, оба кодируются `encode_mid_safe`). Значит e2e в
content-режиме ВСЁ РАВНО прогоняет продакшн-путь продолжения (encode→store→round-trip→decode→IRT); флаг
обходит лишь сам snowflake-генератор — он тривиально покрывается юнит-тестом (уникальность, wrap, `parse`).
Доп.: один serial (`-n0`) тест в snowflake-режиме с ослабленным матчем стабов smoke-ит и его end-to-end;
параллельные isolation-тесты идут в content-режиме. JSON-тесты могут слать MID напрямую (ещё предсказуемее);
cline-тесты — `hash(body)` + distinct-тела (их единственный рычаг content).

**Вывод:** snowflake-MID на ВСЕ письма (вкл. первое) + знак `snowflake_glue` в ответе egress + `In-Reply-To` на
продолжении — ПОЛНОЕ и минимальное по переделкам решение: коллизии невозможны в корне, якорь однозначен,
работает для JSON и SSE без placeholder, осуществимость по Cline проверена. Снимает §5 п.2 (distinct-reply) и
§5 п.3 (broadcast) как ненужные. Для немедленного зелёного `-n2`: §5 п.4 (distinct-тела); затем — внедрить
snowflake+знак.

---

## 6. Отдельная заметка про thread_id в FSM-логике (вне long-hold)

Идея «запрашивать notmuch `thread_id` по месту и не хранить в заголовках» — здравая для thread-scoped логики
ВНУТРИ FSM (после индексации сообщение уже в треде). Два слоя коррелятора теперь раскладываются чисто:
- **до индексации (приём HTTP)** — коррелятор генерит мост (§5-bis placeholder+знак); им же помечается ответ
  и анкеруется Message-ID/тред, поэтому идентичные тела расходятся в разные треды сразу.
- **после индексации (FSM-стадии)** — сообщение уже в notmuch-треде, и thread-scoped логика может брать
  `thread_id` напрямую из notmuch, не таская его в заголовках. Мостовой коррелятор и notmuch thread_id
  однозначно соответствуют (мост анкерует тред), так что переход между слоями детерминирован.
Упрощение FSM-логики на `thread_id` можно вести независимо от внедрения placeholder+знака.
