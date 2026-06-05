# Канал `isomorph` — входящий HTTP-мост для нескольких LLM API

`isomorph` — первый **входящий** HTTP-сервер Threlium. Для агентских клиентов (Cline, Cursor,
Continue, любой OpenAI/Anthropic-совместимый клиент) он выглядит обычным LLM-провайдером, а внутри
прогоняет запрос через стандартный FSM-контур по схеме **long-hold + egress-push**.

Процесс: `threlium-bridge@isomorph` (systemd-инстанс, uvicorn). Реализация —
[`bridges/isomorph/`](../ansible/roles/threlium/files/scripts/threlium/bridges/isomorph/); FSM-стадия —
[`states/egress_isomorph.py`](../ansible/roles/threlium/files/scripts/threlium/states/egress_isomorph.py).

## Endpoints (MVP / Phase A)

| Method | Path | api_surface | FSM | Назначение |
|--------|------|-------------|-----|-----------|
| POST | `/v1/messages` | `anthropic_messages` | да | Anthropic Messages; SSE при `stream`, иначе JSON `Message` |
| POST | `/v1/chat/completions` | `openai_chat_completions` | да | OpenAI chat-completions; `stream:true`→SSE, иначе JSON |
| GET | `/v1/models` | — | нет | `{object:"list",data:[{id}]}` из `settings.litellm` (опц.) |
| GET | `/health` | — | нет | ops/readiness |
| POST | `/internal/v1/push` | — | нет | egress→мост; localhost + `push_secret`; идемпотентно |

Auth: один `bridges.isomorph.api_key` (Anthropic `x-api-key` ИЛИ OpenAI `Authorization: Bearer`).
Base URL клиента: `http://<host>:<listen_port>/v1` (SDK добавляет `/messages` / `/chat/completions`).

## Поток одного хода

```
Клиент → POST (полная история) → мост: tail-extraction + content-addressed MID (чистый compute)
       → register pending(request_id) → 200 + SSE keep-alive (если stream)
       → deliver(ingress email) → FSM (enrich → reasoning → … → egress_router → egress_isomorph)
       → egress: archive glue (FIRST) → POST /internal/v1/push → мост: SSE-чанки + [DONE] / JSON
```

reasoning синхронный и терминальный → **Phase A синтезирует полную SSE-цепочку из одного push**
(не живой стрим). Keep-alive держит соединение, пока FSM работает.

## Тред-непрерывность (контент-адресуемые Message-ID)

Клиент (и production VSCode-расширение, и CLI) шлёт **полную self-contained историю** в каждом
запросе (stateless-природа OpenAI/Anthropic API), без `In-Reply-To`. Мост восстанавливает стык
**без чтения notmuch**, потому что каждый isomorph-MID = `canon(IsomorphContentId(hash(контент)))`:

- `egress_isomorph` минтит glue `Message-ID = canon(hash(ответ Threlium))`;
- следующий запрос несёт этот ответ как **last-assistant** → мост пересчитывает тот же хеш и ставит
  его как `In-Reply-To` нового ingress → notmuch связывает тред сам (MID/IRT-threading);
- `Message-ID` нового ingress = `canon(hash(хвост, parent))` → идемпотентность ретраев.

Это **email-glue с хешем ответа вместо внешнего SMTP-MID** (Cline возвращает текст ответа, не MID).
Для FSM isomorph НЕОТЛИЧИМ от email/tg/mx; **FSM не меняется**. Нормализация хеша — общий модуль
[`types/isomorph_content.py`](../ansible/roles/threlium/files/scripts/threlium/types/isomorph_content.py):
text-блоки + сигнатура tool_use, исключая thinking/`cache_control`; tool-call `id` включается на
Anthropic, исключается на OpenAI (SDK усекает). Детали инвариантов — [THREAD_MODEL §6](THREAD_MODEL.md#6-канал-isomorph-контент-адресуемые-message-id-glue-без-внешнего-mid).

**ARCHIVE-FIRST**: egress пишет glue до push — иначе Cline пришлёт следующий запрос раньше записи
glue → orphan-форк. Первый ход (нет last-assistant) или редкий промах хеша → orphan → новый тред.
Повтор байт-идентичного ответа в треде → один MID → форк ветки (benign, FSM терпит).

## Идентичность сообщений и коллизии (итог исследования)

Контент-адресуемые MID дают **две** коллизии: (A) **межсессионная** — два разных диалога с байт-идентичным
контентом → один MID → notmuch-дедуп → треды СЛИВАЮТСЯ; (B) **внутрисессионный повтор** — один и тот же
ответ дважды в одном диалоге → один MID → форк/дедуп. Подтверждено эмпирически: два запуска `cline "reply pong"`
в одном cwd дают ИДЕНТИЧНЫЙ ingress-MID.

**Жёсткое ограничение (почему нельзя просто добавить id в хеш):** glue-MID должен быть **пересчитываем
мостом** на следующем ходу из echo-контента (last-assistant), и это единственный якорь, **переживающий
компакцию** Cline (хвост/last-assistant компакция не трогает, а начало диалога — режет). Любой
дискриминатор обязан быть: echo-стабилен, пересчитываем обеими сторонами, переживать компакцию.

**Что дают протоколы и Cline (verified vendor/cline):** OpenAI/Anthropic **stateless** — в запросе нет
conversation/session-id и нет per-message id у элементов `messages` (только опц. `metadata.user_id` /
`user` — это end-USER, не сессия, и Cline их **не** шлёт). У Cline есть task-`ulid`, но он идёт только во
внутренней телеметрии и заголовке `X-Task-ID` **cline.bot-провайдера** — не в Anthropic/OpenAI-запросе
(и это было бы привязкой к Cline). `tool_use.id` мост генерирует сам (echo-стабилен на Anthropic, OpenAI
усекает) — но есть только у tool-ответов, не у текстовых.

**Разбор вариантов:**

| Вариант | (A) межсессия | (B) повтор | Переживает компакцию | Continuity | Доступно |
|---|---|---|---|---|---|
| 1. только контент (текущий) | ✗ слив | ✗ форк | ✓ | ✓ | ✓ |
| 2. контент + позиция (index) | ✗ (тот же контент→тот же index) | ✓ | ✗ (компакция сдвигает) | ✗ (egress не знает index) | частично |
| 3. session-id + порядковый № | ✓ (если есть session-id) | ✓ | ✗ (обрезка начала) | ✗ | ✗ нет session-id |
| 4. протокольный per-message id | ✓ | ✓ | ✓ | ✓ | ✗ (в OpenAI/Anthropic запросе нет) |
| 5. nonce моста, вшитый в ответ и echo | ✓ | ✓ | ✓ | ✓ | частично (только tool-ответы; OpenAI усекает id; в тексте — некуда) |
| 6. контент + `metadata.user_id`/`user` | частично (юзер, не сессия) | — | ✓ | ✓ (user-id на каждом запросе) | ✓, но Cline не шлёт |
| 7. контент + хеш всего префикса | ✗ (идентичные диалоги, но реже) | ✓ | ✗ (компакция) | ✗ (egress не видит Cline-массив) | частично |
| 8. бегущее окно последних K assistant-ответов | частично (реже при K>1) | ✓ | ✓ при малом K (окно в хвосте) | ✓ (обе стороны считают окно) | ✓ (protocol-agnostic) |
| 9. nonce во вшитом echo-поле структуры ответа | ✓ | ✓ | ✓ | ✓ | Anthropic: да (thinking `signature`); OpenAI: только tool-ответы |

**Вариант 9 (структурное echo-поле, ответ на «вшить метаданные в поле, а не в текст») — verified vendor/cline.**
Cline пересобирает assistant-сообщение из своей модели → **произвольные кастомные поля отбрасываются**;
echo-стабильны только: (a) **Anthropic `thinking.signature`** — Cline хранит и **возвращает** thinking-блоки
**с** signature (без signature — отбрасывает: `content.ts` `sanitizeAnthropicMessages`); (b) `tool_use.id`
(Anthropic) / усечённый `tool_calls[].id` (OpenAI) — только у tool-ответов. Поэтому для **текстовых** ответов
единственный структурный echo-канал — **Anthropic thinking-`signature`**: мост эмитит к тексту thinking-блок
`{type:"thinking", thinking:"…", signature:"<nonce=request_id>"}`; Cline возвращает его на следующем ходу;
мост читает signature из last-assistant → `In-Reply-To = canon(nonce)`. Это даёт **уникальную идентичность
ответа** (решает A и B, переживает компакцию, не привязано к Cline — это поле протокола Anthropic).
**Оговорки:** требует thinking-режима (CLI `--thinking` ≠ `none`, дефолт `medium` — ок); нужно подтвердить,
что `@ai-sdk/anthropic` пропускает наш signature без валидации (реальный Anthropic подписывает крипто —
наш мост не валидирует, но SDK клиента может); **OpenAI текстовый ответ канала не имеет** → там остаётся
контент-адресация. Статус: **кандидат, требует эмпирической проверки round-trip** (эмит thinking → запуск
cline → проверить echo signature в следующем запросе).

**Вариант 8 (бегущее окно соседних сообщений) — рекомендуемое усиление, без привязки к Cline.** Вместо
`hash(R_N)` якорь привязывает агентское сообщение R_N к его **локальной последовательности** — окну
**соседних** сообщений (любых ролей, не только assistant), которые компакция **не может вырезать все сразу**
(режет середину/начало, хвост сохраняет):

- **Glue-якорь (continuity) = `hash(R_N + K предшествующих сообщений)`.** Обе стороны имеют предисловие:
  **egress** (ход N) — R_N + предыдущие из треда; **мост** (ход N+1) — R_N (last-assistant) + те же
  предыдущие из истории. Окно одинаково → continuity цела. Кросс-сессия требует совпадения R_N **и** K
  предшественников.
- **Ingress-MID = `hash(R_N + хвост-после-R_N)`** (предложение «включать хвост после агентского»): мост на
  ходу N+1 имеет **и** R_N, **и** новый хвост → связывает ingress с предшествующим агентским ответом и его
  продолжением. Симметрия не нужна (MID считает только мост) → окно можно брать шире.

Свойства: **(B) внутрисессионный повтор решён** — `R_N==R_M`, но соседи различны → разные якоря; **(A)
межсессия** — нужно совпадение **всей локальной последовательности** окна, а не одного сообщения →
практически исчезает (полные совпадения K соседей крайне редки); **компакцию переживает** при малом K
(соседи R_N — в сохраняемом хвосте); **continuity сохраняется**. Цена: egress поднимает K−1 предыдущих
сообщений из треда (дешёвый IRT-подъём, уже делается для резолва маршрута). **Граница K:** малое (2–3),
иначе окно уходит в компактированную зону → graceful orphan-fallback. K=1 = текущая контент-адресация.

**Эмпирическая проверка (2026-06-05, реальный Cline CLI против моста):**
- **Заголовки запроса (Anthropic surface)** — захвачены на мосту: `accept, accept-encoding, anthropic-beta,
  anthropic-version, connection, content-length, content-type, host, user-agent, x-api-key`. **Никакого**
  session/task/request/trace-id заголовка. `taskId` в коде Cline — **только телеметрия**, на провод
  Anthropic/OpenAI не уходит (`config.ts:309`; маппинга в `X-Task-Id` в `sdk/packages/llms` нет). →
  заголовочного дискриминатора сессии **нет**.
- **Вариант 9 (thinking `signature`) — НЕ round-trip через CLI.** CLI (`--thinking medium`) **не** просит
  extended thinking в запросе (нет поля `thinking`), поэтому несолицитированный thinking-блок моста Vercel
  AI SDK отбрасывает → на следующем ходу signature не возвращается (проверено: 0 echo). Вариант 9 остаётся
  теоретически валидным лишь для **VSCode-расширения** (офиц. `@anthropic-ai/sdk`, иной стек, хранит
  signed-thinking), но **через CLI непроверяем и неприменим**.

**Ключевой вывод:** идеал — **вариант 4** (per-message id протокола), но его НЕТ в stateless
OpenAI/Anthropic. Единственный полный — **вариант 5** (мост вшивает nonce=`request_id` в ответ, Cline
echo-возвращает), но он работает только для **tool-ответов** (для текстовых — некуда вшить, не пачкая
ответ; OpenAI усекает id). Позиция/префикс/seq (2,3,7) **ломаются компакцией** (точное замечание про
обрезанное начало). Session-id (3,6) **недоступен** в протоколе. Поэтому для общего (текстового) случая
**контент-адресация (вариант 1) — единственный якорь, переживающий компакцию и сохраняющий continuity**;
остаточные коллизии **фундаментальны** для эмуляции stateless-LLM-API (два байт-идентичных запроса
неразличимы — это и есть свойство stateless-провайдера; слияние идентичного = форма кэша/дедупа).

**Рекомендации:**
1. **Оставить контент-адресацию** как базу.
2. **Опц. усиление (вариант 6, protocol-clean, без Cline-привязки):** если запрос несёт `metadata.user_id`
   (Anthropic) / `user` (OpenAI) — подмешивать в хеш (обе стороны видят на каждом ходу) → изоляция по
   end-user. Cline не шлёт → no-op для него; полезно прочим клиентам.
3. **Опц. усиление для tool-ответов (вариант 5):** на Anthropic вшивать `request_id` в `tool_use.id` →
   уникальность ответа. Для текстовых — неприменимо.
4. **В реальности** межсессионная коллизия редка: `environment_details` Cline (cwd, файлы, время в
   VSCode-расширении) делают первый ход уникальным; вырожденный голый промпт в пустом cwd — тест-артефакт.
5. **E2e-тест:** уникальный маркер в промпте (`reply pong [<uuid>]`) → уникальный контент → уникальный
   thread-root и нет меж-прогонной коллизии; плюс чистка isomorph-тредов из Maildir в setup.

## Два клиент-стека (verified против vendor/cline)

| Клиент | SDK | Роль |
|--------|-----|------|
| **VSCode-расширение** | официальные `openai@6.21.0` / `@anthropic-ai/sdk@0.37.0` | **production** (строгая планка wire) |
| **Cline CLI** | Vercel AI SDK (`@ai-sdk/openai`/`@ai-sdk/anthropic` v3) | тест-харнесс (e2e) |

Wire — стандарт-совместимый под обоих; официальные SDK строже. Особенности: `include_usage`
форсирован OpenAI-провайдером → usage-чанк обязателен (с пустым `choices:[]`); `[DONE]` терминатор;
стрим НЕ под жёстким 30 c (это `fetchJson` CLI для не-стрим JSON) — граница ~undici 300 c → keep-alive
~20–30 c; запрос Anthropic несёт `system`-массив + `cache_control` + `betas` — мост их игнорирует.

## Settings (`bridges.isomorph`)

`listen_host`, `listen_port` (bind + таргет push), `api_key`, `push_secret`, `request_timeout_sec`,
`keepalive_sec`, `graceful_shutdown_sec`, `enabled_surfaces`. Env: `THRELIUM_BRIDGES__ISOMORPH__*`.
Пустой `api_key` → мост не стартует (`bridge_readiness`).

## Bake (только e2e)

Cline CLI (Node.js) запекается в SUT-образ **только** e2e-harness'ом
([`tests/e2e/scripts/bake_e2e_sut_image.sh`](../tests/e2e/scripts/bake_e2e_sut_image.sh):
`docker exec` NodeSource Node 22 + `npm i -g cline` перед `docker commit`). **НЕ часть прод-деплоя**
(`site.yml` Node/Cline не ставит). `starlette`/`uvicorn`/`anyio` — обычные prod-зависимости моста.

## Вне scope MVP

Phase 1.5: `POST /v1/responses` (`openai_responses`), `GET /v1/model/info` (LiteLLM-провайдер Cline).
Phase B: живой стриминг (инкрементальный push из стрим-режима reasoning). Embeddings, legacy
`/v1/completions`, Codex, wss — Cline agent loop не использует.
