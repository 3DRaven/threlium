# Threlium: структура сообщений, хранение и идентификаторы

Документ задаёт соглашения системы по структуре сообщений и их хранению (целевая архитектура: контракт хранилища и индексации — [`INDEX.md`](INDEX.md), мастер-источник терминов):

1. **Раскладка хранения** (`$THRELIUM_HOME`): durable stage Maildirs под единым notmuch root'ом, без **legacy** выделенного `archive/Maildir/` вне `stages/`; хвост записи об отправке — **`stages/archive/Maildir`** (см. [`INDEX.md` §12](INDEX.md#12-glossary--cross-references) определения «Archive», «Stage Maildir», «Union index»).
2. **Канонизация идентификаторов** (`Message-ID`, `In-Reply-To`, `References`) на границах Threlium: **мосты** (`threlium.bridges.*`) собирают полный `EmailMessage` на канале: для **email** (IMAP) `Message-ID`, `In-Reply-To` и каждый токен `References` в wire-виде **`<base62(msgspec JSON EmailNativeId)@localhost>`** через `RfcMessageIdWire.from_native(EmailNativeId(v=1, message_id=inner))` / для списка токенов в `References` — `RfcReferencesWire.threlium_canonicalize_refs` (строка `inner` из угловых скобок, без классификации «вида» строки); для **Telegram / Matrix** — **`<base62(utf8(inner))@localhost>`** через `RfcMessageIdWire.from_inner_for_bridge(inner).value` (композитный `inner`); `X-Threlium-Route` — b62(JSON) маршрута (`IngressRouteB62Wire.from_ingress_route(…).value`). Раннер `python -m threlium.runners.bridge <channel>` **только** доставляет байты в ingress (`run_fdm`), заголовки не переписывает. Для email в JSON маршрута поле `reply_target_rfc_message_id` — объект `{"value":"<внешний RFC Message-ID>"}` (`ExternalRfcMidWire` в `EmailIngressRoute`), источник SMTP `In-Reply-To` на ответ агента; цепочка `References` на промежуточных стадиях FSM не переносится — см. `egress_router` / `egress_email`. Эмиссия и грамматика канонического Threlium MID — только маркер `@localhost` (`RfcMessageIdWire`).
3. **Имена файлов и уникальность**: во всех Maildir'ах (всех стадий) — только штатные имена от **`notmuch insert`** (`{time}.M{usec}P{pid}.{host}`); идентификация внутри системы — по содержимому заголовка `Message-ID:`.
4. **Stage submit и RAG-loop LightRAG** (вместо «архивного обработчика» старой схемы): доставка письма выполняется одним терминирующим **`notmuch insert`** в fdm `pipe` ([§3](#3-mailfilter-snippet)); для `To: archive@localhost` insert включает **`+lightrag_indexed`** сразу в **fdm** (`ins_stage_archive`), чтобы RAG-loop не обрабатывал эти MIME; `threlium-work@…` + `threlium-engine` обрабатывают FSM и `nm_settle()`; индексация LightRAG (`ainsert`, тег `+lightrag_indexed`) для остальных стадий выполняется **внутри** `threlium-engine` на выделенном asyncio-loop после settle, **только settled-сообщения** ([`INDEX.md` §5b](INDEX.md#5b-lightrag-worker)). Подробнее — [§5](#5-stage-worker-и-lightrag-worker).
5. **Ошибки engine и bridge** ([`INDEX.md` §5.6](INDEX.md#56-universal-error-handling-в-runnersworkerpy)): логирование в journald; при ошибке до `nm_settle` письмо остаётся `+unread`, submit **`exit 1`**; bridge — лог и **`exit 1`** (`Restart=on-failure`). Отдельной стадии `errors/` и error-mail в FSM нет.
6. **Канонические служебные заголовки FSM на wire** — таблица и правило стиля в [§8](#8-canonical-x-threlium-headers-glossary); пошаговая семантика стеков — в [`SUBAGENT_TABLE.md`](SUBAGENT_TABLE.md). Заголовки `X-Threlium-Channel`, `X-Threlium-Route` и `X-Threlium-Route` **удалены** — маршрутизация определяется заголовком `X-Threlium-Route`.

Собственно оркестрация стадий FSM (systemd-юниты, template-watcher'ы, диспетчер, воркеры, параллелизм, serial-per-thread) — в [`ORCHESTRATION.md`](ORCHESTRATION.md). Контракт Python-скрипта стадии (модуль `threlium.states.<stage>` как функция-состояние `main(msg: EmailMessage, stage: FsmStage) → EmailMessage | None`, граф состояний FSM, билдеры MIME) — в [`FSM.md`](FSM.md). Смежные контракты: [§6](#6-канал-и-routing-данные-в-заголовке-from) (канал в `From:`, маршрут в `X-Threlium-Route`), [§8](#8-canonical-x-threlium-headers-glossary) (имена служебных заголовков `X-Threlium-*` на wire), [`SUBAGENT_TABLE.md`](SUBAGENT_TABLE.md) (FSM-шаги, маркеры `subagent_intent`/`subagent_end`, матрица), [`MEMORY_TABLE.md`](MEMORY_TABLE.md) (память, теги `notmuch`), [`ARCHITECTURE.md`](ARCHITECTURE.md) (общая архитектура, **fdm** + `notmuch insert` в [§8.4](ARCHITECTURE.md#84-доставка-maildrop), `litellm`/LLM tool-calling в [§8.7](ARCHITECTURE.md#87-llm-и-lightrag), изоляция `cli_exec` в [§6](ARCHITECTURE.md#6-слой-cli-и-безопасность-исполнения)).

---

## 1. Раскладка хранения

```text
$THRELIUM_HOME/
  stages/                    # один notmuch root (database.path)
    <stage>/Maildir/{new,cur,tmp}/   # один Maildir на каждую стадию из threlium_fsm_mailbox_stages
  lightrag/working_dir/      # shared LightRAG storage (NanoVectorDB+NetworkX+JSON)
```

Канонический список стадий и роль каждой — [`FSM.md` §2.1](FSM.md#21-канонический-состав-стадий-threlium_fsm_mailbox_stages); Ansible-деплой без per-stage LightRAG path — [`INDEX.md` §6.3](INDEX.md#63-активация-через-ansible).

**Инварианты:**

- **Один Maildir на стадию, общий для всех тредов.** Разделять стадии по тредам не требуется: письма разных тредов не пересекаются благодаря каноничным `Message-ID:` / `In-Reply-To:` ([§2](#2-канонизация-идентификаторов-на-границах-системы)) и per-thread мьютексам (см. [`ORCHESTRATION.md` §3](ORCHESTRATION.md#3-механизм-post-insert-hook--dispatch-script)). Имена файлов — **всегда** штатные Maildir-имена от `notmuch insert` (через fdm `pipe`); кастомного формата нет ([§3](#3-имена-файлов-в-стадиях)). Локальная идентификация треда идёт по содержимому заголовков, а не по имени файла.
- **Stage Maildir = durable mailbox.** Stage worker **не удаляет** файлы. После успешной обработки — `nm_settle()` (`with db.atomic(): discard("unread") + to_maildir_flags()`) — файл переезжает `new/<id>` → `cur/<id>:2,S` и остаётся там навсегда (или до retention, см. [`INDEX.md` §10.1](INDEX.md#101-future-work-deferred)). Ephemeral-семантика старой схемы (`os.remove(file_path)` по успеху) **отменена** ([`INDEX.md` инвариант I4](INDEX.md#2-invariants)).
- **Union notmuch index.** `notmuch database.path = ~/threlium/stages` (один root над всем поддеревом stages/). Все stage Maildir'ы автоматически индексируются вместе; `notmuch search '*'` возвращает все письма всех стадий — это и есть «логический архив» ([`INDEX.md` §12 «Archive», «Union index»](INDEX.md#12-glossary--cross-references)). Никаких отдельных folder'ов вне `stages/` (в частности — `archive/Maildir/` как в ранней модели — нет).
- **LightRAG storage — shared `working_dir`.** Один на всю систему (`$THRELIUM_HOME/lightrag/working_dir/`); writer = процесс `threlium-engine` / RAG-loop ([`INDEX.md` инвариант I2](INDEX.md#2-invariants)). Мягкое разведение тредов — через синтетическую ingest-строку (`X-Threlium-Thread-Id` только для `ainsert`) и Jinja-промпты `aquery`, а не разделением storage'а (см. [`INDEX.md` §7.6](INDEX.md#76-per-thread-scoping-soft-через-маркеры), [ADR 0001](adr/0001-lightrag-ingest-chunking-enrich.md)).

---

## 2. Канонизация идентификаторов на границах системы

### 2.1. Принцип

Внутри Threlium действует **единая каноническая форма** `Message-ID`:

```
<base62(payload) @ localhost>   # email IMAP bridge + FSM: payload = msgspec JSON EmailNativeId; TG/MX bridge: payload = utf8(inner)
```

Стратегия `Message-ID` различается **по каналам**:

- **email (IMAP bridge)**: входящий `Message-ID:` → inner без угловых скобок → `RfcMessageIdWire.from_native(EmailNativeId(v=1, message_id=inner)).value` → wire `@localhost`; каждый `<…>` в `References` — тот же контракт через `RfcReferencesWire.threlium_canonicalize_refs`; `In-Reply-To` при наличии — тот же путь; внешний исходный MID для ответа агента кладётся в `EmailIngressRoute.reply_target_rfc_message_id` (`{"value":"…"}` в JSON маршрута).
- **telegram / matrix (long-running bridge)**: композитный `inner` (`tg:v1:…`, `mx:v1:…`) → `RfcMessageIdWire.from_inner_for_bridge(inner).value`; маршрут и курсоры — в **`X-Threlium-Route`** (`IngressRouteB62Wire.from_ingress_route`). Раннер `threlium.runners.bridge` MIME не меняет, только `run_fdm`.
- **FSM-хопы** (внутренние письма): `RfcMessageIdWire.internal_for_fsm()` — `EmailNativeId` + `@localhost` (msgspec+base62).

Для email `raw_native_id` — **канал-специфичный JSON-блок** (`EmailNativeId`) с единственным полем `message_id`, необходимым для **идентичности** (A) сообщения и **routing'а ответа** (B) через штатные RFC 5322-заголовки. Recovery (C) в email не нужна — IMAP/MDA-pipe не использует курсор уровня приложения.

Для Telegram/Matrix (long-running bridge) маршрут и курсоры живут в **`X-Threlium-Route`** (`IngressRouteB62Wire.from_ingress_route`). Восстановление курсора после рестарта — `notmuch` по `tag:route` + `from:telegram@localhost` / `from:matrix@localhost` и разбор wire-заголовка маршрута.

`EmailNativeId` с полем `message_id` используется для кодирования исходного внешнего RFC MID в каноне (внутри blob слева от `@`). Для FSM-порождённых писем в `EmailNativeId.message_id` лежит результат `email.utils.make_msgid(domain='localhost').strip('<>')`. Канал входа определяется по **`From:`** (`<channel>@localhost`) и сверяется с полем `channel` в JSON **`X-Threlium-Route`** ([§6](#6-канал-и-routing-данные-в-заголовке-from), `ingress_route_resolve`).

Схемы `raw_native_id` **фиксированы per channel** и версионируются обязательным полем `v` (текущая версия — `1`): схема канала не может быть изменена несовместимо, не увеличив `v`, потому что ранее сгенерированные каноничные id живут вечно (в архиве и во внешних In-Reply-To чужих почтовых клиентов). Конкретные схемы — в [§2.2](#22-схемы-raw_native_id-по-каналам).

Сериализация — детерминистическая, через [`msgspec`](https://jcristharif.com/msgspec/): поля кодируются в порядке объявления `msgspec.Struct`, `ensure_ascii=False`, без пробелов. Типизированная декодировка `msgspec.json.decode(payload, type=Cls)` валидирует типы полей и отвергает мусор — отдельный runtime-чек не нужен.

Канонизация — **обратимое преобразование**: инверсия `base62.decodebytes` + `msgspec.json.decode` даёт побайтово исходный Struct.

**Границы, на которых происходит преобразование:**

- `threlium.bridges.telegram` / `threlium.bridges.matrix` — `Message-ID` = `RfcMessageIdWire.from_inner_for_bridge(inner).value`, `From:` = `<channel>@localhost`, `X-Threlium-Route` = `IngressRouteB62Wire.from_ingress_route(…).value`, `In-Reply-To` на wire предка при ответе (Matrix — из `m.relates_to`, см. §2.2.3); `References` на ingress **не** добавляются. Перед `run_fdm` — проверка union-notmuch по каноническому MID (`NotmuchMessageIdInner`, `threlium.nm.notmuch_index_has_message_id` / `…_in_db`): при уже проиндексированном письме с тем же `Message-ID` доставка повторно не выполняется (повторный батч `/sync` или `getUpdates`). Раннер `python -m threlium.runners.bridge <chan>` только `run_fdm`. Matrix к homeserver — **`matrix-nio`**.
- `threlium.bridges.email` (IMAP IDLE) — wire `Message-ID` / `In-Reply-To` / `References` (по токенам) через `EmailNativeId(v=1)` и `RfcReferencesWire.threlium_canonicalize_refs`, `From:` = `email@localhost`, маршрут с `reply_target_rfc_message_id` в JSON; дедуп по каноническому MID в notmuch (`NotmuchMessageIdInner`, `threlium.nm.notmuch_index_has_message_id`).
- `egress_<chan>` — для **email** SMTP `egress_email` декодирует каноничный `Message-ID` через `EmailNativeId`, восстанавливает внешний `Message-ID`, подставляет `In-Reply-To` из `reply_target_rfc_message_id`; базу `References` с route-предка в notmuch переводит `RfcReferencesWire.threlium_decanonicalize_refs`, затем добавляет хвост из `reply_target_rfc_message_id` (§M4); **SMTP `Subject`** — с того же route-предка (IRT), внутренний FSM не меняется. Для **Telegram/Matrix** параметры API и маршрут — из **`X-Threlium-Route`**; предки с `tag:route` и цепочка `In-Reply-To` — см. `ingress_route_resolve` / `egress_router`.
- Внутри FSM новые Message-ID порождает `RfcMessageIdWire.internal_for_fsm` — канон `<b62@localhost>` (`CanonicalMidWire.assert_from_wire` в `fsm_emit`).

**Между границами** — FSM-стадии и индекс видят канон `<b62@localhost>` для порождённых и принятых через мосты писем. У канонизации одна точка на входе моста / `internal_for_fsm` и декодирование на выходе в `egress_*`.

**Почему эта схема корректна:**

- `base62` (алфавит `[0-9A-Za-z]`) — строгое подмножество `atext` RFC 5322, поэтому каноничный `Message-ID` всегда RFC-valid, независимо от того, что содержал исходный JSON (двоеточия Matrix в `event_id`/`room_id`, слэши и `+` в GitLab-msgid, `$` Matrix-v3 — всё шифруется в алфавит `base62`).
- `msgspec` + `base62.decodebytes` — детерминистические чистые функции; цепочка ответов сохраняется: исходный внешний MID кодируется в канон и восстанавливается на SMTP egress (`egress_email`), в том числе через `reply_target_rfc_message_id` и декод `EmailNativeId`.
- Уникальность — инъективность `base62` + инъективность канонической JSON-сериализации `msgspec` (фиксированный порядок полей + обязательная `v`) + внешняя уникальность самого native-id в рамках своего канала.
- Нет state и нет mapping-таблиц: восстановление контекста опирается на каноничный `Message-ID` / `In-Reply-To` / архив, а не на отдельный служебный заголовок для «оригинального» id — всё выводится из самого id и тредовых связей.

### 2.2. Схемы `raw_native_id` по каналам

Bridge / ingress не конструирует искусственных доменов `@telegram.threlium` / `@matrix.threlium`. Для канонического `Message-ID` на входе **email**-моста — `RfcMessageIdWire.from_native(EmailNativeId(v=1, …))`; для **Telegram / Matrix** — `RfcMessageIdWire.from_inner_for_bridge` / `internal_for_fsm` → маркер **`@localhost`** в текущей эмиссии (`from_native` в коде тоже ставит `@localhost`). Полный маршрут и адрес ответа для API кодируются в **`X-Threlium-Route`** (b62 JSON `*IngressRoute`), а не в local-part `From:`. Каждая routing-схема помечена версией `v: int` — текущая для всех каналов `v=1`.

В колонке «Роль» буквы означают: **A** — identity (уникальная идентификация сообщения), **B** — routing ответа (поля, без которых API канала не примет reply), **C** — bridge-recovery (монотонный курсор для возобновления polling/sync после рестарта моста).

**Инвариант мостов:** **`From:` = `<channel>@localhost`** (только имя канала в local-part); **`X-Threlium-Route`** — b62(JSON) с полным `TelegramIngressRoute` / `MatrixIngressRoute` / `EmailIngressRoute`. Сверка канала при резолве маршрута: local-part `From:` (нижний регистр) и поле `channel` в распакованном JSON (`threlium.ingress_route_resolve`).

- **email**: `From:` = `email@localhost`; в JSON маршрута — `origin` (адрес отправителя для ответа) и при необходимости `reply_target_rfc_message_id` как `{"value":"…"}`.
- **telegram**: `From:` = `telegram@localhost`; курсор и поля API — в `TelegramIngressRoute` внутри `X-Threlium-Route`.
- **matrix**: `From:` = `matrix@localhost`; room/event/sync/reply — в `MatrixIngressRoute` внутри `X-Threlium-Route`; при наличии **`m.room.name`** в state комнаты в том же ответе `/sync` — заголовок **`Subject:`** (см. §2.2.3).
- `To:` — адрес FSM-стадии (`ingress@localhost`, …).
- `Subject:`, `Date:`, `In-Reply-To:`, тело, вложения — штатные MIME-поля.

Кодирование JSON маршрута в b62 для заголовка — `IngressRouteB62Wire.from_ingress_route` / `decode_b62_wire`; произвольные символы в полях безопасно уходят в payload UTF-8 под b62.

Соответственно, ни в одной из схем ниже нет `sender` / `from_user_id` / `to_display_name` и прочих производных от display-name в `Message-ID`.

#### 2.2.1. Email и Internal (FSM) — `EmailNativeId`

Один `msgspec.Struct` описывает полезную нагрузку для канонического `Message-ID`: внешний RFC MID входящего письма и внутренне порождённые FSM-письма. Эмиссия и распознавание канона в коде — только **`@localhost`**.

| Поле | Тип | Роль | Источник |
| --- | --- | --- | --- |
| `v` | `int` | — | константа `1` |
| `message_id` | `str` | A | для внешнего email — содержимое `Message-ID:` входящего письма без обрамляющих `<…>`; для internal — `email.utils.make_msgid(domain='localhost').strip('<>')`, вызывается изнутри `RfcMessageIdWire.internal_for_fsm` |

Пример для внешнего письма `Message-ID: <bug/42+comment@gitlab.example>`:

```json
{"v":1,"message_id":"bug/42+comment@gitlab.example"}
```

Результат канонизации входящего внешнего MID в этом репозитории — `<b62(…)@localhost>` (мост email).

Пример для FSM-порождённого письма:

```json
{"v":1,"message_id":"20260421113344.1234.5678@localhost"}
```

Результат: `<b62(…)@localhost>`. И внешний email, и internal FSM используют один маркер `@localhost`; различие только в содержимом поля `message_id` в JSON.

Routing ответа (B) для SMTP: получатель и внешний `In-Reply-To` берутся из полей `EmailIngressRoute` (`origin`, `reply_target_rfc_message_id`), см. `egress_email`. Recovery (C) в email — watermark IMAP UID: `imap_uid` / `imap_uidvalidity` доставленного письма хранятся в `EmailIngressRoute` (`X-Threlium-Route`); при рестарте мост берёт максимальный `imap_uid` из union-notmuch (`tag:route AND from:email@localhost`, непустой uid) и продолжает с `UID <uid+1>:*` (см. `IDENTITY_AND_CHECKPOINTS.md` § Email). Дополнительно мост после обработки делает `UID MOVE` письма из INBOX в `bridges.email.imap_processed_folder` (если задан): INBOX остаётся очередью необработанного, и редеплой с пустым notmuch не пересканирует старую почту. Internal-сообщения наружу не уходят — egress для них не вызывается вовсе, а тредовые связи в FSM строятся `In-Reply-To`-заголовками между каноничными id.

#### 2.2.2. Telegram — `TelegramIngressRoute` в `X-Threlium-Route`

**Message-ID** для Telegram-сообщений — `RfcMessageIdWire.from_inner_for_bridge(tg_inner)` (`@localhost`). **`From:`** — `telegram@localhost`. Поля чата, сообщения и `update_id` — в **`TelegramIngressRoute`** внутри **`X-Threlium-Route`** (`build_bridge_ingress_email` в `threlium.bridges`).

Routing-payload JSON (версионирован полем `v`):

| Поле | Тип | Роль | Источник в `update` |
| --- | --- | --- | --- |
| `v` | `int` | — | константа `1` |
| `chat_id` | `int` | **B** | `update["message"]["chat"]["id"]` |
| `message_id` | `int` | A + B | `update["message"]["message_id"]` (используется и как identity, и как `reply_to_message_id` в reply) |
| `message_thread_id` | `int \| None` | B | `update["message"].get("message_thread_id")`; для forum-топиков supergroups — int, иначе `null` |
| `update_id` | `int` | C | `update["update_id"]` — offset для `getUpdates(offset=max(update_id)+1)` после рестарта |

Пример для сообщения `msg_id=42` в supergroup `-1001234567890`, forum-топик `17`, `update_id=98765`:

```json
{"v":1,"chat_id":-1001234567890,"message_id":42,"message_thread_id":17,"update_id":98765}
```

`From:` в итоговом MIME: **`telegram@localhost`** (см. `build_bridge_ingress_email`).

Без `chat_id` Bot API вызов `sendMessage` физически невозможен: одного `message_id` для reply недостаточно. `update_id` — **не идентификатор сообщения**, а курсор long-polling'а (одно и то же сообщение может прилететь с разными `update_id` в `edited_message`/`channel_post`); хранится в routing payload ради bridge-recovery.

**Checkpoint recovery:** при рестарте моста `update_id` и прочие поля восстанавливаются из **`X-Threlium-Route`** последнего telegram-сообщения в union-notmuch (`tag:route`, `from:telegram@localhost`), а не из разбора local-part `From:`.

#### 2.2.3. Matrix — `MatrixIngressRoute` в `X-Threlium-Route`

**Message-ID** / **In-Reply-To** для Matrix-событий — `RfcMessageIdWire.from_inner_for_bridge` с inner `mx:v1:…`. **`From:`** у bridge-письма — **`matrix@localhost`**; полный маршрут — **`X-Threlium-Route`** с `MatrixIngressRoute` (см. `build_bridge_ingress_email`).

**Subject:** не из текста `m.room.message`, а с уровня **комнаты** — последнее непустое состояние **`m.room.name`** (`content.name`) в `rooms.join[room_id].state` того же ответа `/sync` (аналог темы почтового треда). На границе: `MatrixRoomNameWire` → `matrix_room_name_to_ingress_subject_wire` (`threlium.bridges`) → опциональный аргумент `subject` у `build_bridge_ingress_email` как `RfcSubjectWire` (нормализация переводов строк и длины согласована с канонизацией Subject email-моста). Разбор списка state — `matrix_room_name_wire_from_sync_state_events` в `threlium.bridges.matrix`. Сводка VO — [`TYPES.md`](TYPES.md).

Доступ к homeserver на ingress и egress — **только** библиотека **`matrix-nio`** (`nio.AsyncClient`: `sync`, `room_send`). Отдельного файла состояния sync (`matrix_state.json`) нет: курсор **`sync_batch`** восстанавливается из **`X-Threlium-Route`** последнего доставленного matrix-письма в union-notmuch.

Routing-payload JSON (версионирован полем `v`):

| Поле | Тип | Роль | Источник |
| --- | --- | --- | --- |
| `v` | `int` | — | константа `1` |
| `room_id` | `str` | **B** | ключ комнаты в ответе `/sync` (или эквивалент `matrix-nio`) |
| `event_id` | `str` | A + B | `event_id` текущего `m.room.message` |
| `sync_batch` | `str \| None` | C | `next_batch` того sync-ответа, в котором событие впервые увидено |
| `reply_to_event_id` | `str \| null` | **B** | при Matrix-reply — сырой `event_id` предка из `m.relates_to.m.in_reply_to`; для корня — `null` (в MIME нет `In-Reply-To`) |

Пример (reply):

```json
{"v":1,"room_id":"!abcDEF:server.tld","event_id":"$child:server.tld","sync_batch":"s72_4_0_2_1_1_1_1_1","reply_to_event_id":"$parent:server.tld"}
```

Пример (корень, без reply):

```json
{"v":1,"room_id":"!abcDEF:server.tld","event_id":"$xyz-v3-Abc:server.tld","sync_batch":"s72_4_0_2_1_1_1_1_1","reply_to_event_id":null}
```

`From:` в примерах — **`matrix@localhost`**; структурированные данные совпадают с полями **`X-Threlium-Route`** после b62-декода.

`sender` (`@alice:server.tld`) в routing payload **не** кладём — Matrix-сервер для reply-вызова знает отправителя сам по `room_id`+`event_id`.

**Checkpoint recovery:** при рестарте моста `sync_batch` восстанавливается из заголовка **`X-Threlium-Route`** последнего matrix-сообщения в union-notmuch (`from:matrix` → `IngressRouteB62Wire.parse_route_from_optional_header` / `IngressRouteB62Wire.decode_b62_wire` → поле `sync_batch` типа `MatrixSyncBatchCursor` в коде — opaque `NewType` от строки), а не из `Message-ID`.

---

Правая часть канонического id — **`@localhost`** (единственный маркер в грамматике кодека). Для Telegram/Matrix routing-данные и курсоры живут в **`X-Threlium-Route`**; `Message-ID` остаётся каноничным b62-inner без «упаковки» routing в `From:`.

### 2.3. Канонизация в `threlium.types` (`RfcMessageIdWire`, `IngressRouteB62Wire`, …)

Единственный источник истины по преобразованиям — Value Objects и фабрики в :mod:`threlium.types` (`RfcMessageIdWire`, `IngressRouteB62Wire`, …). Мосты, MDA-скрипты и FSM-стадии опираются на эти типы, а не на отдельный «сервисный» модуль. Оркестрационный уровень ([`ORCHESTRATION.md` §3](ORCHESTRATION.md#3-механизм-post-insert-hook--dispatch-script)) использует `RfcMessageIdWire.threlium_fs_id_left` для канонизации `Message-ID` и логирования, но **не** для ключа инстанса воркера: dispatch-скрипт `threlium-dispatch.sh` находит треды через `notmuch search --output=threads "tag:unread AND folder:<stage>/Maildir"` и запускает `threlium-work@<stage>:<thread_id>.service`, где `thread_id` — нативный notmuch thread id (hex); `threlium.runners.engine_submit` передаёт пару стадия×тред в JSON на сокет **`threlium.runners.engine`**, который ищет файл через notmuch query `tag:unread AND to:<stage>@localhost AND thread:<thread_id>` (`nm.first_message_path(..., sort_newest_first=False)`), затем полный разбор MIME в движке; **непустой** `Message-ID` (inner) обязателен до вызова handler'а — иначе `RuntimeError` ([`FSM.md` §4.2](FSM.md#42-что-делает-воркер-перед-вызовом-handler-а)). Для не-FSM lookup'ов по индексу используется :mod:`threlium.nm` / :meth:`notmuch2.Message.header`, не `BytesHeaderParser` по путям.

Схемы native-id — три `msgspec.Struct`-класса (`EmailNativeId` / `TelegramNativeId` / `MatrixNativeId`), перечисленные в [§2.2](#22-схемы-raw_native_id-по-каналам). `EmailNativeId` покрывает и внешний email, и внутренние FSM-письма; канонический MID на границе **email**-моста и для FSM-порождённых писем — `<b62(JSON(native))@localhost>` через `RfcMessageIdWire.from_native` / `internal_for_fsm`. Для **Telegram / Matrix**-мостов канонический MID — `<b62(utf8(inner))@localhost>` через `from_inner_for_bridge`. `msgspec.json.encode` даёт детерминистический JSON (поля — в порядке объявления, без пробелов, UTF-8), `msgspec.json.decode(payload, type=Cls)` типизированно валидирует входящий блоб: посторонние поля, несовпадающие типы или отсутствие `v` вызывают исключение ещё до того, как оно утечёт в FSM. Поле `v` кодирует версию схемы канала и при эволюции позволяет вводить новые struct'ы рядом со старыми.

Фактическая реализация — модуль [`threlium.types.rfc`](../ansible/roles/threlium/files/scripts/threlium/types/rfc.py) (`RfcMessageIdWire`, `RfcReferencesWire`, `CanonicalMidWire`): канон — ``<base62(msgspec.json.encode(native))@localhost>``; `parse_threlium_canonical_optional`, `native_from_canonical_str` и `threlium_fs_id_left` распознают **только** маркер ``localhost`` справа от ``@``. Цепочки ``References`` перекодируются через `RfcReferencesWire.threlium_canonicalize_refs` / `threlium_decanonicalize_refs`. Поля `*IngressRoute` и b62 wire заголовка маршрута — [`threlium.types.ingress`](../ansible/roles/threlium/files/scripts/threlium/types/ingress.py); сводка типов — [`TYPES.md`](TYPES.md).

Модуль намеренно не содержит функции формирования имени файла: стадии и архив живут **исключительно** со штатными Maildir-именами от `notmuch insert`, кастомного формата нет. Все преобразования над идентификаторами идут по заголовкам писем; файловая система про `base62` и про JSON-схемы ничего не знает.

**Согласование конспекта §2.3 с фактическим кодом:** `RfcMessageIdWire.parse_threlium_canonical_optional` возвращает `RfcMessageIdWire | None`; `canonicalize_refs` / `decanonicalize_refs` → `RfcReferencesWire.threlium_canonicalize_refs` / `threlium_decanonicalize_refs` принимают `str | RfcReferencesWire | None` и возвращают `RfcReferencesWire`. Сборка письма bridge→ingress — `threlium.bridges.build_bridge_ingress_email`, аргумент `in_reply_to` — union `BridgeInReplyTo` (`RfcMessageIdWire` / `RfcInReplyToWire` / `NotmuchMessageIdInner` / `None`); опциональный **`subject`** — `RfcSubjectWire | None` (у Matrix-моста из `m.room.name`, §2.2.3). Схемы `EmailNativeId` / `TelegramNativeId` / `MatrixNativeId` и `*IngressRoute` лежат в `threlium.types.identity`; функции канонизации и b62 — в :mod:`threlium.types` (:class:`~threlium.types.rfc.RfcMessageIdWire`, :class:`~threlium.types.ingress.IngressRouteB62Wire`).

**Зависимости:**

- [`pybase62`](https://pypi.org/project/pybase62/) — pure-Python кодировка `[0-9A-Za-z]`, без транзитивных зависимостей;
- [`msgspec`](https://jcristharif.com/msgspec/) — типизированные Struct'ы и детерминистическая JSON-сериализация; обеспечивает runtime-валидацию native-id на границах системы.

Оба кладутся в `requirements.txt` / соответствующий manifest проекта.

---

<a id="3-mailfilter-snippet"></a>

## 3. fdm.conf snippet (`~/.fdm.conf`)

Шаблон — [`fdm.conf.j2`](../ansible/roles/threlium/templates/config/fdm.conf.j2). Python вызывает **`fdm -m -a stdin fetch`**; после цепочки `match` срабатывает **одно** именованное действие — как правило **`pipe`** → `notmuch insert --folder=<stage>/Maildir … && …/threlium-dispatch.sh` (см. [`INDEX.md` §4](INDEX.md#4-mailfilter-terminating-insert)). Для `To: archive@localhost` — отдельное действие **`ins_stage_archive`**: `notmuch insert --folder=archive/Maildir … +lightrag_indexed` (узкий `match` выше общих правил стадий). Для «остатка» — составное действие `remove-header "to"` + `add-header "To"` + `pipe` с `+error`. Перед общим `ingress@` стоят **три узких** `match` bridge→ingress (`telegram` / `matrix` / `email` в заголовках + `ingress@`) с тегом **`+route`**; остальной трафик на `ingress@` — insert **без** `+route`.

```
# Схема (фактический синтаксис — fdm.conf.j2):
# account "stdin" disabled stdin
# match … action "ins_ingress_route_tg|…"  → pipe → notmuch insert … +route …
# match … action "ins_ingress_plain"      → pipe → notmuch insert … (без +route)
# match … action "ins_stage_archive"   → pipe → notmuch insert … archive/Maildir +lightrag_indexed …
# match … action "ins_stage_<id>"        → pipe для остальных стадий из threlium_fsm_mailbox_stages (без +lightrag_indexed)
# match … unmatched action "insert_ingress_bug"  → remove-header/add-header + pipe +error
```

Полное обоснование (атомарность insert, теги, BUG-ветка) — [`INDEX.md` §4](INDEX.md#4-mailfilter-terminating-insert).

Long-running мосты при сканировании истории по notmuch должны согласовывать запросы с этим контрактом (например `tag:route AND from:telegram@localhost`), иначе в выборку попадут лишние письма с тем же префиксом `From:` без bridge-тега `+route`. Резолв `X-Threlium-Route` для `egress_router` и `egress_*` — ``resolve_route_for_egress_fsm_from_email`` (якорь: ``Message-ID`` текущего письма), затем ``resolve_route_from_in_reply_to_ancestors(start_inner)``: только цепочка ``In-Reply-To``, на каждом предке ``tag:route`` + чтение wire, без union по ``References`` ([`INDEX.md` §10 п. 12](INDEX.md#10-architectural-decisions-log)).

Имя итогового файла — стандартное Maildir-имя от **`notmuch insert`** (`1729400123.M…P….host:2,`); кастомного формата нет. Идентификация внутри системы делается **по содержимому заголовка `Message-ID:`** (канон `<b62@localhost>` для потока через мосты и FSM). Dispatch-скрипт находит треды через `notmuch search --output=threads "tag:unread AND folder:<stage>/Maildir"` и запускает `threlium-work@<stage>:<thread_id>.service`; worker по `%i = stage:thread_id` ищет файл через notmuch query `tag:unread AND to:<stage>@localhost AND thread:<thread_id>` (`nm.first_message_path`, oldest-first FIFO). Заголовки оркестрация **не парсит** — файловый lookup заменён индексной операцией notmuch; `RfcMessageIdWire.threlium_fs_id_left` остаётся для канонизации `Message-ID` в логах и FSM-стадиях, но не для ключа инстанса воркера. Детали — [`ORCHESTRATION.md` §3](ORCHESTRATION.md#3-механизм-post-insert-hook--dispatch-script).

---

## 4. Уникальность и атомарность

- Уникальность имён файлов в stage Maildir'ах — штатный контракт Maildir (`{time}.M{usec}P{pid}.{host}`); собственных гарантий не требуется, собственного именования нет.
- Уникальность цепочек в пределах стадии — инъективность `base62` + внешняя уникальность исходного `raw_native_id` в рамках канала при каноническом маркере `@localhost`: два разных корня дают два разных `thread_id`-ключа и, как следствие, два разных имени инстанса `threlium-work@<stage>:<thread_id>.service`, а одинаковые имена `systemd` автоматически сериализует (подробнее — [`ORCHESTRATION.md` §3](ORCHESTRATION.md#3-механизм-post-insert-hook--dispatch-script)).
- Переход `new/ → cur/` выполняется через `msg.tags.to_maildir_flags()` (метод на `notmuch2.MutableTagSet`, не на `Message`) под `db.atomic()` (см. [`INDEX.md` §5.5.3](INDEX.md#553-notmuch-consistency-через-notmuch2mutabletagset)). Сам `rename(2)` (`new/<id>` → `cur/<id>:2,S`) выполняется libnotmuch'ем **в момент вызова** `to_maildir_flags()`; Xapian-batch (снятие тега + path-update) коммитится при выходе из `db.atomic()` (`AtomicContext` — commit-граница notmuch). Crash в окне между `rename(2)` и Xapian-commit'ом оставляет файл в `cur/<id>:2,S`, а индекс — в состоянии `tag:unread + path=new/<id>`; recovery — `settle_recovery_for_stage()` через `MutableTagSet.from_maildir_flags()` на startup воркера ([`INDEX.md` §9.1](INDEX.md#91-crash-matrix)).
- `notmuch` индексирует весь union (`stages/`-tree) по заголовкам, а не по именам файлов — выбранная именная конвенция его не касается.

---

<a id="5-stage-worker-и-lightrag-worker"></a>

## 5. Stage worker и RAG-loop (LightRAG)

Единого «архивного обработчика» нет. Монолит старой схемы `threlium-archive.{path,service,timer}` (sweep + ainsert + tag в одном процессе) разделён на **две подсистемы**:

- **Stage submit** (`threlium-work@<stage>:<thread_id>.service`, активируется `threlium-dispatch.sh` сразу после `notmuch insert` в fdm `pipe` или из sweep после успешного submit) — FSM-логика стадии и `nm_settle()` для **своего** Maildir внутри `threlium-engine`. `threlium-sweep@…` (backstop: **`OnSuccess=`** воркера после **`exit 0`**) вызывает тот же dispatch-скрипт.
- **RAG-loop** (тот же процесс `threlium-engine`) — глобальный writer `working_dir/`: после успешного settle вызывается `schedule_index_pending`, селектор settled pending — по всему union'у.

Полный контракт — в [`INDEX.md` §5](INDEX.md#5-stage-workers-durable-maildirs) и [`INDEX.md` §5b](INDEX.md#5b-lightrag-worker). Здесь — wire-уровневое описание.

<a id="51-stage-worker-тригер-lifecycle-контракт"></a>

### 5.1. Stage worker: тригер, lifecycle, контракт

Оркестрация: fdm `pipe` (`notmuch insert && threlium-dispatch.sh` (запрашивает `notmuch search --output=threads "tag:unread AND folder:<stage>/Maildir"`) → `threlium-work@<stage>:<thread_id>.service` (`python -m threlium.runners.engine_submit %i` → сокет → `threlium.runners.engine`); при **`exit 0`** submit systemd активирует `threlium-sweep@<stage>:<thread_id>.service` через **`OnSuccess=`** воркера — тот же dispatch-скрипт (race backstop); при **`exit 1`** sweep не стартует — [ORCHESTRATION.md §3](ORCHESTRATION.md#3-механизм-post-insert-hook--dispatch-script). Полный контракт (in-process handler, `nm_settle()` после успешного возврата, universal error handling на исключениях) — [`INDEX.md` §5/§5.5/§5.6](INDEX.md#5-stage-workers-durable-maildirs).

Wire-инварианты:

- Dispatch-скрипт находит треды через `notmuch search --output=threads "tag:unread AND folder:<stage>/Maildir"` — settled треды (без тега `unread`) его не интересуют.
- Per-thread мьютекс — имя инстанса `threlium-work@<stage>:<thread_id>.service`: пока инстанс активен, повторный `start` — no-op. Параллелизм между разными стадиями и тредами — естественный.
- **Никакого `notmuch new`**: `notmuch insert` из fdm-пайпа уже проиндексировал письмо при доставке. Worker делает только tag/path-операции через `notmuch2.MutableTagSet`.
- **Никакого экспорта `.md`-файлов в `input/`-директории**: GraphRAG упразднён ([`INDEX.md` §1](INDEX.md#1-motivation)). LightRAG читает тело прямо из stage Maildir'а ([`INDEX.md` §5b.3](INDEX.md#5b3-цикл-индексации)).
- **Защита от busy-loop**: handler-исключения завершают submit с **`exit 1`** при **`Restart=on-failure`** / **`RestartSec`** (`Type=exec` у `threlium-work@`); sweep не стартует (**`OnSuccess`** только после **`exit 0`**). Backstop-таймеры не нужны ([`INDEX.md` §10 решение 8](INDEX.md#10-architectural-decisions-log)).

### 5.2. RAG-loop: триггер, lifecycle, контракт

Wire-инварианты:

- Триггер — **после успешного `nm_settle`** в FSM движка (`schedule_index_pending` на выделенном asyncio-loop); отдельных systemd `PathChanged` на `cur/` для LightRAG нет.
- Селектор pending: `* AND NOT tag:unread AND NOT tag:lightrag_indexed` (cur-only выражается через `NOT tag:unread`, не через path-glob; см. [`INDEX.md` §11.2](INDEX.md#112-notmuch-query-syntax--glob-ограничения)).
- Не трогает Maildir-файлы — это монополия stage worker'ов на их собственные Maildir'ы ([`INDEX.md` инвариант I3](INDEX.md#2-invariants)).

Полный контракт (single writer-процесс на `working_dir/`, цикл `rag.ainsert` + tag commit, дедуп, без отдельного `threlium-lightrag.service`) — [`INDEX.md` §5b](INDEX.md#5b-lightrag-worker).

### 5.3. Тегирование как state-маркер индексации

Состояние «что уже проиндексировано LightRAG'ом» хранится **мутабельным тегом** на самом письме:

| Тег | Владелец (кто ставит) | Потребитель (кто читает) | Назначение |
| --- | --- | --- | --- |
| `+lightrag_indexed` | RAG-loop в `threlium-engine` **после** успешного `rag.ainsert(batch)`; для писем в **`stages/archive/Maildir`** — **fdm** `ins_stage_archive` **на** `notmuch insert` | следующий RAG-drain (селектор pending) / диагностика | Исключает повторный insert в LightRAG. Дедуп LightRAG делает повтор безопасным даже без этого тега, но тег убирает лишнюю работу. |

**Принадлежность к глобальной памяти** выражается заголовком `To: global_memory@localhost` и содержимым письма (в т.ч. `From:` на этой стадии); отдельного scope-маркера в wire-теле не требуется (см. [`INDEX.md` §7.6](INDEX.md#76-per-thread-scoping-soft-через-маркеры)). Никакого отдельного тега для глобальной памяти нет — `notmuch search to:global_memory@localhost` достаточно.

### 5.4. Idempotent recovery

Crash matrix — в [`INDEX.md` §9.1](INDEX.md#91-crash-matrix). Краткая сводка для wire-уровня:

- Crash до `db.atomic.__exit__` в `nm_settle()` → файл всё ещё `new/+unread`, recovery — sweep / startup `from_maildir_flags()`.
- Crash между fdm `notmuch insert` и `nm_settle()` → файл `new/+unread`, селектор индексации его не видит (`NOT tag:unread`), нет индексации полу-обработанного.
- Crash handler'а → JSON-ошибка на сокете, оригинал остаётся `+unread`, submit **`exit 1`** ([`INDEX.md` §5.6](INDEX.md#56-universal-error-handling-в-runnersworkerpy)); петли через error-mail нет.
- Крах движка между `ainsert` и `+lightrag_indexed` → после рестарта следующий `schedule_index_pending` повторит `ainsert` для тех же `ids` (LightRAG dedup гарантирует безопасность).

### 5.5. Разделение с FSM-стадией `enrich`

Stage submit делает settle. Индексация LightRAG — **в том же процессе** `threlium-engine` (RAG-loop, `schedule_index_pending` **не блокирует** FSM). FSM-стадия `enrich` — потребитель graph'а: seed-план задач формируется **до** `aquery`, и его подзадачи подмешиваются в графовый запрос; сам `aquery` идёт через `run_rag_coroutine` плюс notmuch-контекст треда `unified_messages` в Jinja (см. [`INDEX.md` §7](INDEX.md#7-enrich-notmuch-context--query--lightrag)).

Пока очередь `ainsert` не догнала свежие письма, граф может отставать; параллельно enrich включает те же письма в `<unified-mail-context>` MIME-часть. Контракт Content-ID частей — [`FSM.md` §5.2](FSM.md#52-контракт-тела-enrich--reasoning); сборка — [`INDEX.md` §7.3](INDEX.md#73-composing-the-enrichment-payload-granular-multipart).

---

## 5b. (Удалено) Бывшая стадия `errors`

Ранее существовал side-channel `errors@` и error-mail с `From: error@localhost`. **Текущая модель** — только journald + failed systemd units; см. [`INDEX.md` §5.6](INDEX.md#56-universal-error-handling-в-runnersworkerpy).

---

## 6. Канал и routing-данные в заголовке `From:`

Заголовок `X-Threlium-Channel` **удалён**. Канал задаётся **local-part `From:`** (`<channel>@localhost` у мостов); полный маршрут и курсоры — в **`X-Threlium-Route`** (b62-JSON). У штатных FSM-переходов — `<stage>@localhost` без маршрутного payload в `From:`.

**Формат `From:` по каналам:**

| Канал | Формат `From:` | Маршрут (`X-Threlium-Route`) |
| --- | --- | --- |
| email | `email@localhost` | `EmailIngressRoute`: `origin`, опционально `reply_target_rfc_message_id` как `{"value":"…"}` |
| telegram | `telegram@localhost` | `TelegramIngressRoute` (§2.2.2) |
| matrix | `matrix@localhost` | `MatrixIngressRoute` (§2.2.3) |
| FSM-стадии | `<stage>@localhost` | маршрут наследуется по цепочке предков (`tag:route`), без b62 в `From:` |

**Определение канала** (`egress_router`, `threlium.ingress_route_resolve.resolve_route_for_egress_fsm_from_email`): якорь RA или лист, затем подъём по ``In-Reply-To``; на каждом предке в notmuch — ``tag:route``, непустой ``X-Threlium-Route``, сравнение **local-part `From:`** (без учёта регистра) с полем ``channel`` в JSON маршрута. Исходящее письмо получает свежий wire через ``IngressRouteB62Wire.from_ingress_route``; ``In-Reply-To`` к внешнему пользователю ставится на ``ResolvedRoute.message_id_inner`` (якорь носителя маршрута).

Кодирование JSON маршрута в b62 для заголовка — ``IngressRouteB62Wire``; произвольные символы в полях безопасно уходят в UTF-8 payload под b62.

**Что вне `From:` (в заголовках / теле):**

- **Служебные FSM-поля** (`X-Threlium-*` на wire) — канон имён в [§8](#8-canonical-x-threlium-headers-glossary); пошаговая семантика стеков — [`SUBAGENT_TABLE.md`](SUBAGENT_TABLE.md).
- **Тема, тело, вложения** — штатные MIME-части письма.

Ничего из перечисленного выше в id не дублируется: одно поле — одно место хранения. Дубли (id + заголовок + тег `notmuch`) — прямой путь к рассинхронизации, а не к отказоустойчивости.

**Доступ к заголовкам пост-индексации:** все `X-Threlium-*`-заголовки перечислены в `[index]` `notmuch-config.j2` с префиксами вида `header.Threlium` (не с `a`…`z` в начале имени префикса — см. [notmuch-config(1)](https://notmuchmail.org/doc/latest/man1/notmuch-config.html)); `notmuch insert` кладёт их в header-cache. Читать через `threlium.nm.get_header` / `msg.header()`; произвольные regexp по значению wire — в Python, не в строке `db.messages()` (см. [notmuch-search-terms(7)](https://notmuchmail.org/doc/latest/man7/notmuch-search-terms.html), [notmuch-sexp-queries(7)](https://notmuchmail.org/doc/latest/man7/notmuch-sexp-queries.html)). Подробности — [`INDEX.md` §10 / §11.2](INDEX.md#10-architectural-decisions-and-trade-offs).

---

## 7. Новый канал: чеклист

Добавление нового канала (Slack / Discord / …) сводится к **msgspec-схемам маршрута/native-id**, модулю моста в `threlium.bridges` и правке **`fdm.conf.j2`** для его `egress`-стадии:

- В `threlium.types.identity` добавляется `<Chan>NativeId(msgspec.Struct, frozen=True)` с обязательным `v: int` и минимальным набором полей (identity + routing ответа + recovery, если у канала есть курсор). Схема утверждается один раз — дальше она часть протокола и меняется только через `v2` (см. [§2.2](#22-схемы-raw_native_id-по-каналам)). Добавление класса — **единственное** исключение из правила «схемы identity при добавлении канала не трогаем без причины».
- Реализация мостов — `threlium.bridges.*` (`build_bridge_ingress_email`, channel runners): для **email** IMAP — `Message-ID` / `In-Reply-To` / `References` через `EmailNativeId(v=1)` (`from_native` / `threlium_canonicalize_refs`); для **Telegram / Matrix** — `from_inner_for_bridge` / `internal_for_fsm`; маршрут через `IngressRouteB62Wire.from_ingress_route`, `From:` = `<channel>@localhost`, доставка в **`fdm`** с `To: ingress@localhost`; при необходимости — дополнительные штатные MIME-заголовки по смыслу канала (для Matrix см. **`Subject`** из state комнаты, §2.2.3).
- `threlium.states.egress_<chan>` читает API-поля из распакованного `IngressRoute` (в т.ч. `X-Threlium-Route` на предках с `tag:route`); для SMTP email см. декод канона и внешние заголовки в `egress_email`.
- В [`fdm.conf.j2`](../ansible/roles/threlium/templates/config/fdm.conf.j2) добавляется **одно** именованное действие `pipe` для `egress_<chan>@localhost`: `notmuch insert --folder=stages/egress_<chan>/Maildir … && threlium-dispatch.sh` (штатная схема, [§3](#3-mailfilter-snippet); никакого `cc "$ARCHIVE"` и никакого выделенного archive-Maildir'а — union-индекс делает stage Maildir'ом архивом сам собой).
- В systemd — добавление новой стадии `egress_<chan>` в Ansible-var `threlium_fsm_mailbox_stages` ([`INDEX.md` §6](INDEX.md#6-systemd-units), [`PLAYBOOK.md`](PLAYBOOK.md)); dispatch-скрипт `threlium-dispatch.sh` итерирует все стадии из `threlium_fsm_mailbox_stages`. Отдельного Ansible-loop'а для `threlium-lightrag@*.path` нет.

Ни одна строка FSM-стадий, оркестрации (`threlium.runners.engine` / `threlium.runners.lightrag`, dispatch-скрипт `threlium-dispatch.sh`) или фабрик `RfcMessageIdWire` / `IngressRouteB62Wire` при этом не обязана меняться централизованно — расширение идёт через новые struct-типы в `threlium.types.identity`, мост и одну строку в `threlium_fsm_mailbox_stages`.

---

## 8. Canonical X-Threlium headers (glossary)

В документации и в примерах конвертов Threlium **имена служебных заголовков** на wire задаются только в форме ниже (префикс `X-Threlium-…`). В таблицах и списках заголовков писать, например, `` `X-Threlium-Hop-Budget:` ``, а не сокращения вроде `Hop-Budget:` без префикса.

| Назначение | Каноническое имя |
| --- | --- |
| Данные маршрутизации канала отправителя | `X-Threlium-Route` |

**Удалённые заголовки (не использовать в новых текстах и примерах):** `X-Threlium-Channel`, `X-Threlium-Route`, `X-Threlium-Route` — маршрутизация определяется заголовком `X-Threlium-Route`.

**Устаревшие имена (не использовать в новых текстах и примерах):** `X-Threlium-FSM-Hops` и `X-Threlium-FSM-Hop-Budget` — в документации и на целевом wire везде **`X-Threlium-Hop-Budget`** (роль — лимит шагов / стек бюджетов по фреймам; детали шагов — [`SUBAGENT_TABLE.md`](SUBAGENT_TABLE.md)).

Заголовков, которых в системе **нет**, в доках не вводить: контекст LightRAG для `enrich → reasoning` живёт в **теле** `multipart/mixed` с гранулярными MIME-частями по `Content-ID` (`<user-message>`, `<graph-answer>`, `<unified-mail-context>` и др.; см. [`FSM.md` §5.2](FSM.md#52-контракт-тела-enrich--reasoning), [`INDEX.md` §7.3](INDEX.md#73-composing-the-enrichment-payload-granular-multipart)), отдельного поля «LightRAG-контекст» / «GraphRAG-контекст» в заголовках нет.
