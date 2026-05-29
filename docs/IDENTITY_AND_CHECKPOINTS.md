# NativeId, IngressRoute и Checkpoint Resume

Два struct-семейства описывают сообщение на входе в FSM. Они решают **разные** задачи и содержат **непересекающиеся** наборы данных.

---

## NativeId — идентичность сообщения

`NativeId` — минимальный набор полей, уникально идентифицирующий сообщение **на стороне канала-источника**. Из него строится каноничный `<b62@localhost>` wire-идентификатор через единый путь `RfcMessageIdWire.from_native(native)` → `msgspec.json.encode` → `base62.encodebytes`.

Канонический `<b62@localhost>` далее занимает **любую** роль в FSM-заголовках: `Message-ID`, `In-Reply-To`, `References`.

| Тип | Поля | Фабрика |
|-----|------|---------|
| `EmailNativeId` | `v`, `message_id` | прямой конструктор |
| `TelegramNativeId` | `v`, `chat_id`, `message_id`, `message_thread_id` | конструктор; `.from_route(TelegramIngressRoute)` |
| `MatrixNativeId` | `v`, `room_id`, `event_id` | конструктор; `.from_route(MatrixIngressRoute)` |

```
NativeId = EmailNativeId | TelegramNativeId | MatrixNativeId
```

### Инвариант

`NativeId` **не содержит** checkpoint-данных (`update_id`, `sync_batch`) и routing-данных (`channel`, `origin`). Два сообщения с одинаковым `(chat_id, message_id)` но разным `update_id` дают **одинаковый** wire MID — это необходимо для корректной дедупликации.

### Пример кодирования

```
TelegramNativeId(v=1, chat_id=42, message_id=7, message_thread_id=None)
    ↓ msgspec.json.encode
{"v":1,"chat_id":42,"message_id":7,"message_thread_id":null}
    ↓ base62.encodebytes
665Y7EH6X0piplSRRNGc067gnbhuqCY5ew0lLwJlGm9dzkoxh2viQzkMgRdGVyLuaPHzn40Vr8USN919Z
    ↓ wrap
<665Y7EH6X0pipl…919Z@localhost>
```

---

## IngressRoute — маршрут + checkpoint

`IngressRoute` — полный контекст входящего сообщения, сериализованный в заголовок `X-Threlium-Route` (b62 JSON). Содержит **три категории** данных:

| Категория | Поля | Назначение |
|-----------|------|------------|
| **Дискриминация** | `channel`, `v` | Выбор egress-канала |
| **Identity** | `chat_id`+`message_id`+`message_thread_id` (TG), `room_id`+`event_id` (MX), `origin`+`reply_target_rfc_message_id` (email) | Маршрутизация ответа |
| **Checkpoint** | `update_id` (TG), `sync_batch` (MX), `imap_uid`+`imap_uidvalidity` (email) | Восстановление позиции long-poll/sync/IMAP-watermark |

### EmailIngressRoute

```json
{
  "channel": "email",
  "origin": "user@example.com",
  "v": 1,
  "reply_target_rfc_message_id": {"value": "<original-mid@mail.com>"},
  "imap_uid": 1234,
  "imap_uidvalidity": 1779998802
}
```

`imap_uid` / `imap_uidvalidity` — checkpoint INBOX моста (опциональны: `int | None`). Их ставит только IMAP-мост на ingress; у legacy / e2e писем ключи отсутствуют. Пара по RFC 3501/9051: UID монотонен и валиден лишь в связке с `UIDVALIDITY` папки (см. ниже § Email).

### TelegramIngressRoute

```json
{
  "channel": "telegram",
  "v": 1,
  "chat_id": 123456789,
  "message_id": 42,
  "message_thread_id": null,
  "update_id": 900001
}
```

`update_id` — монотонный offset Telegram Bot API. Не входит в `NativeId` (не identity, а позиция в потоке).

### MatrixIngressRoute

```json
{
  "channel": "matrix",
  "v": 1,
  "room_id": "!abc:matrix.org",
  "event_id": "$xyz",
  "sync_batch": "s1234_5678",
  "reply_to_event_id": "$parent_event"
}
```

`sync_batch` — opaque `next_batch` токен `/sync` API. `reply_to_event_id` — parent event в thread. Оба не входят в `NativeId`.

---

## Сводка: IngressRoute vs NativeId

```
              IngressRoute (X-Threlium-Route wire)
              ┌──────────────────────────────────────┐
              │  channel, v         ← дискриминация  │
              │  origin / reply_*   ← routing        │
              │  update_id /        ← CHECKPOINT     │
              │    sync_batch                        │
              │  ┌──────────────────────────────┐    │
              │  │  NativeId (identity)         │    │
              │  │  chat_id, message_id, ...    │    │
              │  │  → <b62@localhost> wire MID   │    │
              │  └──────────────────────────────┘    │
              └──────────────────────────────────────┘
```

Фабрика `NativeId.from_route(r)` извлекает identity-подмножество из `IngressRoute`, отбрасывая checkpoint и routing.

---

## Checkpoint resume по каналам

Все три канала используют **notmuch как единый checkpoint store**. Отдельного файла или БД для хранения позиции нет — checkpoint сохраняется побочным эффектом обычного ingress flow: каждое доставленное сообщение создаёт MIME-письмо с `X-Threlium-Route` в notmuch-индексе.

### Telegram: `update_id` → `offset`

При старте процесса (`threlium-bridge@telegram.service`):

1. Запрос notmuch: `tag:route AND from:telegram@localhost`, сортировка newest first
2. Decode `X-Threlium-Route` самого нового письма → `TelegramIngressRoute.update_id`
3. `offset = update_id + 1` → передаётся в `bot.get_updates(offset=…)`
4. Если писем нет → `offset = 1` (все обновления с начала)

```
Реализация: bridges/telegram.py → _max_update_id()

    notmuch newest          decode route          Bot API
    tag:route AND     →     .update_id = N    →   get_updates(offset=N+1)
    from:telegram
```

Telegram Bot API гарантирует: `update_id` монотонно возрастает; `getUpdates(offset=N)` возвращает только обновления с `update_id >= N`. Таким образом после рестарта мост продолжает ровно с того обновления, на котором остановился.

**Если проект был остановлен:** обновления копятся на серверах Telegram (до 24 часов). При запуске `offset = max_update_id + 1` → все накопленные обновления придут в первом `getUpdates`.

### Matrix: `sync_batch` → `since`

При старте процесса (`threlium-bridge@matrix.service`):

1. Запрос notmuch: `tag:route AND from:matrix@localhost`, сортировка newest first
2. Decode `X-Threlium-Route` → ищем первый непустой `sync_batch`
3. `client.next_batch = sync_batch` → matrix-nio использует его как `since` в `/sync`
4. Если `sync_batch` не найден → `None` → initial sync (вся история)

```
Реализация: bridges/matrix.py → _sync_since_from_index()

    notmuch newest          decode route            CS API
    tag:route AND     →     .sync_batch = "s…"  →   /sync?since=s…
    from:matrix
```

Matrix CS API гарантирует: `next_batch` — opaque pagination token; `/sync(since=token)` возвращает только события после этой точки. Каждый ответ `/sync` содержит новый `next_batch`, который записывается в route следующего доставленного события.

**Если проект был остановлен:** события на homeserver сохраняются. При запуске `since=last_sync_batch` → инкрементальный sync с точки остановки. Если `sync_batch` нет (первый запуск или потеря индекса) → initial sync, дедупликация через notmuch.

### Email: `imap_uid` → IMAP UID watermark

Email-мост, как Telegram/Matrix, хранит checkpoint в `X-Threlium-Route`: `imap_uid` + `imap_uidvalidity` доставленного письма. Флаг `\Seen` больше не используется как позиция (Gmail помечает письма в прочитанном треде / self-mail как `\Seen` — `UNSEEN`-выборка их теряла).

При старте процесса (`threlium-bridge@email.service`):

1. Запрос notmuch: `tag:route AND from:email@localhost`, newest first → `EmailIngressRoute.imap_uid` / `imap_uidvalidity` первого письма с непустым uid (исходящие/legacy без uid пропускаются)
2. `STATUS INBOX (UIDVALIDITY)`: если `imap_uidvalidity` не совпадает → watermark сбрасывается в `0` (полный хвост + notmuch-дедуп)
3. `effective_start = max(checkpoint_uid, session_high_uid) + 1`; raw `UID SEARCH UID <effective_start>:*` (фильтр `>= effective_start` по возрастанию)
4. Для каждого UID: canonical wire MID → lookup в notmuch → дубль → finalize + skip; новое → canonicalize (`imap_uid`/`imap_uidvalidity` в Route) → deliver (fdm) → finalize
5. `idle.wait(timeout=1740s)` → при событии → снова `process_inbox_tail()` (переносит `session_high_uid`)

**Finalize обработанного UID** (`_imap_finalize_message`): если задан `bridges.email.imap_processed_folder` → `UID MOVE` письма из INBOX в эту папку/label (`imap_tools.move`: серверный `UID MOVE` при capability, иначе `COPY`+`DELETE`+`EXPUNGE`); иначе (пусто) — legacy флаг `\Seen`. Перенос снимает письмо с выборки `UID SEARCH` **независимо** от watermark, поэтому редеплой с пустым notmuch не пересканирует старую почту INBOX. Для Gmail папка — заранее созданный label (`imap_ensure_processed_folder=false`, `CREATE` по IMAP не поддержан); для серверов с `CREATE` (GreenMail/Dovecot) папка заводится при старте моста.

```
Реализация: bridges/email.py → process_inbox_tail(), run_bridge()

    notmuch checkpoint        IMAP UID search          notmuch dedup        fdm
    tag:route from:email  →  UID <wm+1>:*  →  UID  →  wire MID exists?  →  deliver + MOVE/\Seen (uid в Route)
    .imap_uid = wm                                     yes → skip + MOVE/\Seen
```

`session_high_uid` (в рамках сессии) двигается на **каждом** обработанном UID, включая `duplicate_skip`: `UID SEARCH` не фильтрует по `\Seen`, иначе дубли в хвосте крутились бы на каждом IDLE.

**Если проект был остановлен:** письма копятся на IMAP. При запуске watermark = последний `imap_uid` из notmuch → `UID <wm+1>:*` забирает весь backlog. Дедупликация по `Message-ID` через notmuch предотвращает повторную доставку; при включённом `imap_processed_folder` уже обработанная почта вообще не попадает в INBOX-выборку.

---

## Дедупликация

Все три моста используют **одинаковый** механизм дедупликации перед доставкой:

1. Построить `<b62@localhost>` wire MID из `NativeId` (или `EmailNativeId` для email)
2. `NotmuchMessageIdInner.from_present_wire(mid_wire)` → inner id
3. `nm.notmuch_index_has_message_id(mid)` — lookup в notmuch
4. Если найден → skip (уже в FSM)

Это гарантирует идемпотентность: даже при дублировании checkpoint (рестарт между deliver и commit offset) или при пересечении initial sync и incremental sync — одно и то же сообщение не войдёт в FSM дважды.
