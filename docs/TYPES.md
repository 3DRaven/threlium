# Типы и граница (`threlium.types`)

Реализация: пакет [`ansible/roles/threlium/files/scripts/threlium/types/`](ansible/roles/threlium/files/scripts/threlium/types/) — публичный API и `__all__` в [`__init__.py`](ansible/roles/threlium/files/scripts/threlium/types/__init__.py); приватные базы `_OptionalStrip*`, `_kv_dict`, алиас `NonEmptyStr` — в [`_core.py`](ansible/roles/threlium/files/scripts/threlium/types/_core.py) (не в `__all__`). Подмодули: `rfc`, `hop_cap`, `notmuch`, `notmuch_query` (:class:`~threlium.types.notmuch_query.NotmuchQueryConnective` / ``NotmuchQueryField`` / ``NotmuchQuery`` / ``NotmuchBridgeFromLocalhost`` — сборка search-строк), `notmuch_message_id` (`NotmuchMessageIdInner` — inner `messageid`; предикат ``id:`` — ``as_notmuch_term()`` с удвоением ``""`` в кавычках по правилам notmuch, не путать с `RfcMessageIdWire` заголовка), `notmuch_tag` (`NotmuchTag` — StrEnum имён служебных notmuch-тегов; терминальные теги drain LightRAG — `lightrag_indexed` (успех) и `lightrag_skipped` (осознанный пропуск / render-fail), взаимоисключающие в pending-search; dispatch в shell сужает по `folder:<stage>/Maildir`; воркер — по заголовку `To:` / `FsmStage.rfc822_mailbox`, не по `tag:<stage>`), `fsm_strings`, `fsm_stage` (`FsmStage` — замкнутый набор FSM-стадий, виртуальный ящик `<value>@localhost` через `.rfc822_mailbox`, чтение канонического входящего `To:` воркером через `.from_incoming_to`), :mod:`threlium.mail_header_names` (`MailHeaderName` — канонические имена полей RFC 5322 и `X-Threlium-*` на wire; реэкспорт в ``threlium.types``), `bridge_ingress_channel` (`BridgeIngressChannel`), `cli_intent_policy` (`CliIntentPolicy`), `bridges` (`BridgeEmailSubjectLine`, `MatrixOutboundPlainBodyWire`, `MatrixRoomNameWire`, `TelegramBridgeInboundCaptionOrText` и др.), `lightrag`, `lightrag_drain` (`LightragDrainSkipReason` — причина пометки `lightrag_skipped` drain'ом для логов: `render_failed` / `selector_drift`), `ingress` (в т.ч. `EmailStruct`), `enrich_pending`, `engine_socket` (`EngineWireRequest` / `EngineWireOk` / `EngineWireError` — JSON-линия UNIX-engine submit), `cli_mail`, `identity` (native-id, `IngressRoute`, Matrix-поля `MatrixSyncBatchCursor` / `MatrixRoomId` / `MatrixRoomEventId` / `MatrixRoomSendTxnId` как `NewType(str)`), `stack` (`parse_angle_id_stack`), `ingress_hitl` (union `HitlParentRouting`, `classify_hitl_parent_*`), `systemd_status` (`SystemdStatusBody` — однострочный текст для systemd `STATUS=`). Детали каждого подмодуля — в docstrings и `__all__`.

**Перед новым strip-/wire-VO:** проверить, что нет типа с тем же **доменным смыслом** (поиск по имени и по репо, не только совпадение реализации `_kv_dict`). Не вводить второй класс «потому что другой файл». Запрещены резиновые обобщения вроде одного `RfcHeader` на все заголовки. **`NonEmptyStr`** — только msgspec-примитив для полей `Struct`; на границе смысла (письмо, промпт, конкретный заголовок) — **именованный** VO (`Rfc*Wire`, `ReasoningAssistantMessageText`, …).

**Ingress HITL:** union и `classify_hitl_parent_*` — только [`types/ingress_hitl.py`](ansible/roles/threlium/files/scripts/threlium/types/ingress_hitl.py); стадии импортируют из `threlium.types.ingress_hitl`, отдельного shim в `states/` нет.

Ниже — принципы и иллюстрации в формате **антипаттерн / стандарт**.

---

### Уровень 1: Граница системы (The Boundary)
**Утверждение:** Мусор отсекается до валидации. Модель не должна сама себя чистить от пробелов — движок `msgspec` должен получать только нормализованные данные или отсутствие ключа.

❌ **Антипаттерн (Очистка внутри бизнес-логики или модели):**
```python
# Плохо: модель принимает "грязные" данные и надеется на хаки
class IngressHeaders(msgspec.Struct):
    in_reply_to: str = ""

    def __post_init__(self):
        # msgspec уже провалидировал длину, а мы ее меняем. 
        # Если было "   ", станет "", и инварианты рухнут.
        self.in_reply_to = self.in_reply_to.strip() 

# В роутере:
val = msg.get("In-Reply-To")
headers = msgspec.convert({"in_reply_to": val}, type=IngressHeaders)
```

✅ **Стандарт (Смыв пустоты на границе через Фабрику):**
```python
class EmailStruct:
    """Миксин: подкласс объявляет msgspec.Struct; from_message собирает плоский dict."""

    @classmethod
    def from_message(cls, msg: EmailMessage) -> Self:
        raw: dict[str, str] = {}
        for field in msgspec.structs.fields(cls):
            val = msg.get(field.encode_name)
            if val is not None:
                cleaned = str(val).strip()
                if cleaned:
                    raw[field.encode_name] = cleaned
        return msgspec.convert(raw, type=cls)
```

**Приватные базы (механика в `types._core`, не в `__all__`):** публичные VO наследуют только их; смысл несёт **имя класса**, полезная нагрузка — единое поле **`.value`**.

| База | После strip, пусто / отсутствие | Фабрика |
|------|----------------------------------|---------|
| `_OptionalStripEmpty` | `value == ""` | `parse(raw)` |
| `_OptionalStripEmpty` (present) | отсутствие / strip-пусто → нет объекта | `parse_present_optional(raw)` → `Self | None` |
| `_OptionalStripLowerEmpty` | `value == ""` (после strip+lower) | `parse(raw)` |
| `_OptionalStripLowerEmpty` (present) | как выше | `parse_present_optional(raw)` → `Self | None` |
| `_OptionalStripNone` | `value is None` | `parse(raw)` |
| `_RequiredNonEmpty` | `ValueError` (через `msgspec.ValidationError`) | `require(name=..., raw=...)` |

Нормализация до `msgspec.convert`: вспомогательные `_kv_dict` / `_kv_dict_lower` — в dict попадает только непустая строка после нормализации на границе; иначе ключа нет (или ловится обязательность).

**Переменные окружения и конфигурация:** централизованы в `ThreliumSettings(BaseSettings)` (`threlium/settings.py`); отдельных env-VO подклассов `_Env*Base` больше нет. Доступ к параметрам — через экземпляр `settings: ThreliumSettings`, загружаемый один раз при старте процесса вызовом `load_settings()`. В новом коде не вводить голые литералы ``THRELIUM_*`` для обращения к ``os.environ``.

**FSM-стадия и виртуальный ящик:** замкнутый набор — :class:`~threlium.types.fsm_stage.FsmStage` (``StrEnum``, ``value`` = local-part); полный адрес назначения — ``FsmStage.<member>.rfc822_mailbox``; разбор стадии из канонического ``To:`` входного письма (воркер) — :meth:`~threlium.types.fsm_stage.FsmStage.from_incoming_to`. Отдельного mailbox-VO и констант ``FSM_MAILBOX_*`` нет. Состав членов `FsmStage` зеркалирует список `threlium_fsm_mailbox_stages` в Ansible (`ansible/roles/threlium/vars/main.yml`) и реестр `STAGE_MAIN_MODULES` в коде — при добавлении стадии правят все три места.

**Стадии и `ThreliumSettings`:** демон **`threlium-engine`** (`python -m threlium.runners.engine`) вызывает `load_settings()` **один раз при старте** и передаёт снимок в `threlium.states.<stage>.main(..., settings=…)` in-process (keyword-only). Юнит **`threlium-work@`** (`Type=exec`) — только wire-клиент `python -m threlium.runners.engine_submit` (`EngineWireRequest` / `wire_io`); `load_settings()` в submit не вызывается. Перезагрузка конфигурации внутри одного процесса не поддерживается. В тестах — тот же `load_settings()` или узкий ручной `ThreliumSettings(...)`. Поля — уже загруженные и валидированные pydantic-модели или скаляры; `**.value` оставляют для границы с внешними API (HTTP-клиенты, subprocess, litellm, заголовки `EmailMessage`), а не для «раннего» снятия типа в середине доменной функции без необходимости.

**Заголовки RFC822 на границе:** отдельный тип на смысл (`RfcFromWire`, `RfcSubjectWire`, `RfcDateWire`, `RfcSenderWire`, …), без одного «универсального заголовка» на разные роли.

**Matrix bridge→ingress и `Subject`:** имя комнаты из state **`m.room.name`** (CS API) — :class:`~threlium.types.bridges.MatrixRoomNameWire`; строка заголовка **`Subject:`** в синтетическом MIME — :class:`~threlium.types.rfc.RfcSubjectWire` после :func:`~threlium.bridges.matrix_room_name_to_ingress_subject_wire` (см. [MESSAGES.md](MESSAGES.md), §2.2.3).

**Present-or-None (RFC / проекции):** у подклассов `_OptionalStripEmpty` / `_OptionalStripLowerEmpty` — :meth:`~threlium.types._core._OptionalStripEmpty.parse_present_optional` на сырой строке и :meth:`~threlium.types._core._OptionalStripEmpty.parse_present_from_email` / :meth:`~threlium.types._core._OptionalStripEmpty.parse_present_from_nm_message` для чтения именованного заголовка из `EmailMessage` / `notmuch2.Message` с тем же инвариантом present-or-None (реализация в [`_core.py`](ansible/roles/threlium/files/scripts/threlium/types/_core.py)). Инвариант: результат не ``None`` только если после strip есть непустая полезная нагрузка (``bool(wire.value)``). Там, где **намеренно** нужна wire-строка с допустимым ``value == ""`` (например префикс scope из пустого Subject), остаётся :meth:`~threlium.types._core._OptionalStripEmpty.parse` — не подменять present-семантикой.

**Hop-budget:** `HopBudgetLine` / `HopTailToken` — только `parse(...)` и объект с `.value`; методов, возвращающих голый `str` и стирающих тип, нет.

**`X-Threlium-Route` (b62):** заголовок или сырая строка из notmuch — единая граница `IngressRouteB62Wire.parse_route_from_optional_header(wire)` (внутри `IngressRouteB62Wire.parse` → при пустом wire `None`); непустой wire — далее `IngressRouteB62Wire.decode_b62_wire` / `to_ingress_route`. b62-кодек инкапсулирован внутри VO: `from_ingress_route` (encode) и `to_ingress_route` (decode) — публичных функций `str → b62` / `b62 → str` в `threlium.types` нет. Строгие мосты оставляют обёртку `_parse_*_routing`, вызывающую этот путь.

**`X-Threlium-Space-Hash` (sha256):** SHA256 hex (ровно 64 символа) от b62-wire пространства — :class:`ThreliumSpaceHashWire` (фабрика ``from_space_b62_wire``). Xapian ``MAX_PROB_TERM_LENGTH = 64``: оригинальный b62-wire (96+ символов) молча отбрасывался из индекса; хеш проходит лимит. Полный b62-wire по-прежнему доступен через ``X-Threlium-Route``. Для вычисления терма notmuch — ``ThreliumSpaceHashWire.as_notmuch_index_term()`` (или делегирование ``ThreliumSpaceB62Wire.as_notmuch_index_term()``). Модуль: [`types/threlium_space.py`](../ansible/roles/threlium/files/scripts/threlium/types/threlium_space.py).

**`X-Threlium-Irt-Hash` (sha256):** SHA256 hex от значения ``In-Reply-To`` — :class:`IrtHashWire` (фабрика ``from_irt_header_value``). Та же причина: base62-encoded MID (96+ символов local-part) отбрасывается Xapian. Запрос ``IrtHashWire.from_irt_header_value(irt).as_notmuch_index_term()`` — O(1) поиск по индексу. Модуль: [`types/irt_hash.py`](../ansible/roles/threlium/files/scripts/threlium/types/irt_hash.py).

**`X-Threlium-Thread-Id` (только ingest-строка):** не пишется в Maildir; задаётся только в синтетическом RFC822 для `ainsert` (оболочка — `email.message.EmailMessage` + `policy.default`, тело — шаблон `lightrag/ingest_body.j2`, см. [`docs/adr/0001-lightrag-ingest-chunking-enrich.md`](adr/0001-lightrag-ingest-chunking-enrich.md)).

**CLI intent payload:** `CliIntentPayload` — `argv`, опционально `cwd`, `privileged: bool` (default false). Политика `CliIntentPolicy`: `SANDBOX` | `PRIVILEGED`. Sandbox — `systemd-run --user --wait --pipe` с `ProtectSystem=strict`; privileged — `systemd-run --wait --pipe --uid=0` после HITL (если `cli.privileged_hitl_enabled`).

**FSM `emit`:** низкоуровневый `emit_transition_preserving_payload` записывает только карту `MailHeaderName` → VO; семантические обёртки (IRT из `Message-ID` входа, копия Cap, декремент hop на простом шаге) — в `fsm_emit_semantic.py` (например `emit_transition_simple_step_preserving_payload`, `managed_patch_simple_fsm_step`).

**Остаток env (litellm):** `OPENAI_API_KEY`, `OPENAI_API_BASE`, `LITELLM_API_KEY`, `LITELLM_API_BASE` загружаются pydantic-settings в `ThreliumSettings` и доступны через `settings.litellm`. **Маршрутизация вызовов LiteLLM:** конфигурация эндпоинтов — в `settings.litellm` (pydantic-модели `LlmEndpoint` / `EmbeddingEndpoint` / `RerankEndpoint` и замкнутый enum `LitellmRoutingSite`, включая `LIGHTRAG_RERANK`); отдельного JSON-файла `litellm_routing.json` нет. **`chat_template_kwargs`** — опциональный `dict[str, Any]` на `LlmEndpoint` (`llm_endpoints[].chat_template_kwargs`); при непустом значении передаётся в `litellm.acompletion` (для `openai` — в `extra_body`); полный перечень ключей vLLM по моделям — в `#` комментариях `ansible/roles/threlium/defaults/main.yml` и `ansible/host_vars/th-agent.yml` (секция `threlium_litellm.llm_endpoints`). **`length_recovery_max_attempts`** — на `LlmEndpoint` (`null` → `litellm.length_recovery_max_attempts`); стадия `reasoning` при `finish_reason=length` не парсит `tool_calls` и повторяет completion с recovery system-сообщением (число попыток = значение поля). **HTTP/SDK `timeout` для вызовов completion и embedding** — только поля `timeout` в записях каталога (Ansible шаблонизирует из `threlium_reasoning_timeout_sec` / `threlium_lightrag_embedding_timeout_sec` и т.д.), не отдельные `THRELIUM_*_TIMEOUT_SEC` в env. Число SDK-retries — поле в `settings.litellm` (дефолт в плейбуке — 3). Runner `lightrag` при старте процесса использует `settings.lightrag` аналогично — один снимок конфигурации на процесс, не hot reload между итерациями. **`lightrag.query_api`** (`aquery` | `aquery_data` | `aquery_llm`, дефолт `aquery_llm`, Ansible `threlium_lightrag.query_api`) — метод LightRAG для retrieval в `enrich.py`; блок `--- graph answer ---` — всегда весь envelope-dict в Jinja через `| tojson(indent=2)` при `ok`, без `json.dumps` в Python для этого блока. В `EmbeddingEndpoint` опционально поле `encoding_format` (`float` / `base64` / `bytes` / `bytes_only`); при отсутствии в YAML или `null` ключ в вызов SDK не передаётся; для vLLM OpenAI-compatible embeddings обычно задают `float` в `litellm.embedding_endpoints[]`. В `RerankEndpoint` опционально поле `top_n` (`int | None`); список `rerank_endpoints` в `LitellmSettings` — опциональный (пустой по умолчанию, rerank отключён).

**LiteLLM score ladder (multi-endpoint hosts):** `score` 0 → `targets.cli_hitl_resume` (HITL reply classifier, малые `max_tokens`); `score` 1 → `enrich_plan`, `lightrag_llm`, `summarize_context`, `response_observe`; `score` 2 → `reasoning`. E2E correlation: `LitellmCallSite.CLI_HITL_RESUME` = `cli_hitl_resume` (WireMock-стабы).

**Ingress distill tool bridge:** `tool_choice=required` + `ingress_distill_tool_spec.j2`; `ingress_distill_history_parts_from_tool_args` — отдельный Jinja на поле (`distill_history_user_reply_language`, `…_step_back_notes`, `…_open_gaps`, `…_user_query`); несколько `<history>` на письме, **последняя** = `user_query` (`last_history_text` → enrich `<user-message>`); остальные — только unified/LightRAG; CID `<sha256(body)@history>`; без новых MIME-заголовков.

**CLI HITL tool bridge:** `cli_resume` — `tool_choice=required` + Jinja `prompts/cli_resume/tools/confirm_cli_hitl_tool_spec.j2`; `CliHitlToolFunctionName`, `ConfirmCliHitlToolArgs` (`types/cli_hitl_tool_args.py`); сырой JSON args — :class:`~threlium.types.litellm_tool_call.LiteLlmToolCallArgumentsWire` (`from_tool_call` → `validate_tool_args_json` → msgspec); парсинг — `cli_hitl_tool_bridge.parse_confirm_cli_hitl_assistant` / `parse_confirm_cli_hitl_from_wire`; ответ LiteLLM — `litellm_tool_response.require_tool_calls_response`; до 2 повторов completion+parse при `CliHitlBridgeError` / `LiteLlmToolResponseError` / `jsonschema.ValidationError`, затем исключение (стадия падает); `enrich_fast` только при `confirmed=false`, пустом `<system>`-ответе или не найденном intent — не при инфраструктурной ошибке classify.

**LightRAG tool bridge:** фазы LLM — `tool_choice=required` + Jinja `prompts/lightrag/tools/*_tool_spec.j2`; внутри — `LightragToolFunctionName` (`StrEnum`), `ExtractKnowledgeGraphToolArgs` и др. (`types/lightrag_tool_args.py`), wire VO (`LightragExtractionDelimiterText`, …) в `types/lightrag_tool_wire.py`; единственный `str` для библиотеки — `.value` на выходе `build_llm_func`. Ответ LiteLLM: `litellm_tool_response.require_tool_calls_response` (ровно один `tool_call`, `finish_reason=tool_calls`); при bridge-ошибках — до 2 повторов completion в `build_llm_func`. Сырой JSON args: :class:`~threlium.types.litellm_tool_call.LiteLlmToolCallArgumentsWire`. `max_tokens` — только `LlmEndpoint` слота `lightrag_llm`.

**Ответы LiteLLM (не msgspec):** схема chat/embedding/rerank на стороне провайдера — готовые модели `litellm.types.utils` (`ModelResponse`, `Message`, `EmbeddingResponse`, …) и `litellm.types.rerank` (`RerankResponse`); узкое приведение в [`threlium/litellm_wire.py`](ansible/roles/threlium/files/scripts/threlium/litellm_wire.py). Эталон L0-поведения для паритета e2e-стабов — [`tests/e2e/reference_l0/threlium_e2e_l0.py`](../tests/e2e/reference_l0/threlium_e2e_l0.py) (не compose-сервис): декодирует каноничные `@localhost` Message-ID локально (без импорта `threlium.types`), в согласовании с `RfcMessageIdWire.native_from_canonical_str` **только для `EmailNativeId`**. `TelegramNativeId` и `MatrixNativeId` в sidecar не декодируются (разная JSON-схема payload). Это единственное легитимное место с прямым `base62.decodebytes` вне `threlium.types`.

**python-telegram-bot на границе моста:** входящие объекты PTB (например `telegram.Message`) типизируются классами самой библиотеки; отдельные VO в `threlium.types` под сырой PTB-объект не вводятся.

**Каталог данных:** путь `THRELIUM_HOME` — через `settings.home` (из `ThreliumSettings`), единый источник пути к домашней директории проекта.

**Rfc*Wire:** для **опционального** заголовка письма — `parse_present_optional(raw)` или `SomeWire.parse_present_from_email(msg, "Header-Name")` / `parse_present_from_nm_message` (не смешивать с «двойной пустотой» `Wire | None` и пустым `wire.value`). После `parse(...)` (осмысленная пустая строка) снятие к строке — **`.value`**; для границ с `str | None` при present-ветке — только `wire.value` при `wire is not None`, без `(...).value or None` как маски отсутствия заголовка.

---

### Уровень 2: Атомарные типы (Value Objects)
**Утверждение:** Семантика важнее механики. Универсальных строковых утилит под видом домена нет: каждая роль — свой класс на базе `_OptionalStripEmpty` / `_OptionalStripNone` / `_RequiredNonEmpty`.

❌ **Антипаттерн (Одержимость примитивами / Primitive Obsession):**
```python
# Плохо: один обобщённый «обязательный текст» на разные роли — mypy не отличит смыслы.
def process_message(message_id: UndifferentiatedRequiredText, thread_id: UndifferentiatedRequiredText):
    ...
```

✅ **Стандарт (Доменные Value Objects на приватной базе):**
```python
class RfcMessageIdWire(_OptionalStripEmpty):
    """Заголовок Message-ID (wire) после strip на границе."""

class RfcInReplyToWire(_OptionalStripEmpty):
    """Заголовок In-Reply-To (wire) после strip."""

# Граница: parse → VO; для Jinja2 / заголовков / parseaddr — `.value` (или `.value or None` для str|None).
```

**`systemd` `STATUS=` (sd_notify):** непустая однострочная полезная нагрузка для `systemctl status` — :class:`~threlium.types.systemd_status.SystemdStatusBody` (база ``_RequiredNonEmpty``); фиксированные и параметризованные фабрики в том же модуле, отправка в сокет только через :func:`threlium.systemd_notify.notify_status` (усечение длины — внутри пакета `threlium.systemd_notify`, не в FSM/мостах).

**Message-ID (wire): три номинальных слоя** — см. [`identity.py`](ansible/roles/threlium/files/scripts/threlium/types/identity.py) (`ExternalRfcMidWire`), [`rfc.py`](ansible/roles/threlium/files/scripts/threlium/types/rfc.py) (`RfcMessageIdWire`, `CanonicalMidWire`):

| Тип | Назначение |
|-----|------------|
| `RfcMessageIdWire` | VO заголовка `Message-ID` / `In-Reply-To` после strip; кодек native ↔ канон `<b62@localhost>` (`from_native`, `internal_for_fsm`, `native_from_canonical_str`). |
| `CanonicalMidWire` | Инвариант «уже каноничный Threlium MID внутри FSM»; получение через `CanonicalMidWire.assert_from_wire(w)` от `RfcMessageIdWire` (например после `internal_for_fsm()` в `fsm_emit`). |
| `ExternalRfcMidWire` | Внешний SMTP RFC `Message-ID` (уголковая форма в поле `.value`). В JSON маршрута email поле `reply_target_rfc_message_id` — объект `{"value": "<…>"}` или отсутствие ключа / `null` (`EmailIngressRoute`); нормализация dict → `normalize_ingress_route_dict`. На границе IMAP-моста входящий MID задаётся через `ExternalRfcMidWire.parse_optional(...)`. |

**`NativeId` — идентичность сообщения, нативная для канала:**

| Тип | Поля | Фабрика |
|-----|------|---------|
| `EmailNativeId` | `v`, `message_id` | прямой конструктор |
| `TelegramNativeId` | `v`, `chat_id`, `message_id`, `message_thread_id` | конструктор; `.from_route(TelegramIngressRoute)` |
| `MatrixNativeId` | `v`, `room_id`, `event_id` | конструктор; `.from_route(MatrixIngressRoute)` |

Union `NativeId = EmailNativeId | TelegramNativeId | MatrixNativeId`. Все каналы (email, telegram, matrix) строят канонический `<b62@localhost>` через **единый** путь `RfcMessageIdWire.from_native(native: NativeId)` → `msgspec.json.encode` → `base62.encodebytes`. Канонический `<b62@localhost>` далее может занять любую роль в FSM-заголовках: `Message-ID`, `In-Reply-To`, `References`.

`NativeId` содержит **только identity-поля** — минимальный набор, уникально идентифицирующий сообщение на стороне канала. Checkpoint-данные (`update_id` для Telegram, `sync_batch` / `reply_to_event_id` для Matrix, `imap_uid` / `imap_uidvalidity` для email) — только в `TelegramIngressRoute` / `MatrixIngressRoute` / `EmailIngressRoute`, не в `NativeId`. Фабрика `.from_route(r)` извлекает identity из маршрута, отбрасывая checkpoint. Пара `(imap_uidvalidity, imap_uid)` email-моста опциональна (`int | None`): её ставит только IMAP-мост на ingress, у legacy / e2e писем ключи отсутствуют.

**Контракт:** в `threlium.types` **нет** публичных функций `str → b62` / `b62 → str`. b62-кодек — только внутри VO-методов: `from_native` / `native_from_canonical_str` (для MID) и `from_ingress_route` / `to_ingress_route` (для route).

---

### Уровень 3: Комплексные модели и FSM (Composition)
**Утверждение:** Модель гарантирует консистентность через кросс-полевую валидацию и иммутабельность (`struct_replace`). Нарушение FSM — Fail-Fast.

**Проекции почты (сценарные `msgspec.Struct`):** поля заголовков — только именованные VO (`Rfc*Wire`, notmuch-VO и т.д.), не голый `str` / `NonEmptyStr` в роли конкретного RFC-заголовка. Сборка: у соответствующего VO — ``SomeWire.parse_present_from_email(msg, "Header-Name")`` (или ``parse_present_from_nm_message`` для notmuch); иначе поле проекции ``None``. Фабрики ``from_email`` / ``from_notmuch`` — единственное место касания ``EmailMessage`` / notmuch для этой границы. Стадии не вызывают ``parse`` заголовков вручную и не ловят ``msgspec.ValidationError``; инварианты FSM и ошибки разбора — ``RuntimeError`` (или обёртка ``ValidationError`` → ``RuntimeError``) внутри фабрики, валит стадию. **`EmailStruct`** остаётся для подтипов, где удобен `from_message` → `msgspec.convert` по плоским полям; отдельные проекции могут не наследовать миксин и реализовать только `from_email` / `from_bytes` (через `parse_rfc822`) без плоских `str` в полях смысла заголовка.

**Ветвление HITL-родителя:** `HitlParentRouting = NotHitl | HitlParentDetected`; стадия только `match` по результату :func:`~threlium.types.ingress_hitl.classify_hitl_parent_notmuch`, без проверок «есть ли HITL» по опциональным полям. Детекция: IRT-обход вверх от прямого родителя → `From: cli_hitl_out@localhost`.

❌ **Антипаттерн (Роутер парсит заголовки и ветвится по `str | None`):**
```python
irt = nm.header_from_message(msg, "In-Reply-To")
if irt:
    parent = nm.find_message(db, irt)
    if parent and nm.header_from_message(parent, "From") == "cli_hitl_out@localhost":
        ...
```

✅ **Стандарт (фабрика + union):**
```python
from threlium.types.ingress_hitl import (
    HitlParentWithoutIntent,
    HitlParentWithIntent,
    classify_hitl_parent_notmuch,
)

match classify_hitl_parent_notmuch(parent_msg):
    case HitlParentWithoutIntent():
        return _emit_to_enrich(msg, stage)
    case HitlParentWithIntent():
        return _emit_to_cli_resume(msg, stage)
```

---

### Уровень 4: Бизнес-логика (Роутеры)
**Утверждение:** Роутер только маршрутизирует. Никаких вызовов `.get()`, `.strip()`, `.split()` или проверок консистентности.

❌ **Антипаттерн (Роутер-разнорабочий):**
```python
def main(msg: EmailMessage, stage: FsmStage):
    # Роутер занимается парсингом
    irt = msg.get("In-Reply-To", "")
    if irt: 
        irt = irt.strip()
    
    if not irt:
        return enrich(msg)
        
    parent = get_parent(irt)
    intent = parent.get("X-Intent")
    if intent:
        hitl_from = parent.get("From", "")
        if hitl_from == "cli_hitl_out@localhost":
            return cli_resume(msg)
        return enrich(msg)
```

✅ **Стандарт (Кристально чистый Роутер):**
```python
from threlium.types import FsmStage
from threlium.fsm_emit_semantic import emit_transition_simple_step_preserving_payload

def main(msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings) -> EmailMessage:
    child = IngressRouterChildMsg.from_email(msg)
    irt_wire = child.in_reply_to
    if irt_wire is None:
        return _emit_to_enrich(msg, stage, settings=config)

    with nm.open_parent_message_for_in_reply_to(irt_wire) as parent_msg:
        if parent_msg is None:
            return _emit_to_enrich(msg, stage, orphan=True, settings=config)

        match classify_hitl_parent_notmuch(parent_msg):
            case HitlParentWithoutIntent():
                return _emit_to_enrich(msg, stage, settings=config)
            case HitlParentWithIntent():
                return _emit_to_cli_resume(msg, stage, settings=config)

def _emit_to_cli_resume(msg: EmailMessage, stage: FsmStage, *, settings: ThreliumSettings):
    return emit_transition_simple_step_preserving_payload(
        msg,
        to_addr=FsmStage.CLI_RESUME,
        from_stage=stage,
        settings=settings,
    )
```

В таком виде архитектура полностью реализует принцип: **«Неверное состояние должно быть невыразимым в коде» (Make illegal states unrepresentable).**

**Knowledge-стадии (`knowledge_stage.py`):** :class:`~threlium.types.knowledge_stage.FormalReasonStagePayload` — JSON body стадии `formal_reason` (поля `facts_ttl`, `shapes_ttl`, `reasoning`, опционально `ontology_ttl`, `inference` :class:`~threlium.types.knowledge_stage.LogicInferenceMode`, `return_derived`, `query`); разбор — `threlium.knowledge_fsm.parse_formal_reason_payload`. Исходящий machine payload — :class:`~threlium.types.knowledge_stage.FormalReasonResultPayload` (`outcome` :class:`~threlium.types.knowledge_stage.FormalReasonOutcome`, зеркало полей для gate); разбор — `threlium.knowledge_fsm.parse_formal_reason_result_payload`; gate — `threlium.formal_reason_gate.formal_reason_gate_active` ([`FORMAL_REASON_GATE.md`](FORMAL_REASON_GATE.md)). :class:`~threlium.types.knowledge_stage.MemoryQueryStagePayload` — `memory_query`. Выход `formal_reason` в observation тоже через VO (уровень 2), не сырые `str`: фатальная ветка — :class:`~threlium.types.knowledge_stage.FormalReasonErrorKind` (StrEnum `""`/`parse`/`shape`/`runtime`) + `FormalReasonFatalErrorText`; секции отчёта/вывода — `FormalReasonReportText`, `FormalReasonDerivedTtlText`, `FormalReasonQueryResultText`; supplemental-ошибки (не затирают валидацию) — `FormalReasonQueryErrorText` / `FormalReasonDerivedErrorText` через `parse_present_optional`. Стадия упаковывает результат в эти VO до `render_prompt`, в Jinja идёт только `.value` (финальная note — `EnrichObservationNoteText`).

**Reasoning (`reasoning_routes.py`, `reasoning.py`, `reasoning_tool_args.py`):** :data:`~threlium.types.reasoning_routes.REASONING_TARGET_STAGES` — frozenset целевых :class:`~threlium.types.fsm_stage.FsmStage` (лёгкий модуль без цикла импорта с :mod:`prompt_path`). Вход для ``reasoning/user.j2`` — :class:`~threlium.types.reasoning.ReasoningIncomingEnvelope` и :class:`~threlium.types.reasoning.ReasoningEnrichContext` (фабрики ``from_email`` на границе). Ответ LiteLLM: :class:`~threlium.types.reasoning.ReasoningToolFunctionName` (``.target_stage()``), :class:`~threlium.types.litellm_tool_call.LiteLlmToolCallArgumentsWire`, итог — :class:`~threlium.types.reasoning.ReasoningRouteDecision`` (``target`` + :class:`~threlium.types.fsm_strings.ReasoningToolRouteEmailSubject` / :class:`~threlium.types.fsm_strings.ReasoningToolRouteEmailBody`). Роутер ``states/reasoning.py`` только маршрутизирует; tool-spec / jsonschema — ``states/reasoning_tool_spec.py``.

**Task-ledger (`task_ledger.py`):** VO для anti-drift task-ledger (content-addressed CRDT, см. [RESPONSE_TABLE.md §8](RESPONSE_TABLE.md)). :class:`~threlium.types.task_ledger.TaskSubtaskText` — текст подзадачи (база `_RequiredNonEmpty`, метод `.normalized()` — strip + схлопывание пробелов). :class:`~threlium.types.task_ledger.TaskSubtaskContentId` — **identity** подзадачи; фабрика `from_text(text)` инкапсулирует нормализацию + усечённый sha256 hex (прецедент — `IrtHashWire.from_irt_header_value`; публичной `str → hash` в `types` нет). :class:`~threlium.types.task_ledger.SubtaskStatus` — StrEnum монотонной решётки (`pending`/`in_progress`/`done`/`cancelled`) с семантикой **методами на VO**: `.rank`, `.is_terminal`, `.merge(other)` (как `FsmStage` / `FormalReasonErrorKind`). :class:`~threlium.types.task_ledger.TaskSubtaskState` / :class:`~threlium.types.task_ledger.TaskLedger` — reduced-состояние (`msgspec.Struct, frozen`, фабрика `TaskLedger.from_states` с инвариантом уникальных `content_id`). На `TaskLedger` помимо `subtasks` — last-wins meta из последнего `TasksUpsertOp`: `discovery_note` / `next_action` / `blockers` (опц. текст-VO) и `allow_finalize_with_blocker: bool` (пара `blockers` + флаг — единственный bypass fail-closed gate, см. `threlium.task.gate`). Опц. текст-VO `TaskDiscoveryNoteText` / `TaskNextActionText` / `TaskBlockerText` (база `_OptionalStripEmpty`, `parse_present_optional`). Аргументы tool — `TasksUpsertToolArgs` / `NewSubtaskArg` / `SubtaskStatusUpdateArg` в `reasoning_tool_args.py` (примитивы на границе jsonschema → VO в `TasksUpsertOp.from_tool_args`). CRDT-операции и reduce/collect/gate — пакет `threlium.task` (зеркало `threlium.response`).