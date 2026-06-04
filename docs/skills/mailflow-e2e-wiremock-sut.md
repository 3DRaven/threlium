# Mailflow e2e / WireMock / SUT — отладка и правки

Skill для агента: как расследовать падения mailflow и соседних e2e на общем compose (`sut`, `greenmail`, `wiremock`), чинить **тесты и стабы**, не ломая политику проекта. Нормативная база по harness и изоляции — [`docs/TESTING.md`](../TESTING.md), [`docs/E2E_ISOLATION.md`](../E2E_ISOLATION.md). Ручная отладка **без полного pytest** — [`e2e-compose-debug-without-full-run.md`](e2e-compose-debug-without-full-run.md).

---

## 1. Границы ответственности

| Зона | Политика |
|------|----------|
| **Продуктовый код** (`ansible/roles/threlium/files/scripts/threlium/`, плейбук, FSM) | Менять **только после согласования** с пользователем. При подозрении на баг в SUT — описать причину и варианты, не коммитить молча. |
| **Тесты и инфраструктура e2e** (`tests/e2e/**`, в т.ч. `wiremock_stubs/`, хелперы, conftest) | Править **свободно** в рамках этого skill. |
| **Контейнеры** (SUT, GreenMail, WireMock) | Полный доступ для диагностики и **временных** правок (патч скриптов, Admin API WireMock). |
| **Быстрая итерация** | Допустимо править файлы **внутри контейнера** и не трогать git до проверки гипотезы; финальный фикс — в репозитории под `tests/`. |

**Не отвлекать пользователя** промежуточными вопросами «продолжать ли», пока не исчерпаны шаги расследования и нет жёсткого блокера (нужно согласование на SUT, нет Docker, и т.п.).

---

## 2. Жёсткие ограничения

Эти правила **не обсуждаются** при отладке; нарушение — тупик.

### 2.1. Таймауты

- Единый poll-таймаут сценариев: **`TIMEOUT_POLL_SHORT`** в [`tests/e2e/toolkit/constants.py`](../../tests/e2e/toolkit/constants.py) (по умолчанию **30 с**).
- **Запрещено** поднимать таймауты как способ «дождаться медленного пайплайна». Если assert «не дождался» — это **не** «моки долго отвечают» (моки отвечают сразу); искать: логику теста, пропущенные стабы, `unmatched` в WireMock, застрявший FSM, загрязнение среды, неверный `X-Threlium-Route` / state.
- **Замедление — не «что-то работает медленно».** Длинный wall-clock почти всегда следствие **глубже**: ретраи (HTTP, FSM, воркеры), ошибки или пропуски в стабах, циклы/лишние проходы FSM, повторные LLM-вызовы из-за неверного ответа мока, загрязнение state между тестами. Вместо предложения «добавить таймаут» или `sleep` — **расследовать процесс**, который отлаживается, и **связанные** (соседние стадии FSM, bootstrap WireMock, GreenMail/IMAP): journald (`threlium-work@*`, `matched-stub` / `unmatched`), `GET /__admin/requests/unmatched`, notmuch/Maildir, user units SUT, таблица сопоставления (§4.5). Только после объяснения *почему* wall-clock вырос — править стабы, сценарий или ожидания в тесте.
- Отдельные константы (`TIMEOUT_POLL_LIVE_MAIL`, env-переопределения) **не расширять** ради прохождения FAIL; долгий wall-clock — сигнал упростить сценарий, поправить стабы/ожидания или устранить лишние проходы FSM, а не увеличивать ожидание.
- Исключения вроде drain при `sessionfinish` (`THRELIUM_E2E_SESSIONFINISH_*`) к poll-таймауту теста **не относятся**.

### 2.2. Запуск и вывод pytest

Каталог **`test-runs/`** в [`.gitignore`](../../.gitignore) (в git не коммитится), но **обязан быть на диске** у разработчика/агента — там живут runner'ы и артефакты прогонов.

**Нормативный способ** прогонять e2e (пакет или resume после FAIL):

```bash
./test-runs/run_individual_e2e.sh
# resume того же прогона:
THRELIUM_E2E_RESUME_DIR=test-runs/20260602_041825 ./test-runs/run_individual_e2e.sh
```

Скрипт ([`test-runs/run_individual_e2e.sh`](../../test-runs/run_individual_e2e.sh), см. [README.md](../../README.md)):

- серийно гоняет `-n0 -s -vv --tb=short` по `test_list.txt`;
- в **терминал** — **краткие структурированные** строки (`RUN` / `PASS` / `FAIL`, длительность, счётчики, путь к логу);
- **полный** stdout pytest — в `test-runs/<run_id>/logs/<nodeid>.log` и в `summary.json` (итог, `log_file` на каждый тест);
- актуальный прогон: `cat test-runs/.latest`;
- `flock` на `.runner.lock` — не параллелить с другим pytest e2e на том же compose.

**Один тест** при отладке (шаг 2 workflow): тот же контракт — полный лог только под `test-runs/`, в чат не копировать megabyte `-s`:

```bash
pytest -n0 -s -vv --tb=short 'tests/e2e/test_foo_e2e.py::test_bar' \
  2>&1 | tee test-runs/.foo_bar_solo.log
```

Разбор FAIL — из файла (`rg -n 'FAILURES|AssertionError' test-runs/...`), не обрезая его `tail` вместо чтения.

**Запрещено:** `tee … | tail`, перенаправление **только** в файл без живого прогресса, выкидывание хвоста traceback «чтобы короче», сырой `pytest tests/e2e` на десятки тестов без runner'а (засоряет терминал и ломает изоляцию cold reset).

### 2.3. Тяжёлая синхронизация SUT

- **`wipe_bake`**, **`wipe_sync`**, полный bake→sync-цикл — только для **массовых** изменений образа/плейбука, не для каждой итерации фикса стаба.
- Для быстрого цикла: **`docker cp`** нужных файлов в SUT, **рестарт** затронутых `systemd --user` unit'ов / перезапуск контейнера **WireMock** + bootstrap стабов (§4.3).

### 2.4. Unit-тесты

- В проекте **нет** unit-слоя; не добавлять `tests/unit/`.

---

## 3. Модель изоляции (стабы и корреляторы)

Следовать [`docs/E2E_ISOLATION.md`](../E2E_ISOLATION.md) и §4.4 [`docs/TESTING.md`](../TESTING.md).

### 3.1. Между тестами

- Разделение сценариев на общем WireMock: **`X-Threlium-Route`** (b62-wire, тот же расчёт, что у bridge — [`e2e_smtp_inject_ingress_route_wire_for_message_id`](../../tests/e2e/helpers.py)) + **WireMock State Extension** (`hasContext`, сид в setup, teardown только своего контекста).
- Матчер сравнивает **конкретное** значение Route, а не факт «заголовок есть».
- **`stub_tag`** в pytest-модуле согласован с `metadata.threlium_e2e_stub_tag` в JSON стабов каталога `tests/e2e/wiremock_stubs/<имя_теста>/`.

### 3.2. Внутри одного треда

- Уникальная последовательность HTTP к LiteLLM: **`req seq`** (или эквивалент в state) + **call site** + **URL** → однозначный номер вызова на комбинацию.
- **Route** даёт сквозную уникальность одного почтового/FSM-треда от инъекции до ответа.

### 3.3. Запрещённые якоря

- **Не** использовать как **основной** способ изоляции: **имя WireMock scenario**, матч по **subject/body** письма, `doesNotContain` по телу чужих тестов, `priority` между стабами.
- Subject/body допустимы только как **вторичные** маркеры в assert GreenMail (уровень 2 в TESTING.md), не как замена Route/state.

### 3.4. Маппинги

- Тела маппингов живут в **git** (`*.json`); из pytest **не** собирать и не патчить mapping на лету.
- Разрешено в рантайме: seed/delete **state**, upsert готовых JSON с диска, compose bootstrap.

---

## 4. Инструменты диагностики

При любом FAIL собирать доказательства из **всех** слоёв, а не только из assert pytest.

### 4.1. Journald в SUT

```bash
docker exec <sut> journalctl -b --no-pager | grep -E 'threlium-work|threlium-engine|matched-stub|unmatched'
```

Искать: цепочку `threlium-work@<stage>:<thread_id>`, failed units, HTTP к WireMock, предупреждения LightRAG, момент `egress_email` относительно таймаута poll.

User units: `XDG_RUNTIME_DIR=/run/user/$(id -u threlium) systemctl --user list-units 'threlium-work@*' --all`.

### 4.2. Notmuch и Maildir

В SUT под пользователем `threlium` (`HOME`, `NOTMUCH_CONFIG` — см. [`e2e-compose-debug-without-full-run.md`](e2e-compose-debug-without-full-run.md)):

- `notmuch search` / `count` по якорю `id:` из теста;
- `tag:unread` — кто не дошёл до settle;
- `notmuch search --output=files` — путь в `stages/<stage>/Maildir/new|cur`.

Застрявшее в `new/` → воркер не завершил стадию.

### 4.3. WireMock

- Журнал: `GET /__admin/requests`, фильтр по `stub_tag` / заголовкам Route.
- **Обязательно:** `GET /__admin/requests/unmatched` — любой unmatched при прогоне = баг стабов или state, чинить **до** добавления новых assert в тест.
- После правок стабов на уже поднятом стеке: **перезапуск контейнера wiremock** (или координированный cold reset из conftest) → **bootstrap** `compose_bootstrap/` → upsert стабов сценария.

### 4.4. GreenMail

- `docker logs <greenmail>` при SMTP/IMAP сбоях;
- IMAP poll в тесте — по якорям `Message-ID` / `In-Reply-To` ([`docs/TESTING.md`](../TESTING.md) §2, уровни 1–2).

### 4.5. Таблица сопоставления (шаг 1 workflow)

Перед повторным прогоном заполнить и **вывести в чат** таблицу по коду теста + фактам прогона:

| Стадия FSM (ожидаемая → факт) | Maildir / notmuch | WireMock (стаб / matched / seq) | Route wire (ожидание → факт) |
|------------------------------|-------------------|----------------------------------|------------------------------|
| … | … | … | … |

Колонки согласовать с журналом `threlium-work@*`, путями в Maildir и записями journal WireMock с `matched-stub-id`.

---

## 5. Быстрый цикл итерации

1. Узнать compose-проект: `docker ps` → префикс `threlium_e2e_shared_*`.
2. Обновить SUT без bake: `docker cp` из репозитория в `/home/threlium/threlium/agent/scripts/...` (или обратно — вытащить патч для коммита в `tests/`).
3. Очистить WireMock state: рестарт сервиса `wiremock` в compose → bootstrap.
4. Прогнать **один** тест (§2.2) или очередь через `./test-runs/run_individual_e2e.sh`.
5. Не злоупотреблять полным wipe Maildir / global reset state store между тестами при `-n>1` (см. TESTING.md).

Синхронизация кода SUT с репозиторием — точечно (`docker cp`, `FSTS_SYNC.md`), не цепочкой wipe bake → wipe sync для каждой правки стаба.

---

## 6. Запуск тестов

| Задача | Команда |
|--------|---------|
| Вся матрица / resume | `./test-runs/run_individual_e2e.sh` (§2.2) |
| Один упавший (отладка) | `pytest -n0 -s -vv --tb=short '…::…' 2>&1 \| tee test-runs/.<имя>_solo.log` |
| Итоги последнего пакета | `test-runs/.latest` → `summary.json`, логи в `logs/` |

- Общий compose уже поднят session-fixture; повторный `wipe_bake` не нужен для правки стаба.
- С runner'ом не запускать второй pytest e2e на том же стеке (см. заголовок `run_individual_e2e.sh`).
- Параллель (`pytest -n>1` на весь каталог) — только когда серийные одиночные прогоны стабильны.

---

## 7. Проверки после прогона

Перед тем как **добавлять** новые ожидания в тест:

1. **LiteLLM / embeddings / chat:** счётчики вызовов по журналу WireMock совпадают с ожидаемыми для сценария (`req seq`, URL, call site).
2. **`unmatched` пуст** (глобальный guard в [`conftest.py`](../../tests/e2e/conftest.py)).
3. Notmuch: тред **settled** (нет лишнего `unread`), если тест это требует.
4. GreenMail уровень 1: якорь inject **gone** из INBOX fetchmail-учётки.

Расхождение счётчиков при пустом unmatched → ошибка в **логике assert или стабах**, не «добавить sleep».

---

## 8. Порядок работы при падении

Соблюдать **строго по шагам**.

| Шаг | Действие |
|-----|----------|
| **0** | Выбрать **один** упавший тест из последнего прогона. |
| **1** | По уже имеющимся данным (лог pytest, journald, WM, Maildir) построить **таблицу сопоставления** (§4.5) и вывести в чат. |
| **2** | Прогнать **только этот** тест (§2.2: `tee` → `test-runs/.…_solo.log`, краткий итог в терминал). Если снова FAIL — сверить факты с таблицей; в чат: **что расходится** (строка таблицы + факт), traceback — по ссылке на лог, не простынёй. |
| **3** | Дополнительно копать journald / WM / Maildir / код стабов; в чат: **причина** и **предложение фикса** (тесты/стабы; SUT — только с согласованием). **Не коммитить** SUT без согласования. |
| **4** | Если **одиночный** прогон **PASS** — взять **следующий** FAIL; вероятно **загрязнение среды** (вернуться к «флапающим» после зелёных одиночных). |
| **5** | Таймауты не трогать (§2.1). |
| **6** | Продуктовый код — §1. |
| **7** | Новые assert / счётчики — только после §7. |
| **8** | Фикс в git: стабы/тесты; в контейнер — `docker cp` + рестарт; без wipe bake/sync. |
| **9** | Зелёный одиночный → следующий тест из списка FAIL. |

### 8.1. Типичные причины (не путать)

| Симптом | Частая причина | Куда смотреть |
|---------|----------------|---------------|
| Timeout 30 s, в journal ещё нет `egress_email` | Нет стаба / wrong Route / FSM зациклился | WM unmatched + journal work units |
| Timeout 30 s, `egress_email` был, IMAP нет | Уровень 2 GreenMail, неверный `raw_id` | IMAP pytest@, `In-Reply-To` |
| Одиночный PASS, пакет FAIL | State/mappings/journal от соседа | Не делать global reset из теста; уникальный Route |
| Unmatched POST `/embeddings` | Пропущен bootstrap или стаб фазы | `compose_bootstrap/`, seed state |
| «Долгий» shallow subagent chain | Штатный wall-clock нескольких цепочек FSM | Упростить live-сценарий или стабы, **не** TIMEOUT |

---

## 9. Антипаттерны

- Поднять `THRELIUM_E2E_POLL_*` / `TIMEOUT_POLL_LIVE_MAIL` «чтобы прошло».
- Починить unmatched добавлением catch-all стаба без Route/state.
- Матчить LLM по фрагменту subject письма.
- Сырой `pytest tests/e2e` на весь каталог вместо `test-runs/run_individual_e2e.sh`.
- `wipe_bake` / `wipe_sync` на каждую правку JSON-стаба.
- Менять `threlium/states/*.py` без согласования, когда достаточно стаба или ожидания в тесте.
- `tee | tail` / лог только в файл без `test-runs/`; не читать journald при первом же timeout.

---

## 10. Чеклист перед закрытием задачи

- [ ] Одиночный прогон целевого теста зелёный.
- [ ] Unmatched пуст, счётчики LiteLLM сходятся.
- [ ] Таблица сопоставления для FAIL заполнена и объяснена в чате.
- [ ] Изменения в git — под `tests/` (и при необходимости документация); SUT — только с явным ОК пользователя.
- [ ] При подозрении на флап — повтор одиночного + отметка «нужен прогон пакета после всех одиночных».
