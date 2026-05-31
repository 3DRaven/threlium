# Ansible: развёртывание Threlium

Нормативная архитектура: [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) (в т.ч. §2.6.1 — таблица ingress/egress и ограничения `egress_telegram`).

## Локальная машина разработчика

**Ничего из этого каталога на своём ПК устанавливать не обязательно.** Просмотр [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) и правки playbooks не требуют Ansible, пакетов почты, venv с `lightrag-hku` и т.д.

Установка компонентов ниже имеет смысл только там, где вы **реально запускаете** развёртывание: управляющий хост с `ansible-playbook`, либо целевой сервер/ВМ под Threlium, либо CI.

## Требования (для среды, где выполняется деплой)

- Ansible **2.14+** (модуль `ansible.builtin.systemd` с `scope: user`).
- Управляемый хост: **Debian/Ubuntu** с пакетами из задачи «Install Threlium OS packages» в [playbooks/site.yml](playbooks/site.yml), включая метапакеты **python3** и **python3-venv** (дефолтный `/usr/bin/python3` должен быть ≥ 3.11 по `scripts/pyproject.toml`; см. [docs/INDEX.md §11.1](../docs/INDEX.md#111-зависимости-target)).

## Инвентарь и переменные

- Инвентарь: [inventory/hosts.yml](inventory/hosts.yml).
- Общие переменные: [group_vars/all.yml](group_vars/all.yml) и пример [group_vars/all.yml.example](group_vars/all.yml.example).
- E2E-инвентарь: [inventory/e2e/hosts.yml](inventory/e2e/hosts.yml) (для запуска e2e-тестов с `ansible_connection=docker`). Переменные e2e подхватываются автоматически: [inventory/e2e/group_vars/threlium_hosts.yml](inventory/e2e/group_vars/threlium_hosts.yml) — symlink на [group_vars/e2e.yml](group_vars/e2e.yml) (каналы email/matrix/telegram, WireMock, креды мостов). Дополнительно передавайте `-e e2e_sut_container_id=<container_id>`.

### Два пользователя

Плейбук работает в модели **двух UNIX-пользователей**:

- **Ansible connection user** (привилегированный): `ansible_user` — для `apt`, Cockpit, `loginctl`, `chown`. В e2e это `root` (docker).
- **Agent user** (`threlium_user`, по умолчанию `threlium`): отдельный логин для данных, systemd --user, notmuch, IMAP bridge. Создаётся плейбуком автоматически (`ansible.builtin.user` + группа `mail`).

При **дефолтном деплое ничего задавать не нужно** — достаточно дефолтов роли. Раскладка на целевой машине:

```text
/home/threlium/threlium/
  data/     ← THRELIUM_HOME (stages/, lightrag/, logs/, locks/, prompts/, www/cockpit-mail-plugin, …)
  agent/    ← threlium_repo_path по умолчанию (клон, .venv, config/, scripts/, systemd/user)
```

Старые установки с данными вне этой схемы (в т.ч. запечённые образы с путями вроде `/root/...`) **не мигрируются** сами: сделайте rebake / чистый деплой или перенесите данные вручную на `~/threlium/data` и репозиторий агента.

Единственная осмысленная переменная для переопределения — **`threlium_repo_path`** (например `/srv/threlium-repo`). Если путь вынесен **вне** `~/threlium/agent`, плейбук один раз прогоняет рекурсивное выставление владельца на всё дерево репозитория — на очень больших каталогах это заметно по времени; override задавайте осознанно.

Тег **`refresh`**: при ``ansible-playbook … --tags refresh`` сначала цепочка **``deploy``+``refresh``** в ``site.yml`` (файлы ``scripts/``, ``env``, шаблоны; **без** ``pip``), затем **``never``+``refresh``** в ``tasks/refresh.yml`` — очистка Maildir на **GNU** ``find`` (несколько путей, ``-delete``); BusyBox-find не подходит.

## Запуск

Один сценарий — весь контур (пакеты, durable стадийные Maildir'ы под `stages/`, общий `lightrag/working_dir/`, почта, все user systemd units, Python-пакет `scripts/threlium/` с модулями стадий, runners и мостов, notmuch с `database.path = {{ threlium_home }}/stages` (union root, [docs/INDEX.md §1](../docs/INDEX.md#1-storage-model)), **единый venv** в корне клона (`.venv`: `pip install -e .` из корня репозитория и/или `pip install -e` из `scripts/` для пакета `threlium` — в зависимостях уже `lightrag-hku`, `litellm`, `numpy`, `msgspec`, …; отдельные extras `[lightrag]` / `[reasoning]` не нужны; LightRAG-граф поднимается при старте RAG-loop в `threlium-engine.service`, отдельного CLI-bootstrap'а нет), мосты по флагам, приёмка):

```bash
cd ansible
ansible-playbook playbooks/site.yml
```

### Интерактивный bootstrap

Альтернатива для первого деплоя нового хоста, когда подготавливать `group_vars/<host>.yml` или Ansible Vault ещё не хочется — wrapper-плейбук [`playbooks/site-interactive.yml`](playbooks/site-interactive.yml) собирает все ключевые переменные через `vars_prompt`, после чего делает `import_playbook: site.yml`. Прямой запуск `site.yml` (с group_vars/vault/`-e`) этот wrapper не затрагивает.

```bash
cd ansible
ansible-playbook playbooks/site-interactive.yml
```

Спрашиваются: каналы (`email,telegram,matrix`), пути (`threlium_repo_path`, `threlium_home`, `threlium_user`), IMAP/SMTP креды, токены мостов Telegram/Matrix, LLM API key/base/model. Пароли и токены маскируются (`private: true`); пустой ответ означает «оставить role default / group_vars»; неиспользуемые каналы пропускаются Enter'ом и `assert` в начале `site.yml` их не требует. Извлечённые `set_fact`-значения подхватываются вторым play через host fact cache; точечный оверрайд через `-e var=value` остаётся выше по приоритету.

Ограничение: `vars_prompt` всегда показывает все промпты — условного пропуска веток нет. Это осознанный trade-off ради простоты wrapper'а.

Дефолтный [`ansible.cfg`](ansible.cfg) для **прода** — **без** `skip_tags` в `[defaults]`; в `[ssh_connection]` включены **pipelining** и **SSH ControlMaster** (меньше задержек при работе по SSH). Очистка harness в `tasks/refresh.yml` помечена **`never` + `refresh`**: при обычном `ansible-playbook site.yml` без `--tags` она **не** выполняется (см. [docs/PLAYBOOK.md §12A](../docs/PLAYBOOK.md#12a-теги-плейбука)). WireMock — **compose-сервис `wiremock`** в `tests/e2e/compose/docker-compose.yml`, с плейбуком не связан (см. [../docs/TESTING.md §4.4](../docs/TESTING.md#44-wiremock-openai-http-mock-e2e)).

**E2e (docker-SUT, pytest mailflow, bake-образ):** перед `ansible-playbook` задайте конфиг e2e, например из каталога `ansible/`:

```bash
export ANSIBLE_CONFIG="$PWD/ansible-e2e.cfg"
ansible-playbook playbooks/site.yml -i inventory/e2e/hosts.yml -e e2e_sut_container_id='<id>' …
```

Файл [`ansible-e2e.cfg`](ansible-e2e.cfg) — `collections_path`, включающий `./collections` (локальные Galaxy-коллекции). В CI/pytest переменная выставляется в [`tests/e2e/helpers.py`](../tests/e2e/helpers.py) автоматически. Скрипт [`tests/e2e/scripts/bake_e2e_sut_image.sh`](../tests/e2e/scripts/bake_e2e_sut_image.sh) вызывает полный `site.yml` без `--skip-tags`.

Инвентарь e2e использует **`ansible_connection: docker`** — нужна **`community.docker`**. Для задачи post-deploy bundle в `site.yml` используется **`community.general.archive`** (в актуальном ansible-core нет `ansible.builtin.archive`). Список в [`ansible/collections/requirements.yml`](collections/requirements.yml). Pytest и [`tests/e2e/scripts/bake_e2e_sut_image.sh`](../tests/e2e/scripts/bake_e2e_sut_image.sh) ставят коллекции сами; вручную из `ansible/`:

```bash
mkdir -p collections
ansible-galaxy collection install -r collections/requirements.yml -p collections
```

Проверка переменных — в начале `site.yml`. Роль `threlium` поставляет `defaults/`, `vars/` (список стадий FSM), `templates/`, статику `files/scripts/`; сценарии — в `playbooks/site.yml` (включение unit-ов через `ansible.builtin.systemd` с `scope: user` и `daemon_reload`).

Секреты для почты и мостов задавайте через **Ansible Vault** или `host_vars`, не коммитьте пароли в открытом виде.

## Типчек Python-скриптов роли

Конфигурация в корне репозитория: [`pyproject.toml`](../pyproject.toml) (mypy, ruff, optional-dependencies **`dev`**), [`pyrightconfig.json`](../pyrightconfig.json) (`venv` по умолчанию — `.venv` в корне репо). Проверяется каталог `roles/threlium/files/scripts/` (работайте из **корня** этого репозитория на машине разработчика).

На целевом хосте Threlium и в образах деплоя скрипты опираются на **`fdm`** (пакет **fdm** на Debian/Ubuntu) для маршрутизации stdin-письма в **`notmuch insert … && threlium-dispatch.sh`** по **`~/.fdm.conf`**; разбор и канонизация заголовков/MIME на границе Python — stdlib `email` и **`RFC822_FOR_INSERT`** (`mime_reform`). Для **статического анализа** на машине разработчика пакет `fdm` не обязателен, если вы не запускаете почтовый pipeline вручную.

Установите dev-зависимости (**`litellm`** / **`lightrag-hku`** уже в основных `dependencies` пакета `threlium` и в корневом `pyproject.toml` для dev-venv; опционально **`notmuch2`** — см. extra **`[dev]`**, если нужна локальная работа с индексом) и запустите проверки:

```bash
# из корня репозитория (рядом с pyproject.toml)
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/mypy ansible/roles/threlium/files/scripts
.venv/bin/pyright ansible/roles/threlium/files/scripts
.venv/bin/ruff check ansible/roles/threlium/files/scripts
```

**Тесты:** **единственный** автоматизированный pytest-слой — **e2e** в `tests/e2e` (Docker + extras `[e2e]`; изоляция WireMock — [docs/E2E_ISOLATION.md](../docs/E2E_ISOLATION.md)). В корне [`pyproject.toml`](../pyproject.toml) задано `testpaths = ["tests/e2e"]`. Из корня репозитория:

```bash
.venv/bin/pip install -e ".[e2e]"
.venv/bin/pytest -vv
```

Pyright читает установленные пакеты из `.venv`; при смене пути к интерпретатору скорректируйте `venv` / `venvPath` в `pyrightconfig.json`.

## Что выкладывается

- **Durable стадийные Maildir'ы** под единым union notmuch root'ом `stages/` ([docs/INDEX.md §1](../docs/INDEX.md#1-storage-model), [docs/ARCHITECTURE.md §9](../docs/ARCHITECTURE.md#9-структура-хранения)) — по одному на каждую FSM-стадию из `threlium_fsm_mailbox_stages` (`vars/main.yml`). **Отдельного `archive/Maildir` нет** — после `nm_settle()` ([docs/INDEX.md §5.5.3](../docs/INDEX.md#553-notmuch-consistency-через-notmuch2mutabletagset)) каждое письмо durable хранится в `cur/<id>:2,S` своей стадии. Общий граф знаний — в `lightrag/working_dir/` под `$THRELIUM_HOME` (single-writer — RAG-loop в `threlium-engine.service`).
- **systemd --user** (template-юниты + при необходимости Ansible loop по переменным роли):
  - **fdm** (`~/.fdm.conf` из `fdm.conf.j2`): после `notmuch insert` в том же `pipe` вызывается `threlium-dispatch.sh` (запрашивает `notmuch search --output=threads "tag:unread AND folder:<stage>/Maildir"` и батчит `systemctl start` для `threlium-work@…`) + `threlium-engine.service` (`python -m threlium.runners.engine`, внутри RAG-loop) + submit-воркер `threlium-work@.service` (`Type=exec`, см. [docs/ORCHESTRATION.md §6](../docs/ORCHESTRATION.md#6-юниты-systemd-пути-имена-окружение); политика ошибок — [docs/INDEX.md §5.6](../docs/INDEX.md#56-universal-error-handling-в-runnersworkerpy)) + `threlium-sweep@.service` (backstop: **`OnSuccess=`** воркера после **`exit 0`**, тот же dispatch-скрипт, `Type=exec`) ([docs/ORCHESTRATION.md §6](../docs/ORCHESTRATION.md#6-юниты-systemd-пути-имена-окружение)).
  - Отдельных `threlium-lightrag@*.path` / `threlium-lightrag.service` в поставке нет — индексация LightRAG после `nm_settle` в том же процессе ([docs/INDEX.md §5b](../docs/INDEX.md#5b-lightrag-worker)).
  - Инстансы **`threlium-bridge@<chan>.service`** (единый шаблон `threlium-bridge@.service.j2`, `python -m threlium.runners.bridge %i`, `imap-tools` для email) при соответствующих каналах в `threlium_channels` / флагах Telegram/Matrix.
  - **`threlium-archive.path` / `threlium-archive.service` удалены** — индексация notmuch происходит атомарно при **`notmuch insert`** внутри fdm `pipe` ([docs/INDEX.md §4](../docs/INDEX.md#4-mailfilter-terminating-insert)), LightRAG — через RAG-loop в `threlium-engine`. Если на хосте остались старые enabled unit-ы прежней схемы (`threlium-archive.{path,service}`, `threlium-notmuch-refresh.*`, `threlium-lightrag*`) — отключите вручную: `systemctl --user disable --now threlium-archive.path threlium-archive.service threlium-notmuch-refresh.path threlium-notmuch-refresh.service 2>/dev/null || true && systemctl --user daemon-reload` (контекст и инвариантные acceptance-проверки — [docs/PLAYBOOK.md](../docs/PLAYBOOK.md)).
- **Скрипты**: из `roles/threlium/files/scripts/` — Python-пакет `threlium/` (`common`, `delivery` (вызов `fdm`), `nm` — helper `nm_settle()`, `cli_fsm`, подпакеты `runners/` (`engine/`, `lightrag.py`), `states/` и `bridges/`) и `pyproject.toml`; dispatch — shell-шаблон из `templates/`; submit — `python -m threlium.runners.engine_submit` (пакет `threlium`). Пакет устанавливается `pip install -e` в общий `.venv` в корне клона; handler'ы стадий вызываются движком in-process.

После первичной установки плейбук хост больше **не обслуживает**: правки живут в локальном `git` в `threlium_repo_path` на target'е и применяются оператором или самим агентом через `cli_exec` (см. [docs/ARCHITECTURE.md §1.2](../docs/ARCHITECTURE.md#12-жизненный-цикл-хоста-bootstrap-через-ansible-и-автономная-эволюция), [docs/PLAYBOOK.md §12](../docs/PLAYBOOK.md#12-после-bootstrap-локальный-git-саморазвитие-агента-границы-ответственности)). Повторный `ansible-playbook playbooks/site.yml` — это либо bootstrap другого хоста, либо disaster-recovery на этом (он затирает локальные коммиты в `threlium_repo_path/.git`).

## Polkit / privileged `cli_exec` (opt-in)

По умолчанию выключено (`threlium_polkit_agent_systemd_enabled: false`). При включении плейбук ставит `policykit-1`, выкладывает правила в `/etc/polkit-1/rules.d/` для `threlium_user` (`manage-units`, `reload-daemon`) и прогоняет acceptance (`tasks/polkit_agent_systemd.yml`).

Для исполнения команд от root через `cli_exec` задайте **оба** флага в `host_vars`:

```yaml
threlium_polkit_agent_systemd_enabled: true
threlium_cli:
  privileged_hitl_enabled: false
```

`cli_exec` при `privileged: true` в payload вызывает `systemd-run --wait --pipe --uid=0` (system manager). Обычные команды — sandbox через `systemd-run --user --wait --pipe` с `ProtectSystem=strict`. Polkit **не** заменяет HITL в `cli_intent`.

Ручная проверка на target: `sudo -u threlium systemd-run --wait --pipe --uid=0 -- systemctl daemon-reload`.

## Веб-архив почты (Caddy + Roundcube + Dovecot + Cockpit)

По умолчанию включён (`threlium_mail_archive_web_enabled: true`). Один рубильник на весь стек. 
Стек: Caddy (фронт) → Cockpit (через reverse proxy) и Roundcube (через php-fpm).
Dovecot (PAM auth) ограничивает доступ к файлам FSM-Maildir через глобальный read-only ACL (`lr`) и отдаёт почту из каталогов стадий с помощью `DIRNAME=Maildir` и плагина `virtual` (сводная папка `All`). В `virtual_mail/…/dovecot-virtual` перед критерием поиска (например `all`) нужен **ведущий пробел** — иначе Dovecot не создаёт ящик в LIST (см. virtual_plugin в документации Dovecot 2.3). 
Roundcube настроен с `ignore_subscriptions`, чтобы всегда отображать новые папки.

Почтовый архив в UI — **пакет Cockpit**: `manifest.json` и `index.html` (iframe на `/webmail`) в `threlium_mail_archive_web_root`. Symlink `~<threlium_user>/.local/share/cockpit/<пакет>` → `web_root` — пакет виден только в сессии Cockpit под тем же POSIX-пользователем. MHonArc более не используется. Вкладка пользовательских служб в Cockpit опирается на user session D-Bus: вместе с пакетами Cockpit ставится **`dbus-user-session`**, после чего плейбук **каждый раз** перезапускает **`user@`** агента (возможен краткий обрыв сессий).

### Переменные

| Переменная | Дефолт | Назначение |
|-----------|--------|-----------|
| `threlium_mail_archive_web_enabled` | `true` | Единый рубильник веб-стека |
| `threlium_mail_archive_caddy_bind_port` | `8080` | Порт Caddy на loopback (`Caddyfile.j2`, acceptance) |
| `threlium_mail_archive_web_root` | `{{ threlium_home }}/www/cockpit-mail-plugin` | Каталог файлов плагина Cockpit (`defaults/main.yml`) |
| `threlium_mail_archive_cockpit_package_name` | `threlium-mail-archive` | Имя Cockpit-пакета (каталог в `~/.local/share/cockpit/`) |
| `threlium_mail_archive_cockpit_origins_extra` | `[]` | Доп. Origins (см. ниже) |
| `threlium_mail_archive_cockpit_loopback_url` | `https://127.0.0.1:9090` | URL для acceptance: прямой Cockpit на loopback (как в `cockpit.conf.j2`) |

### Cockpit Origins (без wildcard)

Cockpit **запрещает** `Origins = *` (защита от CSWSH: панель даёт root через WebSocket). Шаблон `cockpit.conf` собирает список из `https://127.0.0.1:9090`, `https://localhost:9090` и `threlium_mail_archive_cockpit_origins_extra`:

```yaml
threlium_mail_archive_cockpit_origins_extra:
  - "https://admin.example.com"
  - "https://192.168.1.100:9090"
```

### Что происходит при деплое

Деплоится и настраивается Caddy, Dovecot, Roundcube, `manifest.json`, `index.html` и создаётся symlink Cockpit-пакета.

В **мультипользовательском** сценарии (несколько агентов на одном хосте): каждый инвентарь / `threlium_user` получает свой `web_root` и свой symlink. Пакет виден **только** в Cockpit-сессии под соответствующим login'ом.

### Acceptance

Проверяется (все HTTP(S)-запросы выполняются **на target** к `127.0.0.1`, не с control node):
- Cockpit по `threlium_mail_archive_cockpit_loopback_url` (по умолчанию `https://127.0.0.1:9090`, как в `cockpit.conf.j2`).
- Край Caddy на loopback: корень (reverse_proxy на Cockpit) и `/webmail/` на `threlium_mail_archive_caddy_bind_port` (TLS — `threlium_mail_archive_caddy_tls_enabled`); CSS Elastic под `/webmail/` не должен отдаваться как HTML логина.
- Cockpit package symlink валиден
- Сокет user session D-Bus `/run/user/<uid>/bus` существует (после `dbus-user-session` и рестарта `user@`)
- `manifest.json` и `index.html` на месте
- Webhook/oneshot units отсутствуют (миграция на Caddy/Roundcube)

При сбое любого шага деплой **падает**.

### E2E

В `group_vars/e2e.yml` веб-архив **включён** (`threlium_mail_archive_web_enabled: true`). Cockpit в SUT слушает :9090; compose публикует `9090:9090`. Для Cockpit заданы `cockpit_origins_extra` и пароль PAM (`threlium_agent_login_password`). Roundcube в iframe (`/webmail/`) отдаётся краем Caddy на `threlium_mail_archive_caddy_bind_port` (в e2e по умолчанию :8080, см. compose); при открытии только `https://…:9090` без этого же origin путь `/webmail/` не ведёт на Roundcube — заходите через край Caddy или проброс тот же хост/порт.

### Firewall

Плейбук **не** управляет UFW / iptables. При необходимости откройте порт **9090** вручную.

## Post-deploy bundle артефактов

> **Терминологическая ремарка.** Слово «bundle archive» / `tar.gz` ниже — **не** mail-архив (речь не о выделенном `archive/Maildir` в контуре доставки; см. выше «Что выкладывается»). Это деплой-артефакт: тар-архив выбранных файлов с target'а (env, config, юниты, опционально notmuch root `stages/` или `lightrag/working_dir/` — на усмотрение оператора через `threlium_bundle_paths`). Использовать его для ретеншена/восстановления почты — не штатный путь; durable-источник — стадийные `cur/<id>:2,S` под union notmuch index'ом ([docs/MESSAGES.md §5](../docs/MESSAGES.md#5-stage-worker-и-lightrag-worker)).

После acceptance в `site.yml` **может** выполняться post-deploy bundle (если `threlium_bundle_enabled` истинно):

- На target собирается единый `tar.gz` из `threlium_bundle_paths`.
- Архив скачивается на control node в `artifacts/<inventory_hostname>/`.
- По умолчанию временный архив на target удаляется (`threlium_bundle_cleanup_remote: true`).

Для обычного деплоя в `group_vars` bundle часто включают явно. Для **e2e** (`group_vars/e2e.yml`) по умолчанию **`threlium_bundle_enabled: false`**, чтобы прогоны были быстрее и не засоряли `ansible/artifacts`. Чтобы снять архив при расследовании проблем:

```bash
cd ansible
export ANSIBLE_CONFIG="$PWD/ansible-e2e.cfg"
ansible-playbook playbooks/site.yml -i inventory/e2e/hosts.yml \
  -e e2e_sut_container_id='<id контейнера sut>' \
  -e threlium_bundle_enabled=true
```

На обычном инвентаре (не e2e), если в `group_vars` bundle выключен, тот же флаг: `ansible-playbook playbooks/site.yml -e threlium_bundle_enabled=true`.

Проверка результата после прогона:

- На control node есть файл `ansible/artifacts/<inventory_hostname>/threlium-bundle-<inventory_hostname>-<timestamp>.tar.gz`.
- Acceptance завершён успешно (как и без bundle).
- На target временный архив отсутствует при `threlium_bundle_cleanup_remote=true`.

Безопасность: архив может содержать секреты (`env/threlium.env`, `config/*rc`, bridge env). Для ограничения состава задавайте явный `threlium_bundle_paths`.
