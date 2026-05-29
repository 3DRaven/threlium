"""Идемпотентный полный ``site.yml`` на ``sut`` при поднятом shared compose.

Не входит в дефолтную коллекцию ``pytest tests/e2e`` (имя файла вне ``test_*.py``).
Запуск после ``wipe_bake.py`` или когда стек уже поднят сценарными тестами:

.. code-block:: bash

   pytest -n0 -vv -s tests/e2e/wipe_sync.py

Требует фикстуру ``compose_stack`` (поднимает стек, если ещё не поднят).
Только тег ``refresh``: в ``site.yml`` — ``deploy``+``refresh`` (код, ``env``, шаблоны; **без** ``pip``); в ``refresh.yml`` — ``never``+``refresh`` (чистка и рестарт user-units). Зависимости/venv — полный ``site.yml`` / **deploy**.
Полный ``deploy`` — отдельный прогон ``site.yml`` / bake / FSTS; здесь не вызывается.

**Чистый стек после прогона.** SUT-сторону чистит harness ``refresh`` (playbook
``tasks/refresh.yml``): останавливает worker.slice + engine, стирает Maildir
``cur/new/tmp``, индекс notmuch и кеш LightRAG, ``notmuch new``, затем
рестартит user-units (engine + bridges) — стек остаётся работающим. Чтобы штатный
``pytest_sessionfinish`` не погасил этот pipeline (его обычная teardown-политика),
выставляем :data:`~tests.e2e.conftest.E2E_LEAVE_STACK_RUNNING_ENV`: тогда sessionfinish
лишь чистит **тестовый WireMock** (журнал запросов + State, bootstrap-маппинги остаются)
и оставляет SUT поднятым. Так отдельная инвокация live-only тестов
(``test_mailflow_live_only_e2e.py`` и др.), которая пропускает session-start cold reset
и сама pipeline не поднимает, стартует на чистом работающем стеке.
"""
from __future__ import annotations

import os

import pytest

from .conftest import E2E_LEAVE_STACK_RUNNING_ENV
from .helpers import REPO_ROOT, run_e2e_site_playbook


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_wipe_sync_site_playbook_full(compose_stack) -> None:
    """Harness refresh на ``sut`` (``--tags refresh``); стек оставить поднятым и чистым."""
    # SUT-pipeline должен пережить sessionfinish (его обычно гасят); WireMock чистится там же.
    os.environ[E2E_LEAVE_STACK_RUNNING_ENV] = "1"
    run_e2e_site_playbook(
        compose_stack.project_name,
        checkout="/unused",
        repo_root=REPO_ROOT,
        ansible_tags="refresh",
    )
