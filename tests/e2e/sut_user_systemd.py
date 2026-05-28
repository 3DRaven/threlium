"""SUT: user-scope systemd + ``journalctl`` из ``docker exec`` (root).

Обёртки ``runuser`` + ``XDG_RUNTIME_DIR``: без них ``journalctl --user-unit`` смотрит
не тот user journal. Не импортирует ``helpers`` — только stdlib.
"""
from __future__ import annotations

import os
import shlex

E2E_THRELIUM_USER = os.environ.get("THRELIUM_E2E_THRELIUM_USER", "threlium")

# ``journalctl --user-unit=…`` из ``docker exec`` (root) смотрит user-session **root**;
# сервисы threlium — в user journal UID ``E2E_THRELIUM_USER``.
E2E_THRELIUM_USER_JOURNALCTL_PREFIX = (
    f"runuser -u {E2E_THRELIUM_USER} -- env "
    f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) journalctl"
)
E2E_THRELIUM_USER_JOURNAL_TRANSPORT_MATCH = "_TRANSPORT=journal"


def e2e_threlium_user_unit_journalctl_bash(
    user_unit: str,
    lines: int,
    *,
    transport_journal: bool = True,
    shell_redirect: str = "2>&1 || true",
) -> str:
    """Одна bash-команда: ``journalctl`` user-юнита в журнале ``E2E_THRELIUM_USER`` на SUT (exec от root).

    ``user_unit`` передаётся в ``--user-unit`` (имя или шаблон с ``*``). ``shell_redirect`` — хвост
    команды (например ``2>/dev/null`` для ``if … | grep`` без ``|| true``).

    ``transport_journal=True`` (по умолчанию) оставляет только записи с ``_TRANSPORT=journal``
    (сообщения systemd о start/stop). Логи приложения (structlog на stdout → journald
    ``_TRANSPORT=stdout``) для проверок вроде ``bootstrap_knowledge`` задавайте
    ``transport_journal=False``.
    """
    uq = shlex.quote(user_unit)
    n = max(1, int(lines))
    t = f" {E2E_THRELIUM_USER_JOURNAL_TRANSPORT_MATCH}" if transport_journal else ""
    return f"{E2E_THRELIUM_USER_JOURNALCTL_PREFIX} --user-unit={uq} -n {n}{t} --no-pager {shell_redirect}"


# Стабильные юниты — ``journalctl --user-unit=name``; шаблонные — через :func:`e2e_threlium_user_unit_journalctl_bash`.
E2E_SUT_THRELIUM_USER_UNIT_JOURNAL = f"""
echo '=== journalctl --user-unit threlium-bridge@email.service (as {E2E_THRELIUM_USER}) ==='
{e2e_threlium_user_unit_journalctl_bash("threlium-bridge@email.service", 80)}
echo ''
echo '=== journalctl --user-unit threlium-engine.service (as {E2E_THRELIUM_USER}) ==='
{e2e_threlium_user_unit_journalctl_bash("threlium-engine.service", 80)}
echo ''
echo '=== journalctl --user-unit "threlium-work@*.service" (as {E2E_THRELIUM_USER}) ==='
{e2e_threlium_user_unit_journalctl_bash("threlium-work@*.service", 80)}
echo ''
echo '=== journalctl --user-unit "threlium-sweep@*.service" (as {E2E_THRELIUM_USER}) ==='
{e2e_threlium_user_unit_journalctl_bash("threlium-sweep@*.service", 60)}
"""


def e2e_stop_threlium_user_pipeline_bash() -> str:
    """Bash-скрипт: остановить engine, bridges, work, sweep (user systemd на SUT)."""
    u = E2E_THRELIUM_USER
    return f"""set -eu
uid=$(id -u {u})
export XDG_RUNTIME_DIR=/run/user/$uid
runuser -u {u} -- systemctl --user stop threlium-engine.service 2>/dev/null || true
for u in $(runuser -u {u} -- systemctl --user list-units --all 'threlium-bridge@*.service' --no-legend 2>/dev/null | awk '{{print $1}}' || true); do
  runuser -u {u} -- systemctl --user reset-failed "$u" 2>/dev/null || true
  runuser -u {u} -- systemctl --user stop "$u" 2>/dev/null || true
done
for u in $(runuser -u {u} -- systemctl --user list-units --all 'threlium-work@*.service' --no-legend 2>/dev/null | awk '{{print $1}}' || true); do
  runuser -u {u} -- systemctl --user reset-failed "$u" 2>/dev/null || true
  runuser -u {u} -- systemctl --user stop "$u" 2>/dev/null || true
done
for u in $(runuser -u {u} -- systemctl --user list-units --all 'threlium-sweep@*.service' --no-legend 2>/dev/null | awk '{{print $1}}' || true); do
  runuser -u {u} -- systemctl --user reset-failed "$u" 2>/dev/null || true
  runuser -u {u} -- systemctl --user stop "$u" 2>/dev/null || true
done
echo "[e2e] SUT user-scope pipeline stopped (engine + bridges + work + sweep)"
"""


def e2e_sut_threlium_user_journal_rotate_vacuum_bash() -> str:
    """Bash: ротация и ужатие **user**-журнала ``E2E_THRELIUM_USER`` на SUT (после остановки pipeline).

    Снимает хвост ``journalctl --user-unit`` от прошлых pytest-сессий на долгоживущем контейнере,
    чтобы диагностика e2e не тащила старые ``Failed with result 'exit-code'`` и т.п.
    ``--vacuum-time=1s`` оставляет минимальный хвост по политике journald (см. ``journalctl(1)``).
    """
    u = E2E_THRELIUM_USER
    return f"""set +e
uid=$(id -u {u})
export XDG_RUNTIME_DIR=/run/user/$uid
# User manager must be up (linger/session); cold reset calls this right after pipeline stop.
runuser -u {u} -- env XDG_RUNTIME_DIR=/run/user/$uid journalctl --user --rotate 2>&1
rc_r=$?
runuser -u {u} -- env XDG_RUNTIME_DIR=/run/user/$uid journalctl --user --vacuum-time=1s 2>&1
rc_v=$?
echo "[e2e] SUT user journal (UID $uid): journalctl --user --rotate rc=$rc_r --vacuum-time=1s rc=$rc_v"
exit 0
"""


def e2e_start_threlium_user_pipeline_bash() -> str:
    """Bash-скрипт: journald без rate limit, старт engine и enabled bridge@* (user systemd)."""
    u = E2E_THRELIUM_USER
    return f"""set -eu
uid=$(id -u {u})
export XDG_RUNTIME_DIR=/run/user/$uid

mkdir -p /etc/systemd/journald.conf.d
printf '[Journal]\\nRateLimitIntervalSec=0\\n' > /etc/systemd/journald.conf.d/e2e-no-ratelimit.conf
systemctl restart systemd-journald 2>/dev/null || true

runuser -u {u} -- systemctl --user start threlium-engine.service
for u in $(runuser -u {u} -- systemctl --user list-unit-files 'threlium-bridge@*.service' --no-legend 2>/dev/null | awk '$2=="enabled"{{print $1}}' || true); do
  runuser -u {u} -- systemctl --user start "$u" 2>/dev/null || true
done
sleep 2
st=$(runuser -u {u} -- systemctl --user is-active threlium-engine.service || true)
echo "[e2e] SUT threlium-engine.service is-active: ${{st}}"
test "$st" = active
"""


def e2e_sut_threlium_user_workers_idle_probe_bash() -> str:
    """Bash-скрипт: stdout — число активных ``threlium-work@*`` / ``threlium-sweep@*`` (последняя строка — число)."""
    u = E2E_THRELIUM_USER
    return f"""
set -e
n=$(runuser -u {u} -- bash -lc 'export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user --no-pager list-units "threlium-work@*" "threlium-sweep@*" \\
  --state=running,activating --no-legend 2>/dev/null | grep -v "^$" | wc -l')
echo "$n"
"""


def e2e_sut_threlium_user_workers_stall_diag_bash() -> str:
    """Bash-скрипт для логов при таймауте idle: кто в ``running``/``failed``, срез юнитов, хвост user-journal."""
    u = E2E_THRELIUM_USER
    return f"""set +e
echo "=== E2E_DIAG workers/sweep RUNNING_OR_ACTIVATING ==="
runuser -u {u} -- bash -lc 'export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user --no-pager list-units "threlium-work@*" "threlium-sweep@*" \\
  --state=running,activating --no-legend 2>&1'

echo ""
echo "=== E2E_DIAG workers/sweep FAILED ==="
runuser -u {u} -- bash -lc 'export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user --no-pager list-units "threlium-work@*" "threlium-sweep@*" \\
  --state=failed --no-legend 2>&1'

echo ""
echo "=== E2E_DIAG threlium-engine + bridges (running/failed, head) ==="
runuser -u {u} -- bash -lc 'export XDG_RUNTIME_DIR=/run/user/$(id -u)
echo -n "engine: "
systemctl --user is-active threlium-engine.service 2>&1
systemctl --user --no-pager list-units "threlium-bridge@*" \\
  --state=running,activating,failed --no-legend 2>&1 | head -n 15'

echo ""
echo "=== E2E_DIAG list-units threlium-* (first 50 lines) ==="
runuser -u {u} -- bash -lc 'export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user --no-pager list-units "threlium-*" --all --no-legend 2>&1 | head -n 50'

echo ""
echo "=== E2E_DIAG user journalctl --user -n 60 ==="
runuser -u {u} -- bash -lc 'export XDG_RUNTIME_DIR=/run/user/$(id -u)
journalctl --user -n 60 --no-pager 2>&1'

echo ""
echo "=== E2E_DIAG threlium-work / sweep journal (user-unit glob, last 40 each) ==="
{e2e_threlium_user_unit_journalctl_bash("threlium-work@*.service", 40)}
echo "---"
{e2e_threlium_user_unit_journalctl_bash("threlium-sweep@*.service", 40)}
"""
