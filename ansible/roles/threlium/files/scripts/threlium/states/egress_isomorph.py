#!/usr/bin/env python3
"""egress_isomorph@localhost: контент-адресуемый glue-archive + HTTP-push в мост.

Контракт egress_* (docs/ARCHITECTURE §2.6): доставка наружу + ``build_egress_sent_record_to_archive``
→ ``run_fdm`` (письмо на ``archive@localhost``) → ``return None``. «Наружу» здесь = ``POST
/internal/v1/push`` в процесс ``threlium-bridge@isomorph`` (cross-process на ``127.0.0.1``).

Тред-непрерывность (docs/THREAD_MODEL §isomorph): glue ``Message-ID = canon(IsomorphContentId(hash(reply)))``
— ровно его мост пересчитает из last-assistant следующего хода как ``In-Reply-To`` (без lookup).
**ARCHIVE-FIRST до push**, иначе Cline пришлёт следующий запрос раньше записи glue → orphan-форк.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from email.message import EmailMessage

import msgspec

from threlium.delivery import run_fdm
from threlium.egress_self_archive import (
    build_egress_sent_record_to_archive,
    find_existing_egress_archive,
)
from threlium.ingress_route_resolve import resolve_egress_task_route_ancestor
from threlium.logutil import logger
from threlium.mail import serialize_rfc822_for_wire
from threlium.mime_reform import system_part_text
from threlium.settings import ThreliumSettings
from threlium.bridges.isomorph.push_types import IsomorphBridgePushPayload
from threlium.bridges.isomorph.snowflake_mid import (
    mid_to_snowflake,
    mint_egress_snowflake,
    snowflake_to_mid,
    watermark_reply,
)
from threlium.types import (
    FsmStage,
    IngressRoute,
    IsomorphIngressRoute,
)

log = logger.bind(stage="egress_isomorph")

_PUSH_SECRET_HEADER = "X-Threlium-Push-Secret"


def push_completion_to_bridge(
    payload: IsomorphBridgePushPayload, *, settings: ThreliumSettings
) -> None:
    """``POST http://<listen_host>:<listen_port>/internal/v1/push`` с ``push_secret``.

    Идемпотентность — на стороне моста (unknown/done/cancelled → 200/204). Сетевые ошибки
    логируются и НЕ валят стадию: archive уже записан, мост может быть в рестарте.
    """
    iso = settings.bridges.isomorph
    url = f"http://{iso.listen_host}:{iso.listen_port}/internal/v1/push"
    # Схема push-тела владеется VO (msgspec), не hand-build dict.
    body = msgspec.json.encode(payload)
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            _PUSH_SECRET_HEADER: iso.push_secret,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=iso.request_timeout_sec) as resp:
            log.info("push_ok", mid=payload.ingress_mid, status=resp.status)
    except urllib.error.HTTPError as e:  # noqa: PERF203
        log.warning("push_http_error", mid=payload.ingress_mid, status=e.code)
    except urllib.error.URLError as e:
        log.warning("push_unreachable", mid=payload.ingress_mid, reason=str(e.reason))


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    def _wrong_type(r: IngressRoute) -> str:
        return (
            "egress_isomorph: ожидался IsomorphIngressRoute, получен "
            f"{type(r).__name__} (channel={r.channel!r})"
        )

    route, snap = resolve_egress_task_route_ancestor(
        msg, IsomorphIngressRoute, wrong_route_type_message=_wrong_type
    )
    # Коррелятор push↔pending = Message-ID ЭТОГО хода (ближайший tag:route предок = ingress хода),
    # под ним мост зарегистрировал pending. inner-форма — байтовый паритет с регистрацией моста.
    corr = snap.ancestor_mid.value

    # MVP (фаза A): FSM-ответ — текст (reasoning → … → response_finalize).
    reply_text = system_part_text(msg).strip()

    # glue-MID = snowflake (уникален независимо от тела → нет коллизии тредов при идентичных ответах,
    # см. docs/E2E_PARALLEL_ISOLATION_REPORT §5-bis). Идемпотентность ретраев egress: на повторе берём
    # snowflake из УЖЕ записанного glue-архива, иначе минтим новый — тогда водяной знак (и IRT следующего
    # хода) стабилен между ретраями.
    existing = find_existing_egress_archive(msg)
    if existing is not None:
        glue_mid = existing.glue_message_id
        glue_sf = mid_to_snowflake(glue_mid)
    else:
        glue_sf = mint_egress_snowflake()
        glue_mid = snowflake_to_mid(glue_sf)

    # Водяной знак: glue_sf невидимо в КОНЕЦ ответа → клиент вернёт его в истории как last-assistant,
    # мост следующего хода декодит → IRT = этот glue-MID (без notmuch/голосования).
    watermarked_text = watermark_reply(reply_text, glue_sf) if glue_sf is not None else reply_text

    payload = IsomorphBridgePushPayload(
        ingress_mid=corr,
        api_surface=route.api_surface,
        finish_reason="stop",
        model=route.model,
        text=watermarked_text,
    )

    sent_raw = json.dumps(
        {
            "channel": "isomorph",
            "ingress_mid": corr,
            "api_surface": route.api_surface,
            "model": route.model,
            "stream": route.stream,
            "push": msgspec.to_builtins(payload),  # схема VO, не hand-build dict
        },
        ensure_ascii=False,
        indent=2,
    )

    if existing is None:
        # ARCHIVE-FIRST: glue должен существовать до того, как Cline сможет ответить.
        archive_email = build_egress_sent_record_to_archive(
            msg, stage=stage, sent_raw=sent_raw,
            glue_message_id_wire=glue_mid, settings=config,
        )
        run_fdm(serialize_rfc822_for_wire(archive_email))
        log.info("archive_written", mid=corr)
    else:
        log.info("archive_exists_repush", mid=corr)

    push_completion_to_bridge(payload, settings=config)
    return None
