"""Ingress distill: LLM tool_choice=required → structured brief in ``<history>``."""
from __future__ import annotations

import jsonschema
from email.message import EmailMessage
from threlium.enrich_context import trim_context_text
from threlium.ingress_distill_tool_bridge import parse_ingress_distill_assistant
from threlium.litellm_correlation_headers import fsm_correlation_snap
from threlium.litellm_required_tool import (
    build_site_call,
    invoke_required_tool,
    invoke_with_bridge_retries,
)
from threlium.litellm_tool_response import LiteLlmToolResponseError
from threlium.litellm_tool_spec import load_tool_spec, tool_spec_parameters
from threlium.logutil import logger
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    IngressDistillBridgeError,
    IngressDistillEnvelope,
    IngressDistillResult,
    LiteLlmChatMessage,
    LitellmRoutingSite,
    PromptPath,
    ingress_distill_fallback_history_parts,
    ingress_distill_history_parts_from_tool_args,
)
log = logger.bind(stage="ingress")

_MAX_INGRESS_DISTILL_RETRIES = 2


def ingress_distill_llm(
    envelope: IngressDistillEnvelope,
    msg: EmailMessage,
    *,
    config: ThreliumSettings,
) -> IngressDistillResult:
    """Sync LLM distill with tool bridge; retry; fallback to trimmed full_body."""
    tool_spec = load_tool_spec(
        PromptPath.INGRESS_DISTILL_TOOL_SPEC,
        distill_max_chars=config.ingress.distill_max_chars,
    )
    schema = tool_spec_parameters(tool_spec)
    system = render_prompt(PromptPath.INGRESS_DISTILL_SYSTEM).strip()
    user = render_prompt(
        PromptPath.INGRESS_DISTILL_USER,
        channel=envelope.channel.value,
        from_hdr=envelope.from_hdr.value if envelope.from_hdr else "",
        to_hdr=envelope.to_hdr.value if envelope.to_hdr else "",
        subject=envelope.subject.value if envelope.subject else "",
        date=envelope.date.value if envelope.date else "",
        message_id=envelope.message_id.value if envelope.message_id else "",
        in_reply_to=envelope.in_reply_to.value if envelope.in_reply_to else None,
        orphan_notice=(
            envelope.orphan_notice.value if envelope.orphan_notice else None
        ),
        full_body=envelope.full_body.value,
    ).strip()
    call = build_site_call(
        config,
        LitellmRoutingSite.INGRESS_DISTILL,
        [
            LiteLlmChatMessage(role="system", content=system),
            LiteLlmChatMessage(role="user", content=user),
        ],
    )
    correlation = fsm_correlation_snap(None, config)

    def _attempt() -> IngressDistillResult:
        assistant = invoke_required_tool(
            settings=config,
            call=call,
            tool_spec=tool_spec,
            correlation_snap=correlation,
            context="ingress_distill",
        )
        args = parse_ingress_distill_assistant(assistant, schema=schema)
        parts = ingress_distill_history_parts_from_tool_args(args)
        log.info(
            "ingress_distill_ok",
            history_parts=len(parts),
            user_intent_len=len(parts[-1].text),
        )
        return IngressDistillResult(parts=parts)

    def _on_retry(attempt_no: int, exc: BaseException) -> None:
        log.warning("ingress_distill_retry", attempt=attempt_no, error=str(exc))

    def _fallback(last_error: BaseException) -> IngressDistillResult:
        log.warning("ingress_distill_fallback", error=str(last_error))
        trimmed = trim_context_text(
            envelope.full_body.value,
            config.ingress.distill_fallback_max_chars,
        )
        return IngressDistillResult(parts=ingress_distill_fallback_history_parts(trimmed))

    return invoke_with_bridge_retries(
        max_attempts=_MAX_INGRESS_DISTILL_RETRIES + 1,
        attempt=_attempt,
        retry_errors=(
            IngressDistillBridgeError,
            LiteLlmToolResponseError,
            jsonschema.ValidationError,
        ),
        on_retry=_on_retry,
        on_exhausted=_fallback,
    )


__all__ = ["ingress_distill_llm"]
