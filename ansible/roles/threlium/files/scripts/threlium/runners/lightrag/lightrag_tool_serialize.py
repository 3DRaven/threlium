"""Struct/VO → wire-строка для контракта LightRAG ``llm_func``."""
from __future__ import annotations

import json

import msgspec

from threlium.types.lightrag_tool_args import (
    ExtractKnowledgeGraphToolArgs,
    ExtractQueryKeywordsToolArgs,
    GenerateRagAnswerToolArgs,
    SummarizeDescriptionsToolArgs,
)
from threlium.types.lightrag_tool_wire import (
    LightragCompletionDelimiterWire,
    LightragEntitySummaryText,
    LightragExtractionDelimiterText,
    LightragKeywordsJsonText,
    LightragRagAnswerText,
    LightragTupleDelimiterWire,
)

_DEFAULT_TUPLE = LightragTupleDelimiterWire.require(
    name="tuple_delimiter", raw="<|#|>"
)
_DEFAULT_COMPLETION = LightragCompletionDelimiterWire.require(
    name="completion_delimiter", raw="<|COMPLETE|>"
)


def _title_case_name(name: str) -> str:
    """Title-case entity names when building delimiter text (``entity_extraction_system_prompt.j2`` §1)."""
    return " ".join(part[:1].upper() + part[1:] if part else part for part in name.split())


def lightrag_extraction_delimiter_from_args(
    args: ExtractKnowledgeGraphToolArgs,
    *,
    tuple_delimiter: LightragTupleDelimiterWire = _DEFAULT_TUPLE,
    completion_delimiter: LightragCompletionDelimiterWire = _DEFAULT_COMPLETION,
) -> LightragExtractionDelimiterText:
    """Serialize tool args to delimiter text for ``operate._process_extraction_result``.

    Empty ``entities`` and ``relations`` → only ``completion_delimiter`` (gleaning done);
    LightRAG accepts zero records when the delimiter is present.
    """
    td = tuple_delimiter.value
    cd = completion_delimiter.value
    lines: list[str] = []
    for ent in args.entities:
        name = _title_case_name(ent.name)
        lines.append(
            f"entity{td}{name}{td}{ent.type}{td}{ent.description}"
        )
    for rel in args.relations:
        src = _title_case_name(rel.source_entity)
        tgt = _title_case_name(rel.target_entity)
        lines.append(
            "relation"
            f"{td}{src}{td}{tgt}{td}{rel.relationship_keywords}{td}"
            f"{rel.relationship_description}"
        )
    if not lines:
        body = cd
    else:
        body = "\n".join(lines) + cd
    return LightragExtractionDelimiterText.parse(body)


def lightrag_keywords_json_from_args(
    args: ExtractQueryKeywordsToolArgs,
) -> LightragKeywordsJsonText:
    payload = msgspec.to_builtins(args)
    return LightragKeywordsJsonText.parse(json.dumps(payload, ensure_ascii=False))


def lightrag_summary_from_args(
    args: SummarizeDescriptionsToolArgs,
) -> LightragEntitySummaryText:
    return LightragEntitySummaryText.parse(args.summary)


def lightrag_rag_answer_from_args(
    args: GenerateRagAnswerToolArgs,
) -> LightragRagAnswerText:
    return LightragRagAnswerText.parse(args.answer)


__all__ = [
    "lightrag_extraction_delimiter_from_args",
    "lightrag_keywords_json_from_args",
    "lightrag_rag_answer_from_args",
    "lightrag_summary_from_args",
]
