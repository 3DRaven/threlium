"""Typed snapshots of LightRAG ``aquery_data`` / ``aquery_llm`` ``data`` (retrieval API).

Отдельно от :mod:`lightrag_tool_args` (схема **индексации** ``extract_knowledge_graph``).
Ключи полей 1:1 с ``lightrag.utils.convert_to_user_format`` (lightrag-hku 1.4.x+).
Скалярные значения на wire гетерогенны (``created_at`` — int/float/str); сборка через
:meth:`LightragAqueryEntity.from_wire` приводит всё к ``str`` без msgspec-валидации.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self

import msgspec


def _wire_str(d: Mapping[str, Any], key: str, default: str = "") -> str:
    """Присутствие ключа достаточно; любое значение → ``str`` (для текстового graph-answer)."""
    if key not in d:
        return default
    return str(d[key]).strip()


def _keywords_from_wire(raw: object) -> str:
    if isinstance(raw, list):
        return ", ".join(str(x).strip() for x in raw if str(x).strip())
    if raw is None:
        return ""
    return str(raw).strip()


class LightragAqueryEntity(msgspec.Struct, frozen=True):
    """Одна сущность в ``data.entities`` после ``convert_to_user_format``."""

    entity_name: str
    entity_type: str = "UNKNOWN"
    description: str = ""
    source_id: str = ""
    file_path: str = ""
    created_at: str = ""

    @classmethod
    def from_wire(cls, d: Mapping[str, Any]) -> Self:
        return cls(
            entity_name=_wire_str(d, "entity_name"),
            entity_type=_wire_str(d, "entity_type") or "UNKNOWN",
            description=_wire_str(d, "description"),
            source_id=_wire_str(d, "source_id"),
            file_path=_wire_str(d, "file_path"),
            created_at=_wire_str(d, "created_at"),
        )


class LightragAqueryRelation(msgspec.Struct, frozen=True):
    """Одна связь в ``data.relationships``."""

    src_id: str
    tgt_id: str
    description: str = ""
    keywords: str = ""
    weight: str = "1.0"
    source_id: str = ""
    file_path: str = ""
    created_at: str = ""

    @classmethod
    def from_wire(cls, d: Mapping[str, Any]) -> Self:
        weight_raw = d.get("weight", "1.0")
        return cls(
            src_id=_wire_str(d, "src_id"),
            tgt_id=_wire_str(d, "tgt_id"),
            description=_wire_str(d, "description"),
            keywords=_keywords_from_wire(d.get("keywords", "")),
            weight=str(weight_raw).strip() if weight_raw is not None else "1.0",
            source_id=_wire_str(d, "source_id"),
            file_path=_wire_str(d, "file_path"),
            created_at=_wire_str(d, "created_at"),
        )


class LightragQueryData(msgspec.Struct, frozen=True):
    """``data`` внутри ``QueryDataResponse`` (entities + relationships; chunks опциональны)."""

    entities: list[LightragAqueryEntity] = ()
    relationships: list[LightragAqueryRelation] = ()

    @classmethod
    def from_wire(cls, data: Mapping[str, Any]) -> Self:
        entities: list[LightragAqueryEntity] = []
        for row in data.get("entities") or []:
            if isinstance(row, dict):
                entities.append(LightragAqueryEntity.from_wire(row))
        relationships: list[LightragAqueryRelation] = []
        for row in data.get("relationships") or []:
            if isinstance(row, dict):
                relationships.append(LightragAqueryRelation.from_wire(row))
        return cls(entities=entities, relationships=relationships)


class GraphAnswerEntityRow(msgspec.Struct, frozen=True):
    """Строка сущности для ``lightrag/graph_answer*.j2``."""

    id: str
    name: str
    type: str
    description: str


class GraphAnswerRelationRow(msgspec.Struct, frozen=True):
    """Строка связи для Jinja / mermaid (``src_node``/``tgt_node`` = ``ent_N``)."""

    id: str
    src_id: str
    tgt_id: str
    src_node: str
    tgt_node: str
    description: str
    keywords: str


class GraphAnswerView(msgspec.Struct, frozen=True):
    """Проекция envelope → kwargs ``render_prompt`` (см. ``types/enrich.py`` query plan)."""

    formulated_query: str
    answer: str | None
    entities: tuple[GraphAnswerEntityRow, ...]
    relations: tuple[GraphAnswerRelationRow, ...]
    include_mermaid: bool

    def has_subgraph(self) -> bool:
        return bool(self.entities or self.relations)

    def for_graph_answer_jinja(self) -> dict[str, object]:
        return {
            "formulated_query": self.formulated_query,
            "answer": self.answer,
            "entities": [
                {
                    "id": e.id,
                    "name": e.name,
                    "type": e.type,
                    "description": e.description,
                }
                for e in self.entities
            ],
            "relations": [
                {
                    "id": r.id,
                    "src_id": r.src_id,
                    "tgt_id": r.tgt_id,
                    "src_node": r.src_node,
                    "tgt_node": r.tgt_node,
                    "description": r.description,
                    "keywords": r.keywords,
                }
                for r in self.relations
            ],
            "include_mermaid": self.include_mermaid,
        }

    @classmethod
    def from_query_data(
        cls,
        *,
        formulated_query: str,
        answer: str | None,
        data: LightragQueryData,
        max_entities: int,
        max_relations: int,
        desc_max_chars: int,
        include_mermaid: bool,
    ) -> Self:
        name_to_node: dict[str, str] = {}
        entity_rows: list[GraphAnswerEntityRow] = []
        for i, ent in enumerate(data.entities[:max_entities]):
            node_id = f"ent_{i}"
            name = ent.entity_name.strip()
            name_to_node[name] = node_id
            entity_rows.append(
                GraphAnswerEntityRow(
                    id=node_id,
                    name=name,
                    type=ent.entity_type.strip() or "UNKNOWN",
                    description=_truncate_desc(ent.description, desc_max_chars),
                )
            )

        relation_rows: list[GraphAnswerRelationRow] = []
        for j, rel in enumerate(data.relationships[:max_relations]):
            src = rel.src_id.strip()
            tgt = rel.tgt_id.strip()
            relation_rows.append(
                GraphAnswerRelationRow(
                    id=f"rel_{j}",
                    src_id=src,
                    tgt_id=tgt,
                    src_node=name_to_node.get(src, _fallback_node_id(src)),
                    tgt_node=name_to_node.get(tgt, _fallback_node_id(tgt)),
                    description=_truncate_desc(rel.description, desc_max_chars),
                    keywords=_keywords_wire(rel.keywords),
                )
            )

        answer_stripped = answer.strip() if isinstance(answer, str) and answer.strip() else None
        return cls(
            formulated_query=formulated_query.strip(),
            answer=answer_stripped,
            entities=tuple(entity_rows),
            relations=tuple(relation_rows),
            include_mermaid=include_mermaid,
        )


def _truncate_desc(text: str, max_chars: int) -> str:
    text = text.strip()
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _fallback_node_id(label: str) -> str:
    """Mermaid node id для entity вне cap списка (стабильный короткий id)."""
    safe = "".join(c if c.isalnum() else "_" for c in label.strip())[:24]
    return f"ext_{safe or 'node'}"


def _keywords_wire(raw: str | list[Any] | Any) -> str:
    if isinstance(raw, str):
        return raw.strip()
    if isinstance(raw, list):
        return ", ".join(str(x).strip() for x in raw if str(x).strip())
    return str(raw).strip() if raw is not None else ""


__all__ = [
    "GraphAnswerEntityRow",
    "GraphAnswerRelationRow",
    "GraphAnswerView",
    "LightragAqueryEntity",
    "LightragAqueryRelation",
    "LightragQueryData",
]
