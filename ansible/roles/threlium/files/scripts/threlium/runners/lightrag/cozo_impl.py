"""CozoDB graph storage for LightRAG — embedded, MVCC, concurrent-writer (RocksDB backend).

Заменяет in-memory ``NetworkXStorage``. CozoDB (rocksdb-бэкенд) — встраиваемый (файловый) с **MVCC**:
конкурентная запись в разные ключи + конкурентное чтение безопасны (в отличие от NetworkX, который держится
на единственном asyncio-loop для взаимного исключения над ``self._graph``). Это разблокирует Stage-2 (снятие
единого rag-loop → независимые aquery/ainsert). Регистрируется БЕЗ патча вендора (рантайм-мутация реестра
``lightrag.kg`` в ``_construction._register_cozo_storage``).

Lock-free: без asyncio ``_storage_lock`` — конкуренцию арбитрит CozoDB/RocksDB (Rust-ядро: читатели не блокируют
писателей, снапшот-консистентные чтения). Python-клиент cozo **синхронный**, поэтому каждый запрос идёт через
``asyncio.to_thread`` (НЕ блокирует rag-loop + thread-safe внутренний диспетчер cozo). Три stored-relation на
``(workspace, namespace)``: ``nodes {id => data:Json}``, ``edges {src, tgt => data:Json}`` и
``adj {node, neighbor}``. Рёбра **неориентированные** (как ``nx.Graph``) → ДАННЫЕ ребра хранятся канонически в
``edges`` (src=min, tgt=max — одна строка на пару, для ``get_edge``). Свойства — Json-блоб (lightrag фильтрует
граф ТОЛЬКО по id/структуре, не по свойствам → типизированные индекс-колонки не нужны).

**Adjacency-индекс** ``adj``: каждое ребро (s,t) кладётся в ОБЕ стороны (s→t, t→s). Все обходы по соседям
(``get_node_edges`` / ``node_degree`` / batch-варианты) идут по PK-префиксу ``node`` → O(degree). БЕЗ него
обратный обход (``tgt = $id``) был полным сканом ``edges`` (tgt не в PK-префиксе) — измерено O(edges): 36 мс/оп
при 20k рёбер против ~0.26 мс с индексом (138x на росте графа). ``edges`` остаётся истиной для данных ребра;
``adj`` — производный индекс структуры, синхронно поддерживается на upsert/delete (мульти-блок в одном ``run()``
= одна транзакция) и реконструируется из ``edges`` при создании (backfill).

Идиомы Cozo (выверены в SUT): batch-операции одним запросом через ``input[..] <- $rows`` + semijoin (без N+1);
degree через Datalog-``count``; удаление узла+инцидентных рёбер — атомарным мульти-блоком ``{..}{..}`` в одном
``run()``; upsert = ``:put`` (полная замена; lightrag предварительно мёржит данные узла — нативного JSON-patch
в Cozo нет, и это правильный паттерн).
"""
from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass
from typing import Any

from lightrag.base import BaseGraphStorage
from lightrag.types import KnowledgeGraph, KnowledgeGraphEdge, KnowledgeGraphNode
from lightrag.utils import logger


def _canon(a: str, b: str) -> tuple[str, str]:
    """Канонический порядок неориентированного ребра: (min, max) → одна строка на пару."""
    return (a, b) if a <= b else (b, a)


def _records(res: Any) -> list[dict[str, Any]]:
    """Нормализовать результат pycozo (pandas DataFrame ИЛИ raw dict) в list[dict]."""
    if hasattr(res, "to_dict"):  # pandas DataFrame
        return res.to_dict("records")
    headers = res.get("headers", [])
    return [dict(zip(headers, row)) for row in res.get("rows", [])]


@dataclass
class CozoGraphStorage(BaseGraphStorage):
    def __post_init__(self) -> None:
        wd = self.global_config["working_dir"]
        base = os.path.join(wd, self.workspace) if self.workspace else wd
        self._db_path = os.path.join(base, "cozo_graph")
        suffix = re.sub(r"[^A-Za-z0-9_]", "_", f"{self.workspace}_{self.namespace}").strip("_") or "g"
        self._nodes = f"nodes_{suffix}"
        self._edges = f"edges_{suffix}"
        self._adj = f"adj_{suffix}"  # {node, neighbor} both-directions index over edges (O(degree) traversal)
        self._db: Any = None

    async def initialize(self) -> None:
        from pycozo.client import Client  # noqa: PLC0415 — тяжёлый импорт только при реальном старте

        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._db = Client("rocksdb", self._db_path)
        existing = {r.get("name") for r in _records(self._db.run("::relations"))}
        if self._nodes not in existing:
            self._db.run(f":create {self._nodes} {{id: String => data: Json}}")
        if self._edges not in existing:
            self._db.run(f":create {self._edges} {{src: String, tgt: String => data: Json}}")
        if self._adj not in existing:
            self._db.run(f":create {self._adj} {{node: String, neighbor: String}}")
            # backfill from existing edges (both directions) — one-time index build for a pre-existing graph
            # (no-op on a fresh db; keeps adj consistent if edges predate the index).
            self._db.run(
                f"?[node, neighbor] := *{self._edges}{{src: node, tgt: neighbor}}\n"
                f"?[node, neighbor] := *{self._edges}{{src: neighbor, tgt: node}}\n"
                f":put {self._adj} {{node, neighbor}}"
            )
        logger.debug(
            f"[{self.workspace}] CozoDB graph ready: {self._nodes}/{self._edges}/{self._adj} ({self._db_path})"
        )

    async def finalize(self) -> None:
        self._db = None

    async def _run(self, q: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        # cozo-клиент синхронный → to_thread: не блокирует loop + конкурентно (cozo thread-safe, MVCC).
        res = await asyncio.to_thread(self._db.run, q, params or {})
        return _records(res)

    # ---- existence / read (singular) ----
    async def has_node(self, node_id: str) -> bool:
        return bool(await self._run(f"?[id] := *{self._nodes}{{id: $id}}", {"id": node_id}))

    async def get_node(self, node_id: str) -> dict[str, str] | None:
        rows = await self._run(f"?[data] := *{self._nodes}{{id: $id, data}}", {"id": node_id})
        return rows[0]["data"] if rows else None

    async def has_edge(self, source_node_id: str, target_node_id: str) -> bool:
        s, t = _canon(source_node_id, target_node_id)
        return bool(await self._run(f"?[src] := *{self._edges}{{src: $s, tgt: $t}}", {"s": s, "t": t}))

    async def get_edge(
        self, source_node_id: str, target_node_id: str
    ) -> dict[str, str] | None:
        s, t = _canon(source_node_id, target_node_id)
        rows = await self._run(
            f"?[data] := *{self._edges}{{src: $s, tgt: $t, data}}", {"s": s, "t": t}
        )
        return rows[0]["data"] if rows else None

    async def node_degree(self, node_id: str) -> int:
        # adj holds both directions → degree = count of neighbors, PK-prefix on node (O(degree), no scan).
        rows = await self._run(
            f"?[count(x)] := *{self._adj}{{node: $id, neighbor: x}}",
            {"id": node_id},
        )
        return int(rows[0]["count(x)"]) if rows else 0

    async def edge_degree(self, src_id: str, tgt_id: str) -> int:
        degs = await self.node_degrees_batch([src_id, tgt_id])
        return degs.get(src_id, 0) + degs.get(tgt_id, 0)

    async def get_node_edges(self, source_node_id: str) -> list[tuple[str, str]] | None:
        if not await self.has_node(source_node_id):
            return None
        # adj PK-prefix on node → O(degree); NetworkX-семантика: (source_node_id, neighbour).
        rows = await self._run(
            f"?[neighbor] := *{self._adj}{{node: $id, neighbor}}",
            {"id": source_node_id},
        )
        return [(source_node_id, r["neighbor"]) for r in rows]

    # ---- batch read (1 round-trip через input[..] <- $rows + semijoin) ----
    async def get_nodes_batch(self, node_ids: list[str]) -> dict[str, dict]:
        if not node_ids:
            return {}
        rows = await self._run(
            f"input[id] <- $ids\n?[id, data] := input[id], *{self._nodes}{{id, data}}",
            {"ids": [[i] for i in node_ids]},
        )
        return {r["id"]: r["data"] for r in rows}

    async def has_nodes_batch(self, node_ids: list[str]) -> set[str]:
        if not node_ids:
            return set()
        rows = await self._run(
            f"input[id] <- $ids\n?[id] := input[id], *{self._nodes}{{id}}",
            {"ids": [[i] for i in node_ids]},
        )
        return {r["id"] for r in rows}

    async def node_degrees_batch(self, node_ids: list[str]) -> dict[str, int]:
        if not node_ids:
            return {}
        rows = await self._run(
            f"input[id] <- $ids\n"
            f"?[id, count(x)] := input[id], *{self._adj}{{node: id, neighbor: x}}",
            {"ids": [[i] for i in node_ids]},
        )
        degs = {i: 0 for i in node_ids}  # узлы без рёбер не вернутся → дефолт 0
        for r in rows:
            degs_key = r["id"]
            degs_val = int(r["count(x)"])
            degs[degs_key] = degs_val
        return degs

    async def edge_degrees_batch(
        self, edge_pairs: list[tuple[str, str]]
    ) -> dict[tuple[str, str], int]:
        if not edge_pairs:
            return {}
        endpoints = {n for pair in edge_pairs for n in pair}
        degs = await self.node_degrees_batch(list(endpoints))
        return {(a, b): degs.get(a, 0) + degs.get(b, 0) for a, b in edge_pairs}

    async def get_edges_batch(
        self, pairs: list[dict[str, str]]
    ) -> dict[tuple[str, str], dict]:
        if not pairs:
            return {}
        # Запрос канонически; результат ключуем ОРИГИНАЛЬНЫМ (src,tgt) как ждёт lightrag.
        canon_rows = [list(_canon(p["src"], p["tgt"])) for p in pairs]
        rows = await self._run(
            f"input[s, t] <- $pairs\n?[s, t, data] := input[s, t], *{self._edges}{{src: s, tgt: t, data}}",
            {"pairs": canon_rows},
        )
        by_canon = {(r["s"], r["t"]): r["data"] for r in rows}
        out: dict[tuple[str, str], dict] = {}
        for p in pairs:
            data = by_canon.get(_canon(p["src"], p["tgt"]))
            if data is not None:
                out[(p["src"], p["tgt"])] = data
        return out

    async def get_nodes_edges_batch(
        self, node_ids: list[str]
    ) -> dict[str, list[tuple[str, str]]]:
        if not node_ids:
            return {}
        rows = await self._run(
            f"input[id] <- $ids\n"
            f"?[id, neighbor] := input[id], *{self._adj}{{node: id, neighbor}}",
            {"ids": [[i] for i in node_ids]},
        )
        out: dict[str, list[tuple[str, str]]] = {i: [] for i in node_ids}
        for r in rows:
            nid = r["id"]
            out[nid].append((nid, r["neighbor"]))
        return out

    # ---- write (lock-free MVCC; lightrag предварительно мёржит → :put = replace) ----
    async def upsert_node(self, node_id: str, node_data: dict[str, str]) -> None:
        await self._run(
            f"?[id, data] <- [[$id, $data]] :put {self._nodes} {{id => data}}",
            {"id": node_id, "data": dict(node_data)},
        )

    async def upsert_nodes_batch(self, nodes: list[tuple[str, dict[str, str]]]) -> None:
        if not nodes:
            return
        await self._run(
            f"?[id, data] <- $rows :put {self._nodes} {{id => data}}",
            {"rows": [[nid, dict(data)] for nid, data in nodes]},
        )

    async def upsert_edge(
        self, source_node_id: str, target_node_id: str, edge_data: dict[str, str]
    ) -> None:
        s, t = _canon(source_node_id, target_node_id)
        # one transaction: canonical edge data + both adj directions.
        await self._run(
            f"{{?[src, tgt, data] <- [[$s, $t, $d]] :put {self._edges} {{src, tgt => data}}}}\n"
            f"{{?[node, neighbor] <- [[$s, $t], [$t, $s]] :put {self._adj} {{node, neighbor}}}}",
            {"s": s, "t": t, "d": dict(edge_data)},
        )

    async def upsert_edges_batch(
        self, edges: list[tuple[str, str, dict[str, str]]]
    ) -> None:
        if not edges:
            return
        rows = []
        adj_rows = []
        for src, tgt, data in edges:
            s, t = _canon(src, tgt)
            rows.append([s, t, dict(data)])
            adj_rows.append([s, t])
            adj_rows.append([t, s])
        await self._run(
            f"{{?[src, tgt, data] <- $rows :put {self._edges} {{src, tgt => data}}}}\n"
            f"{{?[node, neighbor] <- $adj :put {self._adj} {{node, neighbor}}}}",
            {"rows": rows, "adj": adj_rows},
        )

    # ---- delete (atomic multi-block: rm инцидентных рёбер + rm узла одним run()) ----
    async def delete_node(self, node_id: str) -> None:
        # O(degree), not O(edges): adj{node:$id} (PK-prefix) gives neighbours, then rm incident edges by
        # explicit key — both (id,n) and (n,id); the non-canonical one is a no-op rm (verified safe in cozo).
        # Single multi-block run() = one transaction. (Old form scanned edges/adj via (src=$id or tgt=$id):
        # 31+42 ms @20k edges → flat ~5 ms here.)
        await self._run(
            f"{{\n"
            f"  nbr[n] := *{self._adj}{{node: $id, neighbor: n}}\n"
            f"  ?[src, tgt] := nbr[n], src = $id, tgt = n\n"
            f"  ?[src, tgt] := nbr[n], src = n, tgt = $id\n"
            f"  :rm {self._edges} {{src, tgt}}\n"
            f"}}\n"
            f"{{\n"
            f"  nbr[n] := *{self._adj}{{node: $id, neighbor: n}}\n"
            f"  ?[node, neighbor] := nbr[n], node = $id, neighbor = n\n"
            f"  ?[node, neighbor] := nbr[n], node = n, neighbor = $id\n"
            f"  :rm {self._adj} {{node, neighbor}}\n"
            f"}}\n"
            f"{{ ?[id] <- [[$id]] :rm {self._nodes} {{id}} }}",
            {"id": node_id},
        )

    async def remove_nodes(self, nodes: list[str]) -> None:
        for n in nodes:
            await self.delete_node(n)

    async def remove_edges(self, edges: list[tuple[str, str]]) -> None:
        if not edges:
            return
        pairs = []
        adj_pairs = []
        for a, b in edges:
            s, t = _canon(a, b)
            pairs.append([s, t])
            adj_pairs.append([s, t])
            adj_pairs.append([t, s])
        await self._run(
            f"{{?[src, tgt] <- $rows :rm {self._edges} {{src, tgt}}}}\n"
            f"{{?[node, neighbor] <- $adj :rm {self._adj} {{node, neighbor}}}}",
            {"rows": pairs, "adj": adj_pairs},
        )

    # ---- labels / bulk (lightrag WebUI; FSM почти не зовёт) ----
    async def get_all_labels(self) -> list[str]:
        rows = await self._run(f"?[id] := *{self._nodes}{{id}}")
        return sorted(str(r["id"]) for r in rows)

    async def get_popular_labels(self, limit: int = 300) -> list[str]:
        labels = await self.get_all_labels()
        if not labels:
            return []
        degs = await self.node_degrees_batch(labels)
        return sorted(labels, key=lambda lbl: degs.get(lbl, 0), reverse=True)[:limit]

    async def search_labels(self, query: str, limit: int = 50) -> list[str]:
        q = (query or "").lower().strip()
        if not q:
            return []
        labels = await self.get_all_labels()
        return sorted(lbl for lbl in labels if q in lbl.lower())[:limit]

    async def get_all_nodes(self) -> list[dict]:
        rows = await self._run(f"?[id, data] := *{self._nodes}{{id, data}}")
        out = []
        for r in rows:
            d = dict(r["data"] or {})
            d["id"] = r["id"]
            out.append(d)
        return out

    async def get_all_edges(self) -> list[dict]:
        rows = await self._run(f"?[src, tgt, data] := *{self._edges}{{src, tgt, data}}")
        out = []
        for r in rows:
            d = dict(r["data"] or {})
            d["source"] = r["src"]
            d["target"] = r["tgt"]
            out.append(d)
        return out

    async def get_knowledge_graph(
        self, node_label: str, max_depth: int = 3, max_nodes: int = None
    ) -> KnowledgeGraph:
        # WebUI-функция (FSM не зовёт): "*" → весь граф; иначе стартовый узел + 1-hop соседи (без глубокой
        # рекурсии — Cozo-рекурсия требует отдельного синтаксиса, для нашего использования избыточна).
        cap = max_nodes or 1000
        all_nodes = await self.get_all_nodes()
        if node_label == "*":
            nodes = all_nodes
        else:
            by_id = {n["id"]: n for n in all_nodes}
            keep = {node_label}
            incident = await self.get_nodes_edges_batch([node_label])
            for _src, other in incident.get(node_label, []):
                keep.add(other)
            nodes = [by_id[i] for i in keep if i in by_id]
        truncated = len(nodes) > cap
        nodes = nodes[:cap]
        ids = {n["id"] for n in nodes}
        kg_nodes = [
            KnowledgeGraphNode(
                id=n["id"], labels=[n["id"]], properties={k: v for k, v in n.items() if k != "id"}
            )
            for n in nodes
        ]
        kg_edges = []
        for e in await self.get_all_edges():
            if e["source"] in ids and e["target"] in ids:
                kg_edges.append(
                    KnowledgeGraphEdge(
                        id=f"{e['source']}-{e['target']}",
                        type=str(e.get("keywords") or e.get("type") or ""),
                        source=e["source"],
                        target=e["target"],
                        properties={k: v for k, v in e.items() if k not in ("source", "target")},
                    )
                )
        return KnowledgeGraph(nodes=kg_nodes, edges=kg_edges, is_truncated=truncated)

    # ---- lifecycle ----
    async def index_done_callback(self) -> None:
        return None  # CozoDB/RocksDB персистит на :put (WAL) — отложенного flush нет

    async def drop(self) -> dict[str, str]:
        try:
            for rel in (self._adj, self._edges, self._nodes):
                try:
                    await asyncio.to_thread(self._db.run, f"::remove {rel}")
                except Exception:  # noqa: BLE001 — нет relation = уже чисто
                    pass
            self._db.run(f":create {self._nodes} {{id: String => data: Json}}")
            self._db.run(f":create {self._edges} {{src: String, tgt: String => data: Json}}")
            self._db.run(f":create {self._adj} {{node: String, neighbor: String}}")
            return {"status": "success", "message": "graph dropped"}
        except Exception as e:  # noqa: BLE001
            logger.error(f"[{self.workspace}] CozoDB drop failed: {e}")
            return {"status": "error", "message": str(e)}
