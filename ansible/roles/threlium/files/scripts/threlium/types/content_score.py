"""VO базового скоринга ``<history>``-части: заголовок ``X-Threlium-Content-Score``.

Скоринг отправителя: источник (``formal_reason``, ``ingress``, …) проставляет на
свою ``<history>``-часть базовый вес из настроек (``settings.history.score_for``).
Потребитель (``enrich``/scoring/reasoning) домножает на позиционные множители
(recency/size). Wire — строка-число после strip; числовой доступ — метод-VO
``as_score()`` (граница типобезопасна, без голых ``float`` в стадиях).

Прецедент: ``HopBudgetLine`` / ``ThreliumCapabilitiesBudgetLine`` (строковый wire +
доменные методы), ``IrtHashWire`` (кодек только внутри VO).
"""
from __future__ import annotations

import math
from typing import Self

from ._core import _OptionalStripEmpty

# Дефолт при отсутствии/непарсимости заголовка: нейтральный вес, чтобы часть не
# исчезала из контекста из-за кривого заголовка (граница не валит весь сбор).
_FALLBACK_SCORE: float = 1.0


class ThreliumContentScoreWire(_OptionalStripEmpty):
    """Wire ``X-Threlium-Content-Score`` (строка-число) после strip.

    ``from_score`` — единственная точка форматирования float → wire; ``as_score`` —
    обратный разбор с безопасным fallback на нейтральный вес.
    """

    @classmethod
    def from_score(cls, score: float) -> Self:
        """Базовый вес ``float`` → wire (``repr`` без экспоненты для типичных весов)."""
        if not math.isfinite(score):
            raise ValueError(f"ThreliumContentScoreWire.from_score: not finite: {score!r}")
        s = max(0.0, float(score))
        return cls(value=f"{s:g}")

    def as_score(self) -> float:
        """Числовой вес для скоринга; пусто/непарсимо/неконечно → нейтральный fallback."""
        raw = self.value.strip()
        if not raw:
            return _FALLBACK_SCORE
        try:
            v = float(raw)
        except ValueError:
            return _FALLBACK_SCORE
        if not math.isfinite(v) or v < 0.0:
            return _FALLBACK_SCORE
        return v
