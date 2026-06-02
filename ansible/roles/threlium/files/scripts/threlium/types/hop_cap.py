"""Hop-budget (wire-строки) + арифметика hop-стека через VO (docs/TYPES.md § hop-budget)."""
from __future__ import annotations

from email.message import EmailMessage
from typing import Self

from ._core import _OptionalStripEmpty, _OptionalStripNone
from threlium.mail_header_names import MailHeaderName


class HopTailToken(_OptionalStripEmpty):
    """Один токен hop-стека после ``strip`` (целое: остаток уровня).

    Граница «строка → int»: парсинг/инкремент только здесь, чтобы стадии/билдеры не
    делали ``split()``/``int()`` вручную (TYPES Level 2).
    """

    def as_int(self) -> int:
        try:
            return int(self.value)
        except ValueError as exc:
            raise RuntimeError(
                f"X-Threlium-Hop-Budget token is not an integer: {self.value!r}"
            ) from exc

    @classmethod
    def from_int(cls, n: int) -> Self:
        return cls.parse(str(n))


class HopBudgetLine(_OptionalStripEmpty):
    """Строка ``X-Threlium-Hop-Budget``: стек целых токенов (хвост = текущий уровень).

    Арифметика стека (``advance_simple_step`` / ``push_subagent`` / ``remaining``) — методы
    VO; дефолты уровней (``budget_root`` / ``budget_sub``) приходят из ``ThreliumSettings``
    через тонкие обёртки в ``threlium.fsm_emit`` (types не зависит от settings).
    """

    def hop_tokens(self) -> list[HopTailToken]:
        return [HopTailToken.parse(t) for t in self.value.split()] if self.value else []

    @classmethod
    def from_tokens(cls, tokens: list[HopTailToken]) -> Self:
        return cls.parse(" ".join(t.value for t in tokens))

    def advance_simple_step(self, *, root_default: int) -> HopBudgetLine:
        """Декремент хвоста: ``'48 44'`` → ``'48 43'``, ``'47'`` → ``'46'``."""
        tokens = self.hop_tokens() or [HopTailToken.from_int(root_default)]
        tokens[-1] = HopTailToken.from_int(max(0, tokens[-1].as_int() - 1))
        return HopBudgetLine.from_tokens(tokens)

    def push_subagent(self, *, root_default: int, sub_max: int) -> HopBudgetLine | None:
        """PUSH: декремент хвоста + ``append(sub_max)``. ``None`` если хвост после step < 1."""
        tokens = self.hop_tokens() or [HopTailToken.from_int(root_default)]
        new_tail = tokens[-1].as_int() - 1
        if new_tail < 1:
            return None
        tokens[-1] = HopTailToken.from_int(new_tail)
        tokens.append(HopTailToken.from_int(sub_max))
        return HopBudgetLine.from_tokens(tokens)

    def remaining(self, *, root_default: int) -> int:
        """Оставшийся бюджет текущего уровня (хвост стека). ``0`` = исчерпан."""
        tokens = self.hop_tokens()
        if not tokens:
            return root_default
        return max(0, tokens[-1].as_int())

    @classmethod
    def parse_from_email(
        cls,
        msg: EmailMessage,
        header_name: str = MailHeaderName.HOP_BUDGET.value,
    ) -> Self:
        """``EmailMessage.get`` + present-or-None; отсутствие → пустая wire-строка."""
        wire = cls.parse_present_from_email(msg, header_name)
        return wire if wire is not None else cls.parse(None)


class XThreliumHopBudgetHeaderWireOptional(_OptionalStripNone):
    """Необязательное значение ``X-Threlium-Hop-Budget`` (reasoning / fatigue)."""
