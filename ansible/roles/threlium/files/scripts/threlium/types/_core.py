"""Приватное ядро VO: ``NonEmptyStr``, ``_kv_*``, базы ``_*`` (не в ``threlium.types.__all__``)."""
from __future__ import annotations

from email.message import EmailMessage
from enum import StrEnum, nonmember
from typing import Annotated, Protocol, Self, TypeAlias, TypeVar

import msgspec
import notmuch2  # pyright: ignore[reportMissingImports]

from threlium.logutil import logger


class _ToolFunctionLike(Protocol):
    """Структурная форма ``litellm`` ``Function``: только ``.name``/``.arguments``."""

    name: str | None
    arguments: str


class ChatCompletionMessageToolCall(Protocol):
    """Структурный контракт tool-call (``.function.name``/``.arguments``).

    Аннотация по shape вместо ``from litellm.types.utils import …``: тот импорт тянет
    весь ``litellm`` (~1.5 c) в фундаментальный ``_core``, который грузится всем доменом.
    Тело ``parse_tool_call`` использует только ``tc.function``/``func.name`` (duck-typing);
    реальный litellm-объект совместим структурно. См. ``threlium.enginewire``, ``docs/TYPES.md``.
    """

    @property
    def function(self) -> _ToolFunctionLike | None: ...

NonEmptyStr: TypeAlias = Annotated[str, msgspec.Meta(min_length=1)]

log = logger.bind(component=__name__)

_PayloadT = TypeVar("_PayloadT")


def parse_json_payload(
    text: str, payload_type: type[_PayloadT], *, log_ctx: str
) -> _PayloadT | None:
    """Wire JSON → msgspec Struct ``payload_type``; невалидный JSON/схема → ``None`` + warning.

    Уровень 1 (``docs/TYPES.md``): единый разбор ``<system>``-payload стадий через
    ``msgspec.json.decode`` (одним шагом decode + validate). ``log_ctx`` — префикс
    стадии для лога при провале.
    """
    try:
        return msgspec.json.decode(text.encode("utf-8"), type=payload_type)
    except (msgspec.DecodeError, msgspec.ValidationError) as e:
        log.warning("parse_json_payload_failed", ctx=log_ctx, err=str(e))
        return None


class ToolFunctionNameBase(StrEnum):
    """База single-tool ``function.name`` enum (``docs/TYPES.md`` § tool bridge).

    Подклассы перечисляют допустимые имена tool и через :func:`enum.nonmember`
    переопределяют ``_bridge_error`` (тип доменного исключения) и ``_label``
    (префикс домена для сообщений). ``parse_tool_call`` / ``assert_matches`` —
    единые: разбор ``tc.function.name`` против фиксированного набора с
    fail-fast доменной ошибкой. Для multi-tool роутера reasoning (динамический
    local-part) применяется отдельный ``ReasoningToolFunctionName`` — by design.
    """

    _bridge_error = nonmember(RuntimeError)
    _label = nonmember("tool")

    @classmethod
    def parse_tool_call(cls, tc: ChatCompletionMessageToolCall) -> Self:
        func = tc.function
        if func is None or not func.name:
            raise cls._bridge_error("tool_call without function.name")
        raw = func.name.strip()
        try:
            return cls(raw)
        except ValueError as exc:
            raise cls._bridge_error(
                f"unknown {cls._label} tool function.name={raw!r}"
            ) from exc

    def assert_matches(self, expected: Self) -> None:
        if self != expected:
            raise type(self)._bridge_error(
                f"expected tool {expected.value!r}, got {self.value!r}"
            )


def _kv_dict(value: str | None, key: str) -> dict[str, str]:
    d: dict[str, str] = {}
    if value is not None:
        t = str(value).strip()
        if t:
            d[key] = t
    return d


def _reject_embedded_crlf_after_strip(s: str) -> str:
    """Fail-fast: в однострочном RFC822-заголовке не допускаются ``\\r``/``\\n`` после strip."""
    t = str(s).strip()
    if "\r" in t or "\n" in t:
        raise ValueError(f"embedded CRLF in single-line header value: {t!r}")
    return t


def _kv_dict_lower(value: str | None, key: str) -> dict[str, str]:
    """Как :func:`_kv_dict`, но после ``strip`` применяется ``lower`` (параметры запросов)."""
    d: dict[str, str] = {}
    if value is not None:
        t = str(value).strip().lower()
        if t:
            d[key] = t
    return d


class _OptionalStripEmpty(msgspec.Struct, frozen=True, kw_only=True):
    """Strip → непустое в dict; иначе ключ отсутствует → ``value`` по умолчанию ``\"\"``."""

    value: str = ""

    @classmethod
    def parse(cls, raw: str | None) -> Self:
        return msgspec.convert(_kv_dict(raw, "value"), type=cls)

    @classmethod
    def parse_present_optional(cls, raw: str | None) -> Self | None:
        """Отсутствие / strip-пусто → ``None``; иначе wire с **непустым** ``value`` (present-or-None)."""
        if raw is None:
            return None
        t = str(raw).strip()
        if not t:
            return None
        out = msgspec.convert(_kv_dict(t, "value"), type=cls)
        if not out.value:
            return None
        return out

    @classmethod
    def parse_present_from_email(cls, msg: EmailMessage, header_name: str) -> Self | None:
        """``EmailMessage.get`` + present-or-None (strip); нет заголовка / пусто → ``None``."""
        val = msg.get(header_name)
        if val is None:
            return None
        return cls.parse_present_optional(str(val))

    @classmethod
    def parse_present_from_nm_message(cls, msg: notmuch2.Message, header_name: str) -> Self | None:
        """``notmuch2.Message.header`` + present-or-None (``LookupError`` как отсутствие заголовка)."""
        try:
            raw = msg.header(header_name)
        except LookupError:
            return None
        return cls.parse_present_optional(str(raw))


class _SingleLineHeaderWire(_OptionalStripEmpty):
    """Subject и прочие однострочные заголовки: strip + reject встроенных CRLF (без replace/slice)."""

    @classmethod
    def parse(cls, raw: str | None) -> Self:
        if raw is None:
            return cls()
        t = _reject_embedded_crlf_after_strip(str(raw))
        return msgspec.convert(_kv_dict(t, "value"), type=cls)

    @classmethod
    def parse_present_optional(cls, raw: str | None) -> Self | None:
        if raw is None:
            return None
        t = str(raw).strip()
        if not t:
            return None
        t = _reject_embedded_crlf_after_strip(t)
        out = msgspec.convert(_kv_dict(t, "value"), type=cls)
        if not out.value:
            return None
        return out


class _OptionalStripLowerEmpty(msgspec.Struct, frozen=True, kw_only=True):
    """Strip + ``lower`` → непустое в dict; иначе ключ отсутствует → ``value`` по умолчанию ``\"\"``."""

    value: str = ""

    @classmethod
    def parse(cls, raw: str | None) -> Self:
        return msgspec.convert(_kv_dict_lower(raw, "value"), type=cls)

    @classmethod
    def parse_present_optional(cls, raw: str | None) -> Self | None:
        if raw is None:
            return None
        t = str(raw).strip().lower()
        if not t:
            return None
        out = msgspec.convert(_kv_dict_lower(t, "value"), type=cls)
        if not out.value:
            return None
        return out

    @classmethod
    def parse_present_from_email(cls, msg: EmailMessage, header_name: str) -> Self | None:
        """Как у :meth:`_OptionalStripEmpty.parse_present_from_email`, с ``strip`` + ``lower``."""

        val = msg.get(header_name)
        if val is None:
            return None
        return cls.parse_present_optional(str(val))

    @classmethod
    def parse_present_from_nm_message(cls, msg: notmuch2.Message, header_name: str) -> Self | None:
        """Как у :meth:`_OptionalStripEmpty.parse_present_from_nm_message`, с ``strip`` + ``lower``."""

        try:
            raw = msg.header(header_name)
        except LookupError:
            return None
        return cls.parse_present_optional(str(raw))


class _OptionalStripNone(msgspec.Struct, frozen=True, kw_only=True):
    """Strip → непустое в dict; иначе ``value is None``."""

    value: str | None = None

    @classmethod
    def parse(cls, raw: str | None) -> Self:
        return msgspec.convert(_kv_dict(raw, "value"), type=cls)


class _RequiredNonEmpty(msgspec.Struct, frozen=True, kw_only=True):
    """Обязательная непустая строка после strip → ``ValueError``."""

    value: NonEmptyStr

    @classmethod
    def require(cls, *, name: str, raw: str | None) -> Self:
        try:
            return msgspec.convert(_kv_dict(raw, "value"), type=cls)
        except msgspec.ValidationError as e:
            raise ValueError(f"{name}: missing or empty after strip") from e


