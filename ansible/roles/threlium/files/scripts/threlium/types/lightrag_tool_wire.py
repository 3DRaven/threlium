"""Wire VO результата фазы LightRAG (до снятия в ``str`` для библиотеки)."""
from __future__ import annotations

from ._core import _OptionalStripEmpty, _RequiredNonEmpty


class LightragTupleDelimiterWire(_RequiredNonEmpty):
    """Delimiter полей entity/relation (по умолчанию ``<|#|>``)."""


class LightragCompletionDelimiterWire(_RequiredNonEmpty):
    """Маркер завершения extraction (по умолчанию ``<|COMPLETE|>``)."""


class LightragExtractionDelimiterText(_OptionalStripEmpty):
    """Delimiter-текст для ``operate._process_extraction_result``."""


class LightragKeywordsJsonText(_OptionalStripEmpty):
    """JSON-string keywords для ``get_keywords_from_query``."""


class LightragEntitySummaryText(_OptionalStripEmpty):
    """Plain summary для summarize entity."""


class LightragRagAnswerText(_OptionalStripEmpty):
    """Plain answer для RAG response."""


__all__ = [
    "LightragCompletionDelimiterWire",
    "LightragEntitySummaryText",
    "LightragExtractionDelimiterText",
    "LightragKeywordsJsonText",
    "LightragRagAnswerText",
    "LightragTupleDelimiterWire",
]
