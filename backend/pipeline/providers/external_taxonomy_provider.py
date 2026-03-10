"""External taxonomy API provider with schema validation and retries."""

from __future__ import annotations

import logging
import os
import random
import time
from typing import List, Sequence

import requests

from ..config import EXTERNAL_TAXONOMY_API_URL
from ..errors import ProviderError
from ..schema_models import TaxonomyRequestItem, TaxonomyResponseItem

logger = logging.getLogger(__name__)


class ExternalTaxonomyProvider:
    """Calls external taxonomy endpoint using strict payload/response schemas."""

    def __init__(
        self,
        request_url: str = EXTERNAL_TAXONOMY_API_URL,
        batch_size: int = 100,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:
        env_timeout = os.getenv("TAXONOMY_REQUEST_TIMEOUT_SECONDS", "").strip()
        resolved_timeout = timeout_seconds
        if env_timeout:
            try:
                parsed = float(env_timeout)
                if parsed > 0:
                    resolved_timeout = parsed
            except Exception:
                pass

        self.request_url = request_url
        self.batch_size = max(1, int(batch_size))
        self.timeout_seconds = float(resolved_timeout)
        self.max_retries = max(1, int(max_retries))
        self.backoff_seconds = max(0.0, float(backoff_seconds))

    def _post_batch(self, payload: List[dict[str, str]]) -> List[TaxonomyResponseItem]:
        response = requests.post(self.request_url, json=payload, timeout=self.timeout_seconds)
        response.raise_for_status()
        raw = response.json()
        if not isinstance(raw, list):
            raise ProviderError(f"Taxonomy API expected a list response, got: {type(raw).__name__}")
        if len(raw) != len(payload):
            raise ProviderError(
                f"Taxonomy API response length mismatch: expected {len(payload)}, got {len(raw)}"
            )

        parsed: List[TaxonomyResponseItem] = []
        for idx, row in enumerate(raw):
            try:
                parsed.append(TaxonomyResponseItem.model_validate(row))
            except Exception as exc:
                raise ProviderError(f"Invalid taxonomy response row at index {idx}: {exc}") from exc
        return parsed

    def categorize(self, items: Sequence[TaxonomyRequestItem]) -> List[TaxonomyResponseItem]:
        if not items:
            return []

        payload = [item.model_dump() for item in items]
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return self._post_batch(payload)
            except Exception as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                sleep_seconds = self.backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, self.backoff_seconds)
                logger.warning(
                    "external_taxonomy_retry",
                    extra={
                        "attempt": attempt,
                        "max_retries": self.max_retries,
                        "sleep_seconds": sleep_seconds,
                        "error": str(exc),
                    },
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        raise ProviderError(f"External taxonomy provider failed after {self.max_retries} attempts: {last_error}")
