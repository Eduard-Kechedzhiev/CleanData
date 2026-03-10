"""Perplexity fallback provider — self-contained, no legacy imports."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

import requests
from requests import RequestException

from ..errors import ProviderError

logger = logging.getLogger(__name__)

_NEGATIVE_INDICATORS = frozenset([
    # Explicit "nothing" phrases
    "not found", "no product", "no information", "unknown", "invalid",
    "doesn't exist", "does not exist", "no results", "no data",
    "could not find", "unable to find", "no match",
    "do not provide any direct information",
    "does not include a lookup", "no product database entry",
    # Availability / inability phrases
    "not available", "is not available", "no longer available",
    "cannot find", "i cannot find", "i could not find",
    "no exact match", "no specific product", "no specific information",
    "unable to locate", "unable to identify", "unable to determine",
    "no details", "no record", "no entry",
    # GTIN-explanation-only responses
    "explain what a gtin is", "how to find or verify gtin",
    "what is a gtin", "gtin stands for",
])

# Secondary: if the "product_name" line matches these patterns, it's not real product info.
_EXPLANATION_KEYWORDS = frozenset([
    "gtin is", "what is a gtin", "gtin stands for",
    "how to find", "how to verify", "where to obtain",
    "gtin database", "gtin tools", "barcode lookup",
])


class PerplexityProvider:
    """Perplexity GTIN search — calls the Perplexity chat API directly."""

    def __init__(self) -> None:
        self.api_key: str = os.getenv("PERPLEXITY_API_KEY", "")
        if not self.api_key:
            logger.warning("PERPLEXITY_API_KEY not set — Perplexity fallback disabled")

    def search_by_gtin(self, gtin: str) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        try:
            return self._search(gtin)
        except ProviderError:
            raise
        except RequestException as exc:
            raise ProviderError(f"Perplexity request failed for GTIN {gtin}: {exc}") from exc
        except Exception as exc:
            raise ProviderError(f"Perplexity lookup failed for GTIN {gtin}: {exc}") from exc

    def _search(self, gtin: str) -> Optional[Dict[str, Any]]:
        query = (
            f"Search for product information, product name, brand, description, "
            f"and size/weight for the product with barcode/GTIN {gtin}. "
            f"Look in product databases, retail catalogs, and manufacturer websites. "
            f"If you cannot find specific product details, please indicate that no "
            f"product information was found."
        )

        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "sonar",
                "messages": [{"role": "user", "content": query}],
                "max_tokens": 1000,
                "temperature": 0.1,
            },
            timeout=30,
        )

        if resp.status_code != 200:
            logger.warning("perplexity_api_error", extra={"status": resp.status_code, "gtin": gtin})
            raise ProviderError(f"Perplexity API returned HTTP {resp.status_code} for GTIN {gtin}")

        content: str = resp.json()["choices"][0]["message"]["content"]
        return self._parse_response(content)

    @staticmethod
    def _parse_response(content: str) -> Optional[Dict[str, Any]]:
        """Extract product info from Perplexity text response."""
        content_lower = content.lower()

        if any(ind in content_lower for ind in _NEGATIVE_INDICATORS):
            return None

        # Basic line-by-line extraction
        product_name = ""
        description = ""
        brand = ""
        size = ""

        for line in content.split("\n"):
            ll = line.lower()
            if any(kw in ll for kw in ("product", "item", "name")) and not product_name:
                product_name = line.strip()
            elif any(kw in ll for kw in ("brand", "manufacturer", "company")) and not brand:
                brand = line.strip()
            elif any(kw in ll for kw in ("size", "weight", "grams", "ounces", "kg", "lb")) and not size:
                size = line.strip()
            elif len(line.strip()) > 20 and not description:
                description = line.strip()

        if not product_name and not description:
            return None

        # Reject if extracted "product_name" is really GTIN explanation text
        if product_name:
            pn_lower = product_name.lower()
            if any(kw in pn_lower for kw in _EXPLANATION_KEYWORDS):
                return None

        return {
            "product_name": product_name,
            "description": description,
            "brand": brand,
            "size": size,
            "source": "Perplexity API",
        }
