"""MongoDB provider wrapper with normalized output keys."""

from __future__ import annotations

import contextlib
import io
from typing import Dict, List

from ..config import (
    MONGODB_COLLECTION,
    MONGODB_DATABASE,
    MONGODB_USERNAME,
    default_cert_bundle_path,
)
from ..errors import ProviderError
from ..legacy.gtin_validation.mongodb_lookup import MongoDBGTINLookup
from ..schema_columns import normalize_taxonomy_record


def _quiet_call(fn, *args, **kwargs):
    """Call *fn* while suppressing stdout from legacy print() calls."""
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*args, **kwargs)


class MongoDBProvider:
    """DocumentDB lookup wrapper."""

    def __init__(self) -> None:
        cert_bundle = default_cert_bundle_path().resolve()
        if not cert_bundle.exists():
            raise ProviderError(f"Certificate bundle not found: {cert_bundle}")

        self.client = _quiet_call(
            MongoDBGTINLookup,
            database_name=MONGODB_DATABASE,
            collection_name=MONGODB_COLLECTION,
            username=MONGODB_USERNAME,
            cert_bundle_path=str(cert_bundle),
        )

    def connect(self) -> None:
        if not _quiet_call(self.client.connect):
            raise ProviderError("Could not connect to MongoDB DocumentDB")

    def disconnect(self) -> None:
        _quiet_call(self.client.disconnect)

    def batch_lookup(self, gtins: List[str]) -> Dict[str, Dict[str, object]]:
        """Return normalized lookup records keyed by GTIN."""
        try:
            raw = _quiet_call(self.client.batch_query_gtins, gtins)
        except Exception as exc:
            raise ProviderError(f"MongoDB batch lookup failed: {exc}") from exc
        normalized: Dict[str, Dict[str, object]] = {}

        for gtin, (exists, info) in raw.items():
            if not exists:
                normalized[gtin] = {
                    "exists": False,
                    "category": "",
                    "subcategory": "",
                    "subsubcategory": "",
                    "query_name": "",
                    "confidence": 0.0,
                }
                continue

            taxonomy = info.get("taxonomy", {})
            search = info.get("search", {})
            product_info = info.get("product_info", {})
            rec = normalize_taxonomy_record(
                {
                    "category": taxonomy.get("category") or taxonomy.get("level1"),
                    "subcategory": taxonomy.get("subcategory") or taxonomy.get("level2"),
                    "subsubcategory": taxonomy.get("subsubcategory") or taxonomy.get("level3"),
                    "query_name": search.get("query_name") or search.get("product_name") or product_info.get("product_name"),
                }
            )
            rec["exists"] = True
            rec["confidence"] = float(search.get("confidence", 0.8) or 0.8)
            rec["level1"] = rec["category"]
            rec["level2"] = rec["subcategory"]
            rec["level3"] = rec["subsubcategory"]
            normalized[gtin] = rec

        return normalized
