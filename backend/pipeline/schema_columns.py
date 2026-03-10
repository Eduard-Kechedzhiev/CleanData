"""Column constants and normalization helpers for compatibility."""

from __future__ import annotations

from typing import Any, Dict


CATEGORY_OUTPUT_COLUMNS = [
    "gtin_category",
    "gtin_subcategory",
    "gtin_subsubcategory",
]

GTIN_STATUS_COLUMNS = [
    "gtin_exists",
    "gtin_query_name",
    "ai_decision",
    "ai_confidence",
    "ai_reasoning",
]

GTIN_OUTPUT_COLUMNS = [
    "gtin_exists",
    *CATEGORY_OUTPUT_COLUMNS,
    "gtin_query_name",
    "ai_decision",
    "ai_confidence",
    "ai_reasoning",
]

CLEANING_OUTPUT_COLUMNS = [
    "Name_cleaned",
    "Description_cleaned",
    "cleaned_brand",
    "Pack_cleaned",
    "Size_cleaned",
]

CLARITY_OUTPUT_COLUMNS = [
    "clarity_rating",
    "clarity_category",
]


def normalize_taxonomy_record(record: Dict[str, Any] | None) -> Dict[str, str]:
    """Normalize taxonomy/search keys from mixed provider shapes."""
    rec = record or {}

    category = (
        rec.get("category")
        or rec.get("Taxo1")
        or rec.get("level1")
        or rec.get("gtin_category")
        or ""
    )
    subcategory = (
        rec.get("subcategory")
        or rec.get("Taxo2")
        or rec.get("level2")
        or rec.get("gtin_subcategory")
        or ""
    )
    subsubcategory = (
        rec.get("subsubcategory")
        or rec.get("Taxo3")
        or rec.get("level3")
        or rec.get("gtin_subsubcategory")
        or ""
    )
    query_name = (
        rec.get("query_name")
        or rec.get("product_name")
        or rec.get("gtin_query_name")
        or ""
    )

    return {
        "category": str(category),
        "subcategory": str(subcategory),
        "subsubcategory": str(subsubcategory),
        "query_name": str(query_name),
    }
