"""Pydantic schemas for AI-generated payloads."""

from __future__ import annotations

from typing import List

from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    """Reject unknown keys to enforce strict response schemas."""

    model_config = ConfigDict(extra="forbid")


class LenientModel(BaseModel):
    """Ignore unknown keys from external APIs that may add fields."""

    model_config = ConfigDict(extra="ignore")


class ItemEnrichmentResponse(StrictModel):
    """Single-row enrichment response from AI cleaning."""

    name_cleaned: str = ""
    description_cleaned: str = ""
    cleaned_brand: str = ""
    standardized_pack_size: str = ""
    clarity_rating: int = 0
    clarity_category: str = ""


class BatchEnrichmentItem(LenientModel):
    """Single item in a batch enrichment response."""

    id: int
    name_cleaned: str = ""
    description_cleaned: str = ""
    cleaned_brand: str = ""
    standardized_pack_size: str = ""
    clarity_rating: int = 0
    clarity_category: str = ""


class BatchEnrichmentResponse(LenientModel):
    """Batch enrichment response containing multiple items."""

    items: List[BatchEnrichmentItem]


class TaxonomyRequestItem(StrictModel):
    """Payload item sent to external taxonomy API."""

    item_name: str
    item_description: str


class TaxonomyResponseItem(LenientModel):
    """Single taxonomy row returned by external taxonomy API."""

    category: str | None = ""
    subcategory: str | None = ""
    subsubcategory: str | None = ""
    query_name: str | None = None
