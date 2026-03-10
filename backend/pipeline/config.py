"""Typed runtime configuration and provider defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


# Provider defaults — all overridable via environment variables.
EXTERNAL_TAXONOMY_API_URL = os.getenv(
    "TAXONOMY_API_URL",
    "https://salt-dev-ai-service-api.greedyants.com/api/v1/validation/taxonomize_items",
)
MONGODB_HOST = os.getenv(
    "MONGODB_HOST",
    "pepper-production-docdb.cluster-cxciwycm3oeg.us-east-1.docdb.amazonaws.com",
)
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "salt")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "ds_gtin_metadata")
MONGODB_USERNAME = os.getenv("MONGODB_USERNAME", "engineering")


@dataclass(frozen=True)
class RunConfig:
    input_path: Path
    output_path: Path
    row_limit: Optional[int]
    description_col: Optional[str]
    brand_col: Optional[str]
    packsize_col: Optional[str]
    chunk_size: int
    workers: int
    fail_fast: bool
    json_logs: bool
    cleaning_batch_size: int = 10
    enable_perplexity_fallback: bool = True
    progress_reporter: Any = None


def project_root() -> Path:
    """Resolve project root for `data_cleaning_v2`."""
    return Path(__file__).resolve().parents[1]


def default_cert_bundle_path() -> Path:
    """Default TLS bundle path used by MongoDB provider."""
    return project_root() / "global-bundle.pem"


def default_taxonomy_csv_path() -> Path:
    """Location of SALT taxonomy CSV used for taxonomy validation."""
    return project_root() / "data" / "SALT Taxonomy.csv"
