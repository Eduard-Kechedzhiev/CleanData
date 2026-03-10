"""Compute summary statistics from pipeline output for the results page."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from app.domain.job_models import JobSummary


def _safe_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _is_gtin_truthy(val: Any) -> bool:
    """Check if a gtin_exists value is actually truthy after CSV round-tripping."""
    if pd.isna(val):
        return False
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s not in ("", "0", "false", "no", "none", "nan", "n/a")


def compute_summary(job_id: str, output_path: Path) -> JobSummary:
    """Read the pipeline output CSV and compute summary stats."""
    df = pd.read_csv(output_path, low_memory=False)

    row_count = len(df)
    column_count = len(df.columns)

    # --- Quality score ---
    avg_quality = None
    quality_dist: List[Dict[str, Any]] = []
    if "clarity_rating" in df.columns:
        scores = pd.to_numeric(df["clarity_rating"], errors="coerce").dropna()
        if len(scores) > 0:
            avg_quality = round(float(scores.mean()), 1)
            bins = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)]
            for lo, hi in bins:
                count = int(((scores >= lo) & (scores <= hi)).sum())
                label = f"{lo}-{hi}"
                quality_dist.append({"score": label, "count": count})

    # --- Brands ---
    brands_extracted = 0
    top_brands: List[Dict[str, Any]] = []
    if "cleaned_brand" in df.columns:
        brand_series = df["cleaned_brand"].dropna().astype(str)
        brand_series = brand_series[brand_series.str.strip() != ""]
        brands_extracted = int(brand_series.nunique())
        top = brand_series.value_counts().head(10)
        top_brands = [{"name": str(name), "count": int(cnt)} for name, cnt in top.items()]

    # --- GTINs ---
    gtins_found = 0
    gtins_total = 0
    if "gtin_exists" in df.columns:
        gtin_col = df["gtin_exists"]
        gtins_total = int(gtin_col.notna().sum())
        # After CSV round-tripping, boolean values become strings like "True"/"False".
        # Using .astype(bool) on strings is wrong: bool("False") == True.
        gtins_found = int(gtin_col.apply(_is_gtin_truthy).sum())

    # --- Categories ---
    category_breakdown: List[Dict[str, Any]] = []
    if "gtin_category" in df.columns:
        cat_series = df["gtin_category"].dropna().astype(str)
        cat_series = cat_series[cat_series.str.strip() != ""]
        top_cats = cat_series.value_counts().head(10)
        category_breakdown = [{"name": str(name), "count": int(cnt)} for name, cnt in top_cats.items()]

    # --- Sample rows (before/after) ---
    sample_rows: List[Dict[str, Any]] = []
    # Pick rows that have cleaned data to showcase the transformation
    original_name_col = None
    for candidate in df.columns:
        if candidate.lower() in ("name", "display_name", "product_name", "item_name", "description"):
            original_name_col = candidate
            break
    if original_name_col is None:
        # Use first non-pipeline column
        pipeline_cols = {
            "Name_cleaned", "Description_cleaned", "cleaned_brand",
            "Pack_cleaned", "Size_cleaned", "clarity_rating", "clarity_category",
            "gtin_category", "gtin_subcategory", "gtin_subsubcategory",
            "gtin_exists", "gtin_query_name", "ai_decision", "ai_confidence", "ai_reasoning",
        }
        for col in df.columns:
            if col not in pipeline_cols:
                original_name_col = col
                break

    if original_name_col and "Name_cleaned" in df.columns:
        # Pick up to 8 rows with good quality scores for the showcase
        showcase = df.copy()
        if "clarity_rating" in showcase.columns:
            showcase["_score"] = pd.to_numeric(showcase["clarity_rating"], errors="coerce")
            showcase = showcase.dropna(subset=["_score"]).sort_values("_score", ascending=False)
        for _, row in showcase.head(8).iterrows():
            pack = str(row.get("Pack_cleaned", "") or "")
            size = str(row.get("Size_cleaned", "") or "")
            pack_display = pack or size
            sample_rows.append({
                "original": str(row.get(original_name_col, "")),
                "cleaned": str(row.get("Name_cleaned", "")),
                "brand": str(row.get("cleaned_brand", "")),
                "pack": pack_display,
                "category": " > ".join(filter(None, [
                    str(row.get("gtin_category", "") or ""),
                    str(row.get("gtin_subcategory", "") or ""),
                    str(row.get("gtin_subsubcategory", "") or ""),
                ])),
                "score": _safe_float(row.get("clarity_rating", 0)),
            })

    return JobSummary(
        job_id=job_id,
        row_count=row_count,
        column_count=column_count,
        avg_quality_score=avg_quality,
        quality_distribution=quality_dist,
        brands_extracted=brands_extracted,
        top_brands=top_brands,
        gtins_found=gtins_found,
        gtins_total=gtins_total,
        category_breakdown=category_breakdown,
        sample_rows=sample_rows,
    )
