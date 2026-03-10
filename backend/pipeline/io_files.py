"""Input/output helpers for CSV/XLSX processing."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .errors import ConfigError

_GTIN_COLUMN_NAME_RE = re.compile(
    r"^(?:gtin(?:[-_]?14)?|upc(?:[123])?|ean(?:[-_]?13)?|barcode)$",
    re.IGNORECASE,
)


def _is_gtin_like_column(column_name: object) -> bool:
    normalized = str(column_name or "").strip().strip('"').strip("'")
    return bool(_GTIN_COLUMN_NAME_RE.fullmatch(normalized))


def _build_gtin_dtype_map(columns: list[object]) -> dict[str, str]:
    dtype_map: dict[str, str] = {}
    for col in columns:
        if _is_gtin_like_column(col):
            dtype_map[str(col)] = "string"
    return dtype_map


def _looks_gtin_like_value(value: object) -> bool:
    if pd.isna(value):
        return False
    raw = str(value).strip()
    if not raw or raw.casefold() in {"nan", "none", "<na>"}:
        return False
    if raw[0] in "+-":
        return False
    if re.search(r"[a-zA-Z]", raw):
        return False
    digits = re.sub(r"[^\d]", "", raw)
    return digits.isdigit() and 8 <= len(digits) <= 14


def _infer_gtin_candidate_columns(sample_df: pd.DataFrame) -> dict[str, str]:
    dtype_map = _build_gtin_dtype_map(list(sample_df.columns))

    for col in sample_df.columns:
        col_name = str(col)
        if col_name in dtype_map:
            continue
        series = sample_df[col]
        total = 0
        gtin_like = 0
        for value in series:
            text = str(value or "").strip()
            if not text:
                continue
            total += 1
            if _looks_gtin_like_value(text):
                gtin_like += 1
        if total > 0 and (gtin_like / total) > 0.3:
            dtype_map[col_name] = "string"

    return dtype_map


def _sniff_gtin_dtype_map_csv(path: Path, sep: str) -> dict[str, str]:
    try:
        sample_df = pd.read_csv(path, sep=sep, nrows=200, dtype=str, keep_default_na=False)
    except Exception:
        try:
            header = pd.read_csv(path, nrows=0, sep=sep)
            return _build_gtin_dtype_map(list(header.columns))
        except Exception:
            return {}
    return _infer_gtin_candidate_columns(sample_df)


def _sniff_gtin_converter_map_excel(path: Path) -> dict[str, object]:
    try:
        sample_df = pd.read_excel(path, nrows=200, dtype=str, keep_default_na=False)
    except Exception:
        try:
            header = pd.read_excel(path, nrows=0)
            candidate_columns = _build_gtin_dtype_map(list(header.columns))
        except Exception:
            candidate_columns = {}
    else:
        candidate_columns = _infer_gtin_candidate_columns(sample_df)

    return {column: _gtin_converter for column in candidate_columns}


def _gtin_converter(value: object) -> str:
    """Convert a cell value to string, handling NaN."""
    if pd.isna(value):
        return ""
    return str(value).strip()


def read_dataframe(path: Path, row_limit: int | None = None) -> pd.DataFrame:
    """Read CSV/XLSX with robust defaults and optional row limit."""
    if not path.exists():
        raise ConfigError(f"Input file not found: {path}")

    ext = path.suffix.lower()
    if ext in {".csv", ".tsv"}:
        sep = "\t" if ext == ".tsv" else ","
        # Sniff a small all-string sample so GTIN-like columns keep leading zeros
        # even when the column name is non-standard and later auto-detected.
        dtype_map = _sniff_gtin_dtype_map_csv(path, sep)
        try:
            df = pd.read_csv(path, sep=sep, dtype=dtype_map or None)
        except Exception:
            try:
                df = pd.read_csv(path, sep=sep, dtype=dtype_map or None, engine="python")
            except Exception as exc:
                raise ConfigError(f"Failed to read delimited file {path.name}: {exc}") from exc
    elif ext in {".xlsx", ".xls"}:
        try:
            converters = _sniff_gtin_converter_map_excel(path) or None
            df = pd.read_excel(path, converters=converters)
        except Exception as exc:
            raise ConfigError(f"Failed to read Excel file {path.name}: {exc}") from exc
    else:
        raise ConfigError(f"Unsupported input format: {ext}. Use CSV/TSV/XLSX/XLS")

    if row_limit and row_limit > 0 and len(df) > row_limit:
        df = df.head(row_limit)

    return df


def write_dataframe(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to CSV/XLSX based on output extension."""
    path.parent.mkdir(parents=True, exist_ok=True)
    ext = path.suffix.lower()

    if ext == ".csv":
        df.to_csv(path, index=False)
    elif ext in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
    else:
        raise ConfigError(f"Unsupported output format: {ext}. Use CSV/XLSX/XLS")
