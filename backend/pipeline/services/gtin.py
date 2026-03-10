"""GTIN validation/enrichment stage with normalized key mapping."""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

from ..errors import StageError
from ..providers.mongodb_provider import MongoDBProvider
from ..providers.perplexity_provider import PerplexityProvider
from ..schema_columns import CATEGORY_OUTPUT_COLUMNS, GTIN_STATUS_COLUMNS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Standalone GTIN utilities (extracted from legacy pipeline to avoid importing
# google.generativeai and the print-heavy legacy code).
# ---------------------------------------------------------------------------

_GTIN_NAME_PATTERNS = [
    r'upcode', r'unit.*code', r'case.*code',
    r'^gtin$', r'^g\.t\.i\.n$', r'^global.*trade.*item.*number$',
    r'^barcode$', r'^bar.*code$', r'^bar-code$',
    r'^ean$', r'^e\.a\.n$', r'^european.*article.*number$',
    r'^upc$', r'^u\.p\.c$', r'^universal.*product.*code$',
]


def _normalize_gtin(gtin: str) -> str:
    """Normalise a raw GTIN string, rejecting values that are clearly not barcodes.

    Accepts purely numeric strings and common formatting artifacts (spaces,
    hyphens between digit groups, trailing ``.0`` from float coercion).
    Rejects strings containing letters or leading sign characters so that
    alphanumeric SKUs (``SKU-ABC-12345678-Z``) and signed numbers
    (``-12345678``) are not silently coerced into valid-looking GTINs.
    """
    if pd.isna(gtin) or gtin == '':
        return ""
    raw = str(gtin).strip()
    # Reject if the value contains any letter — it's an alphanumeric ID, not a barcode.
    if re.search(r'[a-zA-Z]', raw):
        return ""
    # Reject leading sign characters (e.g. "-12345678" from negative numbers).
    if raw and raw[0] in '+-':
        return ""
    return re.sub(r'[^\d]', '', raw)


def _validate_gtin_format(gtin: str) -> bool:
    """Return True if *gtin* looks like a valid 8-14 digit barcode."""
    if pd.isna(gtin) or gtin == '':
        return False
    clean = _normalize_gtin(gtin)
    return clean.isdigit() and 8 <= len(clean) <= 14


def _categorize_perplexity_product(product_info: Dict[str, str]) -> Optional[str]:
    """Rough category bucket for a Perplexity-sourced product."""
    try:
        text = (
            (product_info.get('product_name') or '')
            + ' ' + (product_info.get('description') or '')
        ).lower()
        if any(w in text for w in ('food', 'snack', 'beverage', 'drink', 'meal')):
            return 'Food & Beverage'
        if any(w in text for w in ('cleaning', 'soap', 'detergent', 'cleaner')):
            return 'Cleaning & Household'
        if any(w in text for w in ('paper', 'tissue', 'napkin', 'towel')):
            return 'Paper & Disposables'
        if any(w in text for w in ('equipment', 'tool', 'machine', 'device')):
            return 'Equipment & Supplies'
        return 'Other'
    except Exception:
        return 'Uncategorized'


def _auto_detect_gtin_column(df: pd.DataFrame) -> str:
    """Detect a GTIN column by header patterns then data analysis. No AI/Gemini."""
    columns = list(df.columns)
    columns_lower = [str(col).lower().strip() for col in columns]

    # Pass 1 — header pattern matching
    for pattern in _GTIN_NAME_PATTERNS:
        for col, col_lower in zip(columns, columns_lower):
            if re.search(pattern, col_lower):
                logger.info("gtin_column_detected", extra={"column": col, "method": "pattern"})
                return col

    # Pass 2 — data analysis (look for columns where >30% values are 8-14 digit numbers)
    best_column = None
    best_score = 0.0
    for col in columns:
        sample = df[col].dropna().astype(str).head(100)
        total = 0
        gtin_like = 0
        for val in sample:
            clean = _normalize_gtin(val)
            total += 1
            if clean and clean.isdigit() and 8 <= len(clean) <= 14:
                gtin_like += 1
        if total > 0:
            score = gtin_like / total
            if score > best_score and score > 0.3:
                best_score = score
                best_column = col

    if best_column:
        logger.info("gtin_column_detected", extra={"column": best_column, "method": "data_analysis", "score": f"{best_score:.2f}"})
        return best_column

    raise ValueError("GTIN column not found — no column matches barcode patterns or data characteristics.")


class GTINService:
    """Run GTIN validation and optional Perplexity fallback."""

    perplexity_workers: int = 10

    def __init__(
        self,
        enable_perplexity_fallback: bool = True,
        perplexity_workers: int = 10,
        progress_callback: Callable[[int, int, str | None], None] | None = None,
    ) -> None:
        self.enable_perplexity_fallback = enable_perplexity_fallback
        self.perplexity_workers = max(1, int(perplexity_workers))
        self.progress_callback = progress_callback

    @staticmethod
    def _find_casefold_columns(df: pd.DataFrame, candidates: Sequence[str]) -> List[str]:
        by_casefold = {str(col).strip().casefold(): str(col) for col in df.columns}
        found: List[str] = []
        for candidate in candidates:
            resolved = by_casefold.get(candidate.casefold())
            if resolved and resolved not in found:
                found.append(resolved)
        return found

    def _resolve_gtin_columns(self, df: pd.DataFrame, gtin_column: Optional[str]) -> List[str]:
        if gtin_column:
            if gtin_column not in df.columns:
                raise StageError(f"GTIN column not found: {gtin_column}")
            return [gtin_column]

        gtin_cols = self._find_casefold_columns(df, ("gtin_14", "gtin14", "gtin-14", "gtin"))
        upc_cols = self._find_casefold_columns(df, ("upc1", "upc2", "upc3", '"upc1"', '"upc2"', '"upc3"'))
        preferred = gtin_cols + [col for col in upc_cols if col not in gtin_cols]
        if preferred:
            return preferred

        detected = _auto_detect_gtin_column(df)
        if str(detected).strip().casefold() in {"external_item_id", "item_id"}:
            raise StageError(
                "Auto-detected GTIN column looks like an item identifier, not a barcode. "
                "Rename the column or verify the data contains barcodes."
            )
        return [detected]

    @staticmethod
    def _coerce_gtin_cell(value: object) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        text = str(value).strip()
        if not text or text.casefold() in {"nan", "none", "<na>"}:
            return ""
        if text.isdigit():
            return text

        if text.endswith(".0"):
            whole = text[:-2].strip()
            if whole.isdigit():
                return whole

        numeric_like = re.fullmatch(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", text)
        if numeric_like:
            try:
                dec = Decimal(text)
                normalized = format(dec, "f")
                if "." in normalized:
                    normalized = normalized.rstrip("0").rstrip(".")
                return normalized
            except (InvalidOperation, ValueError):
                return text
        return text

    def _initialize_output_columns(self, df: pd.DataFrame) -> None:
        for col in CATEGORY_OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = None

        for col in GTIN_STATUS_COLUMNS:
            if col == "gtin_exists":
                df[col] = False
            elif col == "ai_confidence":
                df[col] = 0.0
            elif col == "gtin_query_name":
                df[col] = ""
            elif col == "ai_decision":
                df[col] = ""
            elif col == "ai_reasoning":
                df[col] = ""
            else:
                df[col] = None

    def _collect_gtins(
        self, df: pd.DataFrame, gtin_columns: List[str]
    ) -> Tuple[List[str], List[Dict[str, str]], Set[int]]:
        all_valid_gtins = set()
        row_gtin_refs: List[Dict[str, str]] = []
        valid_row_indices: Set[int] = set()

        for col in gtin_columns:
            clean_col = f"{col}_clean"
            valid_col = f"{col}_valid"
            df[clean_col] = df[col].apply(self._coerce_gtin_cell).apply(_normalize_gtin)
            df[valid_col] = df[clean_col].apply(_validate_gtin_format)

            valid_gtins_col = df[df[valid_col]][clean_col].dropna().unique().tolist()
            all_valid_gtins.update(valid_gtins_col)

            for idx, row in df.iterrows():
                if row[valid_col] and row[clean_col]:
                    valid_row_indices.add(int(idx))
                    row_gtin_refs.append({"row_idx": idx, "gtin": row[clean_col], "column": col})

        return list(all_valid_gtins), row_gtin_refs, valid_row_indices

    def _apply_salt_result(self, df: pd.DataFrame, row_idx: int, rec: Dict[str, object]) -> None:
        confidence = float(rec.get("confidence", 0.8) or 0.8)
        query_name = str(rec.get("query_name", "") or "")

        df.at[row_idx, "gtin_exists"] = True
        df.at[row_idx, "gtin_category"] = rec.get("category")
        df.at[row_idx, "gtin_subcategory"] = rec.get("subcategory")
        df.at[row_idx, "gtin_subsubcategory"] = rec.get("subsubcategory")
        df.at[row_idx, "gtin_query_name"] = query_name
        df.at[row_idx, "ai_decision"] = "SALT Database"
        df.at[row_idx, "ai_confidence"] = confidence
        df.at[row_idx, "ai_reasoning"] = f"Product found in SALT database: {query_name or 'Unknown'}"

    def _apply_perplexity_result(self, df: pd.DataFrame, row_idx: int, gtin: str, product_info: Dict[str, str]) -> None:
        category = _categorize_perplexity_product(product_info)
        df.at[row_idx, "gtin_exists"] = True
        df.at[row_idx, "gtin_category"] = category
        df.at[row_idx, "gtin_subcategory"] = None
        df.at[row_idx, "gtin_subsubcategory"] = None
        df.at[row_idx, "gtin_query_name"] = product_info.get("product_name")
        df.at[row_idx, "ai_decision"] = "Perplexity API"
        df.at[row_idx, "ai_confidence"] = 0.8
        df.at[row_idx, "ai_reasoning"] = f"Product found via Perplexity API for GTIN {gtin}"

    def _mark_lookup_error_rows(
        self,
        df: pd.DataFrame,
        lookup_errors: Dict[int, str],
    ) -> None:
        if not lookup_errors:
            return

        for row_idx, error in lookup_errors.items():
            df.at[row_idx, "ai_decision"] = "GTIN Lookup Error"
            df.at[row_idx, "ai_confidence"] = 0.0
            df.at[row_idx, "ai_reasoning"] = error[:500]

    def _mark_unresolved_rows(
        self,
        df: pd.DataFrame,
        valid_row_indices: Set[int],
    ) -> None:
        missing_mask = df["ai_decision"].isna() | df["ai_decision"].astype(str).str.strip().eq("")
        valid_mask = pd.Series(False, index=df.index)
        if valid_row_indices:
            valid_mask.loc[list(valid_row_indices)] = True

        no_gtin_mask = missing_mask & ~valid_mask
        unresolved_mask = missing_mask & valid_mask

        df.loc[no_gtin_mask, "ai_decision"] = "No GTINs Found"
        df.loc[no_gtin_mask, "ai_confidence"] = 0.0
        df.loc[no_gtin_mask, "ai_reasoning"] = "No valid GTINs found in configured GTIN columns"

        df.loc[unresolved_mask, "ai_decision"] = "GTIN Not Found"
        df.loc[unresolved_mask, "ai_confidence"] = 0.0
        df.loc[unresolved_mask, "ai_reasoning"] = "No product match found in SALT database or Perplexity fallback"

    def _process_pass(
        self,
        df: pd.DataFrame,
        gtin_columns: List[str],
        mongo: MongoDBProvider,
        perplexity: Optional[PerplexityProvider],
        perplexity_cache: Optional[Dict[str, Optional[Dict[str, str]]]] = None,
    ) -> pd.DataFrame:
        """Process GTINs in two passes: SALT first (all columns), then Perplexity for remaining."""
        out = df.copy()
        valid_gtins, row_refs, valid_row_indices = self._collect_gtins(out, gtin_columns)
        cache = perplexity_cache if perplexity_cache is not None else {}

        if not valid_gtins:
            self._mark_unresolved_rows(out, set())
            temp_cols = [c for c in out.columns if c.endswith("_clean") or c.endswith("_valid")]
            if temp_cols:
                out = out.drop(columns=temp_cols)
            return out

        lookup = mongo.batch_lookup(valid_gtins)

        # Pass 1: Apply all SALT matches across all GTIN columns.
        # This ensures a SALT match in column B isn't missed because column A was checked first.
        for row_ref in row_refs:
            row_idx = row_ref["row_idx"]
            gtin = row_ref["gtin"]

            if bool(out.at[row_idx, "gtin_exists"]):
                continue

            rec = lookup.get(gtin)
            if rec and rec.get("exists"):
                self._apply_salt_result(out, row_idx, rec)

        # Pass 2: Perplexity fallback only for rows still unresolved after all SALT lookups.
        lookup_error_gtins: Dict[str, str] = {}
        lookup_error_rows: Dict[int, str] = {}
        if perplexity:
            # Collect unique unresolved GTINs
            unresolved_refs: List[Dict[str, str]] = []
            unresolved_gtins: Set[str] = set()
            for row_ref in row_refs:
                row_idx = row_ref["row_idx"]
                gtin = row_ref["gtin"]
                if bool(out.at[row_idx, "gtin_exists"]):
                    continue
                unresolved_refs.append(row_ref)
                if gtin not in cache:
                    unresolved_gtins.add(gtin)

            # Parallel Perplexity lookups for uncached GTINs
            if unresolved_gtins:
                logger.info(
                    "perplexity_parallel_start",
                    extra={"gtins": len(unresolved_gtins), "workers": self.perplexity_workers},
                )

                def _lookup(gtin: str) -> Tuple[str, Optional[Dict[str, str]], Optional[str]]:
                    try:
                        return gtin, perplexity.search_by_gtin(gtin), None
                    except Exception as exc:
                        logger.warning("perplexity_lookup_failed", extra={"gtin": gtin, "error": str(exc)})
                        return gtin, None, str(exc)

                with ThreadPoolExecutor(max_workers=self.perplexity_workers) as executor:
                    futures = {executor.submit(_lookup, g): g for g in unresolved_gtins}
                    done_count = 0
                    for future in as_completed(futures):
                        gtin, product_info, error = future.result()
                        if product_info is not None:
                            cache[gtin] = product_info
                        elif error is None:
                            cache[gtin] = None
                        elif error:
                            lookup_error_gtins[gtin] = error
                        done_count += 1
                        if done_count % 50 == 0 or done_count == len(unresolved_gtins):
                            logger.info(
                                "perplexity_parallel_progress",
                                extra={"done": done_count, "total": len(unresolved_gtins)},
                            )

            # Apply cached results (re-check gtin_exists since a prior ref may have resolved this row)
            for row_ref in unresolved_refs:
                row_idx = row_ref["row_idx"]
                if bool(out.at[row_idx, "gtin_exists"]):
                    continue
                gtin = row_ref["gtin"]
                product_info = cache.get(gtin)
                if product_info and product_info.get("product_name"):
                    self._apply_perplexity_result(out, row_idx, gtin, product_info)
                elif gtin in lookup_error_gtins:
                    lookup_error_rows[int(row_idx)] = (
                        f"Perplexity lookup failed for GTIN {gtin}: {lookup_error_gtins[gtin]}"
                    )

        self._mark_lookup_error_rows(out, lookup_error_rows)
        self._mark_unresolved_rows(out, valid_row_indices)

        temp_cols = [c for c in out.columns if c.endswith("_clean") or c.endswith("_valid")]
        if temp_cols:
            out = out.drop(columns=temp_cols)

        return out

    def run(self, df: pd.DataFrame, gtin_column: Optional[str], chunk_size: int) -> pd.DataFrame:
        """Run GTIN stage and return updated DataFrame."""
        out = df.copy()
        self._initialize_output_columns(out)
        try:
            gtin_columns = self._resolve_gtin_columns(out, gtin_column)
        except (StageError, ValueError) as exc:
            message = str(exc)
            if "GTIN column not found" in message:
                self._mark_unresolved_rows(out, set())
                logger.info("gtin_stage_skipped", extra={"reason": "no_gtin_column"})
                if self.progress_callback:
                    self.progress_callback(len(out), len(out), "No GTIN column found")
                return out
            raise
        use_chunking = bool(chunk_size and chunk_size > 0 and len(out) > chunk_size)

        mongo = MongoDBProvider()
        perplexity = PerplexityProvider() if self.enable_perplexity_fallback else None
        perplexity_cache: Dict[str, Optional[Dict[str, str]]] = {}

        try:
            mongo.connect()

            if use_chunking:
                chunks = []
                total_rows = len(out)
                for start in range(0, len(out), chunk_size):
                    end = min(start + chunk_size, len(out))
                    chunk = out.iloc[start:end].copy()
                    chunk_processed = self._process_pass(
                        chunk, gtin_columns, mongo, perplexity, perplexity_cache=perplexity_cache
                    )
                    chunks.append(chunk_processed)
                    if self.progress_callback:
                        chunk_num = (start // chunk_size) + 1
                        total_chunks = (len(out) + chunk_size - 1) // chunk_size
                        self.progress_callback(end, total_rows, f"Processed chunk {chunk_num}/{total_chunks}")
                out = pd.concat(chunks, axis=0).sort_index(kind="stable")
            else:
                out = self._process_pass(out, gtin_columns, mongo, perplexity, perplexity_cache=perplexity_cache)
                if self.progress_callback:
                    self.progress_callback(len(out), len(out), "Processed 1/1 chunks")

        except Exception as exc:
            raise StageError(f"GTIN stage failed: {exc}") from exc
        finally:
            try:
                mongo.disconnect()
            except Exception:
                pass

        logger.info(
            "gtin_stage_complete",
            extra={
                "rows": len(out),
                "gtin_hits": int(out["gtin_exists"].sum()) if "gtin_exists" in out.columns else 0,
                "chunk_size": chunk_size,
                "chunked": use_chunking,
            },
        )
        return out
