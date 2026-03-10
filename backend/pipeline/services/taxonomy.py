"""Taxonomy stage service: external-only classification with strict cleaned-input contract."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, List, Sequence, Tuple

import pandas as pd

from ..errors import StageError
from ..providers.external_taxonomy_provider import ExternalTaxonomyProvider
from ..schema_columns import CATEGORY_OUTPUT_COLUMNS
from ..schema_models import TaxonomyRequestItem, TaxonomyResponseItem

logger = logging.getLogger(__name__)

_REQUIRED_INPUT_COLUMNS = ("Name_cleaned", "Description_cleaned")
_EMPTY_LIKE = {"", "n/a", "na", "none", "null", "nan"}


def _normalize_taxonomy_value(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.casefold() in _EMPTY_LIKE:
        return ""
    return text


class TaxonomyHierarchyValidator:
    """Validates and canonicalizes Level I/II/III values from SALT taxonomy CSV."""

    def __init__(self, csv_path: Path) -> None:
        if not csv_path.exists():
            raise StageError(f"Taxonomy CSV not found: {csv_path}")
        df = pd.read_csv(csv_path)
        required = {"Level I", "Level II", "Level III"}
        if not required.issubset(df.columns):
            raise StageError(
                f"Taxonomy CSV missing required columns: {sorted(required - set(df.columns))}"
            )

        self.level1: Dict[str, str] = {}
        self.level2: Dict[Tuple[str, str], str] = {}
        self.level3: Dict[Tuple[str, str, str], str] = {}

        for _, row in df.iterrows():
            raw_l1 = _normalize_taxonomy_value(row.get("Level I"))
            raw_l2 = _normalize_taxonomy_value(row.get("Level II"))
            raw_l3 = _normalize_taxonomy_value(row.get("Level III"))
            if not raw_l1:
                continue

            l1_key = raw_l1.casefold()
            self.level1[l1_key] = raw_l1

            if raw_l2:
                l2_key = (l1_key, raw_l2.casefold())
                self.level2[l2_key] = raw_l2

            if raw_l2 and raw_l3:
                l3_key = (l1_key, raw_l2.casefold(), raw_l3.casefold())
                self.level3[l3_key] = raw_l3

    def canonicalize(
        self,
        category: str,
        subcategory: str,
        subsubcategory: str,
    ) -> tuple[str, str, str, bool]:
        c1 = _normalize_taxonomy_value(category)
        c2 = _normalize_taxonomy_value(subcategory)
        c3 = _normalize_taxonomy_value(subsubcategory)

        if not c1 and not c2 and not c3:
            return "", "", "", True
        if not c1:
            return "", "", "", False

        c1_key = c1.casefold()
        canonical_l1 = self.level1.get(c1_key)
        if not canonical_l1:
            return "", "", "", False

        if not c2 and not c3:
            return canonical_l1, "", "", True

        if not c2:
            return "", "", "", False

        l2_key = (c1_key, c2.casefold())
        canonical_l2 = self.level2.get(l2_key)
        if not canonical_l2:
            return "", "", "", False

        if not c3:
            return canonical_l1, canonical_l2, "", True

        l3_key = (c1_key, canonical_l2.casefold(), c3.casefold())
        canonical_l3 = self.level3.get(l3_key)
        if not canonical_l3:
            return "", "", "", False

        return canonical_l1, canonical_l2, canonical_l3, True


class TaxonomyService:
    """External-only taxonomy classification over cleaned fields."""

    def __init__(
        self,
        project_root: Path,
        batch_size: int = 100,
        provider: ExternalTaxonomyProvider | None = None,
        progress_callback: Callable[[int, int, str | None], None] | None = None,
    ) -> None:
        self.project_root = project_root
        self.batch_size = max(1, int(batch_size))
        taxonomy_csv = self.project_root / "data" / "SALT Taxonomy.csv"
        self.hierarchy = TaxonomyHierarchyValidator(taxonomy_csv)
        self.provider = provider or ExternalTaxonomyProvider(batch_size=self.batch_size)
        self.progress_callback = progress_callback

    def _validate_required_columns(self, df: pd.DataFrame) -> None:
        missing = [col for col in _REQUIRED_INPUT_COLUMNS if col not in df.columns]
        if missing:
            raise StageError(
                f"Taxonomy stage requires cleaned columns {list(_REQUIRED_INPUT_COLUMNS)}; missing: {missing}"
            )

    def _initialize_output_columns(self, df: pd.DataFrame) -> None:
        for col in CATEGORY_OUTPUT_COLUMNS:
            df[col] = ""

    def _build_requests(self, df: pd.DataFrame) -> tuple[List[int], List[TaxonomyRequestItem]]:
        row_indices: List[int] = []
        items: List[TaxonomyRequestItem] = []

        for row_idx, row in df.iterrows():
            name = _normalize_taxonomy_value(row.get("Name_cleaned", ""))
            description = _normalize_taxonomy_value(row.get("Description_cleaned", ""))
            if not name and not description:
                continue
            items.append(TaxonomyRequestItem(item_name=name, item_description=description))
            row_indices.append(int(row_idx))

        return row_indices, items

    def _apply_batch(
        self,
        out: pd.DataFrame,
        batch_row_indices: Sequence[int],
        batch_results: Sequence[TaxonomyResponseItem],
    ) -> None:
        if len(batch_row_indices) != len(batch_results):
            raise StageError(
                f"Taxonomy batch result mismatch: expected {len(batch_row_indices)}, got {len(batch_results)}"
            )

        for row_idx, rec in zip(batch_row_indices, batch_results):
            raw_category = _normalize_taxonomy_value(rec.category)
            raw_subcategory = _normalize_taxonomy_value(rec.subcategory)
            raw_subsubcategory = _normalize_taxonomy_value(rec.subsubcategory)

            category, subcategory, subsubcategory, valid = self.hierarchy.canonicalize(
                raw_category,
                raw_subcategory,
                raw_subsubcategory,
            )
            if not valid:
                logger.warning(
                    "taxonomy_invalid_hierarchy",
                    extra={
                        "row_idx": row_idx,
                        "category": raw_category,
                        "subcategory": raw_subcategory,
                        "subsubcategory": raw_subsubcategory,
                    },
                )
                continue

            out.at[row_idx, "gtin_category"] = category
            out.at[row_idx, "gtin_subcategory"] = subcategory
            out.at[row_idx, "gtin_subsubcategory"] = subsubcategory

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        self._validate_required_columns(out)
        self._initialize_output_columns(out)

        row_indices, items = self._build_requests(out)
        if not items:
            logger.info("taxonomy_stage_skipped", extra={"reason": "no_non_empty_cleaned_rows"})
            if self.progress_callback:
                self.progress_callback(len(out), len(out), "No non-empty cleaned rows")
            return out

        skipped_rows = len(out) - len(items)
        try:
            for start in range(0, len(items), self.batch_size):
                end = min(start + self.batch_size, len(items))
                batch_items = items[start:end]
                batch_row_indices = row_indices[start:end]
                batch_results = self.provider.categorize(batch_items)
                self._apply_batch(out, batch_row_indices, batch_results)
                if self.progress_callback:
                    batch_num = (start // self.batch_size) + 1
                    total_batches = (len(items) + self.batch_size - 1) // self.batch_size
                    completed_rows = skipped_rows + end
                    self.progress_callback(
                        completed_rows,
                        len(out),
                        f"Processed batch {batch_num}/{total_batches}",
                    )
        except Exception as exc:
            raise StageError(f"Taxonomy stage failed: {exc}") from exc

        logger.info(
            "taxonomy_stage_complete",
            extra={
                "rows": len(out),
                "classified_rows": len(items),
                "batch_size": self.batch_size,
            },
        )
        return out
