"""AI cleaning stage with batched inference for throughput."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, TypeVar

import pandas as pd
from pydantic import BaseModel
from tqdm.auto import tqdm

from ..errors import StageError
from ..schema_columns import CLARITY_OUTPUT_COLUMNS, CLEANING_OUTPUT_COLUMNS
from ..schema_models import BatchEnrichmentResponse, ItemEnrichmentResponse

logger = logging.getLogger(__name__)
SchemaModelT = TypeVar("SchemaModelT", bound=BaseModel)

_UNIT_ALIASES = {
    "COUNT": "CT",
    "COUNTS": "CT",
    "EACH": "EA",
    "LBS": "LB",
    "POUND": "LB",
    "POUNDS": "LB",
    "OUNCE": "OZ",
    "OUNCES": "OZ",
    "GRAM": "G",
    "GRAMS": "G",
    "KILOGRAM": "KG",
    "KILOGRAMS": "KG",
    "MILLILITER": "ML",
    "MILLILITERS": "ML",
    "LITER": "L",
    "LITERS": "L",
    "LT": "L",
    "INCH": "IN",
    "INCHES": "IN",
    "PINT": "PT",
    "PINTS": "PT",
    "QUART": "QT",
    "QUARTS": "QT",
    "GALLON": "GAL",
    "GALLONS": "GAL",
    "CS": "CASE",
    "CASES": "CASE",
    "BX": "BOX",
    "BOXES": "BOX",
    "PK": "PACK",
    "PACKS": "PACK",
}

# Mirrors legacy-normalized units and common container tokens seen in historical data.
_KNOWN_PACK_TOKENS = {
    "CT",
    "PCS",
    "PACK",
    "UNIT",
    "EA",
    "LB",
    "OZ",
    "G",
    "KG",
    "ML",
    "L",
    "IN",
    "PT",
    "QT",
    "GAL",
    "CASE",
    "FLAT",
    "BUSHEL",
    "BOX",
    "CTN",
    "BAG",
    "TRAY",
    "CAN",
    "BTL",
    "JAR",
}
_ADDED_OUTPUT_COLUMNS = CLEANING_OUTPUT_COLUMNS + CLARITY_OUTPUT_COLUMNS
_RESUME_ROW_COL = "__row_position"
_RESUME_FINGERPRINT_COL = "__row_fingerprint"
_MAX_DESCRIPTION_LEN = 240
_MAX_BRAND_LEN = 120
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\s,&'()./%:+\-#\"]*$")
_NAME_STRIP_PATTERNS = [
    re.compile(r"\b\d+(?:\.\d+)?\s*[xX]\s*\d+(?:\.\d+)?\b", re.IGNORECASE),
    re.compile(
        r"\b\d+(?:\.\d+)?\s*[xX]\s*\d+(?:\.\d+)?(?:/\d+(?:\.\d+)?)?(?:-\d+(?:\.\d+)?)?\s*"
        r"(?:CT|PCS|EA|UNIT|PACK|LB|OZ|G|KG|ML|L|IN|PT|QT|GAL)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b\d+(?:\.\d+)?(?:/\d+(?:\.\d+)?)?(?:-\d+(?:\.\d+)?)?\s*"
        r"(?:CT|PCS|EA|UNIT|PACK|LB|OZ|G|KG|ML|L|IN|PT|QT|GAL)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b\d+(?:\.\d+)?\s*[xX]\s*(?:CASE|BOX|PACK|FLAT|BUSHEL|CTN|BAG|TRAY|CAN|BTL|JAR)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:CASE|BOX|PACK|FLAT|BUSHEL|CTN|BAG|TRAY|CAN|BTL|JAR|CT|PCS|EA|UNIT|LB|OZ|G|KG|ML|L|IN|PT|QT|GAL)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b\d+(?:\.\d+)?(?:/\d+(?:\.\d+)?)?(?:-\d+(?:\.\d+)?)?\b", re.IGNORECASE),
]
_DESC_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\s,&'()./%:+\-#\"]{0,239}$")
_BRAND_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\s&'().\-]{0,119}$")
_FAMILY_HINT_MAP = {
    "PRODUCE": "Produce",
    "GROCERY": "Grocery",
    "DAIRY": "Dairy",
    "BEEF": "Beef",
    "PORK": "Pork",
    "POULTRY": "Poultry",
    "SEAFOOD": "Seafood",
    "PROCESSED MEAT": "Processed Meat",
    "CHEESE": "Cheese",
}
_PLACEHOLDER_BRANDS = {"PACKER", "N/A", "NA", "UNKNOWN", "UNBRANDED", "GENERIC"}
_BRAND_FROM_TEXT_PATTERN = re.compile(r"\bBrand\s*:\s*(.+)", re.IGNORECASE)
_PACK_SINGLE_PATTERN = re.compile(r"^\d+(?:\.\d+)?(?:-\d+(?:\.\d+)?)? [A-Z][A-Z0-9\-]{0,11}$")
_PACK_SINGLE_SLASH_PATTERN = re.compile(r"^\d+(?:\.\d+)?/\d+(?:\.\d+)? [A-Z][A-Z0-9\-]{0,11}$")
_PACK_MULTI_QTY_PATTERN = re.compile(
    r"^\d+(?:\.\d+)? x \d+(?:\.\d+)?(?:-\d+(?:\.\d+)?)? [A-Z][A-Z0-9\-]{0,11}$"
)
_PACK_MULTI_SLASH_PATTERN = re.compile(
    r"^\d+(?:\.\d+)? x \d+(?:\.\d+)?/\d+(?:\.\d+)? [A-Z][A-Z0-9\-]{0,11}$"
)
_PACK_MULTI_TOKEN_PATTERN = re.compile(r"^\d+(?:\.\d+)? x [A-Z][A-Z0-9\-]{0,11}$")

_VALID_CLARITY_CATEGORIES = {"Excellent", "Good", "Fair", "Poor", "Very Poor", "Extremely Poor"}


def _score_to_category(rating: int) -> str:
    if rating >= 9:
        return "Excellent"
    if rating >= 7:
        return "Good"
    if rating >= 5:
        return "Fair"
    if rating >= 3:
        return "Poor"
    if rating >= 1:
        return "Very Poor"
    return "Extremely Poor"


_AI_SYSTEM_PROMPT = """
You are an enterprise data-cleaning assistant for product catalog pipelines.
You will receive one or more product rows as JSON and must infer the most likely product details for each.
Always return strict JSON only, with no markdown, no extra keys, and no commentary.

If a row includes source_description, source_brand, or source_pack_size, treat those fields as the authoritative
source for description, brand, and pack/size respectively when those concepts are present.

Output rules:
1. name_cleaned:
- Canonical product title for downstream taxonomy/search.
- Build in this order: "<Family Hint?> <Brand?> <Product> <Variation/Key Details?>".
- Keep only product identity facts, remove operational/vendor noise.
- NEVER include pack/size/count info in the name:
  - no quantities
  - no units (CT/LB/OZ/ML/L/PT/QT/GAL/etc.)
  - no container terms (CASE/BOX/PACK/FLAT/BUSHEL/etc.)
  - no multipack expressions ("x")
- If brand is clearly available and useful in the title, include it; otherwise omit brand from title.
- Do not include placeholder/generic brands in title (for example "PACKER").
- Preserve product-family context when explicitly present in row data (for example PRODUCE/GROCERY/DAIRY),
  using normalized title case as a lightweight hint.
- Keep product-state qualifiers that affect identity (for example fresh/refrigerated/frozen/dry/dehydrated, sliced/diced/florets).
- Must be non-empty plain text.

2. description_cleaned:
- A concise, normalized product description.
- Keep core facts from the row text only. Do not infer category/product details not explicitly present.
- Remove noise and obvious duplicates.

3. cleaned_brand:
- Canonical brand string.
- If a field/value explicitly contains "Brand:" or a clear manufacturer token, use that exact brand (normalized spacing/case).
- Use title case when reasonable.
- If unknown, return empty string.

4. standardized_pack_size:
- Prefer explicit "Pack Size" values in the row when present.
- Return empty string if pack/size cannot be inferred from explicit row text.
- Use one of these formats:
  - "<quantity> <UNIT>" (example: "12 OZ")
  - "<outer_count>/<each_count> <UNIT>" (example: "24/5 CT")
  - "<count> x <quantity> <UNIT>" (example: "6 x 12 OZ")
  - "<count> x <outer_count>/<each_count> <UNIT>" (example: "10 x 24/5 CT")
  - "<count> x <PACKTYPE>" (example: "1 x CASE")
- Keep ranges when present (example: "7-10 CT", "1 x 24-30 CT").
- Slash semantics are strict and ONLY for nested package structure:
  - "24/5 CT" means 24 inner packs of 5 CT each.
  - "10 x 24/5 CT" means 10 outer cases, each case 24 inner packs of 5 CT each.
  - Do not use slash for ranges. Ranges must use "-" (example: "7-10 CT").
- Canonical token rules:
  - Use "CASE" (never "CS").
  - Use "BOX" (never "BX").
  - Use "PACK" (never "PK").
  - Use "L" for liters (never "LT").
- UNIT/PACKTYPE should be normalized abbreviations/tokens when possible (e.g., LB/OZ/ML/L/PT/QT/GAL/CT/EA/CASE/BOX/PACK/FLAT/BUSHEL).
- Use normalized spacing and uppercase units.

5. clarity_rating:
- Rate the ORIGINAL raw input item name (not your cleaned version) on a 0-10 scale for readability/clarity.
- 10: Clear, descriptive, contains product type, brand, size, pack info, standard terminology.
- 7-9: Generally clear, minor abbreviation or formatting issues.
- 4-6: Somewhat unclear, missing key info, non-standard abbreviations.
- 1-3: Very abbreviated or cryptic, difficult to understand.
- 0: Just codes or numbers, no descriptive information.
- Must be an integer 0-10.

6. clarity_category:
- Derived from clarity_rating:
  - 9-10: "Excellent"
  - 7-8: "Good"
  - 5-6: "Fair"
  - 3-4: "Poor"
  - 1-2: "Very Poor"
  - 0: "Extremely Poor"

Validation constraints:
- name_cleaned must be non-empty and plain text.
- description_cleaned must be non-empty and plain text.
- cleaned_brand may be empty, but if non-empty it must be plain text.
- standardized_pack_size, if non-empty, must match one of:
  - "^\\d+(\\.\\d+)?(-\\d+(\\.\\d+)?)? [A-Z][A-Z0-9-]{0,11}$"
  - "^\\d+(\\.\\d+)?/\\d+(\\.\\d+)? [A-Z][A-Z0-9-]{0,11}$"
  - "^\\d+(\\.\\d+)? x \\d+(\\.\\d+)?(-\\d+(\\.\\d+)?)? [A-Z][A-Z0-9-]{0,11}$"
  - "^\\d+(\\.\\d+)? x \\d+(\\.\\d+)?/\\d+(\\.\\d+)? [A-Z][A-Z0-9-]{0,11}$"
  - "^\\d+(\\.\\d+)? x [A-Z][A-Z0-9-]{0,11}$"
- clarity_rating must be an integer 0-10.
- clarity_category must be one of: "Excellent", "Good", "Fair", "Poor", "Very Poor", "Extremely Poor".
""".strip()


class AICleaningService:
    """Cleaning stage with batched AI inference for throughput."""

    def __init__(
        self,
        use_schema_generation: bool = True,
        stream_output_path: Optional[Path] = None,
        workers: int = 10,
        batch_size: int = 10,
        progress_callback: Optional[Callable[[int, int, str | None], None]] = None,
    ) -> None:
        self.use_schema_generation = use_schema_generation
        self.workers = max(1, int(workers))
        self.batch_size = max(1, int(batch_size))
        self.progress_callback = progress_callback
        self._gemini: Optional[Any] = None  # lazy-init shared GeminiProvider
        env_stream_path = os.getenv("AI_CLEANING_STREAM_OUTPUT")
        if stream_output_path is None and env_stream_path:
            self.stream_output_path = Path(env_stream_path).expanduser().resolve()
        elif stream_output_path is None:
            self.stream_output_path = None
        else:
            self.stream_output_path = Path(stream_output_path).expanduser().resolve()
        self._description_override_col: Optional[str] = None
        self._brand_override_col: Optional[str] = None
        self._packsize_override_col: Optional[str] = None

    @staticmethod
    def _row_fingerprint(row: pd.Series) -> str:
        """Deterministic hash of source row values for resume identity verification."""
        values = "|".join(str(v) for v in row.values)
        return hashlib.md5(values.encode("utf-8", errors="replace")).hexdigest()[:12]

    def _get_gemini(self):
        """Shared GeminiProvider — single HTTP client for all concurrent tasks."""
        if self._gemini is None:
            from ..providers.gemini_provider import GeminiProvider

            self._gemini = GeminiProvider()
        return self._gemini

    @staticmethod
    def _collapse_spaces(value: Any) -> str:
        return " ".join(str(value or "").split())

    @staticmethod
    def _coerce_row_value(value: Any) -> Any:
        if pd.isna(value):
            return None
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return str(value)
        return value

    @staticmethod
    def _resolve_optional_column(columns: Sequence[object], requested: Optional[str], label: str) -> Optional[str]:
        if not requested:
            return None
        requested_text = str(requested).strip()
        if requested_text in columns:
            return requested_text

        matches = [str(col) for col in columns if str(col).strip().casefold() == requested_text.casefold()]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise StageError(f"Multiple columns match requested {label} override {requested_text!r}")
        raise StageError(f"Requested {label} override column not found: {requested_text}")

    def _configure_source_overrides(
        self,
        columns: Sequence[object],
        description_col: Optional[str],
        brand_col: Optional[str],
        packsize_col: Optional[str],
    ) -> None:
        self._description_override_col = self._resolve_optional_column(columns, description_col, "description")
        self._brand_override_col = self._resolve_optional_column(columns, brand_col, "brand")
        self._packsize_override_col = self._resolve_optional_column(columns, packsize_col, "packsize")

    @staticmethod
    def _find_casefold_key(source_row: Dict[str, Any], candidates: Sequence[str]) -> Optional[str]:
        by_casefold = {str(key).strip().casefold(): str(key) for key in source_row.keys()}
        for candidate in candidates:
            resolved = by_casefold.get(candidate.casefold())
            if resolved:
                return resolved
        return None

    def _value_from_candidates(self, source_row: Dict[str, Any], candidates: Sequence[str]) -> str:
        for candidate in candidates:
            resolved = self._find_casefold_key(source_row, (candidate,))
            if not resolved:
                continue
            value = self._collapse_spaces(source_row.get(resolved))
            if value:
                return value
        return ""

    def _build_source_row(self, row: pd.Series) -> Dict[str, Any]:
        source_row = {str(col): self._coerce_row_value(row[col]) for col in row.index}

        if self._description_override_col:
            override_value = self._coerce_row_value(row[self._description_override_col])
            source_row["source_description"] = override_value
            source_row["description"] = override_value
        if self._brand_override_col:
            override_value = self._coerce_row_value(row[self._brand_override_col])
            source_row["source_brand"] = override_value
            source_row["brand"] = override_value
        if self._packsize_override_col:
            override_value = self._coerce_row_value(row[self._packsize_override_col])
            source_row["source_pack_size"] = override_value
            source_row["pack_size"] = override_value

        return source_row

    # Columns that carry no signal for AI cleaning — IDs, barcodes, URLs, etc.
    _PAYLOAD_SKIP_COLUMNS: frozenset[str] = frozenset(
        col.casefold()
        for col in (
            "uuid",
            "salt_id",
            "photo_url_list",
            "gtin_14",
            "gtin14",
            "gtin",
            "upc1",
            "upc2",
            "upc3",
            "external_item_id",
            "item_id",
            "mpn",
            "id",
        )
    )

    def _row_to_payload(self, row: pd.Series) -> Dict[str, Any]:
        source_row = self._build_source_row(row)
        payload: Dict[str, Any] = {}
        for key, value in source_row.items():
            if not str(key).startswith("source_") and str(key).casefold() in self._PAYLOAD_SKIP_COLUMNS:
                continue
            payload[str(key)] = value
        return payload

    async def _ai_with_retries_async(
        self,
        prompt: str,
        schema_model: Type[SchemaModelT],
        validate: Optional[Callable[[SchemaModelT], SchemaModelT]] = None,
        retries: int = 3,
    ) -> SchemaModelT:
        last_error: Optional[Exception] = None
        attempt_prompt = prompt
        for attempt in range(1, retries + 1):
            try:
                payload = await self._get_gemini().generate_with_schema_async(
                    prompt=attempt_prompt,
                    schema_model=schema_model,
                    use_schema_generation=self.use_schema_generation,
                    system_prompt=_AI_SYSTEM_PROMPT,
                )
                if validate is not None:
                    payload = validate(payload)
                return payload
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "ai_validation_retry",
                    extra={"attempt": attempt, "retries": retries, "error": str(exc)},
                )
                if attempt < retries:
                    error_text = self._collapse_spaces(str(exc))[:600]
                    attempt_prompt = (
                        f"{prompt}\n\n"
                        "Your previous response failed validation.\n"
                        f"Validation error: {error_text}\n"
                        "Return the full JSON object again, strictly matching the schema and fixing the error.\n"
                        "Do not include any explanation or markdown."
                    )
        raise StageError(f"AI validation failed after {retries} retries: {last_error}")

    @staticmethod
    def _normalize_number_text(token: str) -> str:
        value = float(token)
        if value <= 0:
            raise ValueError("number must be > 0")
        if value.is_integer():
            return str(int(value))
        text = f"{value:.6f}".rstrip("0").rstrip(".")
        return text

    @classmethod
    def _normalize_quantity_token(cls, token: str) -> str:
        token = token.strip()
        if not token:
            raise ValueError("empty quantity")

        if re.fullmatch(r"\d+(?:\.\d+)?", token):
            return cls._normalize_number_text(token)

        if re.fullmatch(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?", token):
            left, right = token.split("-", 1)
            return f"{cls._normalize_number_text(left)}-{cls._normalize_number_text(right)}"

        raise ValueError(f"invalid quantity token: {token}")

    @classmethod
    def _normalize_pack_token(cls, token: str) -> str:
        upper = token.strip().upper()
        if not upper:
            raise ValueError("empty pack token")
        if upper in _UNIT_ALIASES:
            upper = _UNIT_ALIASES[upper]
        if upper in _KNOWN_PACK_TOKENS:
            return upper
        # Keep unknown tokens (legacy behavior did not hard-reject unrecognized units).
        if re.fullmatch(r"[A-Z][A-Z0-9\-]{0,11}", upper):
            return upper
        raise ValueError(f"invalid pack token: {token}")

    @classmethod
    def _normalize_slash_quantity(cls, token: str) -> str:
        token = token.strip()
        if not re.fullmatch(r"\d+(?:\.\d+)?/\d+(?:\.\d+)?", token):
            raise ValueError(f"invalid slash quantity token: {token}")
        left, right = token.split("/", 1)
        return f"{cls._normalize_number_text(left)}/{cls._normalize_number_text(right)}"

    def _normalize_pack_size(self, raw: str) -> str:
        text = self._collapse_spaces(raw)
        if not text:
            return ""

        text = text.upper().replace("×", "X")
        text = re.sub(r"#", " LB", text)
        text = re.sub(r"(?<=\d)\s*X\s*(?=\d|[A-Z])", " x ", text)
        text = re.sub(r"\s*/\s*", "/", text)
        text = re.sub(r"(?<=\d)(?=[A-Z])", " ", text)
        text = re.sub(r"(?<=[A-Z])(?=\d)", " ", text)
        text = self._collapse_spaces(text)

        parts = text.split(" ")
        if len(parts) == 2:
            qty, unit = parts[0], parts[1]
            normalized_unit = self._normalize_pack_token(unit)
            if "/" in qty:
                normalized_qty = self._normalize_slash_quantity(qty)
            else:
                normalized_qty = self._normalize_quantity_token(qty)
            return f"{normalized_qty} {normalized_unit}"

        if len(parts) == 3:
            count, marker, pack_token = parts
            if marker != "x":
                raise ValueError("multipack marker must be x")
            normalized_count = self._normalize_quantity_token(count)
            normalized_pack_token = self._normalize_pack_token(pack_token)
            return f"{normalized_count} x {normalized_pack_token}"

        if len(parts) == 4:
            count, marker, qty, unit = parts
            if marker != "x":
                raise ValueError("multipack marker must be x")
            normalized_count = self._normalize_quantity_token(count)
            normalized_unit = self._normalize_pack_token(unit)
            if "/" in qty:
                normalized_qty = self._normalize_slash_quantity(qty)
            else:
                normalized_qty = self._normalize_quantity_token(qty)
            return f"{normalized_count} x {normalized_qty} {normalized_unit}"

        raise ValueError("invalid standardized_pack_size format")

    @staticmethod
    def _strict_match(value: str, pattern: re.Pattern[str], field_name: str, *, allow_empty: bool) -> None:
        if not value:
            if allow_empty:
                return
            raise ValueError(f"{field_name} cannot be empty")
        if not pattern.fullmatch(value):
            raise ValueError(f"{field_name} failed regex validation: {value!r}")

    @staticmethod
    def _strict_pack_match(value: str) -> None:
        if not value:
            return
        if _PACK_SINGLE_PATTERN.fullmatch(value):
            return
        if _PACK_SINGLE_SLASH_PATTERN.fullmatch(value):
            return
        if _PACK_MULTI_QTY_PATTERN.fullmatch(value):
            return
        if _PACK_MULTI_SLASH_PATTERN.fullmatch(value):
            return
        if _PACK_MULTI_TOKEN_PATTERN.fullmatch(value):
            return
        raise ValueError(f"standardized_pack_size failed regex validation: {value!r}")

    def _strip_name_size_pack_tokens(self, value: str) -> str:
        text = self._collapse_spaces(value)
        if not text:
            return ""
        for pattern in _NAME_STRIP_PATTERNS:
            text = pattern.sub(" ", text)
        text = re.sub(r"\b\d+\s*/\s*\d+\s*(?:\"|IN(?:CH(?:ES)?)?)\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*[/,;:]\s*", " ", text)
        text = re.sub(r"(^|\s)[\"']+(\s|$)", " ", text)
        text = re.sub(r"\s+[.-](?=\s|$)", " ", text)
        text = self._collapse_spaces(text)
        return text

    def _sanitize_name_cleaned(self, name_value: str, description_value: str) -> str:
        stripped_name = self._strip_name_size_pack_tokens(name_value)
        if stripped_name:
            return stripped_name
        stripped_description = self._strip_name_size_pack_tokens(description_value)
        if stripped_description:
            return stripped_description
        raise ValueError("name_cleaned empty after removing size/pack information")

    def _sanitize_description_cleaned(self, description_value: str, fallback_name: str) -> str:
        text = self._collapse_spaces(description_value)
        if not text:
            text = self._collapse_spaces(fallback_name)
        if not text:
            raise ValueError("description_cleaned empty after normalization")
        text = re.sub(r"(?i)[|,;]?\s*Brand\s*:\s*[^|,;.]*(?:\.)?", " ", text)
        text = re.sub(r"(?i)[|,;]?\s*Pack\s*Size\s*:\s*[^|,;.]*(?:\.)?", " ", text)
        text = re.sub(r"\s*[|]+\s*", " ", text)
        text = re.sub(r"\s+([,.;:])", r"\1", text)
        text = text.strip(" ,;|.")
        text = self._collapse_spaces(text)
        if not text:
            text = self._collapse_spaces(fallback_name)
        if not text:
            raise ValueError("description_cleaned empty after noise stripping")
        return text

    def _extract_brand_from_text(self, text: str) -> str:
        raw = self._collapse_spaces(text)
        if not raw:
            return ""
        match = _BRAND_FROM_TEXT_PATTERN.search(raw)
        if not match:
            return ""
        tail = match.group(1)
        tail = re.split(r"(?i)\bPack\s*Size\s*:", tail, maxsplit=1)[0]
        tail = re.split(r"[|,;]", tail, maxsplit=1)[0]
        brand = self._collapse_spaces(tail.strip(" .:|,;"))
        if not brand:
            return ""
        if brand.isupper():
            brand = brand.title()
        return brand

    def _extract_brand_from_source_row(self, source_row: Dict[str, Any]) -> str:
        override_brand = self._value_from_candidates(source_row, ("source_brand", "brand"))
        if override_brand:
            return override_brand
        for value in source_row.values():
            candidate = self._extract_brand_from_text(str(value or ""))
            if candidate:
                return candidate
        return ""

    def _fallback_name_from_source_row(self, source_row: Dict[str, Any]) -> str:
        name_value = self._value_from_candidates(
            source_row,
            (
                "display_name",
                "name",
                "item_name",
                "product_name",
                "title",
                "item_title",
            ),
        )
        if name_value:
            return name_value

        description_value = self._fallback_description_from_source_row(source_row)
        if description_value:
            try:
                return self._sanitize_name_cleaned(description_value, description_value)
            except Exception:
                return description_value

        for key, value in source_row.items():
            key_text = str(key).strip().casefold()
            if key_text.startswith("source_"):
                continue
            if key_text in self._PAYLOAD_SKIP_COLUMNS:
                continue
            if any(token in key_text for token in ("brand", "pack", "size", "gtin", "upc", "ean", "barcode", "id", "category")):
                continue
            candidate = self._collapse_spaces(value)
            if candidate and re.search(r"[A-Za-z]", candidate):
                return candidate
        return ""

    def _fallback_description_from_source_row(self, source_row: Dict[str, Any]) -> str:
        description_value = self._value_from_candidates(
            source_row,
            (
                "source_description",
                "description",
                "item_description",
                "product_description",
                "long_description",
                "details",
                "detail",
            ),
        )
        if description_value:
            return description_value

        for key, value in source_row.items():
            key_text = str(key).strip().casefold()
            if key_text in self._PAYLOAD_SKIP_COLUMNS:
                continue
            candidate = self._collapse_spaces(value)
            if candidate and len(candidate) > 20 and re.search(r"[A-Za-z]", candidate):
                return candidate
        return ""

    def _fallback_pack_from_source_row(self, source_row: Dict[str, Any]) -> str:
        return self._value_from_candidates(
            source_row,
            (
                "source_pack_size",
                "pack_size",
                "packsize",
                "size",
                "case_pack",
                "pack",
            ),
        )

    def _infer_family_hint(self, source_row: Dict[str, Any]) -> str:
        candidate_keys = (
            "category",
            "Category",
            "CATEGORY",
            "department",
            "Department",
            "product_category",
            "Product Category",
        )
        raw_value = ""
        for key in candidate_keys:
            if key in source_row and source_row[key] is not None:
                raw_value = self._collapse_spaces(source_row[key])
                if raw_value:
                    break
        normalized = raw_value.upper()
        if not normalized:
            return ""
        if normalized in _FAMILY_HINT_MAP:
            return _FAMILY_HINT_MAP[normalized]
        for key, mapped in sorted(_FAMILY_HINT_MAP.items(), key=lambda item: len(item[0]), reverse=True):
            if re.search(rf"\b{re.escape(key)}\b", normalized):
                return mapped
        return ""

    @staticmethod
    def _is_placeholder_brand(brand_value: str) -> bool:
        brand = " ".join(str(brand_value).strip().upper().split())
        return bool(brand) and brand in _PLACEHOLDER_BRANDS

    def _remove_placeholder_brand_prefix(self, name_value: str, cleaned_brand: str) -> str:
        name = self._collapse_spaces(name_value)
        brand = self._collapse_spaces(cleaned_brand).upper()
        if not name:
            return name
        tokens = name.split(" ")
        if not tokens:
            return name
        first_token = tokens[0].upper()
        if first_token in _PLACEHOLDER_BRANDS and len(tokens) > 1:
            return " ".join(tokens[1:])
        if self._is_placeholder_brand(brand) and brand and len(tokens) > 1 and first_token == brand:
            return " ".join(tokens[1:])
        return name

    def _prepend_non_placeholder_brand(self, name_value: str, cleaned_brand: str) -> str:
        name = self._collapse_spaces(name_value)
        brand = self._collapse_spaces(cleaned_brand)
        if not name or not brand or self._is_placeholder_brand(brand):
            return name
        if re.search(rf"\b{re.escape(brand)}\b", name, re.IGNORECASE):
            return name
        return f"{brand} {name}"

    def _apply_source_name_rules(
        self,
        name_value: str,
        cleaned_brand: str,
        description_value: str,
        source_row: Dict[str, Any],
    ) -> str:
        name = self._remove_placeholder_brand_prefix(name_value, cleaned_brand)
        name = self._prepend_non_placeholder_brand(name, cleaned_brand)
        family_hint = self._infer_family_hint(source_row)
        if family_hint:
            if not re.search(rf"\b{re.escape(family_hint)}\b", name, re.IGNORECASE):
                name = f"{family_hint} {name}"
        normalized = self._sanitize_name_cleaned(name, description_value)
        self._strict_match(normalized, _NAME_PATTERN, "name_cleaned", allow_empty=False)
        return self._collapse_spaces(normalized)

    def _validate_enrichment(self, payload: ItemEnrichmentResponse) -> ItemEnrichmentResponse:
        payload.name_cleaned = self._sanitize_name_cleaned(payload.name_cleaned, payload.description_cleaned)
        payload.description_cleaned = self._sanitize_description_cleaned(
            payload.description_cleaned,
            payload.name_cleaned,
        )
        payload.cleaned_brand = self._collapse_spaces(payload.cleaned_brand)
        payload.standardized_pack_size = self._normalize_pack_size(payload.standardized_pack_size)

        if len(payload.description_cleaned) > _MAX_DESCRIPTION_LEN:
            raise ValueError("description_cleaned exceeds max length")
        if len(payload.cleaned_brand) > _MAX_BRAND_LEN:
            raise ValueError("cleaned_brand exceeds max length")

        self._strict_match(payload.name_cleaned, _NAME_PATTERN, "name_cleaned", allow_empty=False)
        self._strict_match(payload.description_cleaned, _DESC_PATTERN, "description_cleaned", allow_empty=False)
        self._strict_match(payload.cleaned_brand, _BRAND_PATTERN, "cleaned_brand", allow_empty=True)
        self._strict_pack_match(payload.standardized_pack_size)

        # Clamp clarity_rating to 0-10 and fix category
        payload.clarity_rating = max(0, min(10, int(payload.clarity_rating)))
        payload.clarity_category = _score_to_category(payload.clarity_rating)
        return payload

    async def _enrich_row_ai(self, row_payload: Dict[str, Any]) -> ItemEnrichmentResponse:
        prompt = f"""
Single row JSON:
{json.dumps(row_payload, ensure_ascii=True)}

Return ONLY valid JSON in this exact schema:
{{
  "name_cleaned": "...",
  "description_cleaned": "...",
  "cleaned_brand": "...",
  "standardized_pack_size": "",
  "clarity_rating": 0,
  "clarity_category": "..."
}}
""".strip()
        return await self._ai_with_retries_async(prompt, ItemEnrichmentResponse, self._validate_enrichment, retries=3)

    async def _enrich_batch_ai(
        self,
        indexed_payloads: List[Tuple[int, Dict[str, Any]]],
    ) -> BatchEnrichmentResponse:
        """Send multiple items in a single API call and return batch response."""
        items_for_prompt = []
        for idx, payload in indexed_payloads:
            item = {"_idx": idx, **payload}
            items_for_prompt.append(item)

        prompt = f"""
Product rows JSON array:
{json.dumps(items_for_prompt, ensure_ascii=True)}

For EACH row, return the enrichment. Return a JSON object with an "items" array,
one result per input row, in the SAME ORDER as the input.
Each item must have an "id" field matching the input "_idx".

Return ONLY valid JSON:
{{
  "items": [
    {{"id": 0, "name_cleaned": "...", "description_cleaned": "...", "cleaned_brand": "...", "standardized_pack_size": "", "clarity_rating": 0, "clarity_category": "..."}},
    ...
  ]
}}
""".strip()
        return await self._ai_with_retries_async(prompt, BatchEnrichmentResponse, retries=2)

    def _post_process_item(
        self,
        position: int,
        fingerprint: str,
        enrichment: ItemEnrichmentResponse,
        source_row: Dict[str, Any],
    ) -> Tuple[int, str, str, str, str, str, int, str, Dict[str, Any]]:
        """Validate and post-process a single enrichment result. Raises on failure."""
        validated = self._validate_enrichment(enrichment)
        name = self._apply_source_name_rules(
            validated.name_cleaned,
            validated.cleaned_brand,
            validated.description_cleaned,
            source_row,
        )
        description = self._sanitize_description_cleaned(validated.description_cleaned, name)
        brand = self._collapse_spaces(validated.cleaned_brand)
        if not brand:
            brand = self._extract_brand_from_source_row(source_row)
        self._strict_match(brand, _BRAND_PATTERN, "cleaned_brand", allow_empty=True)
        pack = validated.standardized_pack_size
        size = self._size_from_pack(pack)
        clarity_rating = validated.clarity_rating
        clarity_category = validated.clarity_category
        stream_row = dict(source_row)
        stream_row.update(
            {
                _RESUME_ROW_COL: position,
                _RESUME_FINGERPRINT_COL: fingerprint,
                "Name_cleaned": name,
                "Description_cleaned": description,
                "cleaned_brand": brand,
                "Pack_cleaned": pack,
                "Size_cleaned": size,
                "clarity_rating": clarity_rating,
                "clarity_category": clarity_category,
            }
        )
        return position, name, description, brand, pack, size, clarity_rating, clarity_category, stream_row

    def _load_resume_rows(
        self,
        row_count: int,
        source_df: pd.DataFrame,
    ) -> Tuple[set[int], List[str], List[str], List[str], List[str], List[str], List[int], List[str]]:
        completed_positions: set[int] = set()
        names: List[str] = [""] * row_count
        descriptions: List[str] = [""] * row_count
        brands: List[str] = [""] * row_count
        packs: List[str] = [""] * row_count
        sizes: List[str] = [""] * row_count
        clarity_ratings: List[int] = [0] * row_count
        clarity_categories: List[str] = [""] * row_count

        if self.stream_output_path is None or not self.stream_output_path.exists():
            return completed_positions, names, descriptions, brands, packs, sizes, clarity_ratings, clarity_categories

        try:
            existing = pd.read_csv(self.stream_output_path)
        except Exception as exc:
            logger.warning(
                "cleaning_resume_invalid",
                extra={"path": str(self.stream_output_path), "error": str(exc)},
            )
            return completed_positions, names, descriptions, brands, packs, sizes, clarity_ratings, clarity_categories

        required_cols = {"Description_cleaned", "cleaned_brand", "Pack_cleaned", "Size_cleaned"}
        if not required_cols.issubset(set(existing.columns)):
            logger.warning(
                "cleaning_resume_missing_columns",
                extra={"path": str(self.stream_output_path), "columns": list(existing.columns)},
            )
            return completed_positions, names, descriptions, brands, packs, sizes, clarity_ratings, clarity_categories

        has_name_col = "Name_cleaned" in existing.columns

        has_fingerprint_col = _RESUME_FINGERPRINT_COL in existing.columns

        if _RESUME_ROW_COL not in existing.columns:
            logger.warning(
                "cleaning_resume_missing_row_col",
                extra={
                    "path": str(self.stream_output_path),
                    "note": "Stream file lacks row position column; cannot safely resume. Re-processing all rows.",
                },
            )
            return completed_positions, names, descriptions, brands, packs, sizes, clarity_ratings, clarity_categories

        for _, row in existing.iterrows():
            try:
                position = int(row[_RESUME_ROW_COL])
            except Exception:
                continue
            if position < 0 or position >= row_count or position in completed_positions:
                continue
            if has_fingerprint_col:
                saved_fp = str(row.get(_RESUME_FINGERPRINT_COL, ""))
                actual_fp = self._row_fingerprint(source_df.iloc[position])
                if saved_fp and saved_fp != actual_fp:
                    logger.warning(
                        "cleaning_resume_fingerprint_mismatch",
                        extra={"position": position, "saved": saved_fp, "actual": actual_fp},
                    )
                    continue

            if has_name_col:
                names[position] = self._collapse_spaces(row["Name_cleaned"])
            else:
                names[position] = self._collapse_spaces(row["Description_cleaned"])
            descriptions[position] = self._collapse_spaces(row["Description_cleaned"])
            brands[position] = self._collapse_spaces(row["cleaned_brand"])
            try:
                pack_normalized = self._normalize_pack_size(str(row["Pack_cleaned"]))
            except Exception:
                pack_normalized = ""
            packs[position] = pack_normalized
            size_cleaned = self._collapse_spaces(row["Size_cleaned"])
            sizes[position] = size_cleaned or self._size_from_pack(pack_normalized)
            try:
                clarity_ratings[position] = max(0, min(10, int(row.get("clarity_rating", 0))))
            except (ValueError, TypeError):
                clarity_ratings[position] = 0
            clarity_categories[position] = _score_to_category(clarity_ratings[position])
            completed_positions.add(position)

        logger.info(
            "cleaning_resume_loaded",
            extra={"path": str(self.stream_output_path), "resume_rows": len(completed_positions), "total_rows": row_count},
        )
        return completed_positions, names, descriptions, brands, packs, sizes, clarity_ratings, clarity_categories

    async def _process_single_row(
        self,
        position: int,
        fingerprint: str,
        row_payload: Dict[str, Any],
        source_row: Dict[str, Any],
    ) -> Tuple[int, str, str, str, str, str, int, str, Dict[str, Any]]:
        """Process a single row via individual AI call (used for retries)."""
        result = await self._enrich_row_ai(row_payload)
        return self._post_process_item(position, fingerprint, result, source_row)

    @staticmethod
    def _size_from_pack(pack_size: str) -> str:
        if not pack_size:
            return ""
        parts = pack_size.split(" ")
        if len(parts) == 2:
            if "/" in parts[0]:
                _, each_qty = parts[0].split("/", 1)
                return f"{each_qty} {parts[1]}"
            return pack_size
        if len(parts) == 4:
            if "/" in parts[2]:
                _, each_qty = parts[2].split("/", 1)
                return f"{each_qty} {parts[3]}"
            return f"{parts[2]} {parts[3]}"
        return ""

    def run(
        self,
        df: pd.DataFrame,
        description_col: Optional[str],
        brand_col: Optional[str],
        packsize_col: Optional[str],
    ) -> pd.DataFrame:
        """Apply one-request-per-row AI cleaning and append minimal unique outputs."""
        out = df.copy()
        self._configure_source_overrides(out.columns, description_col, brand_col, packsize_col)
        if self._description_override_col or self._brand_override_col or self._packsize_override_col:
            logger.info(
                "cleaning_column_overrides_enabled",
                extra={
                    "description_col": self._description_override_col,
                    "brand_col": self._brand_override_col,
                    "packsize_col": self._packsize_override_col,
                },
            )

        row_count = len(out)
        completed_positions: set[int] = set()
        resume_count = 0
        names: List[str] = [""] * row_count
        descriptions: List[str] = [""] * row_count
        brands: List[str] = [""] * row_count
        packs: List[str] = [""] * row_count
        sizes: List[str] = [""] * row_count
        clarity_ratings: List[int] = [0] * row_count
        clarity_categories: List[str] = [""] * row_count

        if self.stream_output_path:
            completed_positions, names, descriptions, brands, packs, sizes, clarity_ratings, clarity_categories = self._load_resume_rows(row_count, out)
            resume_count = len(completed_positions)

        stream_file = None
        stream_columns = [_RESUME_ROW_COL, _RESUME_FINGERPRINT_COL] + list(out.columns) + _ADDED_OUTPUT_COLUMNS
        if self.stream_output_path:
            self.stream_output_path.parent.mkdir(parents=True, exist_ok=True)
            existing_has_row_col = False
            if self.stream_output_path.exists():
                try:
                    existing_header = pd.read_csv(self.stream_output_path, nrows=0)
                    existing_has_row_col = _RESUME_ROW_COL in existing_header.columns
                except Exception:
                    existing_has_row_col = False

            mode = "a" if resume_count > 0 and existing_has_row_col else "w"
            stream_file = self.stream_output_path.open(mode, encoding="utf-8", newline="")
            if mode == "w":
                header_df = pd.DataFrame(columns=stream_columns)
                header_df.to_csv(stream_file, index=False, header=True)
                if resume_count > 0:
                    for pos in sorted(completed_positions):
                        seeded = {
                            _RESUME_ROW_COL: pos,
                            _RESUME_FINGERPRINT_COL: self._row_fingerprint(out.iloc[pos]),
                            **out.iloc[pos].to_dict(),
                            "Name_cleaned": names[pos],
                            "Description_cleaned": descriptions[pos],
                            "cleaned_brand": brands[pos],
                            "Pack_cleaned": packs[pos],
                            "Size_cleaned": sizes[pos],
                            "clarity_rating": clarity_ratings[pos],
                            "clarity_category": clarity_categories[pos],
                        }
                        pd.DataFrame([seeded], columns=stream_columns).to_csv(
                            stream_file,
                            index=False,
                            header=False,
                        )
                stream_file.flush()

        processed_count = 0
        try:
            if row_count > resume_count:
                # Run the async processing loop
                processed_count = asyncio.run(
                    self._run_async(
                        out,
                        row_count,
                        resume_count,
                        completed_positions,
                        names,
                        descriptions,
                        brands,
                        packs,
                        sizes,
                        clarity_ratings,
                        clarity_categories,
                        stream_file,
                        stream_columns,
                    )
                )
            else:
                logger.info(
                    "cleaning_resume_complete",
                    extra={"rows": row_count, "path": str(self.stream_output_path) if self.stream_output_path else ""},
                )
        finally:
            if stream_file is not None:
                stream_file.close()

        # Fallback for rows where all AI retries failed — use source data
        fallback_count = 0
        for pos in range(row_count):
            if names[pos] and descriptions[pos]:
                continue
            source_dict = self._build_source_row(out.iloc[pos])
            if not names[pos]:
                names[pos] = self._fallback_name_from_source_row(source_dict) or "Unknown Product"
            if not descriptions[pos]:
                raw_desc = self._fallback_description_from_source_row(source_dict)
                if raw_desc:
                    try:
                        descriptions[pos] = self._sanitize_description_cleaned(raw_desc, names[pos])
                    except Exception:
                        descriptions[pos] = names[pos]
                else:
                    descriptions[pos] = names[pos]
            if not brands[pos]:
                brands[pos] = self._extract_brand_from_source_row(source_dict)
            if not packs[pos]:
                raw_pack = self._fallback_pack_from_source_row(source_dict)
                if raw_pack:
                    try:
                        packs[pos] = self._normalize_pack_size(raw_pack)
                    except Exception:
                        packs[pos] = ""
            if not sizes[pos]:
                sizes[pos] = self._size_from_pack(packs[pos])
            if not clarity_categories[pos]:
                clarity_categories[pos] = _score_to_category(clarity_ratings[pos])
            fallback_count += 1
            logger.warning(
                "cleaning_fallback_used",
                extra={"position": pos, "fallback_name": names[pos][:50]},
            )
        if fallback_count:
            logger.info("cleaning_fallback_total", extra={"count": fallback_count, "total": row_count})
            if self.progress_callback:
                finalized_count = resume_count + processed_count + fallback_count
                progress_message = f"Processed {finalized_count} of {row_count} rows"
                self.progress_callback(finalized_count, row_count, progress_message)

        out["Name_cleaned"] = names
        out["Description_cleaned"] = descriptions
        out["cleaned_brand"] = brands
        out["Pack_cleaned"] = packs
        out["Size_cleaned"] = sizes
        out["clarity_rating"] = clarity_ratings
        out["clarity_category"] = clarity_categories

        logger.info("cleaning_stage_complete", extra={"rows": len(out), "columns": len(out.columns), "output": str(self.stream_output_path) if self.stream_output_path else ""})
        return out

    def _record_result(
        self,
        position: int,
        name: str,
        description: str,
        brand: str,
        pack: str,
        size: str,
        clarity_rating: int,
        clarity_category: str,
        stream_row: Dict[str, Any],
        names: List[str],
        descriptions: List[str],
        brands: List[str],
        packs: List[str],
        sizes: List[str],
        clarity_ratings: List[int],
        clarity_categories: List[str],
        stream_file: Any,
        stream_columns: List[str],
    ) -> int:
        """Store a completed result and stream to checkpoint."""
        names[position] = name
        descriptions[position] = description
        brands[position] = brand
        packs[position] = pack
        sizes[position] = size
        clarity_ratings[position] = clarity_rating
        clarity_categories[position] = clarity_category
        if stream_file is not None:
            if not hasattr(stream_file, '_csv_writer'):
                stream_file._csv_writer = csv.writer(stream_file)
            stream_file._csv_writer.writerow([stream_row.get(col, "") for col in stream_columns])
            if not hasattr(stream_file, '_csv_row_count'):
                stream_file._csv_row_count = 0
            stream_file._csv_row_count += 1
            if stream_file._csv_row_count % 50 == 0:
                stream_file.flush()

    async def _run_async(
        self,
        out: pd.DataFrame,
        row_count: int,
        resume_count: int,
        completed_positions: set[int],
        names: List[str],
        descriptions: List[str],
        brands: List[str],
        packs: List[str],
        sizes: List[str],
        clarity_ratings: List[int],
        clarity_categories: List[str],
        stream_file: Any,
        stream_columns: List[str],
    ) -> int:
        """Batched async processing — N items per API call with individual retry fallback."""
        semaphore = asyncio.Semaphore(self.workers)
        positions_to_process = [pos for pos in range(row_count) if pos not in completed_positions]
        total_remaining = len(positions_to_process)

        self._get_gemini()

        # Group positions into batches
        batches: List[List[int]] = []
        for i in range(0, total_remaining, self.batch_size):
            batches.append(positions_to_process[i : i + self.batch_size])

        progress = tqdm(
            total=row_count,
            initial=resume_count,
            desc=f"cleaning ({resume_count}/{row_count} resumed, batch={self.batch_size})",
            unit="row",
            dynamic_ncols=True,
            file=sys.stderr,
        )
        processed = 0
        failed_count = 0
        retry_queue: List[int] = []

        async def process_batch(batch_positions: List[int]) -> Tuple[List[Tuple], List[int]]:
            """Process a batch. Returns (successful_results, failed_df_positions)."""
            async with semaphore:
                indexed_payloads = [
                    (i, self._row_to_payload(out.iloc[pos]))
                    for i, pos in enumerate(batch_positions)
                ]
                try:
                    batch_resp = await self._enrich_batch_ai(indexed_payloads)
                except Exception as exc:
                    logger.warning("cleaning_batch_failed", extra={"batch_size": len(batch_positions), "error": str(exc)})
                    return [], list(batch_positions)

                # Index results by id for lookup
                result_by_id: Dict[int, Any] = {}
                for item in batch_resp.items:
                    result_by_id[item.id] = item

                successful = []
                failed_positions = []
                for i, pos in enumerate(batch_positions):
                    item = result_by_id.get(i)
                    if item is None:
                        failed_positions.append(pos)
                        continue
                    try:
                        source_row = self._build_source_row(out.iloc[pos])
                        enrichment = ItemEnrichmentResponse(
                            name_cleaned=item.name_cleaned,
                            description_cleaned=item.description_cleaned,
                            cleaned_brand=item.cleaned_brand,
                            standardized_pack_size=item.standardized_pack_size,
                            clarity_rating=item.clarity_rating,
                            clarity_category=item.clarity_category,
                        )
                        result = self._post_process_item(
                            pos,
                            self._row_fingerprint(out.iloc[pos]),
                            enrichment,
                            source_row,
                        )
                        successful.append(result)
                    except Exception:
                        failed_positions.append(pos)

                return successful, failed_positions

        async def process_batch_with_meta(batch_positions: List[int]) -> Tuple[List[int], List[Tuple], List[int]]:
            successful, failed = await process_batch(batch_positions)
            return batch_positions, successful, failed

        try:
            # Phase 1: Process all batches concurrently
            tasks = [asyncio.create_task(process_batch_with_meta(batch)) for batch in batches]

            for coro in asyncio.as_completed(tasks):
                batch_positions: List[int] = []
                try:
                    batch_positions, successful_results, failed_positions = await coro
                except Exception as exc:
                    logger.warning("cleaning_batch_task_failed", extra={"error": str(exc)})
                    if batch_positions:
                        retry_queue.extend(batch_positions)
                        failed_count += len(batch_positions)
                    continue

                for position, name, description, brand, pack, size, c_rating, c_category, stream_row in successful_results:
                    self._record_result(
                        position, name, description, brand, pack, size, c_rating, c_category, stream_row,
                        names, descriptions, brands, packs, sizes, clarity_ratings, clarity_categories,
                        stream_file, stream_columns,
                    )
                    processed += 1
                    progress.update(1)

                for pos in failed_positions:
                    retry_queue.append(pos)
                    failed_count += 1

                if self.progress_callback:
                    progress_message = f"Processed {resume_count + processed} of {row_count} rows"
                    self.progress_callback(resume_count + processed, row_count, progress_message)
                if processed % 100 == 0 or processed == total_remaining:
                    logger.info(
                        "cleaning_progress",
                        extra={
                            "processed_rows": resume_count + processed,
                            "total_rows": row_count,
                            "workers": self.workers,
                            "batch_size": self.batch_size,
                            "failed_rows": failed_count,
                            "retry_queue": len(retry_queue),
                        },
                    )

            # Phase 2: Retry failed items individually
            if retry_queue:
                logger.info("cleaning_retry_start", extra={"retry_count": len(retry_queue)})

                async def retry_single(position: int) -> Tuple[int, str, str, str, str, str, int, str, Dict[str, Any]]:
                    async with semaphore:
                        source_row = self._build_source_row(out.iloc[position])
                        return await self._process_single_row(
                            position,
                            self._row_fingerprint(out.iloc[position]),
                            self._row_to_payload(out.iloc[position]),
                            source_row,
                        )

                retry_tasks = [asyncio.create_task(retry_single(pos)) for pos in retry_queue]
                for coro in asyncio.as_completed(retry_tasks):
                    try:
                        position, name, description, brand, pack, size, c_rating, c_category, stream_row = await coro
                        self._record_result(
                            position, name, description, brand, pack, size, c_rating, c_category, stream_row,
                            names, descriptions, brands, packs, sizes, clarity_ratings, clarity_categories,
                            stream_file, stream_columns,
                        )
                    except Exception as exc:
                        logger.warning("cleaning_retry_row_failed", extra={"error": str(exc)})
                        continue
                    processed += 1
                    progress.update(1)
                    if self.progress_callback:
                        progress_message = f"Processed {resume_count + processed} of {row_count} rows"
                        self.progress_callback(resume_count + processed, row_count, progress_message)

        except KeyboardInterrupt:
            logger.warning(
                "cleaning_interrupted",
                extra={
                    "processed_rows": resume_count + processed,
                    "total_rows": row_count,
                },
            )
            raise
        except Exception:
            raise
        finally:
            progress.close()
        return processed
