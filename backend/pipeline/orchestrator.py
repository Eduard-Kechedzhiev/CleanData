"""Pipeline orchestrator — always runs all stages with stage-level checkpointing."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Set, Tuple

import pandas as pd

from .config import RunConfig, project_root
from .errors import StageError
from .io_files import read_dataframe, write_dataframe
from .services.ai_cleaning import AICleaningService
from .services.gtin import GTINService
from .services.taxonomy import TaxonomyService

logger = logging.getLogger(__name__)

_STAGE_ORDER = ["cleaning", "taxonomy", "gtin"]
_TAXONOMY_REQUIRED_COLUMNS: Set[str] = {"Name_cleaned", "Description_cleaned"}


def _checkpoint_path(output_path: Path) -> Path:
    return output_path.parent / f".{output_path.stem}_checkpoint.csv"


def _meta_path(output_path: Path) -> Path:
    """JSON sidecar storing stage name, input identity, and row count."""
    return output_path.parent / f".{output_path.stem}_meta.json"


def _cleaning_stream_path(output_path: Path) -> Path:
    return output_path.parent / f".{output_path.stem}_cleaning_stream.csv"


def _file_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class PipelineOrchestrator:
    """Executes all stages in order with stage-level resume."""

    def __init__(self, cfg: RunConfig) -> None:
        self.cfg = cfg

    @property
    def _progress(self):
        return getattr(self.cfg, "progress_reporter", None)

    def _run_stage(
        self,
        name: str,
        df: pd.DataFrame,
        stage_fn: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> Tuple[pd.DataFrame, bool]:
        """Run a stage, returning (result_df, success). On failure with fail_fast=False, returns original df."""
        if self._progress is not None:
            self._progress.on_stage_started(name, total_rows=len(df))
        logger.info("stage_start", extra={"stage": name, "rows": len(df)})
        try:
            out = stage_fn(df)
            logger.info("stage_complete", extra={"stage": name, "rows": len(out), "columns": len(out.columns)})
            if self._progress is not None:
                self._progress.on_stage_completed(name, completed=len(out), total=len(out))
            return out, True
        except Exception as exc:
            logger.error("stage '%s' failed: %s", name, exc, exc_info=True)
            if self._progress is not None:
                self._progress.on_stage_failed(name, str(exc), fatal=self.cfg.fail_fast)
            if self.cfg.fail_fast:
                raise StageError(f"Stage '{name}' failed: {exc}") from exc
            return df, False

    def _save_checkpoint(self, df: pd.DataFrame, stage: str) -> None:
        cp = _checkpoint_path(self.cfg.output_path)
        mf = _meta_path(self.cfg.output_path)
        cp.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cp, index=False)
        meta = {
            "stage": stage,
            "input_path": str(self.cfg.input_path),
            "row_count": len(df),
            "input_fingerprint": _file_fingerprint(self.cfg.input_path),
        }
        mf.write_text(json.dumps(meta), encoding="utf-8")
        logger.info("checkpoint_saved", extra={"stage": stage, "rows": len(df), "path": str(cp)})

    def _load_checkpoint(self) -> tuple[pd.DataFrame | None, str | None]:
        cp = _checkpoint_path(self.cfg.output_path)
        mf = _meta_path(self.cfg.output_path)
        if not cp.exists() or not mf.exists():
            return None, None
        try:
            meta = json.loads(mf.read_text(encoding="utf-8"))
            stage = meta.get("stage", "")
            if stage not in _STAGE_ORDER:
                logger.warning("checkpoint_invalid_stage", extra={"stage": stage})
                return None, None

            # Verify checkpoint belongs to current input file
            saved_input = meta.get("input_path", "")
            if saved_input != str(self.cfg.input_path):
                logger.warning(
                    "checkpoint_input_mismatch",
                    extra={"saved": saved_input, "current": str(self.cfg.input_path)},
                )
                return None, None

            saved_fingerprint = meta.get("input_fingerprint", "")
            current_fingerprint = _file_fingerprint(self.cfg.input_path)
            if not saved_fingerprint or saved_fingerprint != current_fingerprint:
                logger.warning(
                    "checkpoint_input_fingerprint_mismatch",
                    extra={"saved": saved_fingerprint, "current": current_fingerprint},
                )
                return None, None

            df = read_dataframe(cp, row_limit=None)

            # Verify row count integrity
            expected_rows = meta.get("row_count")
            if expected_rows is not None and len(df) != expected_rows:
                logger.warning(
                    "checkpoint_row_count_mismatch",
                    extra={"expected": expected_rows, "actual": len(df)},
                )
                return None, None

            logger.info("checkpoint_loaded", extra={"stage": stage, "rows": len(df), "path": str(cp)})
            return df, stage
        except Exception as exc:
            logger.warning("checkpoint_load_failed", extra={"error": str(exc)})
            return None, None

    def _cleanup_checkpoints(self) -> None:
        for p in [
            _checkpoint_path(self.cfg.output_path),
            _meta_path(self.cfg.output_path),
            _cleaning_stream_path(self.cfg.output_path),
        ]:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass

    def run(self) -> Dict[str, Any]:
        root = project_root()

        # Try resuming from checkpoint
        df, last_stage = self._load_checkpoint()
        if df is not None and last_stage is not None:
            start_idx = _STAGE_ORDER.index(last_stage) + 1
            logger.info(
                "resuming_pipeline",
                extra={"last_completed_stage": last_stage, "next_stage": _STAGE_ORDER[start_idx] if start_idx < len(_STAGE_ORDER) else "done"},
            )
        else:
            df = read_dataframe(self.cfg.input_path, self.cfg.row_limit)
            start_idx = 0
            logger.info("input_loaded", extra={"rows": len(df), "columns": len(df.columns)})

        cfg = self.cfg

        # --- Cleaning ---
        if start_idx <= _STAGE_ORDER.index("cleaning"):
            stream_path = _cleaning_stream_path(cfg.output_path)
            df, ok = self._run_stage(
                "cleaning",
                df,
                lambda src, _cfg=cfg, _sp=stream_path: AICleaningService(
                    workers=_cfg.workers,
                    batch_size=_cfg.cleaning_batch_size,
                    stream_output_path=_sp,
                    progress_callback=(
                        None
                        if self._progress is None
                        else lambda completed, total, message=None: self._progress.on_stage_progress(
                            "cleaning",
                            completed=completed,
                            total=total,
                            message=message,
                        )
                    ),
                ).run(
                    src,
                    description_col=_cfg.description_col,
                    brand_col=_cfg.brand_col,
                    packsize_col=_cfg.packsize_col,
                ),
            )
            if ok:
                self._save_checkpoint(df, "cleaning")

        # --- Taxonomy ---
        if start_idx <= _STAGE_ORDER.index("taxonomy"):
            missing = _TAXONOMY_REQUIRED_COLUMNS - set(df.columns)
            taxonomy_ok = False
            if missing:
                if cfg.fail_fast:
                    raise StageError(f"Taxonomy requires columns {sorted(_TAXONOMY_REQUIRED_COLUMNS)}; missing: {sorted(missing)}")
                logger.warning("taxonomy_skipped_missing_columns", extra={"missing": sorted(missing)})
                if self._progress is not None:
                    self._progress.on_stage_skipped("taxonomy", f"Missing cleaned columns: {sorted(missing)}")
            else:
                df, taxonomy_ok = self._run_stage(
                    "taxonomy",
                    df,
                    lambda src, _cfg=cfg, _root=root: TaxonomyService(
                        project_root=_root,
                        batch_size=_cfg.chunk_size,
                        progress_callback=(
                            None
                            if self._progress is None
                            else lambda completed, total, message=None: self._progress.on_stage_progress(
                                "taxonomy",
                                completed=completed,
                                total=total,
                                message=message,
                            )
                        ),
                    ).run(src),
                )
            if taxonomy_ok:
                self._save_checkpoint(df, "taxonomy")

        # --- GTIN ---
        if start_idx <= _STAGE_ORDER.index("gtin"):
            df, _ok = self._run_stage(
                "gtin",
                df,
                lambda src, _cfg=cfg: GTINService(
                    enable_perplexity_fallback=_cfg.enable_perplexity_fallback,
                    progress_callback=(
                        None
                        if self._progress is None
                        else lambda completed, total, message=None: self._progress.on_stage_progress(
                            "gtin",
                            completed=completed,
                            total=total,
                            message=message,
                        )
                    ),
                ).run(src, gtin_column=None, chunk_size=_cfg.chunk_size),
            )

        # Write final output and clean up
        write_dataframe(df, self.cfg.output_path)
        self._cleanup_checkpoints()

        summary = {
            "input": str(self.cfg.input_path),
            "output": str(self.cfg.output_path),
            "rows": len(df),
            "columns": len(df.columns),
            "fail_fast": self.cfg.fail_fast,
            "workers": self.cfg.workers,
        }
        logger.info("run_complete", extra={"summary": summary})
        return summary
