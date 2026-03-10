from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.config import RunConfig
from pipeline.orchestrator import PipelineOrchestrator
from processing.reporting import ProgressReporter


def run_pipeline(input_path: Path, output_path: Path, reporter: ProgressReporter | None = None) -> dict[str, Any]:
    cfg = RunConfig(
        input_path=input_path,
        output_path=output_path,
        row_limit=None,
        description_col=None,
        brand_col=None,
        packsize_col=None,
        chunk_size=100,
        workers=4,
        fail_fast=True,
        json_logs=False,
        cleaning_batch_size=10,
        enable_perplexity_fallback=True,
        progress_reporter=reporter,
    )
    orchestrator = PipelineOrchestrator(cfg)
    return orchestrator.run()
