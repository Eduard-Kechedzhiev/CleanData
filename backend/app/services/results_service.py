from __future__ import annotations

from pathlib import Path

import results
from app.domain.job_models import JobSummary


def compute_summary(job_id: str, output_path: Path) -> JobSummary:
    return results.compute_summary(job_id, output_path)
