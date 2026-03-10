from __future__ import annotations

from app.infrastructure import job_store
from processing.reporting import ProgressReporter


class JobProgressReporter(ProgressReporter):
    def __init__(self, job_id: str) -> None:
        self.job_id = job_id

    def on_stage_started(self, stage: str, total_rows: int | None = None, message: str | None = None) -> None:
        job_store.start_stage(self.job_id, stage, total_rows=total_rows, message=message)

    def on_stage_progress(
        self,
        stage: str,
        completed: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        job_store.update_stage_progress(self.job_id, stage, completed=completed, total=total, message=message)

    def on_stage_completed(
        self,
        stage: str,
        completed: int | None = None,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        job_store.complete_stage(self.job_id, stage, completed=completed, total=total, message=message)

    def on_stage_failed(self, stage: str, error: str, fatal: bool) -> None:
        job_store.fail_stage(self.job_id, stage, error=error, fatal=fatal)

    def on_stage_skipped(self, stage: str, message: str) -> None:
        job_store.skip_stage(self.job_id, stage, message=message)
