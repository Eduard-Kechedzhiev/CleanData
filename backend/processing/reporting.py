from __future__ import annotations

from typing import Protocol


class ProgressReporter(Protocol):
    def on_stage_started(self, stage: str, total_rows: int | None = None, message: str | None = None) -> None:
        ...

    def on_stage_progress(
        self,
        stage: str,
        completed: int,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        ...

    def on_stage_completed(
        self,
        stage: str,
        completed: int | None = None,
        total: int | None = None,
        message: str | None = None,
    ) -> None:
        ...

    def on_stage_failed(self, stage: str, error: str, fatal: bool) -> None:
        ...

    def on_stage_skipped(self, stage: str, message: str) -> None:
        ...
