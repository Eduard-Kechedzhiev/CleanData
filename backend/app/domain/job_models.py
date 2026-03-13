from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

STAGE_ORDER = ["reading", "cleaning", "taxonomy", "gtin"]
STAGE_WEIGHTS = {
    "reading": 5.0,
    "cleaning": 70.0,
    "taxonomy": 15.0,
    "gtin": 10.0,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobState(str, Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    completed_with_warnings = "completed_with_warnings"
    failed = "failed"


class StageState(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    skipped = "skipped"
    failed = "failed"


class StageCounts(BaseModel):
    completed: int = 0
    total: int = 0


class FailureInfo(BaseModel):
    stage: Optional[str] = None
    message: str
    retryable: bool = False
    occurred_at: str = Field(default_factory=utc_now_iso)


class StageSnapshot(BaseModel):
    name: str
    state: StageState = StageState.pending
    percent: float = 0.0
    counts: StageCounts = Field(default_factory=StageCounts)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None


class PipelineSnapshot(BaseModel):
    current_stage: Optional[str] = None
    percent: float = 0.0
    stage_order: List[str] = Field(default_factory=lambda: list(STAGE_ORDER))
    stages: List[StageSnapshot] = Field(default_factory=list)


class JobSummaryMeta(BaseModel):
    row_count: int = 0
    input_filename: str = ""


class JobSnapshot(BaseModel):
    job_id: str
    state: JobState
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    summary: JobSummaryMeta
    pipeline: PipelineSnapshot
    failure: Optional[FailureInfo] = None
    warnings: List[FailureInfo] = Field(default_factory=list)


class JobRecord(BaseModel):
    job_id: str
    state: JobState = JobState.queued
    created_at: str = Field(default_factory=utc_now_iso)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    input_filename: str = ""
    row_count: int = 0
    columns: List[str] = Field(default_factory=list)
    pipeline: PipelineSnapshot = Field(default_factory=lambda: PipelineSnapshot(
        stages=[StageSnapshot(name=name) for name in STAGE_ORDER]
    ))
    failure: Optional[FailureInfo] = None
    warnings: List[FailureInfo] = Field(default_factory=list)
    lead_email: Optional[str] = None
    lead_company: str = ""
    lead_distributor_type: str = ""
    sequence: int = 0

    def stage_by_name(self, stage_name: str) -> StageSnapshot:
        for stage in self.pipeline.stages:
            if stage.name == stage_name:
                return stage
        raise KeyError(stage_name)

    def touch(self) -> None:
        self.sequence += 1
        self._refresh_derived_fields()

    def refresh_derived_fields(self) -> None:
        self._refresh_derived_fields()

    def _refresh_derived_fields(self) -> None:
        running_stage = None
        total_weighted = 0.0

        for stage in self.pipeline.stages:
            if stage.state == StageState.completed:
                stage.percent = 100.0
            elif stage.state == StageState.skipped:
                stage.percent = 100.0
            elif stage.state == StageState.failed:
                if stage.counts.total > 0 and stage.counts.completed > 0:
                    stage.percent = round(min(stage.counts.completed / stage.counts.total, 1.0) * 100, 1)
                else:
                    stage.percent = 0.0
            elif stage.counts.total > 0:
                stage.percent = round(min(stage.counts.completed / stage.counts.total, 1.0) * 100, 1)
            else:
                stage.percent = 0.0

            if stage.state == StageState.running:
                running_stage = stage.name

            total_weighted += STAGE_WEIGHTS.get(stage.name, 0.0) * (stage.percent / 100.0)

        self.pipeline.current_stage = running_stage
        self.pipeline.percent = round(total_weighted, 1)

    def to_snapshot(self) -> JobSnapshot:
        return JobSnapshot(
            job_id=self.job_id,
            state=self.state,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            summary=JobSummaryMeta(
                row_count=self.row_count,
                input_filename=self.input_filename,
            ),
            pipeline=self.pipeline.model_copy(deep=True),
            failure=self.failure.model_copy(deep=True) if self.failure else None,
            warnings=[warning.model_copy(deep=True) for warning in self.warnings],
        )


class JobSummary(BaseModel):
    job_id: str
    row_count: int
    column_count: int
    avg_quality_score: Optional[float] = None
    quality_distribution: List[Dict[str, Any]] = Field(default_factory=list)
    brands_extracted: int = 0
    top_brands: List[Dict[str, Any]] = Field(default_factory=list)
    gtins_found: int = 0
    gtins_total: int = 0
    category_breakdown: List[Dict[str, Any]] = Field(default_factory=list)
    sample_rows: List[Dict[str, Any]] = Field(default_factory=list)


class EmailCapture(BaseModel):
    email: str
    company: str = ""
    distributor_type: str = ""

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip()
        if "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError("Invalid email address")
        return v


class UploadResponse(BaseModel):
    job_id: str
    file_name: str
    row_count: int
    columns: List[str]


class JobEventEnvelope(BaseModel):
    type: Literal["job.updated", "job.deleted"] = "job.updated"
    sequence: int
    job_id: str
    job: Optional[JobSnapshot] = None

    @classmethod
    def updated(cls, sequence: int, job: JobSnapshot) -> "JobEventEnvelope":
        return cls(type="job.updated", sequence=sequence, job_id=job.job_id, job=job)

    @classmethod
    def deleted(cls, sequence: int, job_id: str) -> "JobEventEnvelope":
        return cls(type="job.deleted", sequence=sequence, job_id=job_id, job=None)
