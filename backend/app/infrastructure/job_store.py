from __future__ import annotations

import json
import logging
import os
import re
import secrets
import shutil
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from pydantic import BaseModel, Field

from app.domain.job_models import (
    FailureInfo,
    JobRecord,
    JobState,
    JobSnapshot,
    STAGE_ORDER,
    StageState,
)

logger = logging.getLogger(__name__)

JOBS_DIR = Path(__file__).resolve().parents[2] / "jobs"
LEADS_PATH = JOBS_DIR / "leads.jsonl"

_lock = threading.Lock()
_JOB_ID_RE = re.compile(r"^[a-f0-9]{32}$")
_TERMINAL_STATES = {JobState.completed, JobState.completed_with_warnings, JobState.failed}


@dataclass
class RecoveryResult:
    failed_jobs: int = 0
    resumed_job_ids: list[str] = field(default_factory=list)


class _LegacyStageProgress(BaseModel):
    name: str
    status: str = "pending"
    rows_done: int = 0
    rows_total: int = 0


class _LegacyJobStatus(BaseModel):
    job_id: str
    stage: str = "queued"
    stages: list[_LegacyStageProgress] = Field(default_factory=list)
    row_count: int = 0
    file_name: str = ""
    created_at: str = ""
    completed_at: Optional[str] = None
    error: Optional[str] = None
    email: Optional[str] = None
    download_tokens: list[str] = Field(default_factory=list)


def validate_job_id(job_id: str) -> bool:
    return bool(_JOB_ID_RE.fullmatch(job_id))


def _status_path(job_id: str) -> Path:
    return JOBS_DIR / job_id / "status.json"


def _ensure_dir(job_id: str) -> Path:
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def _write_json_atomically(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    os.replace(str(tmp_path), str(path))


def _stage_state_from_legacy(status: str) -> StageState:
    mapping = {
        "pending": StageState.pending,
        "running": StageState.running,
        "complete": StageState.completed,
        "completed": StageState.completed,
        "failed": StageState.failed,
        "skipped": StageState.skipped,
    }
    return mapping.get(status, StageState.pending)


def _job_state_from_legacy(stage: str) -> JobState:
    mapping = {
        "queued": JobState.queued,
        "reading": JobState.running,
        "cleaning": JobState.running,
        "taxonomy": JobState.running,
        "gtin": JobState.running,
        "complete": JobState.completed,
        "completed": JobState.completed,
        "failed": JobState.failed,
    }
    return mapping.get(stage, JobState.queued)


def _load_legacy_job(text: str) -> Optional[JobRecord]:
    try:
        legacy = _LegacyJobStatus.model_validate_json(text)
    except Exception:
        return None

    record = JobRecord(
        job_id=legacy.job_id,
        state=_job_state_from_legacy(legacy.stage),
        created_at=legacy.created_at,
        completed_at=legacy.completed_at,
        input_filename=legacy.file_name,
        row_count=legacy.row_count,
        download_tokens=list(legacy.download_tokens),
        lead_email=legacy.email,
    )

    reading = record.stage_by_name("reading")
    reading.state = StageState.completed if legacy.row_count > 0 else StageState.pending
    reading.counts.total = legacy.row_count
    reading.counts.completed = legacy.row_count
    reading.completed_at = legacy.created_at or None

    for legacy_stage in legacy.stages:
        if legacy_stage.name not in STAGE_ORDER:
            continue
        stage = record.stage_by_name(legacy_stage.name)
        stage.state = _stage_state_from_legacy(legacy_stage.status)
        stage.counts.total = legacy_stage.rows_total
        stage.counts.completed = legacy_stage.rows_done
        if stage.state in {StageState.running, StageState.completed, StageState.failed}:
            stage.started_at = legacy.created_at or None
        if stage.state in {StageState.completed, StageState.failed}:
            stage.completed_at = legacy.completed_at
        if stage.state == StageState.failed:
            stage.error = legacy.error

    if legacy.error:
        record.failure = FailureInfo(stage=legacy.stage, message=legacy.error, retryable=False)

    record.touch()
    record.sequence = 0
    return record


def save_job(record: JobRecord) -> None:
    record.touch()
    _write_json_atomically(_status_path(record.job_id), record.model_dump_json(indent=2))


def load_job(job_id: str) -> Optional[JobRecord]:
    path = _status_path(job_id)
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8")
        payload = json.loads(text)
        if isinstance(payload, dict) and "pipeline" in payload and "state" in payload:
            record = JobRecord.model_validate_json(text)
            record.refresh_derived_fields()
            return record
        legacy = _load_legacy_job(text)
        if legacy is not None:
            return legacy
        record = JobRecord.model_validate_json(text)
        record.refresh_derived_fields()
        return record
    except Exception as exc:
        logger.warning("Corrupt status.json for job %s: %s", job_id[:8], exc)
        return None


def load_snapshot(job_id: str, ttl_hours: int) -> Optional[JobSnapshot]:
    record = load_job(job_id)
    if not record:
        return None
    return record.to_snapshot(ttl_hours)


def _mutate(job_id: str, mutator: Callable[[JobRecord], None]) -> Optional[JobRecord]:
    with _lock:
        record = load_job(job_id)
        if not record:
            return None
        mutator(record)
        save_job(record)
        return record


def _mark_stage_failed(
    record: JobRecord,
    *,
    error: str,
    stage_name: str | None = None,
    completed_at: str | None = None,
) -> str | None:
    completed_at = completed_at or datetime.now(timezone.utc).isoformat()
    target_names: list[str] = []

    if stage_name and stage_name in STAGE_ORDER:
        target_names.append(stage_name)

    for stage in record.pipeline.stages:
        if stage.state == StageState.running and stage.name not in target_names:
            target_names.append(stage.name)

    if not target_names and record.pipeline.current_stage and record.pipeline.current_stage in STAGE_ORDER:
        target_names.append(record.pipeline.current_stage)

    for name in target_names:
        stage = record.stage_by_name(name)
        stage.state = StageState.failed
        stage.error = error
        stage.message = error
        if not stage.started_at:
            stage.started_at = completed_at
        if record.row_count and stage.counts.total == 0:
            stage.counts.total = record.row_count
        stage.completed_at = completed_at

    return target_names[0] if target_names else None


def create_job(job_id: str, file_name: str) -> JobRecord:
    _ensure_dir(job_id)
    record = JobRecord(job_id=job_id, input_filename=file_name)
    record.state = JobState.running
    record.started_at = record.created_at
    reading = record.stage_by_name("reading")
    reading.state = StageState.running
    reading.started_at = record.created_at
    save_job(record)
    return record


def complete_reading(job_id: str, row_count: int, columns: list[str]) -> Optional[JobRecord]:
    def _apply(record: JobRecord) -> None:
        record.row_count = row_count
        record.columns = list(columns)
        reading = record.stage_by_name("reading")
        reading.state = StageState.completed
        reading.counts.total = row_count
        reading.counts.completed = row_count
        reading.completed_at = datetime.now(timezone.utc).isoformat()
        reading.message = None
        reading.error = None
        for stage_name in ("cleaning", "taxonomy", "gtin"):
            stage = record.stage_by_name(stage_name)
            stage.counts.total = row_count
        record.state = JobState.queued

    return _mutate(job_id, _apply)


def start_stage(job_id: str, stage_name: str, total_rows: int | None = None, message: str | None = None) -> Optional[JobRecord]:
    def _apply(record: JobRecord) -> None:
        stage = record.stage_by_name(stage_name)
        stage.state = StageState.running
        if not stage.started_at:
            stage.started_at = datetime.now(timezone.utc).isoformat()
        if total_rows is not None:
            stage.counts.total = total_rows
        if stage.counts.total == 0 and record.row_count:
            stage.counts.total = record.row_count
        stage.message = message
        stage.error = None
        stage.completed_at = None
        record.state = JobState.running
        if not record.started_at:
            record.started_at = datetime.now(timezone.utc).isoformat()

    return _mutate(job_id, _apply)


def update_stage_progress(
    job_id: str,
    stage_name: str,
    completed: int,
    total: int | None = None,
    message: str | None = None,
) -> Optional[JobRecord]:
    def _apply(record: JobRecord) -> None:
        stage = record.stage_by_name(stage_name)
        stage.state = StageState.running
        if not stage.started_at:
            stage.started_at = datetime.now(timezone.utc).isoformat()
        stage.counts.completed = max(0, completed)
        if total is not None:
            stage.counts.total = total
        elif stage.counts.total == 0 and record.row_count:
            stage.counts.total = record.row_count
        stage.message = message
        record.state = JobState.running

    return _mutate(job_id, _apply)


def complete_stage(
    job_id: str,
    stage_name: str,
    completed: int | None = None,
    total: int | None = None,
    message: str | None = None,
) -> Optional[JobRecord]:
    def _apply(record: JobRecord) -> None:
        stage = record.stage_by_name(stage_name)
        stage.state = StageState.completed
        if completed is not None:
            stage.counts.completed = completed
        elif record.row_count and stage.counts.completed == 0:
            stage.counts.completed = record.row_count
        if total is not None:
            stage.counts.total = total
        elif record.row_count and stage.counts.total == 0:
            stage.counts.total = record.row_count
        stage.message = message
        stage.error = None
        if not stage.started_at:
            stage.started_at = datetime.now(timezone.utc).isoformat()
        stage.completed_at = datetime.now(timezone.utc).isoformat()

    return _mutate(job_id, _apply)


def skip_stage(job_id: str, stage_name: str, message: str) -> Optional[JobRecord]:
    def _apply(record: JobRecord) -> None:
        stage = record.stage_by_name(stage_name)
        stage.state = StageState.skipped
        stage.message = message
        stage.error = None
        if record.row_count and stage.counts.total == 0:
            stage.counts.total = record.row_count
        stage.counts.completed = stage.counts.total
        if not stage.started_at:
            stage.started_at = datetime.now(timezone.utc).isoformat()
        stage.completed_at = datetime.now(timezone.utc).isoformat()

    return _mutate(job_id, _apply)


def fail_stage(job_id: str, stage_name: str, error: str, fatal: bool) -> Optional[JobRecord]:
    def _apply(record: JobRecord) -> None:
        completed_at = datetime.now(timezone.utc).isoformat()
        _mark_stage_failed(record, error=error, stage_name=stage_name, completed_at=completed_at)
        failure = FailureInfo(stage=stage_name, message=error, retryable=not fatal)
        record.warnings = [warning for warning in record.warnings if warning.stage != stage_name]
        if fatal:
            record.state = JobState.failed
            record.completed_at = completed_at
            record.failure = failure
        else:
            record.warnings.append(failure)

    return _mutate(job_id, _apply)


def mark_failed(job_id: str, error: str, stage_name: str | None = None, retryable: bool = False) -> Optional[JobRecord]:
    def _apply(record: JobRecord) -> None:
        completed_at = datetime.now(timezone.utc).isoformat()
        failed_stage = _mark_stage_failed(record, error=error, stage_name=stage_name, completed_at=completed_at)
        record.state = JobState.failed
        record.completed_at = completed_at
        record.failure = FailureInfo(stage=failed_stage or stage_name, message=error, retryable=retryable)

    return _mutate(job_id, _apply)


def mark_complete(job_id: str) -> Optional[JobRecord]:
    def _apply(record: JobRecord) -> None:
        record.completed_at = datetime.now(timezone.utc).isoformat()
        record.state = JobState.completed_with_warnings if record.warnings else JobState.completed
        for stage in record.pipeline.stages:
            if stage.state == StageState.running:
                stage.state = StageState.completed
                if record.row_count and stage.counts.total == 0:
                    stage.counts.total = record.row_count
                if record.row_count and stage.counts.completed == 0:
                    stage.counts.completed = record.row_count
                stage.completed_at = record.completed_at

    return _mutate(job_id, _apply)


def set_email(job_id: str, email: str, company: str = "", grant_download: bool = False) -> str | None:
    token = secrets.token_urlsafe(24) if grant_download else None

    def _apply(record: JobRecord) -> None:
        record.lead_email = email
        record.lead_company = company
        if token:
            record.download_tokens.append(token)

    record = _mutate(job_id, _apply)
    if not record:
        return None

    lead = {
        "email": email,
        "company": company,
        "job_id": job_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    LEADS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        with open(LEADS_PATH, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(lead) + "\n")

    return token


def validate_download_token(job_id: str, token: str) -> bool:
    record = load_job(job_id)
    if not record:
        return False
    return token in record.download_tokens


def input_path(job_id: str, ext: str = ".csv") -> Path:
    return JOBS_DIR / job_id / f"input{ext}"


def find_input_file(job_id: str) -> Path | None:
    job_dir = JOBS_DIR / job_id
    for ext in (".csv", ".tsv", ".xlsx", ".xls"):
        path = job_dir / f"input{ext}"
        if path.exists():
            return path
    return None


def output_path(job_id: str) -> Path:
    return JOBS_DIR / job_id / "output.csv"


def cleanup_job_dir(job_id: str) -> None:
    job_dir = JOBS_DIR / job_id
    if job_dir.exists():
        shutil.rmtree(job_dir, ignore_errors=True)


def recover_stuck_jobs() -> RecoveryResult:
    if not JOBS_DIR.exists():
        return RecoveryResult()

    result = RecoveryResult()
    with _lock:
        for job_dir in JOBS_DIR.iterdir():
            if not job_dir.is_dir():
                continue
            job_id = job_dir.name
            if not validate_job_id(job_id):
                continue
            record = load_job(job_id)
            if not record or record.state in _TERMINAL_STATES:
                continue
            if record.state == JobState.queued:
                result.resumed_job_ids.append(job_id)
                continue

            completed_at = datetime.now(timezone.utc).isoformat()
            failed_stage = _mark_stage_failed(
                record,
                error="Server restarted while job was in progress",
                stage_name=record.pipeline.current_stage,
                completed_at=completed_at,
            )
            record.state = JobState.failed
            record.completed_at = completed_at
            record.failure = FailureInfo(
                stage=failed_stage,
                message="Server restarted while job was in progress",
                retryable=True,
            )
            save_job(record)
            result.failed_jobs += 1
    return result


def cleanup_expired(ttl_hours: int = 24) -> int:
    if not JOBS_DIR.exists():
        return 0
    cutoff = time.time() - (ttl_hours * 3600)
    deleted = 0
    with _lock:
        for job_dir in JOBS_DIR.iterdir():
            if not job_dir.is_dir():
                continue
            status_file = job_dir / "status.json"
            if not status_file.exists():
                continue

            record = load_job(job_dir.name)
            if not record or record.state not in _TERMINAL_STATES or not record.completed_at:
                continue
            try:
                expiry_ref = datetime.fromisoformat(record.completed_at).timestamp()
            except Exception:
                continue
            if expiry_ref < cutoff:
                shutil.rmtree(job_dir, ignore_errors=True)
                deleted += 1
    return deleted
