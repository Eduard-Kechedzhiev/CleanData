from __future__ import annotations

import uuid
from pathlib import Path

from app.domain.job_models import DownloadState, EmailCapture, JobSnapshot, JobState, UploadResponse
from app.infrastructure import job_store, thread_runner
from pipeline.io_files import read_dataframe


class BusyError(RuntimeError):
    pass


def upload_file(
    *,
    file_name: str,
    content: bytes,
    max_upload_bytes: int,
    max_concurrent_jobs: int,
) -> UploadResponse:
    if not file_name:
        raise ValueError("No file provided")

    ext = Path(file_name).suffix.lower()
    if ext not in {".csv", ".tsv", ".xlsx", ".xls"}:
        raise ValueError("Unsupported file type. Please upload CSV, TSV, or Excel.")
    if len(content) > max_upload_bytes:
        raise ValueError(f"File exceeds {max_upload_bytes // (1024 * 1024)}MB limit.")
    if len(content) == 0:
        raise ValueError("File is empty.")

    job_id = uuid.uuid4().hex
    started = False
    if not thread_runner.try_reserve_slot(job_id, max_concurrent_jobs):
        raise BusyError("Server is busy. Please try again shortly.")

    try:
        input_file = job_store.input_path(job_id, ext)
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_file.write_bytes(content)
        job_store.create_job(job_id, file_name)

        try:
            df = read_dataframe(input_file, row_limit=None)
            row_count = len(df)
            columns = list(df.columns)
        except Exception as exc:
            job_store.fail_stage(job_id, "reading", f"Failed to parse file: {exc}", fatal=True)
            thread_runner.release_slot(job_id)
            raise ValueError(f"Failed to parse file: {exc}") from exc

        if row_count < 1:
            job_store.fail_stage(job_id, "reading", "File has no data rows.", fatal=True)
            thread_runner.release_slot(job_id)
            raise ValueError("File has no data rows.")

        job_store.complete_reading(job_id, row_count=row_count, columns=columns)
        thread_runner.start_job_async(job_id)
        started = True
        return UploadResponse(job_id=job_id, file_name=file_name, row_count=row_count, columns=columns)
    except Exception:
        if not started:
            thread_runner.release_slot(job_id)
        raise


def get_job_snapshot(job_id: str, ttl_hours: int) -> JobSnapshot:
    snapshot = job_store.load_snapshot(job_id, ttl_hours)
    if snapshot is None:
        raise FileNotFoundError("Job not found")
    return snapshot


def capture_email(job_id: str, body: EmailCapture) -> dict[str, str | bool | None]:
    record = job_store.load_job(job_id)
    if not record:
        raise FileNotFoundError("Job not found")
    grant_download = bool(body.company.strip()) and record.state in {
        JobState.completed,
        JobState.completed_with_warnings,
    }
    token = job_store.set_email(job_id, body.email, body.company, grant_download=grant_download)
    return {"ok": True, "email": body.email, "download_token": token}


def get_download_path(job_id: str, token: str, ttl_hours: int) -> tuple[str, str]:
    record = job_store.load_job(job_id)
    if not record:
        raise FileNotFoundError("Job not found")
    if record.state not in {JobState.completed, JobState.completed_with_warnings}:
        raise ValueError("Job is not complete")
    if record.to_snapshot(ttl_hours).download.state == DownloadState.expired:
        raise RuntimeError("Results have expired")
    if not job_store.validate_download_token(job_id, token):
        raise PermissionError("Invalid or missing download token. Submit your email first.")
    output_file = job_store.output_path(job_id)
    if not output_file.exists():
        raise RuntimeError("Output file missing")
    return str(output_file), record.input_filename
