from __future__ import annotations

import logging
import uuid
from pathlib import Path

from app.domain.job_models import EmailCapture, JobSnapshot, UploadResponse
from app.infrastructure import job_store, thread_runner
from pipeline.io_files import read_dataframe

logger = logging.getLogger(__name__)


class BusyError(RuntimeError):
    pass


# TODO: Replace with a real notification channel (e.g. Slack webhook, SES email,
# or a DB-backed queue that a rep dashboard polls).  For now this just logs —
# the lead is already persisted in leads.jsonl by job_store.save_lead().
def _notify_rep(job_id: str, email: str, company: str, distributor_type: str) -> None:
    logger.info(
        "rep_notification",
        extra={
            "job_id": job_id,
            "email": email,
            "company": company,
            "distributor_type": distributor_type,
        },
    )


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


def get_job_snapshot(job_id: str) -> JobSnapshot:
    snapshot = job_store.load_snapshot(job_id)
    if snapshot is None:
        raise FileNotFoundError("Job not found")
    return snapshot


def capture_lead(job_id: str, body: EmailCapture) -> dict[str, str | bool]:
    """Save lead info and notify a rep to follow up."""
    record = job_store.load_job(job_id)
    if not record:
        raise FileNotFoundError("Job not found")

    job_store.save_lead(job_id, body.email, body.company, body.distributor_type)
    _notify_rep(job_id, body.email, body.company, body.distributor_type)

    return {"ok": True, "email": body.email}
