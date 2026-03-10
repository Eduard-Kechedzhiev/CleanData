from __future__ import annotations

import logging
import threading
import time

from app.infrastructure import job_store
from app.services.progress_service import JobProgressReporter
from processing.runner import run_pipeline

logger = logging.getLogger(__name__)

_active_jobs: set[str] = set()
_active_threads: list[threading.Thread] = []
_active_lock = threading.Lock()


def active_job_count() -> int:
    with _active_lock:
        return len(_active_jobs)


def try_reserve_slot(job_id: str, max_concurrent: int) -> bool:
    with _active_lock:
        if len(_active_jobs) >= max_concurrent:
            return False
        _active_jobs.add(job_id)
        return True


def release_slot(job_id: str) -> None:
    with _active_lock:
        _active_jobs.discard(job_id)


def wait_active_threads(timeout: float = 10) -> list[str]:
    deadline = time.monotonic() + max(timeout, 0)
    while True:
        with _active_lock:
            threads = [thread for thread in _active_threads if thread.is_alive()]
        if not threads:
            return []

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return [thread.name for thread in threads]

        slice_timeout = min(0.25, remaining)
        for thread in threads:
            thread.join(timeout=slice_timeout)


def _run_job(job_id: str) -> None:
    try:
        input_path = job_store.find_input_file(job_id)
        output_path = job_store.output_path(job_id)
        if input_path is None:
            job_store.mark_failed(job_id, "Input file not found", stage_name="reading", retryable=False)
            return

        reporter = JobProgressReporter(job_id)
        summary = run_pipeline(input_path=input_path, output_path=output_path, reporter=reporter)
        job_store.mark_complete(job_id)
        logger.info(
            "Job %s completed: %s rows, %s columns",
            job_id[:8],
            summary.get("rows", 0),
            summary.get("columns", 0),
        )
    except Exception as exc:
        current_stage = None
        record = job_store.load_job(job_id)
        if record:
            current_stage = record.pipeline.current_stage
        job_store.mark_failed(job_id, str(exc), stage_name=current_stage, retryable=False)
        logger.exception("Job %s failed", job_id[:8])
    finally:
        release_slot(job_id)
        current = threading.current_thread()
        with _active_lock:
            _active_threads[:] = [thread for thread in _active_threads if thread is not current]


def start_job_async(job_id: str) -> threading.Thread:
    thread = threading.Thread(target=_run_job, args=(job_id,), daemon=True, name=f"pipeline-{job_id[:8]}")
    with _active_lock:
        _active_jobs.add(job_id)
        _active_threads[:] = [active for active in _active_threads if active.is_alive()]
        _active_threads.append(thread)
    thread.start()
    return thread
