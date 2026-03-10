from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse, StreamingResponse

from app.api.errors import ApiError
from app.domain.job_models import JobEventEnvelope, JobState
from app.infrastructure import job_store
from app.services import results_service
from app.services.job_service import get_download_path, get_job_snapshot
from app.settings import settings

router = APIRouter()

_SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._-]")
_SAMPLE_RESULTS_PATH = Path(__file__).resolve().parents[3] / "sample_results.json"


def _sanitize_filename(name: str) -> str:
    return _SAFE_FILENAME_RE.sub("_", name)


def _validate_job_id(job_id: str) -> None:
    if not job_store.validate_job_id(job_id):
        raise ApiError(400, "invalid_request", "Invalid job ID")


@router.get("/api/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    _validate_job_id(job_id)
    try:
        return get_job_snapshot(job_id, settings.job_ttl_hours)
    except FileNotFoundError as exc:
        raise ApiError(404, "job_not_found", str(exc)) from exc


@router.get("/api/jobs/{job_id}/stream")
async def stream_job_status(job_id: str):
    _validate_job_id(job_id)
    initial = job_store.load_job(job_id)
    if not initial:
        raise ApiError(404, "job_not_found", "Job not found")

    async def event_generator():
        last_sequence = -1
        heartbeat_counter = 0
        heartbeat_ticks = 8

        while True:
            record = job_store.load_job(job_id)
            if not record:
                envelope = JobEventEnvelope.deleted(sequence=last_sequence + 1, job_id=job_id)
                yield f"data: {envelope.model_dump_json()}\n\n"
                return

            if record.sequence != last_sequence:
                snapshot = record.to_snapshot(settings.job_ttl_hours)
                envelope = JobEventEnvelope.updated(sequence=record.sequence, job=snapshot)
                yield f"data: {envelope.model_dump_json()}\n\n"
                last_sequence = record.sequence
                heartbeat_counter = 0
            else:
                heartbeat_counter += 1
                if heartbeat_counter >= heartbeat_ticks:
                    yield ": heartbeat\n\n"
                    heartbeat_counter = 0

            if record.state in {JobState.completed, JobState.completed_with_warnings, JobState.failed}:
                return

            await asyncio.sleep(2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/api/jobs/{job_id}/results")
async def get_job_results(job_id: str):
    _validate_job_id(job_id)
    record = job_store.load_job(job_id)
    if not record:
        raise ApiError(404, "job_not_found", "Job not found")
    snapshot = record.to_snapshot(settings.job_ttl_hours)
    if snapshot.download.state.value == "expired":
        raise ApiError(410, "results_expired", "Results have expired. Upload again to regenerate them.")
    if record.state not in {JobState.completed, JobState.completed_with_warnings}:
        raise ApiError(
            400,
            "job_not_complete",
            f"Job is not complete (current state: {record.state.value})",
            details={"state": record.state.value},
        )

    output_file = job_store.output_path(job_id)
    if not output_file.exists():
        raise ApiError(500, "artifact_missing", "Output file missing")

    try:
        return results_service.compute_summary(job_id, output_file)
    except Exception as exc:
        raise ApiError(500, "internal_error", "Failed to compute results", retryable=True) from exc


@router.get("/api/jobs/{job_id}/download")
async def download_results(job_id: str, token: str = Query(..., description="Download token from email submission")):
    _validate_job_id(job_id)
    try:
        path, input_filename = get_download_path(job_id, token, settings.job_ttl_hours)
    except FileNotFoundError as exc:
        raise ApiError(404, "job_not_found", str(exc)) from exc
    except PermissionError as exc:
        raise ApiError(403, "download_forbidden", str(exc)) from exc
    except ValueError as exc:
        raise ApiError(400, "job_not_complete", str(exc)) from exc
    except RuntimeError as exc:
        if str(exc) == "Results have expired":
            raise ApiError(410, "results_expired", str(exc)) from exc
        raise ApiError(500, "artifact_missing", str(exc)) from exc

    safe_name = _sanitize_filename(f"cleandata_{input_filename}")
    if not safe_name.endswith(".csv"):
        safe_name += ".csv"
    return FileResponse(path=path, filename=safe_name, media_type="text/csv")


@router.get("/api/config")
async def get_config():
    return {"contact_email": settings.contact_email}


@router.get("/api/sample")
async def get_sample_results():
    if _SAMPLE_RESULTS_PATH.exists():
        return json.loads(_SAMPLE_RESULTS_PATH.read_text(encoding="utf-8"))

    return {
        "job_id": "sample",
        "row_count": 8863,
        "column_count": 22,
        "avg_quality_score": 8.4,
        "quality_distribution": [
            {"score": "1-2", "count": 12},
            {"score": "3-4", "count": 45},
            {"score": "5-6", "count": 198},
            {"score": "7-8", "count": 3842},
            {"score": "9-10", "count": 4766},
        ],
        "brands_extracted": 347,
        "top_brands": [
            {"name": "Tyson", "count": 423},
            {"name": "Sysco", "count": 387},
            {"name": "Hatfield", "count": 312},
            {"name": "Heinz", "count": 289},
            {"name": "Bounty", "count": 201},
            {"name": "McCain", "count": 178},
            {"name": "Kellogg's", "count": 156},
            {"name": "Kraft", "count": 142},
        ],
        "gtins_found": 6381,
        "gtins_total": 8863,
        "category_breakdown": [
            {"name": "Meats", "count": 2134},
            {"name": "Frozen", "count": 1567},
            {"name": "Grocery", "count": 1245},
            {"name": "Dairy", "count": 987},
            {"name": "Beverage", "count": 876},
            {"name": "Produce", "count": 654},
            {"name": "Janitorial", "count": 543},
            {"name": "Paper & Disposables", "count": 432},
        ],
        "sample_rows": [
            {"original": "PORK LOINS BNLS CNTR CUT REF", "cleaned": "Pork Loins Boneless Center Cut Refrigerated", "brand": "Hatfield", "pack": "5 x 8 LB", "category": "Meats > Pork > Loins", "score": 9},
            {"original": "CHKN BRST BNLS SKNLS FRZ 4OZ", "cleaned": "Chicken Breast Boneless Skinless Frozen 4oz", "brand": "Tyson", "pack": "2 x 10 LB", "category": "Meats > Poultry > Breast", "score": 8},
            {"original": "PPR TOWEL 2PLY CS/30", "cleaned": "Paper Towel 2-Ply Case of 30", "brand": "Bounty", "pack": "1 x 30 CT", "category": "Janitorial > Paper > Towels", "score": 7},
            {"original": "MAYO PKT .44OZ 200CT", "cleaned": "Mayonnaise Packet 0.44oz 200 Count", "brand": "Heinz", "pack": "1 x 200 CT", "category": "Condiments > Mayo > Packets", "score": 9},
            {"original": "FRZ FRIES CRINKLE 5LB", "cleaned": "Frozen French Fries Crinkle Cut 5lb", "brand": "McCain", "pack": "6 x 5 LB", "category": "Frozen > Potatoes > Fries", "score": 8},
        ],
    }
