from __future__ import annotations

from fastapi import APIRouter, File, UploadFile

from app.api.errors import ApiError
from app.services.job_service import BusyError, upload_file
from app.settings import settings

router = APIRouter()


@router.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename:
        raise ApiError(400, "invalid_request", "No file provided")

    try:
        content = await file.read()
        return upload_file(
            file_name=file.filename,
            content=content,
            max_upload_bytes=settings.max_upload_bytes,
            max_concurrent_jobs=settings.max_concurrent_jobs,
        )
    except BusyError as exc:
        raise ApiError(429, "server_busy", str(exc), retryable=True) from exc
    except ValueError as exc:
        raise ApiError(400, "validation_error", str(exc)) from exc
