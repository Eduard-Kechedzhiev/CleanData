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
        # Read in chunks to reject oversized files without loading them fully into memory.
        # In production nginx client_max_body_size rejects them first, but this protects local dev.
        max_bytes = settings.max_upload_bytes
        chunks: list[bytes] = []
        total = 0
        while chunk := await file.read(1024 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise ValueError(f"File exceeds {max_bytes // (1024 * 1024)}MB limit.")
            chunks.append(chunk)
        content = b"".join(chunks)

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
