from __future__ import annotations

from fastapi import APIRouter

from app.api.errors import ApiError
from app.domain.job_models import EmailCapture
from app.infrastructure import job_store
from app.services.job_service import capture_lead

router = APIRouter()


def _validate_job_id(job_id: str) -> None:
    if not job_store.validate_job_id(job_id):
        raise ApiError(400, "invalid_request", "Invalid job ID")


@router.post("/api/jobs/{job_id}/email")
async def capture_lead_route(job_id: str, body: EmailCapture):
    _validate_job_id(job_id)
    try:
        return capture_lead(job_id, body)
    except FileNotFoundError as exc:
        raise ApiError(404, "job_not_found", str(exc)) from exc
