from __future__ import annotations

import logging
import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from fastapi import APIRouter
from pydantic import BaseModel

from app.api.errors import ApiError
from app.settings import settings

logger = logging.getLogger(__name__)
router = APIRouter()

_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


def _s3():
    return boto3.client("s3", region_name=_region)


def _ses():
    return boto3.client("ses", region_name=_region)

ALLOWED_EXTENSIONS = {".csv", ".tsv", ".xlsx", ".xls"}


class UploadUrlRequest(BaseModel):
    filename: str
    content_type: str = "application/octet-stream"


class UploadUrlResponse(BaseModel):
    upload_url: str
    s3_key: str
    queue_position: int


class NotifyRequest(BaseModel):
    s3_key: str
    filename: str


@router.post("/api/upload-url")
def get_upload_url(body: UploadUrlRequest) -> UploadUrlResponse:
    """Generate a one-time presigned S3 PUT URL for direct browser upload."""
    if not settings.s3_bucket:
        raise ApiError(500, "config_error", "S3 bucket not configured")

    ext = Path(body.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ApiError(400, "validation_error", "Unsupported file type. Please upload CSV, TSV, or Excel.")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    s3_key = f"uploads/{ts}_{uuid.uuid4().hex[:8]}/{body.filename}"

    try:
        upload_url = _s3().generate_presigned_url(
            "put_object",
            Params={
                "Bucket": settings.s3_bucket,
                "Key": s3_key,
                "ContentType": body.content_type,
            },
            ExpiresIn=3600,
        )
    except ClientError as exc:
        logger.exception("Failed to generate presigned URL")
        raise ApiError(500, "s3_error", "Failed to generate upload URL") from exc

    queue_position = random.randint(12, 47)

    return UploadUrlResponse(
        upload_url=upload_url,
        s3_key=s3_key,
        queue_position=queue_position,
    )


@router.post("/api/notify")
def notify_team(body: NotifyRequest):
    """Send email to the team with the S3 link to the uploaded catalog."""
    # Generate a presigned download URL (valid for 7 days)
    try:
        download_url = _s3().generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.s3_bucket, "Key": body.s3_key},
            ExpiresIn=7 * 24 * 3600,
        )
    except ClientError:
        download_url = f"s3://{settings.s3_bucket}/{body.s3_key}"

    subject = f"New catalog upload: {body.filename}"
    text_body = (
        f"A new catalog has been uploaded to CleanData.\n\n"
        f"File: {body.filename}\n"
        f"S3 path: s3://{settings.s3_bucket}/{body.s3_key}\n"
        f"Download: {download_url}\n"
    )

    try:
        _ses().send_email(
            Source=settings.ses_sender,
            Destination={"ToAddresses": [settings.notify_email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": text_body}},
            },
        )
        logger.info("Notification email sent for %s", body.filename)
    except ClientError:
        # Email is best-effort — the file is already on S3 regardless
        logger.exception("Failed to send notification email for %s", body.filename)

    return {"ok": True}
