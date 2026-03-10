from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    max_upload_bytes: int
    job_ttl_hours: int
    max_concurrent_jobs: int
    contact_email: str
    cors_origins: list[str]


def load_settings() -> Settings:
    default_origins = "http://localhost:8080,http://localhost:5173,http://localhost:3000"
    cors_origins = [origin.strip() for origin in os.getenv("CORS_ORIGINS", default_origins).split(",") if origin.strip()]
    return Settings(
        max_upload_bytes=int(os.getenv("MAX_UPLOAD_MB", "50")) * 1024 * 1024,
        job_ttl_hours=int(os.getenv("JOB_TTL_HOURS", "24")),
        max_concurrent_jobs=int(os.getenv("MAX_CONCURRENT_JOBS", "3")),
        contact_email=os.getenv("CONTACT_EMAIL", "hello@cleandata.com"),
        cors_origins=cors_origins,
    )


settings = load_settings()
