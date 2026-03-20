from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    s3_bucket: str
    notify_email: str
    ses_sender: str
    cors_origins: list[str] = field(default_factory=list)


def load_settings() -> Settings:
    default_origins = "http://localhost:8080,http://localhost:5173,http://localhost:3000"
    cors_origins = [
        origin.strip()
        for origin in os.getenv("CORS_ORIGINS", default_origins).split(",")
        if origin.strip()
    ]
    return Settings(
        s3_bucket=os.getenv("S3_BUCKET", ""),
        notify_email=os.getenv("NOTIFY_EMAIL", "salt@usepepper.com"),
        ses_sender=os.getenv("SES_SENDER", "noreply@cleandata.com"),
        cors_origins=cors_origins,
    )


settings = load_settings()
