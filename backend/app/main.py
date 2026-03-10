from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.errors import register_exception_handlers
from app.api.routes.health import router as health_router
from app.api.routes.jobs import router as jobs_router
from app.api.routes.leads import router as leads_router
from app.api.routes.uploads import router as uploads_router
from app.infrastructure import job_store, thread_runner
from app.settings import settings

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


async def _cleanup_loop() -> None:
    while True:
        await asyncio.sleep(3600)
        try:
            job_store.cleanup_expired(settings.job_ttl_hours)
        except Exception:
            logging.getLogger(__name__).exception("Cleanup error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    job_store.JOBS_DIR.mkdir(parents=True, exist_ok=True)
    recovery = job_store.recover_stuck_jobs()
    if recovery.failed_jobs:
        logging.getLogger(__name__).warning("Recovered %s interrupted jobs as failed", recovery.failed_jobs)
    for job_id in recovery.resumed_job_ids:
        thread_runner.start_job_async(job_id)
    if recovery.resumed_job_ids:
        logging.getLogger(__name__).info("Resumed %s queued jobs on startup", len(recovery.resumed_job_ids))
    cleanup_task = asyncio.create_task(_cleanup_loop())
    yield
    cleanup_task.cancel()
    with suppress(asyncio.CancelledError):
        await cleanup_task
    still_running = thread_runner.wait_active_threads(timeout=5)
    if still_running:
        logging.getLogger(__name__).warning("Shutdown timed out with active workers still running: %s", ", ".join(still_running))


app = FastAPI(
    title="CleanData API",
    description="Backend for the CleanData free catalog cleaning tool",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_exception_handlers(app)

app.include_router(uploads_router)
app.include_router(jobs_router)
app.include_router(leads_router)
app.include_router(health_router)
