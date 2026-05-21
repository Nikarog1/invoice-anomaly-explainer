from uuid import UUID

from fastapi import APIRouter, Depends
from sqlmodel import Session

from api.models.ingestion_jobs import FileResult, IngestionJobResponse
from data.sqlite import get_session, load_ingestion_job



router = APIRouter(prefix="/ingestion-jobs", tags=["ingestion-jobs"])

@router.get("/{job_id}")
async def get_ingestion_job(job_id: UUID, session: Session = Depends(get_session)) -> IngestionJobResponse:
    job = load_ingestion_job(session, job_id)
    return IngestionJobResponse(
        job_id=job.job_id,
        status=job.status, # type: ignore[arg-type]
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        file_results=[
            FileResult(
                filename=file["filename"],
                status=file["status"],
                invoice_id=file["invoice_id"],
                error_message=file["error_message"],
            )
            for file in job.file_results
        ],
        error_message=job.error_message,
    )