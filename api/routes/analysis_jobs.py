from uuid import UUID

from fastapi import APIRouter, Depends
from sqlmodel import Session

from api.models.analysis_jobs import AnalysisJobResponse
from data.sqlite import get_session, load_analysis_job



router = APIRouter(prefix="/analysis-jobs", tags=["analysis-jobs"])

@router.get("/{job_id}")
async def get_analysis_job(job_id: UUID, session: Session = Depends(get_session)) -> AnalysisJobResponse:
    """Fetch analysis job state by job id."""
    job = load_analysis_job(session, job_id)
    return AnalysisJobResponse(
        job_id=job.job_id,
        invoice_id=job.invoice_id,
        status=job.status, # type: ignore[arg-type]
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error_message=job.error_message,
        anomaly_report_id=job.anomaly_report_id,
    )