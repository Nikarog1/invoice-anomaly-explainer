from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from api.models.ingestion_jobs import FileResult
from core.exceptions import (
    JobNotFoundError, IngestionRepositoryError,
    InvalidCSVError, InvoiceMappingNotFoundError,
)
from core.logging import get_logger
from data.sqlite import get_session
from ingestion.service import IngestionService
from schemas.jobs import IngestionJob

logger = get_logger(__name__)



async def run_ingestion(job_id: UUID, file_paths: list[Path]) -> None:
    """Background function to run ingestion service."""
    with get_session() as session:
        try:
            session.rollback()
            
            n_files = len(file_paths)
            logger.info(f"Starting ingestion job {job_id} with {n_files} "
                        f"file{"" if n_files == 1 else "s"}"
            )
            job = session.get(IngestionJob, job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                raise JobNotFoundError(job_id)
            job.status = "running"
            job.started_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
            session.commit()
            
            file_results: list[dict] = []
            service = IngestionService()
            
            for path in file_paths:
                try:
                    invoice_id = await service.run(path)
                    file_results.append(
                        FileResult(
                            filename=path.name,
                            status="succeeded",
                            invoice_id=invoice_id,
                            error_message=None,
                        ).model_dump(mode="json")
                    )
                    path.unlink()
                    logger.info(f"File succeeded: {path.name}")
                    
                except (
                    FileNotFoundError, IngestionRepositoryError,
                    InvalidCSVError, InvoiceMappingNotFoundError, ValueError
                ) as e:
                    file_results.append(
                        FileResult(
                            filename=path.name,
                            status="failed",
                            invoice_id=None,
                            error_message=str(e),
                        ).model_dump(mode="json")
                    )
                    logger.warning(f"File failed: {path.name}")
                    
            successes = sum(1 for result in file_results if result["status"]=="succeeded")
            if successes == len(file_results):
                final_status = "succeeded"
            elif successes == 0:
                final_status = "failed"
            else:
                final_status = "partial"
                
            job.finished_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
            job.file_results = file_results
            job.status = final_status
            
            session.commit()
            
            logger.info(f"Job {job_id} finished: status={job.status}")
            

        except Exception as e:
            logger.exception(f"Job {job_id} crashed unexpectedly")
            
            job = session.get(IngestionJob, job_id)
            if not job:
                logger.error(f"Job {job_id} not found")
                raise JobNotFoundError(job_id)
            
            job.status = "failed"
            job.finished_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
            job.error_message = str(e)
            
            session.commit()
            
            logger.info(f"Job {job_id} finished: status={job.status}")