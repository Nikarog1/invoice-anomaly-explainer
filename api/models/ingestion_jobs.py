from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel



class FileResult(BaseModel):
    filename: str
    status: Literal["succeeded", "failed"]
    invoice_id: UUID | None
    error_message: str | None
    
class IngestionJobResponse(BaseModel):
    job_id: UUID
    status: Literal["queued", "running", "succeeded", "partial", "failed"]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    file_results: list[FileResult]
    error_message: str | None  # job-level error (e.g., disk full), not per-file
    
class IngestionJobCreated(BaseModel):
    job_id: UUID
    status: Literal["queued"]