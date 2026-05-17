from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel



class AnalysisJobResponse(BaseModel):
    job_id: UUID
    invoice_id: UUID
    status: Literal["queued", "running", "succeeded", "failed"]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    anomaly_report_id: UUID | None
    
class AnalysisJobCreated(BaseModel):
    job_id: UUID
    invoice_id: UUID
    status: Literal["queued"]