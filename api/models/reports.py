from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from schemas.anomaly import Severity, Source



class AnomalyFlagDTO(BaseModel):
    name: str
    severity: Severity
    source: Source
    deviation: float | None = None
    notes: dict | None = None
    
class AnomalyReportDTO(BaseModel):
    anomaly_report_id: UUID
    invoice_id: UUID
    anomalies_count: int
    agent_explanation: str
    explanation_date: datetime
    flags: list[AnomalyFlagDTO]
    
class ReportResponse(BaseModel):
    status: Literal["not_analyzed", "analyzing", "ready", "failed"]
    report: AnomalyReportDTO | None
    error_message: str | None