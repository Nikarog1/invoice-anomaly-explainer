from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlmodel import Column, Field, JSON, SQLModel



class IngestionJob(SQLModel, table=True):
    job_id: UUID = Field(default_factory=uuid4, primary_key=True)
    status: str = "queued"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    file_results: list[dict] = Field(sa_column=Column(JSON))
    error_message: str | None = None  # job-level error (e.g., disk full), not per-file
    
class AnalysisJob(SQLModel, table=True):
    job_id: UUID = Field(default_factory=uuid4, primary_key=True)
    anomaly_report_id: UUID | None = Field(default=None, foreign_key="anomalyreport.anomaly_report_id")
    invoice_id: UUID = Field(foreign_key="invoice.invoice_id")
    status: str = "queued"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None