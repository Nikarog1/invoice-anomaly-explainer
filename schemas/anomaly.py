from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel 



class AnomalyFlag(SQLModel, table=True):
    anomaly_flag_id: UUID = Field(default_factory=uuid4, primary_key=True)
    anomaly_report_id: UUID = Field(foreign_key="anomalyreport.anomaly_report_id")
    invoice_id: UUID = Field(foreign_key="invoice.invoice_id")
    anomaly_name: str
    anomaly_severity: Literal["red", "yellow"]
    anomaly_source: Literal["statistical_vs_history", "statistical_vs_contract", "completeness_check", "contract_matching"]
    anomaly_found_datetime: datetime
    anomaly_deviation: float | int | None = None
    anomaly_notes: str | None = None
    
class AnomalyReport(SQLModel, table=True):
    anomaly_report_id: UUID = Field(default_factory=uuid4, primary_key=True)
    invoice_id: UUID = Field(foreign_key="invoice.invoice_id")
    anomalies_count: int
    agent_explanation: str | None = None
    explanation_date: datetime | None = None