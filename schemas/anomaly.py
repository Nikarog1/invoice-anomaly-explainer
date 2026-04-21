from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel 



class Severity(str, Enum):
    red = "red"
    yellow = "yellow"
    
class Source(str, Enum):
    statistical_vs_history = "statistical_vs_history"
    statistical_vs_contract = "statistical_vs_contract"
    completeness_check_ingestion = "completeness_check_ingestion"
    completeness_check_historical = "completeness_check_historical"
    contract_matching = "contract_matching"
    
class AnomalyFlag(SQLModel, table=True):
    anomaly_flag_id: UUID = Field(default_factory=uuid4, primary_key=True)
    anomaly_report_id: UUID | None = Field(foreign_key="anomalyreport.anomaly_report_id")
    invoice_id: UUID = Field(foreign_key="invoice.invoice_id")
    anomaly_name: str
    anomaly_severity: Severity
    anomaly_source: Source
    anomaly_deviation: float | None = None
    anomaly_notes: str | None = None
    
class AnomalyReport(SQLModel, table=True):
    anomaly_report_id: UUID = Field(default_factory=uuid4, primary_key=True)
    invoice_id: UUID = Field(foreign_key="invoice.invoice_id")
    anomalies_count: int
    agent_explanation: str | None = None
    explanation_date: datetime | None = None