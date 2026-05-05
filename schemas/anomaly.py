from datetime import datetime
from uuid import UUID, uuid4

from enum import Enum
from pydantic import BaseModel
from sqlmodel import Field, SQLModel 

from schemas.invoice import InvoiceLineItem
from schemas.junction import LineItemMatch



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
    
    
class MatchedPair(BaseModel):
    invoice_description: str
    matched_contract_name: str
    score: float

class NotExactMatchNotes(BaseModel):
    fuzzy_resolved: list[MatchedPair]
    vector_resolved: list[MatchedPair]
    llm_resolved: list[MatchedPair]
    
class UnresolvedMatchNotes(BaseModel):
    unresolved_invoice_line_items: list[str]


class Metric(str, Enum):
    unit_price = "unit_price"
    quantity = "quantity"
    
class AnomalousStatisticalLine(BaseModel):
    description: str
    invoice: float
    contract: float
    deviation: float
    metric: Metric
    
class AnomalousStatisticalNotes(BaseModel):
    anomalous_lines: list[AnomalousStatisticalLine]
    

class StatisticalMissingField(BaseModel):
    field: str
    side: str

class StatisticalMissingFieldLine(BaseModel):
    description: str
    missing_fields: list[StatisticalMissingField]
    
class StatisticalMissingFieldNotes(BaseModel):
    lines_with_missing_fields: list[StatisticalMissingFieldLine]
    