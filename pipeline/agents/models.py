from datetime import date

from pydantic import BaseModel

from schemas.anomaly import Severity, Source
from schemas.contract import DegradationReason as DR_Contract
from schemas.history import DegradationReason as DR_History




class LineItemSummary(BaseModel):
    """Invoice Line Item fields exposed to the explanation agent."""
    description: str
    unit_price: float | None
    quantity: float | None
    vat_rate: float | None
    amount_gross: float   
    
class InvoiceSummary(BaseModel):
    """Invoice fields exposed to the explanation agent."""
    invoice_number: str
    supplier_name: str
    issue_date: date | None
    total_amount: float
    currency: str | None
    line_items: list[LineItemSummary]
    
class FlagEntry(BaseModel):
    """Single anomaly flag with its parsed notes payload."""
    anomaly_name: str
    anomaly_severity: Severity
    anomaly_source: Source
    anomaly_notes: dict | None

class ExplanationContext(BaseModel):
    """Aggregated input for explanation agent: invoice, degradation status, all flags."""
    invoice_summary: InvoiceSummary
    historical_degradation: DR_History | None
    contract_degradation: DR_Contract | None
    anomaly_flags: list[FlagEntry]
    

class ConcernEntry(BaseModel):
    anomaly_name: str
    anomaly_severity: Severity
    anomaly_source: Source
    reason: str
    
class FlagGroup(BaseModel):
    theme: str
    flags: list[ConcernEntry]
    
class ExplanationPlan(BaseModel):
    """Step 1 LLM output. Structured analysis used to condition step 2 narrative."""
    summary: str
    top_concerns: list[ConcernEntry]
    degradation_caveats: list[str]
    flag_groupings: list[FlagGroup]