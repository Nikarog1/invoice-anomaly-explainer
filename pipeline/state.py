from datetime import datetime
import operator
from typing import Annotated, TypedDict
from uuid import UUID

from schemas.anomaly import AnomalyFlag
from schemas.contract import ContractWithLineItems
from schemas.history import HistoricalSummary
from schemas.junction import LineItemMatch
from schemas.invoice import Invoice, InvoiceLineItem



class PipelineState(TypedDict):
    invoice_id: UUID # input
    invoice: Invoice | None
    invoice_line_items: list[InvoiceLineItem] | None
    historical_summary: HistoricalSummary | None
    contracts: list[ContractWithLineItems] | None 
    line_item_matches: list[LineItemMatch] | None
    anomaly_flags: Annotated[list[AnomalyFlag], operator.add] # should accumulate different anomaly_flags, each flag has it's own model
    agent_explanation: str | None  # produced by explanation agent
    explanation_datetime: datetime | None
    